// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/Pack.h"

#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <limits>
#include <unordered_set>
#include <unistd.h>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "filemanager/OutputStream.h"
#include "storage/BinlogReader.h"
#include "storage/Event.h"
#include "storage/Types.h"
#include "storage/plugin/PluginInterface.h"

namespace {

constexpr const char* kPackedIndexFileVersionDelimiter = "_v";

bool
IsSafeRelativePath(const std::string& path_str) {
    namespace fs = std::filesystem;
    fs::path path(path_str);
    if (path_str.empty() || path.is_absolute() || path.has_root_name() ||
        path.has_root_directory()) {
        return false;
    }
    for (const auto& part : path) {
        if (part == "." || part == "..") {
            return false;
        }
    }
    return true;
}

bool
IsLowercaseToken(const std::string& token) {
    if (token.empty()) {
        return false;
    }
    for (char c : token) {
        if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '_') {
            return false;
        }
    }
    return true;
}

// Write vector to file
void
WriteFile(const std::string& path, const uint8_t* data, size_t size) {
    std::ofstream file(path, std::ios::binary);
    AssertInfo(file.is_open(), "Failed to create file: {}", path);
    file.write(reinterpret_cast<const char*>(data), size);
}

}  // namespace

namespace milvus {

//==============================================================================
// DirectoryTable Implementation
//==============================================================================

std::vector<uint8_t>
DirectoryTable::Serialize() const {
    // Calculate size
    size_t size = sizeof(uint32_t);  // entry count
    for (const auto& entry : entries) {
        size += sizeof(uint32_t);   // name_len
        size += entry.name.size();  // name
        size += sizeof(uint64_t);   // offset
        size += sizeof(uint64_t);   // size
    }

    std::vector<uint8_t> buffer(size);
    size_t offset = 0;

    // Write entry count
    offset +=
        WriteLE(buffer.data() + offset, static_cast<uint32_t>(entries.size()));

    // Write each directory entry
    for (const auto& entry : entries) {
        // name_len
        offset += WriteLE(buffer.data() + offset,
                          static_cast<uint32_t>(entry.name.size()));
        // name
        std::memcpy(
            buffer.data() + offset, entry.name.data(), entry.name.size());
        offset += entry.name.size();
        // offset
        offset += WriteLE(buffer.data() + offset, entry.offset);
        // size
        offset += WriteLE(buffer.data() + offset, entry.size);
    }

    return buffer;
}

DirectoryTable
DirectoryTable::Deserialize(const uint8_t* data, size_t size) {
    DirectoryTable table;
    size_t offset = 0;

    AssertInfo(size >= sizeof(uint32_t), "Invalid directory table: too small");
    uint32_t count = ReadLE<uint32_t>(data, offset);

    for (uint32_t i = 0; i < count; ++i) {
        AssertInfo(offset + sizeof(uint32_t) <= size,
                   "Invalid directory table: truncated name length");
        uint32_t name_len = ReadLE<uint32_t>(data, offset);

        AssertInfo(offset + name_len <= size,
                   "Invalid directory table: truncated name");
        std::string name(reinterpret_cast<const char*>(data + offset),
                         name_len);
        offset += name_len;

        AssertInfo(offset + sizeof(uint64_t) * 2 <= size,
                   "Invalid directory table: truncated offset/size");
        uint64_t entry_offset = ReadLE<uint64_t>(data, offset);
        uint64_t entry_size = ReadLE<uint64_t>(data, offset);

        table.entries.push_back({std::move(name), entry_offset, entry_size});
    }

    return table;
}

const DirectoryEntry*
DirectoryTable::Find(const std::string& name) const {
    for (const auto& entry : entries) {
        if (entry.name == name) {
            return &entry;
        }
    }
    return nullptr;
}

//==============================================================================
// Streaming Write Functions
//==============================================================================

size_t
ComputeDirectoryTableSize(const std::vector<SerializeEntry>& entries) {
    // Directory table content size
    size_t dir_content_size = sizeof(uint32_t);  // entry count
    for (const auto& entry : entries) {
        dir_content_size += sizeof(uint32_t);   // name_len
        dir_content_size += entry.name.size();  // name
        dir_content_size += sizeof(uint64_t);   // offset
        dir_content_size += sizeof(uint64_t);   // size
    }

    // Directory entry header + content
    return GetEntryHeaderSize(kDirectoryTableEntryName) + dir_content_size;
}

size_t
ComputePayloadSize(const std::vector<SerializeEntry>& entries) {
    size_t size = sizeof(uint32_t);  // entry_count

    // Directory table entry
    size += ComputeDirectoryTableSize(entries);

    // Data entries
    for (const auto& entry : entries) {
        size += GetEntryHeaderSize(entry.name);
        size += entry.size;
    }

    return size;
}

DirectoryTable
BuildDirectoryTable(const std::vector<SerializeEntry>& entries) {
    DirectoryTable table;

    // Calculate starting offset (after entry_count + directory entry)
    uint64_t current_offset =
        sizeof(uint32_t) + ComputeDirectoryTableSize(entries);

    for (const auto& entry : entries) {
        uint64_t entry_header_size = GetEntryHeaderSize(entry.name);
        table.entries.push_back({
            entry.name,
            current_offset +
                entry_header_size,  // offset to data (after header)
            entry.size,
        });
        current_offset += entry_header_size + entry.size;
    }

    return table;
}

size_t
WriteEntryHeader(uint8_t* buffer, const std::string& name, uint64_t data_size) {
    size_t offset = 0;

    // name_len
    offset += WriteLE(buffer + offset, static_cast<uint32_t>(name.size()));

    // name
    std::memcpy(buffer + offset, name.data(), name.size());
    offset += name.size();

    // data_size
    offset += WriteLE(buffer + offset, data_size);

    return offset;
}

//==============================================================================
// Unpack Functions (for Load path)
//==============================================================================

void
UnpackBlobToDirectory(const uint8_t* data,
                      size_t size,
                      const std::string& dir_path) {
    namespace fs = std::filesystem;

    fs::create_directories(dir_path);

    size_t offset = 0;

    AssertInfo(size >= 4, "Invalid packed blob: too small");

    uint32_t count = ReadLE<uint32_t>(data, offset);

    std::unordered_set<std::string> file_names;

    for (uint32_t i = 0; i < count; ++i) {
        AssertInfo(offset + 4 <= size,
                   "Invalid packed blob: truncated name length");
        uint32_t name_len = ReadLE<uint32_t>(data, offset);

        AssertInfo(offset + name_len <= size,
                   "Invalid packed blob: truncated name");
        std::string name(reinterpret_cast<const char*>(data + offset),
                         name_len);
        offset += name_len;

        AssertInfo(offset + 8 <= size,
                   "Invalid packed blob: truncated data size");
        uint64_t data_size = ReadLE<uint64_t>(data, offset);

        AssertInfo(offset + data_size <= size,
                   "Invalid packed blob: truncated data");

        // Skip directory table entry
        if (name == kDirectoryTableEntryName) {
            offset += data_size;
            continue;
        }

        AssertInfo(IsSafeRelativePath(name),
                   "Invalid packed blob: unsafe path {}",
                   name);

        auto [_, inserted] = file_names.insert(name);
        AssertInfo(inserted, "Duplicate file name in packed blob: {}", name);

        auto file_path = fs::path(dir_path) / name;
        fs::create_directories(file_path.parent_path());
        WriteFile(file_path.string(), data + offset, data_size);
        offset += data_size;
    }
}

BinarySet
UnpackBlobToBinarySet(const uint8_t* data, size_t size) {
    BinarySet binary_set;

    AssertInfo(size >= 4, "Invalid packed blob: too small");

    size_t offset = 0;
    uint32_t count = ReadLE<uint32_t>(data, offset);

    std::unordered_set<std::string> file_names;

    for (uint32_t i = 0; i < count; ++i) {
        AssertInfo(offset + 4 <= size,
                   "Invalid packed blob: truncated name length");
        uint32_t name_len = ReadLE<uint32_t>(data, offset);

        AssertInfo(offset + name_len <= size,
                   "Invalid packed blob: truncated name");
        std::string name(reinterpret_cast<const char*>(data + offset),
                         name_len);
        offset += name_len;

        AssertInfo(offset + 8 <= size,
                   "Invalid packed blob: truncated data size");
        uint64_t data_size = ReadLE<uint64_t>(data, offset);

        AssertInfo(offset + data_size <= size,
                   "Invalid packed blob: truncated data");

        // Skip directory table entry
        if (name == kDirectoryTableEntryName) {
            offset += data_size;
            continue;
        }

        auto [_, inserted] = file_names.insert(name);
        AssertInfo(inserted, "Duplicate file name in packed blob: {}", name);

        auto binary = std::shared_ptr<uint8_t[]>(new uint8_t[data_size]);
        std::memcpy(binary.get(), data + offset, data_size);
        binary_set.Append(name, binary, data_size);
        offset += data_size;
    }

    return binary_set;
}

//==============================================================================
// Directory Streaming Helpers
//==============================================================================

std::vector<SerializeEntry>
CollectDirectoryEntries(const std::string& dir_path) {
    namespace fs = std::filesystem;
    std::vector<SerializeEntry> entries;

    AssertInfo(fs::exists(dir_path) && fs::is_directory(dir_path),
               "Directory does not exist: {}",
               dir_path);

    for (const auto& entry : fs::recursive_directory_iterator(dir_path)) {
        if (entry.is_regular_file()) {
            auto rel_path = fs::relative(entry.path(), dir_path).string();
            auto file_size = static_cast<uint64_t>(entry.file_size());
            entries.push_back({rel_path, file_size});
        }
    }

    return entries;
}

//==============================================================================
// Streaming Write to OutputStream
//==============================================================================

size_t
StreamWritePackedEntries(OutputStream* output,
                         const std::vector<SerializeEntry>& entries,
                         WriteEntryDataFn write_entry_data) {
    size_t total_written = 0;

    // Build directory table
    auto dir_table = BuildDirectoryTable(entries);
    auto dir_table_data = dir_table.Serialize();

    // Write entry count (data entries + directory table entry)
    uint32_t entry_count = static_cast<uint32_t>(entries.size() + 1);
    uint8_t entry_count_buf[sizeof(uint32_t)];
    WriteLE(entry_count_buf, entry_count);
    total_written += output->Write(entry_count_buf, sizeof(uint32_t));

    // Write directory table entry header
    size_t dir_header_size = GetEntryHeaderSize(kDirectoryTableEntryName);
    std::vector<uint8_t> dir_header_buf(dir_header_size);
    WriteEntryHeader(
        dir_header_buf.data(), kDirectoryTableEntryName, dir_table_data.size());
    total_written += output->Write(dir_header_buf.data(), dir_header_size);

    // Write directory table data
    total_written +=
        output->Write(dir_table_data.data(), dir_table_data.size());

    // Write data entries
    for (const auto& entry : entries) {
        // Write entry header
        size_t header_size = GetEntryHeaderSize(entry.name);
        std::vector<uint8_t> header_buf(header_size);
        WriteEntryHeader(header_buf.data(), entry.name, entry.size);
        total_written += output->Write(header_buf.data(), header_size);

        // Write entry data using callback
        write_entry_data(output, entry.name);
        total_written += entry.size;
    }

    return total_written;
}

size_t
StreamWritePackedDirectory(OutputStream* output, const std::string& dir_path) {
    namespace fs = std::filesystem;

    // Phase 1: Collect entries (zero memory overhead)
    auto entries = CollectDirectoryEntries(dir_path);

    // Phase 2: Stream data using callback
    auto write_file_data = [&dir_path](OutputStream* out,
                                       const std::string& name) {
        namespace fs = std::filesystem;
        auto file_path = fs::path(dir_path) / name;

        int fd = ::open(file_path.c_str(), O_RDONLY);
        AssertInfo(fd >= 0, "Failed to open file: {}", file_path.string());

        // Get file size
        auto file_size = fs::file_size(file_path);

        // Use zero-copy write from fd
        out->Write(fd, file_size);

        ::close(fd);
    };

    return StreamWritePackedEntries(output, entries, write_file_data);
}

size_t
StreamWriteIndexFile(OutputStream* output,
                     const storage::FieldDataMeta& field_meta,
                     const storage::IndexMeta& index_meta,
                     const std::vector<SerializeEntry>& entries,
                     WriteEntryDataFn write_entry_data) {
    size_t total_written = 0;

    // Compute payload size
    size_t payload_size = ComputePayloadSize(entries);

    // Create and write DescriptorEvent
    storage::DescriptorEvent descriptor_event;
    auto& des_event_data = descriptor_event.event_data;
    auto& des_fix_part = des_event_data.fix_part;
    des_fix_part.collection_id = field_meta.collection_id;
    des_fix_part.partition_id = field_meta.partition_id;
    des_fix_part.segment_id = field_meta.segment_id;
    des_fix_part.field_id = field_meta.field_id;
    des_fix_part.start_timestamp = 0;
    des_fix_part.end_timestamp = 0;
    des_fix_part.data_type = milvus::proto::schema::DataType::None;

    for (auto i = int8_t(storage::EventType::DescriptorEvent);
         i < int8_t(storage::EventType::EventTypeEnd);
         i++) {
        des_event_data.post_header_lengths.push_back(
            storage::GetEventFixPartSize(storage::EventType(i)));
    }
    des_event_data.extras[ORIGIN_SIZE_KEY] = std::to_string(payload_size);
    des_event_data.extras[INDEX_BUILD_ID_KEY] =
        std::to_string(index_meta.build_id);

    auto& des_event_header = descriptor_event.event_header;
    des_event_header.timestamp_ = 0;

    auto des_event_bytes = descriptor_event.Serialize();
    total_written +=
        output->Write(des_event_bytes.data(), des_event_bytes.size());

    // Create IndexEvent header
    // IndexEvent format: EventHeader + start_timestamp + end_timestamp + payload
    // We need to compute the total event length including payload
    storage::Timestamp start_ts = 0;
    storage::Timestamp end_ts = 0;
    size_t event_data_size = sizeof(start_ts) + sizeof(end_ts) + payload_size;

    storage::EventHeader index_event_header;
    index_event_header.timestamp_ = 0;
    index_event_header.event_type_ = storage::EventType::IndexFileEvent;
    index_event_header.event_length_ =
        storage::GetEventHeaderSize(index_event_header) + event_data_size;
    index_event_header.next_position_ =
        index_event_header.event_length_ + des_event_bytes.size();

    auto header_bytes = index_event_header.Serialize();
    total_written += output->Write(header_bytes.data(), header_bytes.size());

    // Write timestamps
    total_written += output->Write(&start_ts, sizeof(start_ts));
    total_written += output->Write(&end_ts, sizeof(end_ts));

    // Stream write packed entries (payload)
    total_written +=
        StreamWritePackedEntries(output, entries, write_entry_data);

    return total_written;
}

//==============================================================================
// Streaming Read from InputStream (for Load path)
//==============================================================================

size_t
StreamReadEventHeaders(InputStream* input) {
    // File format:
    // ┌─────────────────────────────────────────────────────────────┐
    // │ MAGIC_NUM (4 bytes)                                         │
    // │ DescriptorEvent:                                            │
    // │   EventHeader: timestamp(8) + type(1) + length(4) + next(4) │
    // │   DescriptorEventData: (variable)                           │
    // │ IndexEvent:                                                  │
    // │   EventHeader: timestamp(8) + type(1) + length(4) + next(4) │
    // │   start_timestamp (8 bytes)                                 │
    // │   end_timestamp (8 bytes)                                   │
    // │   PAYLOAD (packed data)                                     │
    // └─────────────────────────────────────────────────────────────┘
    //
    // DescriptorEvent.next_position = offset to IndexEvent start (from file start)
    // We use this to skip directly to IndexEvent.

    // Read MAGIC_NUM (4 bytes) + partial EventHeader to get next_position
    // EventHeader: timestamp(8) + type(1) + length(4) + next_position(4) = 17 bytes
    // We need: MAGIC(4) + timestamp(8) + type(1) + length(4) + next_position(4) = 21 bytes
    constexpr size_t kMagicSize = 4;
    constexpr size_t kEventHeaderSize = 8 + 1 + 4 + 4;  // 17 bytes

    uint8_t header_buf[kMagicSize + kEventHeaderSize];
    input->Read(header_buf, sizeof(header_buf));

    // Parse DescriptorEvent's next_position (at offset 4 + 8 + 1 + 4 = 17)
    size_t offset = kMagicSize + 8 + 1 + 4;  // Skip to next_position field
    int32_t desc_next_position = 0;
    for (size_t i = 0; i < 4; ++i) {
        desc_next_position |= static_cast<int32_t>(header_buf[offset + i])
                              << (i * 8);
    }

    // desc_next_position points to the start of IndexEvent
    // Seek to IndexEvent start and read its header
    input->Seek(desc_next_position);

    // Read IndexEvent header
    uint8_t index_header_buf[kEventHeaderSize];
    input->Read(index_header_buf, kEventHeaderSize);

    // IndexEvent data starts after header: start_timestamp(8) + end_timestamp(8)
    // Then comes the payload
    constexpr size_t kIndexEventFixPartSize = 8 + 8;  // start_ts + end_ts

    // Payload starts at: desc_next_position + EventHeader + start_ts + end_ts
    size_t payload_start =
        desc_next_position + kEventHeaderSize + kIndexEventFixPartSize;
    input->Seek(payload_start);

    return payload_start;
}

DirectoryTable
StreamReadDirectoryTable(InputStream* input) {
    // At this point, input is positioned at payload start
    // Payload format: entry_count(4) + entries...
    // First entry is always the directory table

    // Read entry count
    uint32_t entry_count = 0;
    input->Read(&entry_count, sizeof(entry_count));

    AssertInfo(entry_count > 0, "Invalid packed data: no entries");

    // Read first entry (directory table)
    // Entry format: name_len(4) + name + data_size(8) + data
    uint32_t name_len = 0;
    input->Read(&name_len, sizeof(name_len));

    std::string name(name_len, '\0');
    input->Read(name.data(), name_len);

    AssertInfo(name == kDirectoryTableEntryName,
               "First entry must be directory table, got: {}",
               name);

    uint64_t data_size = 0;
    input->Read(&data_size, sizeof(data_size));

    // Read directory table data
    std::vector<uint8_t> dir_data(data_size);
    input->Read(dir_data.data(), data_size);

    return DirectoryTable::Deserialize(dir_data.data(), data_size);
}

void
StreamUnpackEntryToDirectory(InputStream* input,
                             size_t entry_offset,
                             size_t entry_size,
                             const std::string& dir_path) {
    namespace fs = std::filesystem;

    // The entry data is itself a packed directory
    // We need to parse it and extract files

    // Read the nested packed data
    std::vector<uint8_t> nested_data(entry_size);
    input->ReadAt(nested_data.data(), entry_offset, entry_size);

    // Use existing UnpackBlobToDirectory
    UnpackBlobToDirectory(nested_data.data(), entry_size, dir_path);
}

std::pair<std::shared_ptr<uint8_t[]>, size_t>
StreamReadEntryToMemory(InputStream* input,
                        size_t entry_offset,
                        size_t entry_size) {
    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[entry_size]);
    input->ReadAt(data.get(), entry_offset, entry_size);
    return {data, entry_size};
}

//==============================================================================
// Helpers
//==============================================================================

std::string
FormatPackedIndexFileName(const std::string& index_type_token,
                          int32_t version) {
    AssertInfo(IsLowercaseToken(index_type_token),
               "Invalid packed index type token: {}",
               index_type_token);
    AssertInfo(version > 0, "Invalid packed index version: {}", version);

    return std::string(kPackedIndexFilePrefix) + index_type_token +
           kPackedIndexFileVersionDelimiter + std::to_string(version);
}

bool
TryParsePackedIndexFileName(const std::string& filename,
                            std::string* index_type_token,
                            int32_t* version) {
    if (index_type_token == nullptr || version == nullptr) {
        return false;
    }

    const size_t prefix_len_check = std::strlen(kPackedIndexFilePrefix);
    if (filename.size() < prefix_len_check ||
        filename.compare(0, prefix_len_check, kPackedIndexFilePrefix) != 0) {
        return false;
    }

    auto delimiter_pos = filename.rfind(kPackedIndexFileVersionDelimiter);
    if (delimiter_pos == std::string::npos) {
        return false;
    }

    const size_t prefix_len = std::strlen(kPackedIndexFilePrefix);
    if (delimiter_pos <= prefix_len) {
        return false;
    }

    std::string token = filename.substr(prefix_len, delimiter_pos - prefix_len);
    if (!IsLowercaseToken(token)) {
        return false;
    }

    std::string ver_str = filename.substr(
        delimiter_pos + std::strlen(kPackedIndexFileVersionDelimiter));
    if (ver_str.empty()) {
        return false;
    }

    int64_t ver_val = 0;
    for (char c : ver_str) {
        if (c < '0' || c > '9') {
            return false;
        }
        ver_val = ver_val * 10 + (c - '0');
        if (ver_val > std::numeric_limits<int32_t>::max()) {
            return false;
        }
    }

    if (ver_val <= 0) {
        return false;
    }

    *index_type_token = std::move(token);
    *version = static_cast<int32_t>(ver_val);
    return true;
}

//==============================================================================
// Encryption Support
//==============================================================================

namespace {

// Stream write packed entries with encryption
// Each entry's data is encrypted individually
// Note: This function is not currently used directly, but kept for reference
// The main encrypted write is done through StreamWriteIndexFileEncrypted
size_t
StreamWritePackedEntriesEncrypted(
    OutputStream* output,
    const std::vector<SerializeEntry>& entries,
    WriteEntryDataFn write_entry_data,
    std::shared_ptr<storage::plugin::IEncryptor> encryptor) {
    size_t total_written = 0;

    // We need to encrypt all entries to know their encrypted sizes
    // This is required to build the correct DirectoryTable
    std::vector<std::string> encrypted_data;
    encrypted_data.reserve(entries.size());

    std::vector<SerializeEntry> encrypted_entries;
    encrypted_entries.reserve(entries.size());

    // Collect and encrypt each entry
    for (const auto& entry : entries) {
        // Buffer the entry data
        std::vector<uint8_t> plain_buffer(entry.size);

        // Create a simple buffer-backed output stream
        class BufferOutputStream : public OutputStream {
         public:
            std::vector<uint8_t>& buffer;
            size_t offset = 0;

            explicit BufferOutputStream(std::vector<uint8_t>& buf)
                : buffer(buf) {
            }

            size_t
            Tell() const override {
                return offset;
            }

            size_t
            Write(const void* data, size_t size) override {
                std::memcpy(buffer.data() + offset, data, size);
                offset += size;
                return size;
            }

            size_t
            Write(int fd, size_t size) override {
                // Read from fd to buffer
                std::vector<uint8_t> temp(size);
                ssize_t bytes_read = ::read(fd, temp.data(), size);
                if (bytes_read > 0) {
                    std::memcpy(
                        buffer.data() + offset, temp.data(), bytes_read);
                    offset += bytes_read;
                    return bytes_read;
                }
                return 0;
            }

            void
            Close() override {
            }
        };

        BufferOutputStream buffer_stream(plain_buffer);
        write_entry_data(&buffer_stream, entry.name);

        // Encrypt the data
        std::string plain_str(
            reinterpret_cast<const char*>(plain_buffer.data()),
            plain_buffer.size());
        std::string cipher_str = encryptor->Encrypt(plain_str);

        encrypted_data.push_back(std::move(cipher_str));
        encrypted_entries.push_back({entry.name, encrypted_data.back().size()});
    }

    // Now build directory table with encrypted sizes
    auto dir_table = BuildDirectoryTable(encrypted_entries);
    auto dir_table_data = dir_table.Serialize();

    // Write entry count (data entries + directory table entry)
    uint32_t entry_count = static_cast<uint32_t>(entries.size() + 1);
    uint8_t entry_count_buf[sizeof(uint32_t)];
    WriteLE(entry_count_buf, entry_count);
    total_written += output->Write(entry_count_buf, sizeof(uint32_t));

    // Write directory table entry header
    size_t dir_header_size = GetEntryHeaderSize(kDirectoryTableEntryName);
    std::vector<uint8_t> dir_header_buf(dir_header_size);
    WriteEntryHeader(
        dir_header_buf.data(), kDirectoryTableEntryName, dir_table_data.size());
    total_written += output->Write(dir_header_buf.data(), dir_header_size);

    // Write directory table data (not encrypted - for efficient seeking)
    total_written +=
        output->Write(dir_table_data.data(), dir_table_data.size());

    // Write encrypted data entries
    for (size_t i = 0; i < encrypted_entries.size(); ++i) {
        const auto& entry = encrypted_entries[i];
        const auto& cipher = encrypted_data[i];

        // Write entry header (with encrypted size)
        size_t header_size = GetEntryHeaderSize(entry.name);
        std::vector<uint8_t> header_buf(header_size);
        WriteEntryHeader(header_buf.data(), entry.name, entry.size);
        total_written += output->Write(header_buf.data(), header_size);

        // Write encrypted data
        total_written += output->Write(cipher.data(), cipher.size());
    }

    return total_written;
}

// Compute payload size for encrypted format
size_t
ComputeEncryptedPayloadSize(
    const std::vector<SerializeEntry>& encrypted_entries) {
    return ComputePayloadSize(encrypted_entries);
}

}  // namespace

size_t
StreamWriteIndexFileEncrypted(
    OutputStream* output,
    const storage::FieldDataMeta& field_meta,
    const storage::IndexMeta& index_meta,
    const std::vector<SerializeEntry>& entries,
    WriteEntryDataFn write_entry_data,
    std::shared_ptr<storage::plugin::IEncryptor> encryptor,
    const std::string& edek,
    const std::string& ezid) {
    // If no encryptor provided, fall back to non-encrypted version
    if (!encryptor) {
        return StreamWriteIndexFile(
            output, field_meta, index_meta, entries, write_entry_data);
    }

    size_t total_written = 0;

    // First, we need to encrypt all data to know the encrypted payload size
    // This is necessary because the DescriptorEvent needs to know the payload size

    // Collect and encrypt all entries first
    std::vector<std::string> encrypted_data;
    encrypted_data.reserve(entries.size());
    std::vector<SerializeEntry> encrypted_entries;
    encrypted_entries.reserve(entries.size());

    for (const auto& entry : entries) {
        // Buffer the entry data
        std::vector<uint8_t> plain_buffer(entry.size);

        class BufferOutputStream : public OutputStream {
         public:
            std::vector<uint8_t>& buffer;
            size_t offset = 0;

            explicit BufferOutputStream(std::vector<uint8_t>& buf)
                : buffer(buf) {
            }

            size_t
            Tell() const override {
                return offset;
            }

            size_t
            Write(const void* data, size_t size) override {
                std::memcpy(buffer.data() + offset, data, size);
                offset += size;
                return size;
            }

            size_t
            Write(int fd, size_t size) override {
                ssize_t bytes_read = ::read(fd, buffer.data() + offset, size);
                if (bytes_read > 0) {
                    offset += bytes_read;
                    return bytes_read;
                }
                return 0;
            }

            void
            Close() override {
            }
        };

        BufferOutputStream buffer_stream(plain_buffer);
        write_entry_data(&buffer_stream, entry.name);

        // Encrypt the data
        std::string plain_str(
            reinterpret_cast<const char*>(plain_buffer.data()),
            plain_buffer.size());
        std::string cipher_str = encryptor->Encrypt(plain_str);

        encrypted_data.push_back(std::move(cipher_str));
        encrypted_entries.push_back({entry.name, encrypted_data.back().size()});
    }

    // Compute encrypted payload size
    size_t payload_size = ComputePayloadSize(encrypted_entries);

    // Create and write DescriptorEvent with encryption metadata
    storage::DescriptorEvent descriptor_event;
    auto& des_event_data = descriptor_event.event_data;
    auto& des_fix_part = des_event_data.fix_part;
    des_fix_part.collection_id = field_meta.collection_id;
    des_fix_part.partition_id = field_meta.partition_id;
    des_fix_part.segment_id = field_meta.segment_id;
    des_fix_part.field_id = field_meta.field_id;
    des_fix_part.start_timestamp = 0;
    des_fix_part.end_timestamp = 0;
    des_fix_part.data_type = milvus::proto::schema::DataType::None;

    for (auto i = int8_t(storage::EventType::DescriptorEvent);
         i < int8_t(storage::EventType::EventTypeEnd);
         i++) {
        des_event_data.post_header_lengths.push_back(
            storage::GetEventFixPartSize(storage::EventType(i)));
    }
    des_event_data.extras[ORIGIN_SIZE_KEY] = std::to_string(payload_size);
    des_event_data.extras[INDEX_BUILD_ID_KEY] =
        std::to_string(index_meta.build_id);

    // Add encryption metadata
    des_event_data.extras[EDEK] = edek;
    des_event_data.extras[EZID] = ezid;

    auto& des_event_header = descriptor_event.event_header;
    des_event_header.timestamp_ = 0;

    auto des_event_bytes = descriptor_event.Serialize();
    total_written +=
        output->Write(des_event_bytes.data(), des_event_bytes.size());

    // Create IndexEvent header
    storage::Timestamp start_ts = 0;
    storage::Timestamp end_ts = 0;
    size_t event_data_size = sizeof(start_ts) + sizeof(end_ts) + payload_size;

    storage::EventHeader index_event_header;
    index_event_header.timestamp_ = 0;
    index_event_header.event_type_ = storage::EventType::IndexFileEvent;
    index_event_header.event_length_ =
        storage::GetEventHeaderSize(index_event_header) + event_data_size;
    index_event_header.next_position_ =
        index_event_header.event_length_ + des_event_bytes.size();

    auto header_bytes = index_event_header.Serialize();
    total_written += output->Write(header_bytes.data(), header_bytes.size());

    // Write timestamps
    total_written += output->Write(&start_ts, sizeof(start_ts));
    total_written += output->Write(&end_ts, sizeof(end_ts));

    // Build and write directory table with encrypted sizes
    auto dir_table = BuildDirectoryTable(encrypted_entries);
    auto dir_table_data = dir_table.Serialize();

    // Write entry count
    uint32_t entry_count = static_cast<uint32_t>(encrypted_entries.size() + 1);
    uint8_t entry_count_buf[sizeof(uint32_t)];
    WriteLE(entry_count_buf, entry_count);
    total_written += output->Write(entry_count_buf, sizeof(uint32_t));

    // Write directory table entry header
    size_t dir_header_size = GetEntryHeaderSize(kDirectoryTableEntryName);
    std::vector<uint8_t> dir_header_buf(dir_header_size);
    WriteEntryHeader(
        dir_header_buf.data(), kDirectoryTableEntryName, dir_table_data.size());
    total_written += output->Write(dir_header_buf.data(), dir_header_size);

    // Write directory table data (not encrypted)
    total_written +=
        output->Write(dir_table_data.data(), dir_table_data.size());

    // Write encrypted data entries
    for (size_t i = 0; i < encrypted_entries.size(); ++i) {
        const auto& entry = encrypted_entries[i];
        const auto& cipher = encrypted_data[i];

        // Write entry header
        size_t header_size = GetEntryHeaderSize(entry.name);
        std::vector<uint8_t> header_buf(header_size);
        WriteEntryHeader(header_buf.data(), entry.name, entry.size);
        total_written += output->Write(header_buf.data(), header_size);

        // Write encrypted data
        total_written += output->Write(cipher.data(), cipher.size());
    }

    return total_written;
}

EventHeaderInfo
StreamReadEventHeadersWithEncryptionInfo(InputStream* input) {
    EventHeaderInfo info;

    // Read entire DescriptorEvent to get encryption info
    // First, read MAGIC_NUM + EventHeader to get next_position
    constexpr size_t kMagicSize = 4;
    constexpr size_t kEventHeaderSize = 8 + 1 + 4 + 4;  // 17 bytes

    // Read enough to get next_position
    uint8_t header_buf[kMagicSize + kEventHeaderSize];
    input->Read(header_buf, sizeof(header_buf));

    // Parse next_position
    size_t offset = kMagicSize + 8 + 1 + 4;
    int32_t desc_next_position = 0;
    for (size_t i = 0; i < 4; ++i) {
        desc_next_position |= static_cast<int32_t>(header_buf[offset + i])
                              << (i * 8);
    }

    // Read the entire DescriptorEvent
    input->Seek(0);
    std::vector<uint8_t> desc_event_data(desc_next_position);
    input->Read(desc_event_data.data(), desc_next_position);

    // Parse DescriptorEvent to extract EDEK and EZID
    // Use BinlogReader to properly parse the event
    auto desc_reader = std::make_shared<storage::BinlogReader>(
        std::shared_ptr<uint8_t[]>(desc_event_data.data(),
                                   [](uint8_t*) {}),  // no-op deleter
        desc_next_position);

    // Skip magic number
    uint32_t magic;
    desc_reader->Read(sizeof(magic), &magic);

    // Parse DescriptorEvent
    storage::DescriptorEvent desc_event(desc_reader);

    // Extract encryption info from extras
    auto& extras = desc_event.event_data.extras;
    if (extras.find(EDEK) != extras.end()) {
        info.edek = std::any_cast<std::string>(extras[EDEK]);
    }
    if (extras.find(EZID) != extras.end()) {
        info.ezid = std::any_cast<std::string>(extras[EZID]);
    }
    info.collection_id = desc_event.event_data.fix_part.collection_id;

    // Calculate payload start
    input->Seek(desc_next_position);
    uint8_t index_header_buf[kEventHeaderSize];
    input->Read(index_header_buf, kEventHeaderSize);

    constexpr size_t kIndexEventFixPartSize = 8 + 8;
    info.payload_start =
        desc_next_position + kEventHeaderSize + kIndexEventFixPartSize;
    input->Seek(info.payload_start);

    return info;
}

std::pair<std::shared_ptr<uint8_t[]>, size_t>
StreamReadEntryToMemoryDecrypted(
    InputStream* input,
    size_t entry_offset,
    size_t entry_size,
    std::shared_ptr<storage::plugin::IDecryptor> decryptor) {
    // Read encrypted data
    auto encrypted_data = std::shared_ptr<uint8_t[]>(new uint8_t[entry_size]);
    input->ReadAt(encrypted_data.get(), entry_offset, entry_size);

    if (!decryptor) {
        // No decryption needed
        return {encrypted_data, entry_size};
    }

    // Decrypt the data
    std::string cipher_str(reinterpret_cast<const char*>(encrypted_data.get()),
                           entry_size);
    std::string plain_str = decryptor->Decrypt(cipher_str);

    // Copy to output buffer
    auto decrypted_data =
        std::shared_ptr<uint8_t[]>(new uint8_t[plain_str.size()]);
    std::memcpy(decrypted_data.get(), plain_str.data(), plain_str.size());

    return {decrypted_data, plain_str.size()};
}

}  // namespace milvus
