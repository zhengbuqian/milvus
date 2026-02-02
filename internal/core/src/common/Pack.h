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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "filemanager/OutputStream.h"
#include "storage/Types.h"

namespace milvus {
namespace storage {
namespace plugin {
class IEncryptor;
class IDecryptor;
}  // namespace plugin
}  // namespace storage

// Unified scalar index format version that introduces single-file storage
constexpr int32_t kUnifiedScalarIndexVersion = 3;

// kLastScalarIndexEngineVersionWithoutMeta is the last engine version where
// scalar index metadata might not include a version field.
constexpr int32_t kLastScalarIndexEngineVersionWithoutMeta = 2;

// Packed file name format: packed_<index_type>_v<ver>
// index_type should be a short, stable, lowercase token.
constexpr const char* kPackedIndexFilePrefix = "packed_";

// Reserved name for the directory table entry
constexpr const char* kDirectoryTableEntryName = "__index_dir__";

//==============================================================================
// Streaming Upload Data Structures
//==============================================================================

// Entry metadata collected during Phase 1 (only name and size, no data)
struct SerializeEntry {
    std::string name;
    uint64_t size;
};

// Directory entry for random access during load
struct DirectoryEntry {
    std::string name;
    uint64_t offset;  // byte offset from start of payload
    uint64_t size;    // entry data size
};

// Directory table stored as the first entry in packed format
struct DirectoryTable {
    std::vector<DirectoryEntry> entries;

    // Serialize to binary
    std::vector<uint8_t>
    Serialize() const;

    // Deserialize from binary
    static DirectoryTable
    Deserialize(const uint8_t* data, size_t size);

    // Find entry by name, returns nullptr if not found
    const DirectoryEntry*
    Find(const std::string& name) const;
};

//==============================================================================
// Pack Format V3 (with Directory Table):
// ┌─────────────────────────────────────────────────────────────────┐
// │ Entry Count (4 bytes, uint32, little-endian) - includes dir    │
// ├─────────────────────────────────────────────────────────────────┤
// │ Entry 0: DIRECTORY TABLE (always first)                        │
// │   - Name Length (4 bytes): len("__index_dir__")                │
// │   - Name: "__index_dir__"                                      │
// │   - Data Size (8 bytes)                                        │
// │   - Data: serialized DirectoryTable                            │
// ├─────────────────────────────────────────────────────────────────┤
// │ Entry 1: DATA ENTRY                                            │
// │   - Name Length (4 bytes, uint32)                              │
// │   - Name (variable bytes, UTF-8, no null terminator)           │
// │   - Data Size (8 bytes, uint64)                                │
// │   - Data (variable bytes)                                      │
// ├─────────────────────────────────────────────────────────────────┤
// │ Entry 2: DATA ENTRY ...                                        │
// └─────────────────────────────────────────────────────────────────┘
//==============================================================================

//==============================================================================
// Streaming Write Functions
//==============================================================================

// Compute total payload size from entries (includes directory table)
size_t
ComputePayloadSize(const std::vector<SerializeEntry>& entries);

// Compute directory table size
size_t
ComputeDirectoryTableSize(const std::vector<SerializeEntry>& entries);

// Build directory table from entries
DirectoryTable
BuildDirectoryTable(const std::vector<SerializeEntry>& entries);

// Write entry header (name_len + name + data_size) to buffer
// Returns number of bytes written
size_t
WriteEntryHeader(uint8_t* buffer, const std::string& name, uint64_t data_size);

// Get entry header size
inline size_t
GetEntryHeaderSize(const std::string& name) {
    return sizeof(uint32_t) + name.size() + sizeof(uint64_t);
}

//==============================================================================
// Unpack Functions (for Load path - preserved from V3 draft)
//==============================================================================

// UnpackBlobToDirectory unpacks from raw pointer and size.
void
UnpackBlobToDirectory(const uint8_t* data,
                      size_t size,
                      const std::string& dir_path);

// UnpackBlobToBinarySet unpacks from raw pointer and size.
BinarySet
UnpackBlobToBinarySet(const uint8_t* data, size_t size);

//==============================================================================
// Directory Streaming Helpers
//==============================================================================

// Collect directory entries (file names and sizes) - zero memory overhead
std::vector<SerializeEntry>
CollectDirectoryEntries(const std::string& dir_path);

//==============================================================================
// Streaming Write to OutputStream
//==============================================================================

// Stream write packed directory to OutputStream
// Writes: entry_count + directory_table_entry + data entries (streamed from files)
// Returns total bytes written
size_t
StreamWritePackedDirectory(OutputStream* output, const std::string& dir_path);

// Stream write packed entries to OutputStream
// entries: metadata collected in Phase 1
// write_entry_data: callback to write each entry's data
// Returns total bytes written
using WriteEntryDataFn = std::function<void(OutputStream*, const std::string&)>;
size_t
StreamWritePackedEntries(OutputStream* output,
                         const std::vector<SerializeEntry>& entries,
                         WriteEntryDataFn write_entry_data);

// Stream write complete index file with event headers
// Writes: DescriptorEvent + IndexEventHeader + packed payload
// Returns total bytes written
size_t
StreamWriteIndexFile(OutputStream* output,
                     const storage::FieldDataMeta& field_meta,
                     const storage::IndexMeta& index_meta,
                     const std::vector<SerializeEntry>& entries,
                     WriteEntryDataFn write_entry_data);

// Stream write complete index file with encryption support
// When encryptor is provided, each entry's data will be encrypted
// EDEK and EZID are stored in DescriptorEvent extras
// Returns total bytes written
size_t
StreamWriteIndexFileEncrypted(
    OutputStream* output,
    const storage::FieldDataMeta& field_meta,
    const storage::IndexMeta& index_meta,
    const std::vector<SerializeEntry>& entries,
    WriteEntryDataFn write_entry_data,
    std::shared_ptr<storage::plugin::IEncryptor> encryptor,
    const std::string& edek,
    const std::string& ezid);

//==============================================================================
// Streaming Read from InputStream (for Load path)
//==============================================================================

// Read event headers (DescriptorEvent + IndexEvent header) and return payload start offset
// Returns the offset where payload data begins
size_t
StreamReadEventHeaders(InputStream* input);

// Read directory table from payload start
// input: positioned at payload start (after event headers)
// Returns: DirectoryTable with entry offsets relative to payload start
DirectoryTable
StreamReadDirectoryTable(InputStream* input);

// Stream unpack a nested packed directory entry to local directory
// Uses DirectoryTable info for random access
// entry_offset: absolute offset in the file where the entry data starts
// entry_size: size of the entry data
void
StreamUnpackEntryToDirectory(InputStream* input,
                             size_t entry_offset,
                             size_t entry_size,
                             const std::string& dir_path);

// Read an entry to memory buffer
// Returns shared_ptr to data and size
std::pair<std::shared_ptr<uint8_t[]>, size_t>
StreamReadEntryToMemory(InputStream* input,
                        size_t entry_offset,
                        size_t entry_size);

// Read an encrypted entry to memory buffer and decrypt it
// decryptor: the decryptor to use for decryption
// entry_offset: offset to the encrypted data in the file
// entry_size: size of the encrypted data
// Returns: decrypted data and its size
std::pair<std::shared_ptr<uint8_t[]>, size_t>
StreamReadEntryToMemoryDecrypted(
    InputStream* input,
    size_t entry_offset,
    size_t entry_size,
    std::shared_ptr<storage::plugin::IDecryptor> decryptor);

// Read event headers and return payload start offset, EDEK and EZID
// This is used for encrypted files to get decryption metadata
struct EventHeaderInfo {
    size_t payload_start;
    std::string edek;
    std::string ezid;
    int64_t collection_id;
};
EventHeaderInfo
StreamReadEventHeadersWithEncryptionInfo(InputStream* input);

//==============================================================================
// Helpers
//==============================================================================

// Helpers for packed filename.
std::string
FormatPackedIndexFileName(const std::string& index_type_token, int32_t version);

bool
TryParsePackedIndexFileName(const std::string& filename,
                            std::string* index_type_token,
                            int32_t* version);

// Helper to check if version supports unified format
inline bool
IsUnifiedScalarIndexVersion(int32_t version) {
    return version >= kUnifiedScalarIndexVersion;
}

// Helper to write little-endian value to buffer, returns bytes written
template <typename T>
size_t
WriteLE(uint8_t* buffer, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        buffer[i] = static_cast<uint8_t>(value & 0xFF);
        value >>= 8;
    }
    return sizeof(T);
}

// Helper to read little-endian value from buffer
template <typename T>
T
ReadLE(const uint8_t* data, size_t& offset) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(data[offset + i]) << (i * 8);
    }
    offset += sizeof(T);
    return value;
}

}  // namespace milvus
