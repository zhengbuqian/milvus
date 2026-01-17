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
#include <filesystem>
#include <fstream>
#include <limits>
#include <unordered_set>

#include "common/EasyAssert.h"

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

}  // namespace

namespace milvus {

// Helper to append little-endian value to blob
template <typename T>
void
AppendLE(std::vector<uint8_t>& blob, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        blob.push_back(static_cast<uint8_t>(value & 0xFF));
        value >>= 8;
    }
}

// Helper to read little-endian value from blob
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

// Read entire file into vector
std::vector<uint8_t>
ReadFile(const std::string& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    AssertInfo(file.is_open(), "Failed to open file: {}", path);

    auto size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<uint8_t> data(size);
    file.read(reinterpret_cast<char*>(data.data()), size);
    return data;
}

// Write vector to file
void
WriteFile(const std::string& path, const uint8_t* data, size_t size) {
    std::ofstream file(path, std::ios::binary);
    AssertInfo(file.is_open(), "Failed to create file: {}", path);
    file.write(reinterpret_cast<const char*>(data), size);
}

}  // namespace milvus

std::vector<uint8_t>
milvus::PackDirectoryToBlob(const std::string& dir_path) {
    namespace fs = std::filesystem;

    std::vector<std::pair<std::string, std::vector<uint8_t>>> files;

    std::unordered_set<std::string> file_names;

    // Collect all regular files in directory recursively
    for (const auto& entry : fs::recursive_directory_iterator(dir_path)) {
        if (fs::is_regular_file(entry)) {
            auto relative_path = fs::relative(entry.path(), dir_path).string();
            auto [_, inserted] = file_names.insert(relative_path);
            AssertInfo(inserted,
                       "Duplicate file name in packed directory: {}",
                       relative_path);
            auto data = ReadFile(entry.path().string());
            files.emplace_back(std::move(relative_path), std::move(data));
        }
    }

    std::vector<uint8_t> blob;

    // Write file count
    AppendLE<uint32_t>(blob, static_cast<uint32_t>(files.size()));

    // Write each file entry
    for (const auto& [name, data] : files) {
        // Name length
        AppendLE<uint32_t>(blob, static_cast<uint32_t>(name.size()));
        // Name
        blob.insert(blob.end(), name.begin(), name.end());
        // Data size
        AppendLE<uint64_t>(blob, static_cast<uint64_t>(data.size()));
        // Data
        blob.insert(blob.end(), data.begin(), data.end());
    }

    return blob;
}

void
milvus::UnpackBlobToDirectory(const uint8_t* data,
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

        AssertInfo(IsSafeRelativePath(name),
                   "Invalid packed blob: unsafe path {}",
                   name);

        auto [_, inserted] = file_names.insert(name);
        AssertInfo(inserted, "Duplicate file name in packed blob: {}", name);

        AssertInfo(offset + 8 <= size,
                   "Invalid packed blob: truncated data size");
        uint64_t data_size = ReadLE<uint64_t>(data, offset);

        AssertInfo(offset + data_size <= size,
                   "Invalid packed blob: truncated data");

        auto file_path = fs::path(dir_path) / name;
        fs::create_directories(file_path.parent_path());
        WriteFile(file_path.string(), data + offset, data_size);
        offset += data_size;
    }
}

void
milvus::UnpackBlobToDirectory(const std::vector<uint8_t>& blob,
                              const std::string& dir_path) {
    UnpackBlobToDirectory(blob.data(), blob.size(), dir_path);
}

std::vector<uint8_t>
milvus::PackBinarySetToBlob(const BinarySet& binary_set) {
    std::vector<uint8_t> blob;

    // Write file count
    AppendLE<uint32_t>(blob,
                       static_cast<uint32_t>(binary_set.binary_map_.size()));

    // Write each entry
    for (const auto& [name, binary] : binary_set.binary_map_) {
        // Name length
        AppendLE<uint32_t>(blob, static_cast<uint32_t>(name.size()));
        // Name
        blob.insert(blob.end(), name.begin(), name.end());
        // Data size
        AppendLE<uint64_t>(blob, static_cast<uint64_t>(binary->size));
        // Data
        blob.insert(
            blob.end(), binary->data.get(), binary->data.get() + binary->size);
    }

    return blob;
}

milvus::BinarySet
milvus::UnpackBlobToBinarySet(const std::vector<uint8_t>& blob) {
    return UnpackBlobToBinarySet(blob.data(), blob.size());
}

milvus::BinarySet
milvus::UnpackBlobToBinarySet(const uint8_t* data, size_t size) {
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

        auto [_, inserted] = file_names.insert(name);
        AssertInfo(inserted, "Duplicate file name in packed blob: {}", name);

        AssertInfo(offset + 8 <= size,
                   "Invalid packed blob: truncated data size");
        uint64_t data_size = ReadLE<uint64_t>(data, offset);

        AssertInfo(offset + data_size <= size,
                   "Invalid packed blob: truncated data");

        auto binary = std::shared_ptr<uint8_t[]>(new uint8_t[data_size]);
        std::memcpy(binary.get(), data + offset, data_size);
        binary_set.Append(name, binary, data_size);
        offset += data_size;
    }

    return binary_set;
}

std::string
milvus::FormatPackedIndexFileName(const std::string& index_type_token,
                                  int32_t version) {
    AssertInfo(IsLowercaseToken(index_type_token),
               "Invalid packed index type token: {}",
               index_type_token);
    AssertInfo(version > 0, "Invalid packed index version: {}", version);

    return std::string(kPackedIndexFilePrefix) + index_type_token +
           kPackedIndexFileVersionDelimiter + std::to_string(version);
}

bool
milvus::TryParsePackedIndexFileName(const std::string& filename,
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

std::pair<std::shared_ptr<uint8_t[]>, size_t>
milvus::PackBinarySetToBinary(const BinarySet& binary_set) {
    // First, calculate the total size needed
    size_t total_size = 4;  // file count
    for (const auto& [name, binary] : binary_set.binary_map_) {
        total_size += 4;              // name length
        total_size += name.size();    // name
        total_size += 8;              // data size
        total_size += binary->size;   // data
    }

    // Allocate the buffer directly
    auto buffer = std::shared_ptr<uint8_t[]>(new uint8_t[total_size]);
    size_t offset = 0;

    // Helper to write little-endian value
    auto writeLE = [&buffer, &offset](auto value) {
        using T = decltype(value);
        for (size_t i = 0; i < sizeof(T); ++i) {
            buffer[offset++] = static_cast<uint8_t>(value & 0xFF);
            value >>= 8;
        }
    };

    // Write file count
    writeLE(static_cast<uint32_t>(binary_set.binary_map_.size()));

    // Write each entry
    for (const auto& [name, binary] : binary_set.binary_map_) {
        // Name length
        writeLE(static_cast<uint32_t>(name.size()));
        // Name
        std::memcpy(buffer.get() + offset, name.data(), name.size());
        offset += name.size();
        // Data size
        writeLE(static_cast<uint64_t>(binary->size));
        // Data
        std::memcpy(buffer.get() + offset, binary->data.get(), binary->size);
        offset += binary->size;
    }

    return {buffer, total_size};
}

std::pair<std::shared_ptr<uint8_t[]>, size_t>
milvus::PackDirectoryToBinary(const std::string& dir_path) {
    namespace fs = std::filesystem;

    std::vector<std::pair<std::string, std::vector<uint8_t>>> files;
    std::unordered_set<std::string> file_names;

    // Collect all regular files in directory recursively
    for (const auto& entry : fs::recursive_directory_iterator(dir_path)) {
        if (fs::is_regular_file(entry)) {
            auto relative_path = fs::relative(entry.path(), dir_path).string();
            auto [_, inserted] = file_names.insert(relative_path);
            AssertInfo(inserted,
                       "Duplicate file name in packed directory: {}",
                       relative_path);
            auto data = ReadFile(entry.path().string());
            files.emplace_back(std::move(relative_path), std::move(data));
        }
    }

    // Calculate total size
    size_t total_size = 4;  // file count
    for (const auto& [name, data] : files) {
        total_size += 4;           // name length
        total_size += name.size(); // name
        total_size += 8;           // data size
        total_size += data.size(); // data
    }

    // Allocate the buffer directly
    auto buffer = std::shared_ptr<uint8_t[]>(new uint8_t[total_size]);
    size_t offset = 0;

    // Helper to write little-endian value
    auto writeLE = [&buffer, &offset](auto value) {
        using T = decltype(value);
        for (size_t i = 0; i < sizeof(T); ++i) {
            buffer[offset++] = static_cast<uint8_t>(value & 0xFF);
            value >>= 8;
        }
    };

    // Write file count
    writeLE(static_cast<uint32_t>(files.size()));

    // Write each file entry
    for (const auto& [name, data] : files) {
        // Name length
        writeLE(static_cast<uint32_t>(name.size()));
        // Name
        std::memcpy(buffer.get() + offset, name.data(), name.size());
        offset += name.size();
        // Data size
        writeLE(static_cast<uint64_t>(data.size()));
        // Data
        std::memcpy(buffer.get() + offset, data.data(), data.size());
        offset += data.size();
    }

    return {buffer, total_size};
}
