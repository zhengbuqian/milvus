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
#include <string>
#include <vector>

#include "common/Types.h"

namespace milvus {

// Unified scalar index format version that introduces single-file storage
constexpr int32_t kUnifiedScalarIndexVersion = 3;

// kLastScalarIndexEngineVersionWithoutMeta is the last engine version where
// scalar index metadata might not include a version field.
constexpr int32_t kLastScalarIndexEngineVersionWithoutMeta = 2;

// Packed file name format: packed_<index_type>_v<ver>
// index_type should be a short, stable, lowercase token.
constexpr const char* kPackedIndexFilePrefix = "packed_";

// Pack Format:
// ┌─────────────────────────────────────────────────────────────────┐
// │ File Count (4 bytes, uint32, little-endian)                     │
// ├─────────────────────────────────────────────────────────────────┤
// │ File Entry 1:                                                   │
// │   - Name Length (4 bytes, uint32)                               │
// │   - Name (variable bytes, UTF-8, no null terminator)            │
// │   - Data Size (8 bytes, uint64)                                 │
// │   - Data (variable bytes)                                       │
// ├─────────────────────────────────────────────────────────────────┤
// │ File Entry 2: ...                                               │
// └─────────────────────────────────────────────────────────────────┘

// PackDirectoryToBlob packs all files in a directory into a single binary blob.
// Returns the packed blob.
std::vector<uint8_t>
PackDirectoryToBlob(const std::string& dir_path);

// UnpackBlobToDirectory unpacks a binary blob back to a directory of files.
// Creates the directory if it doesn't exist.
void
UnpackBlobToDirectory(const std::vector<uint8_t>& blob,
                      const std::string& dir_path);

// UnpackBlobToDirectory unpacks from raw pointer and size.
void
UnpackBlobToDirectory(const uint8_t* data,
                      size_t size,
                      const std::string& dir_path);

// PackBinarySetToBlob packs a BinarySet (map of filename -> binary data) into a single blob.
// This is useful for indexes that already produce a BinarySet with multiple entries.
std::vector<uint8_t>
PackBinarySetToBlob(const BinarySet& binary_set);

// PackBinarySetToBinary packs a BinarySet into a shared_ptr<uint8_t[]> directly,
// avoiding an extra memory copy compared to PackBinarySetToBlob.
std::pair<std::shared_ptr<uint8_t[]>, size_t>
PackBinarySetToBinary(const BinarySet& binary_set);

// PackDirectoryToBinary packs all files in a directory into a shared_ptr<uint8_t[]> directly,
// avoiding an extra memory copy compared to PackDirectoryToBlob.
std::pair<std::shared_ptr<uint8_t[]>, size_t>
PackDirectoryToBinary(const std::string& dir_path);

// UnpackBlobToBinarySet unpacks a binary blob back to a BinarySet.
BinarySet
UnpackBlobToBinarySet(const std::vector<uint8_t>& blob);

// UnpackBlobToBinarySet unpacks from raw pointer and size.
BinarySet
UnpackBlobToBinarySet(const uint8_t* data, size_t size);

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

}  // namespace milvus
