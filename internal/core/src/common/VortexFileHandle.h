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

#include <sys/mman.h>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>

#include "common/FieldMeta.h"
#include "common/Types.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"

namespace milvus {

/// Per-CG-file handle: manages memfd, mmap, and shared Reader.
///
/// All VortexFieldTranslators for the same column group file share
/// one VortexFileHandle.  Downloads the full vortex file into a memfd,
/// maps it with mmap(MAP_SHARED), and registers it in BufferFileSystem.
///
/// Benefits over plain Arrow Buffer:
/// - PUNCH_HOLE releases physical pages on cell eviction
/// - pwrite + MAP_SHARED makes re-loaded data immediately visible
///
/// When all VortexFieldTranslators are destroyed, VortexFileHandle
/// cleans up: munmap + close(fd) + BufferFileSystem::Unregister.
class VortexFileHandle {
 public:
    /// Create a VortexFileHandle by downloading the vortex file from S3.
    ///
    /// @param s3_fs       S3/remote filesystem for downloading
    /// @param s3_path     Full S3 path to the vortex file
    /// @param mem_path    mem:// path for BufferFileSystem registration
    /// @param column_groups  Column groups (with mem:// rewritten paths)
    /// @param arrow_schema   Arrow schema for Reader creation
    /// @param properties     Storage properties for Reader creation
    /// @param column_group_index  Index of this CG in column_groups
    /// @param lazy        If true, only download footer (lazy load v2)
    static std::shared_ptr<VortexFileHandle> Create(
        const std::shared_ptr<arrow::fs::FileSystem>& s3_fs,
        const std::string& s3_path,
        const std::string& mem_path,
        const std::shared_ptr<milvus_storage::api::ColumnGroups>&
            column_groups,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t column_group_index,
        bool lazy = false);

    ~VortexFileHandle();

    VortexFileHandle(const VortexFileHandle&) = delete;
    VortexFileHandle& operator=(const VortexFileHandle&) = delete;

    /// Shared Reader for data access (all fields share this).
    std::shared_ptr<milvus_storage::api::Reader>
    reader() const {
        return reader_;
    }

    /// S3 filesystem (for cell re-load via range reads after eviction).
    std::shared_ptr<arrow::fs::FileSystem>
    s3_fs() const {
        return s3_fs_;
    }

    /// S3 path (for cell re-load via range reads after eviction).
    const std::string&
    s3_path() const {
        return s3_path_;
    }

    /// memfd file descriptor (for pwrite and PUNCH_HOLE).
    int
    memfd() const {
        return memfd_;
    }

    /// Logical file size (= original vortex file size).
    size_t
    file_size() const {
        return file_size_;
    }

    /// Column group index within the column_groups.
    int64_t
    column_group_index() const {
        return column_group_index_;
    }

    /// Whether this handle was created in lazy mode (footer-only download).
    bool
    is_lazy() const {
        return lazy_;
    }

    /// Get per-cell segment byte ranges for PUNCH_HOLE, for multiple fields.
    /// Uses VortexFile metadata APIs (FieldChunkOffsets + SegmentBytes).
    /// Opens VortexFile once and extracts ranges for all requested fields.
    /// Returns: field_name → vector-of-cells, each cell = vector of (offset, length).
    std::unordered_map<std::string,
                       std::vector<std::vector<std::pair<off_t, size_t>>>>
    GetAllFieldsCellSegmentRanges(
        const std::vector<std::string>& field_names,
        size_t chunks_per_cell) const;

 private:
    VortexFileHandle() = default;

    int memfd_ = -1;
    void* mmap_ptr_ = MAP_FAILED;
    size_t file_size_ = 0;
    std::string mem_path_;
    std::string buffer_key_;  // resolved key used for BufferFileSystem registration
    int64_t column_group_index_ = -1;
    bool lazy_ = false;

    std::shared_ptr<arrow::fs::FileSystem> s3_fs_;
    std::string s3_path_;

    std::shared_ptr<milvus_storage::api::Reader> reader_;
    std::shared_ptr<milvus_storage::api::ColumnGroups> column_groups_;
};

}  // namespace milvus
