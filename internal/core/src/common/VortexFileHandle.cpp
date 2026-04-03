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

#include "common/VortexFileHandle.h"

#include <sys/mman.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

#include "common/BufferFileSystem.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "milvus-storage/filesystem/ffi/filesystem_internal.h"
#include "milvus-storage/filesystem/fs.h"
#include "vortex_bridge.h"

namespace milvus {

// Conservative footer download size for lazy mode.
// Vortex footer for typical Milvus segments (≤16M rows) is well under 1MB.
// 2MB provides ample margin; if insufficient, VortexFile::Open will fail
// and the caller should fall back to non-lazy mode.
static constexpr size_t kLazyFooterDownloadSize = 2 * 1024 * 1024;

std::shared_ptr<VortexFileHandle>
VortexFileHandle::Create(
    const std::shared_ptr<arrow::fs::FileSystem>& s3_fs,
    const std::string& s3_path,
    const std::string& mem_path,
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    int64_t column_group_index,
    bool lazy) {
    auto handle = std::shared_ptr<VortexFileHandle>(new VortexFileHandle());
    handle->s3_fs_ = s3_fs;
    handle->s3_path_ = s3_path;
    handle->mem_path_ = mem_path;
    handle->column_group_index_ = column_group_index;
    handle->column_groups_ = column_groups;
    handle->lazy_ = lazy;

    // --- 1. Get file size from S3 ---
    auto info_result = s3_fs->GetFileInfo(s3_path);
    AssertInfo(info_result.ok(),
               "VortexFileHandle: GetFileInfo failed for {}: {}",
               s3_path,
               info_result.status().ToString());
    handle->file_size_ = info_result->size();

    auto input_result = s3_fs->OpenInputFile(s3_path);
    AssertInfo(input_result.ok(),
               "VortexFileHandle: OpenInputFile failed for {}: {}",
               s3_path,
               input_result.status().ToString());
    auto input = input_result.ValueOrDie();

    // --- 2. Create memfd + ftruncate ---
    handle->memfd_ = memfd_create("vortex_cg", MFD_CLOEXEC);
    AssertInfo(handle->memfd_ >= 0,
               "VortexFileHandle: memfd_create failed: {}",
               strerror(errno));

    int rc = ftruncate(handle->memfd_, handle->file_size_);
    AssertInfo(rc == 0,
               "VortexFileHandle: ftruncate failed: {}",
               strerror(errno));

    // --- 3. Download and pwrite to memfd ---
    if (lazy) {
        // Lazy mode: only download the footer (tail of the file)
        size_t footer_size =
            std::min(static_cast<size_t>(handle->file_size_),
                     kLazyFooterDownloadSize);
        int64_t footer_offset =
            static_cast<int64_t>(handle->file_size_) -
            static_cast<int64_t>(footer_size);

        auto buf_result = input->ReadAt(footer_offset, footer_size);
        AssertInfo(buf_result.ok(),
                   "VortexFileHandle: ReadAt footer failed for {}: {}",
                   s3_path,
                   buf_result.status().ToString());
        auto footer_buf = buf_result.ValueOrDie();

        size_t written = 0;
        while (written < footer_size) {
            ssize_t n = pwrite(handle->memfd_,
                               footer_buf->data() + written,
                               footer_size - written,
                               footer_offset + written);
            AssertInfo(
                n > 0,
                "VortexFileHandle: pwrite footer failed at offset {}: {}",
                footer_offset + written,
                strerror(errno));
            written += n;
        }
    } else {
        // Non-lazy mode: download the entire file
        auto buf_result = input->Read(handle->file_size_);
        AssertInfo(buf_result.ok(),
                   "VortexFileHandle: Read failed for {}: {}",
                   s3_path,
                   buf_result.status().ToString());
        auto download_buf = buf_result.ValueOrDie();

        size_t written = 0;
        while (written < handle->file_size_) {
            ssize_t n = pwrite(handle->memfd_,
                               download_buf->data() + written,
                               handle->file_size_ - written,
                               written);
            AssertInfo(
                n > 0,
                "VortexFileHandle: pwrite failed at offset {}: {}",
                written,
                strerror(errno));
            written += n;
        }
    }

    // --- 4. mmap the memfd ---
    handle->mmap_ptr_ = mmap(nullptr,
                             handle->file_size_,
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED,
                             handle->memfd_,
                             0);
    AssertInfo(handle->mmap_ptr_ != MAP_FAILED,
               "VortexFileHandle: mmap failed: {}",
               strerror(errno));

    // --- 5. Register mmap'd buffer in BufferFileSystem ---
    // Non-owning buffer: VortexFileHandle owns the mmap lifetime
    auto mmap_buffer = std::make_shared<arrow::Buffer>(
        static_cast<const uint8_t*>(handle->mmap_ptr_),
        handle->file_size_);
    auto buffer_fs = BufferFileSystem::getInstance();

    // FormatReader::create() re-parses the mem:// path and extracts
    // uri.key as the resolved path for OpenInputFile.  We must register
    // with that same resolved key so the lookup succeeds.
    auto mem_uri_result = milvus_storage::StorageUri::Parse(mem_path);
    AssertInfo(mem_uri_result.ok(),
               "VortexFileHandle: failed to parse mem path: {}",
               mem_path);
    handle->buffer_key_ = mem_uri_result.ValueOrDie().key;
    buffer_fs->Register(handle->buffer_key_, mmap_buffer);

    auto& fs_cache = milvus_storage::FilesystemCache::getInstance();
    fs_cache.put("mem", buffer_fs);

    // --- 6. Create Reader ---
    handle->reader_ = std::shared_ptr<milvus_storage::api::Reader>(
        milvus_storage::api::Reader::create(
            column_groups, arrow_schema, nullptr, *properties)
            .release());

    LOG_INFO(
        "[StorageV2] VortexFileHandle created: {} ({} bytes, memfd={}, {})",
        mem_path,
        handle->file_size_,
        handle->memfd_,
        lazy ? "lazy" : "full");

    return handle;
}

VortexFileHandle::~VortexFileHandle() {
    // Unregister from BufferFileSystem using the resolved key
    auto buffer_fs = BufferFileSystem::getInstance();
    buffer_fs->Unregister(buffer_key_);

    // Unmap
    if (mmap_ptr_ != MAP_FAILED && mmap_ptr_ != nullptr) {
        munmap(mmap_ptr_, file_size_);
        mmap_ptr_ = MAP_FAILED;
    }

    // Close memfd
    if (memfd_ >= 0) {
        close(memfd_);
        memfd_ = -1;
    }

    LOG_INFO("[StorageV2] VortexFileHandle destroyed: {}", mem_path_);
}

namespace {

// Parse FieldChunkOffsets output and resolve byte ranges per cell.
std::vector<std::vector<std::pair<off_t, size_t>>>
ExtractCellRanges(
    milvus_storage::vortex::VortexFile& vxfile,
    const std::string& field_name,
    size_t chunks_per_cell) {
    auto chunk_offsets = vxfile.FieldChunkOffsets(field_name);
    // Format: [num_chunked_layouts, total_chunk_children,
    //   chunk_idx, row_offset, row_count, num_segments,
    //   seg_id0, seg_id1, ..., ...]
    if (chunk_offsets.size() < 2) {
        return {};
    }

    size_t total_chunks = static_cast<size_t>(chunk_offsets[1]);

    // Parse per-chunk segment IDs
    struct ChunkSegInfo {
        std::vector<uint64_t> seg_ids;
    };
    std::vector<ChunkSegInfo> chunk_segs;
    chunk_segs.reserve(total_chunks);

    size_t pos = 2;
    while (pos < chunk_offsets.size()) {
        if (pos + 3 >= chunk_offsets.size())
            break;
        size_t num_segs = static_cast<size_t>(chunk_offsets[pos + 3]);
        ChunkSegInfo info;
        for (size_t s = 0; s < num_segs; ++s) {
            if (pos + 4 + s < chunk_offsets.size()) {
                info.seg_ids.push_back(chunk_offsets[pos + 4 + s]);
            }
        }
        chunk_segs.push_back(std::move(info));
        pos += 4 + num_segs;
    }

    // Group chunks into cells and resolve byte ranges
    size_t num_cells =
        (total_chunks + chunks_per_cell - 1) / chunks_per_cell;
    std::vector<std::vector<std::pair<off_t, size_t>>> result;
    result.resize(num_cells);

    for (size_t cid = 0; cid < num_cells; ++cid) {
        size_t rg_start = cid * chunks_per_cell;
        size_t rg_end =
            std::min(rg_start + chunks_per_cell, total_chunks);

        for (size_t rg = rg_start; rg < rg_end; ++rg) {
            if (rg >= chunk_segs.size())
                break;
            for (auto seg_id : chunk_segs[rg].seg_ids) {
                auto seg_bytes = vxfile.SegmentBytes(seg_id);
                if (seg_bytes.size() == 2) {
                    result[cid].emplace_back(
                        static_cast<off_t>(seg_bytes[0]),
                        static_cast<size_t>(seg_bytes[1]));
                }
            }
        }
    }

    return result;
}

}  // namespace

std::unordered_map<std::string,
                   std::vector<std::vector<std::pair<off_t, size_t>>>>
VortexFileHandle::GetAllFieldsCellSegmentRanges(
    const std::vector<std::string>& field_names,
    size_t chunks_per_cell) const {
    // Open VortexFile once for all fields
    auto buffer_fs = BufferFileSystem::getInstance();
    auto fs_holder =
        std::make_shared<FileSystemWrapper>(buffer_fs);
    auto vxfile =
        milvus_storage::vortex::VortexFile::OpenUnique(
            reinterpret_cast<uint8_t*>(fs_holder.get()),
            buffer_key_);

    std::unordered_map<std::string,
                       std::vector<std::vector<std::pair<off_t, size_t>>>>
        result;
    for (const auto& name : field_names) {
        result[name] = ExtractCellRanges(*vxfile, name, chunks_per_cell);
    }
    return result;
}

}  // namespace milvus
