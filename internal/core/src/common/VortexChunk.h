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
#include <memory>
#include <string>
#include <vector>

#include "common/Chunk.h"
#include "common/ChunkDataView.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "cachinglayer/Utils.h"
#include "milvus-storage/format/format_reader.h"

namespace milvus {

/// Lightweight Chunk implementation for Vortex format.
///
/// VortexChunk does NOT hold decompressed data. GetAnyDataView() returns a
/// lazy VortexDataView that decompresses on demand:
///   - operator[](idx): uses Reader::take() for efficient Vortex point query
///   - Data(): uses ChunkReader::get_chunks() to decompress all row groups
///
/// ValueAt() / Data() are not supported — callers must use the DataView path.
class VortexChunk : public Chunk {
 public:
    /// @param format_reader Per-field FormatReader (read_with_range / take)
    /// @param row_start     Global row offset for this chunk
    VortexChunk(FieldId field_id,
                const FieldMeta& field_meta,
                int64_t row_nums,
                int64_t compressed_size,
                std::shared_ptr<milvus_storage::FormatReader> format_reader,
                int column_in_batch,
                int64_t row_start)
        : field_id_(field_id),
          field_meta_(field_meta),
          row_nums_(row_nums),
          compressed_size_(compressed_size),
          format_reader_(std::move(format_reader)),
          column_in_batch_(column_in_batch),
          row_start_(row_start) {
    }

    ~VortexChunk() override = default;

    int64_t
    RowNums() const override {
        return row_nums_;
    }

    uint64_t
    Size() const override {
        return compressed_size_;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return cachinglayer::ResourceUsage(
            static_cast<int64_t>(compressed_size_), 0);
    }

    bool
    IsValid(int /*offset*/) const override {
        return true;
    }

    const char*
    ValueAt(int64_t /*idx*/) const override {
        ThrowInfo(NotImplemented,
                  "VortexChunk::ValueAt is not supported; use GetAnyDataView");
    }

    const char*
    Data() const override {
        ThrowInfo(NotImplemented,
                  "VortexChunk::Data is not supported; use GetAnyDataView");
    }

    // Per-field FormatReader for all data access.
    std::shared_ptr<milvus_storage::FormatReader>
    GetFormatReader() const {
        return format_reader_;
    }

    std::string
    GetFieldName() const {
        return std::to_string(field_id_.get());
    }

    // Returns owned raw bytes for the given chunk-local offsets.
    // Issues a single batched take(), extracts the field's binary column, and
    // returns the decompressed data directly — no extra allocation beyond what
    // Arrow decompression itself requires.
    // For string/JSON: each element is the raw string/JSON bytes.
    // For ARRAY: each element is the proto-serialized ScalarField bytes.
    std::vector<std::string>
    BulkOwnData(const int64_t* local_offsets, int64_t count) const override;

    // Returns a lazy VortexDataView — no decompression until accessed.
    AnyDataView
    GetAnyDataView() const override;

    AnyDataView
    GetAnyDataView(int64_t offset, int64_t length) const override;

    AnyDataView
    GetAnyDataView(const FixedVector<int32_t>& offsets) const override;

 private:
    const std::type_info&
    GetTypeInfo() const override;

    FieldId field_id_;
    FieldMeta field_meta_;
    int64_t row_nums_;
    int64_t compressed_size_;
    std::shared_ptr<milvus_storage::FormatReader> format_reader_;
    int column_in_batch_;
    int64_t row_start_;
};

}  // namespace milvus
