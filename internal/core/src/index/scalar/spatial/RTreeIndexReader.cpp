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

#include "index/scalar/spatial/RTreeIndexReader.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::index {
namespace {

class GeosContextGuard {
 public:
    GeosContextGuard() : context_(GEOS_init_r()) {
        if (context_ == nullptr) {
            ThrowInfo(UnexpectedError,
                      "failed to initialize GEOS for R-Tree query");
        }
    }

    GeosContextGuard(const GeosContextGuard&) = delete;
    GeosContextGuard&
    operator=(const GeosContextGuard&) = delete;

    ~GeosContextGuard() {
        GEOS_finish_r(context_);
    }

    GEOSContextHandle_t
    Get() const {
        return context_;
    }

 private:
    GEOSContextHandle_t context_;
};

}  // namespace

RTreeIndexState::RTreeIndexState(
    std::shared_ptr<const RTreeQueryEngine> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t total_num_rows,
    int64_t memory_usage)
    : engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      total_num_rows_(total_num_rows),
      memory_usage_(memory_usage) {
}

std::shared_ptr<const RTreeIndexState>
RTreeIndexState::Create(std::shared_ptr<const RTreeQueryEngine> engine,
                        std::shared_ptr<const std::vector<size_t>> null_offsets,
                        int64_t total_num_rows) {
    AssertInfo(engine != nullptr, "R-Tree state engine must not be null");
    AssertInfo(null_offsets != nullptr,
               "R-Tree state NULL offsets must not be null");
    AssertInfo(total_num_rows >= 0,
               "R-Tree state row count must not be negative");
    size_t previous = 0;
    bool first = true;
    for (const auto offset : *null_offsets) {
        if ((!first && offset <= previous) ||
            offset >= static_cast<size_t>(total_num_rows)) {
            ThrowInfo(DataFormatBroken,
                      "invalid R-Tree null offset {} for row count {}",
                      offset,
                      total_num_rows);
        }
        previous = offset;
        first = false;
    }
    engine->ValidateCoordinates(total_num_rows, *null_offsets);

    const auto engine_bytes = engine->ByteSize();
    AssertInfo(engine_bytes >= 0,
               "R-Tree engine memory size must not be negative");
    const auto available = std::numeric_limits<int64_t>::max() - engine_bytes;
    constexpr auto kStateBytes = static_cast<int64_t>(
        sizeof(RTreeIndexState) + sizeof(std::vector<size_t>));
    const auto null_capacity = null_offsets->capacity();
    int64_t memory_usage = std::numeric_limits<int64_t>::max();
    if (available >= kStateBytes &&
        null_capacity <=
            static_cast<size_t>((available - kStateBytes) / sizeof(size_t))) {
        memory_usage = engine_bytes + kStateBytes +
                       static_cast<int64_t>(null_capacity) *
                           static_cast<int64_t>(sizeof(size_t));
    }
    return std::shared_ptr<const RTreeIndexState>(
        new RTreeIndexState(std::move(engine),
                            std::move(null_offsets),
                            total_num_rows,
                            memory_usage));
}

const RTreeQueryEngine&
RTreeIndexState::Engine() const {
    return *engine_;
}

const std::vector<size_t>&
RTreeIndexState::NullOffsets() const {
    return *null_offsets_;
}

int64_t
RTreeIndexState::Count() const {
    return total_num_rows_;
}

int64_t
RTreeIndexState::MemoryUsage() const {
    return memory_usage_;
}

RTreeIndexReader::RTreeIndexReader(std::shared_ptr<const RTreeIndexState> state)
    : state_(std::move(state)) {
    AssertInfo(state_ != nullptr, "R-Tree reader state must not be null");
}

RTreeIndexReader::~RTreeIndexReader() = default;

ReaderCaps
RTreeIndexReader::Caps() const {
    // `exact = false`: the MBR filter returns a SUPERSET and exec refines
    // (`PhyGISRefineConjunctExpr`). This is the candidate-family contract that
    // §5.6 says ngram should copy — spatial got it right first.
    return ReaderCaps{.spatial = true, .exact = false};
}

Domain
RTreeIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
RTreeIndexReader::Count() const {
    return state_->Count();
}

DataType
RTreeIndexReader::ValueType() const {
    return DataType::GEOMETRY;
}

int64_t
RTreeIndexReader::MemoryUsage() const {
    return state_->MemoryUsage();
}

cachinglayer::ResourceUsage
RTreeIndexReader::CellByteSize() const {
    // Boost deserializes the complete tree. Staging files are deleted after
    // Open and no mmap/file-backed bytes remain owned by this reader.
    return {MemoryUsage(), 0};
}

TargetBitmap
RTreeIndexReader::Candidates(SpatialOp op, const Geometry& query_geom) const {
    static_cast<void>(op);
    if (!query_geom.IsValid()) {
        return IsNotNull();
    }
    GeosContextGuard context;
    TargetBitmap result(static_cast<size_t>(state_->Count()), false);
    const auto has_query_box = state_->Engine().ForEachCandidate(
        query_geom.GetGeometry(), context.Get(), [&](int64_t offset) {
            AssertInfo(offset >= 0 && offset < state_->Count(),
                       "R-Tree candidate offset {} is outside [0, {})",
                       offset,
                       state_->Count());
            result.set(static_cast<size_t>(offset));
        });
    if (!has_query_box) {
        return IsNotNull();
    }
    return result;
}

TargetBitmap
RTreeIndexReader::IsNull() const {
    TargetBitmap result(static_cast<size_t>(state_->Count()), false);
    for (const auto offset : state_->NullOffsets()) {
        result.set(offset);
    }
    return result;
}

TargetBitmap
RTreeIndexReader::IsNotNull() const {
    auto result = IsNull();
    result.flip();
    return result;
}

}  // namespace milvus::index
