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

#include "index/vector/KnowhereEngine.h"

// SKELETON. Bodies are the re-homing map, not the implementation: every TODO
// names the file and lines the logic moves from, VERBATIM (§11.3 — re-homing,
// not redesign; the knowhere interaction does not change and the benchmarks are
// expected to be unchanged).
//
// !! LINE REFERENCES POINT AT THE PRE-W1 TREE (master e255009e01) !!
// `index/VectorIndex.h`, `index/VectorMemIndex.{h,cpp}` and
// `index/VectorDiskIndex.{h,cpp}` are deleted by this change — that IS the
// re-homing. Retrieve a cited body with
//   git show e255009e01:internal/core/src/index/VectorMemIndex.cpp

namespace milvus::index {

KnowhereEngine::KnowhereEngine(DataType elem_type,
                               IndexType index_type,
                               MetricType metric_type,
                               IndexVersion version,
                               bool use_knowhere_build_pool)
    : index_type_(std::move(index_type)),
      metric_type_(std::move(metric_type)),
      version_(version),
      elem_type_(elem_type),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
    // TODO: move existing logic here (see VectorMemIndex.cpp:168-202 for the
    // in-memory families and VectorDiskIndex.cpp:263-300 for DiskANN). Both
    // bodies do the same three things: CheckCompatible(version), then
    // `knowhere::IndexFactory::Instance().Create<T>(index_type, version, ...)`,
    // then stash the file manager — and THE THIRD ONE DOES NOT COME ALONG
    // (§3 principle 6: no `FileManagerContext` on a builder/reader).
}

KnowhereEngine::KnowhereEngine(DataType elem_type,
                               IndexType index_type,
                               MetricType metric_type,
                               IndexVersion version,
                               knowhere::ViewDataOp view_data,
                               bool use_knowhere_build_pool)
    : index_type_(std::move(index_type)),
      metric_type_(std::move(metric_type)),
      version_(version),
      elem_type_(elem_type),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
    // TODO: move existing logic here (see VectorMemIndex.cpp:203-233) — the
    // DataView variant: pack `view_data` into a knowhere Object and create the
    // index through it. This is the ctor the interim/growing path uses, which is
    // why `index/growing/KnowhereGrowingVectorIndex.h` composes this engine
    // rather than re-deriving one.
}

knowhere::Json
KnowhereEngine::PrepareSearchParams(const VectorSearchParams& params) const {
    // TODO: move existing logic here (see VectorIndex.h:171-190). Verbatim, with
    // `SearchInfo` swapped for the narrow §12.1(a) type — the body only ever
    // read `search_params_`, `metric_type_`, `topk_` and `trace_ctx_`, which is
    // precisely the audit that closed §12.1(a).
    return {};
}

bool
KnowhereEngine::MmapSupported() const {
    // TODO: move existing logic here (see VectorIndex.h:165-169).
    return false;
}

void
KnowhereEngine::CheckCompatible(IndexVersion version) const {
    // TODO: move existing logic here (see VectorIndex.h:153-163).
}

int64_t
KnowhereEngine::RawCount() const {
    // TODO: move existing logic here (see VectorMemIndex.h:85-95 /
    // VectorDiskIndex.h:217-227), minus the two zero-cases that belong to the
    // caller (all-null nullable field; empty embedding-list index).
    return 0;
}

bool
KnowhereEngine::HasRawData() const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:816-825 /
    // VectorDiskIndex.cpp:822-831).
    return false;
}

bool
KnowhereEngine::RefineEnabled() const {
    // TODO: move existing logic here (see VectorMemIndex.cpp:826-835 /
    // VectorDiskIndex.cpp:832-841) — today's `IsIndexRefineEnabled`.
    return false;
}

template <typename T>
std::vector<uint8_t>
DecodeVectorByIdsResult(const knowhere::DataSetPtr& result) {
    // TODO: move existing logic here (see VectorIndex.h:244-255).
    return {};
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult(const knowhere::DataSetPtr& result) {
    // TODO: move existing logic here (see VectorIndex.h:257-276).
    return {};
}

}  // namespace milvus::index
