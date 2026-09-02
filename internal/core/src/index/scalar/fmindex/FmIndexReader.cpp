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

#include "index/scalar/fmindex/FmIndexReader.h"

#include <utility>

namespace milvus::index {

FmIndexReader::FmIndexReader(fmindex::FMIndex engine,
                             TargetBitmap null_bitmap,
                             int64_t total_rows,
                             int64_t total_tokens,
                             double cost_ratio)
    : engine_(std::move(engine)),
      null_bitmap_(std::move(null_bitmap)),
      total_rows_(total_rows),
      total_tokens_(total_tokens),
      cost_ratio_(cost_ratio) {
}

FmIndexReader::~FmIndexReader() {
    // TODO: move existing logic here (see FMIndex.h:56-71 — the mmap teardown.
    // The `munmap` half belongs to whatever owns the mapping produced by
    // `FmIndexLoader`; `disk_file_manager_->RemoveIndexFiles()` does NOT come
    // along, §10 rule 2).
}

ReaderCaps
FmIndexReader::Caps() const {
    // No `predicate` bit: In/NotIn/Range are throw shells today
    // (FMIndex.cpp:379,386; FMIndex.h:148,156) and are deleted outright.
    // No `value_lookup`: `HasRawData()` is false (FMIndex.h:261-264) and
    // `Reverse_Lookup` throws (FMIndex.h:268).
    return ReaderCaps{.pattern_match = true};
}

Domain
FmIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
FmIndexReader::Count() const {
    return total_rows_;
}

DataType
FmIndexReader::ValueType() const {
    return DataType::VARCHAR;
}

int64_t
FmIndexReader::MemoryUsage() const {
    // TODO: move existing logic here (see FMIndex.cpp:283-294 ComputeByteSize:
    // engine resident heap + null bitmap bytes, saturating).
}

ResourceUsage
FmIndexReader::CellByteSize() const {
    // TODO: move existing logic here (see FMIndex.cpp:708-715, which re-measures
    // after load and overwrites the memory half of the estimate).
    // FMIndex is one of only two families that report MEASURED memory instead of
    // file size — the inconsistency §12.3 is about. Do not resolve it per family.
}

TargetBitmap
FmIndexReader::PatternMatch(std::string_view pattern, PatternOp op) const {
    // TODO: move existing logic here (see FMIndex.cpp:315-368):
    //   empty pattern      -> IsNotNull()                 (FMIndex.cpp:326-335)
    //   PrefixMatch        -> engine_.LocatePrefixDocs    (:339-341)
    //   PostfixMatch       -> engine_.LocateSuffixDocs    (:342-344)
    //   InnerMatch         -> engine_.VisitMatchingDocs   (:345-357)
    // The `default:` throw at :358-366 becomes unreachable-by-construction:
    // `Match` / `RegexMatch` are declined by `ShouldUsePattern`, so if one still
    // arrives that is a caller bug and an AssertInfo, not an `Unsupported`.
}

TargetBitmap
FmIndexReader::IsNull() const {
    // TODO: move existing logic here (see FMIndex.cpp:391-395).
}

TargetBitmap
FmIndexReader::IsNotNull() const {
    // TODO: move existing logic here (see FMIndex.cpp:397-403).
}

bool
FmIndexReader::ShouldUsePattern(std::string_view pattern, PatternOp op) const {
    // TODO: move existing logic here (see FMIndex.h:199-244), replacing the
    // `SegcoreConfig::default_config().get_fmindex_cost_ratio()` read at
    // FMIndex.h:226-228 with `cost_ratio_`. That single substitution is what
    // erases the `index/ -> segcore/` header edge (§10 rule 1).
}

int64_t
FmIndexReader::PatternCount(std::string_view pattern, PatternOp op) const {
    // TODO: move existing logic here (see FMIndex.h:279-294).
}

TargetBitmap
FmIndexReader::DocsToBitmap(const std::vector<uint64_t>& docs) const {
    // TODO: move existing logic here (see FMIndex.cpp:296-313).
}

}  // namespace milvus::index
