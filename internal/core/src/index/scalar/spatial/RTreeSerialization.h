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

#include <string>

#include <boost/geometry.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/serialization/vector.hpp>

// Boost-serialization of the R-tree structure. Was `RTreeIndexSerialization.h`.
//
// Two changes from the original, both deletions:
//
// 1. IT NOW LIVES IN `milvus::index`. The original declared `class
//    RTreeSerializer` in the GLOBAL namespace — a header in `index/` putting a
//    symbol at global scope.
//
// 2. FOUR OF THE SIX METHODS ARE GONE. `saveText` / `loadText` /
//    `serializeToString` / `deserializeFromString` had zero callers anywhere in
//    the repository; only `saveBinary` (RTreeIndexWrapper.cpp:188) and
//    `loadBinary` (:241) were ever used.
//
// The `bool` returns are kept for now but both call sites DISCARD them, which
// turns a serialization failure into a silent success (see RTreeEngine.cpp).
// Deciding whether these become `void`-plus-throw is part of fixing that; it is
// an error-classification decision at the construction site, so it is flagged
// where the discard happens rather than resolved here.

namespace milvus::index {

class RTreeSerializer {
 public:
    template <typename RTreeType>
    static bool
    saveBinary(const RTreeType& tree, const std::string& filename) {
        // TODO: move existing logic here (see RTreeIndexSerialization.h:36-56
        // in the pre-refactor tree).
        return false;
    }

    template <typename RTreeType>
    static bool
    loadBinary(RTreeType& tree, const std::string& filename) {
        // TODO: move existing logic here (see RTreeIndexSerialization.h:58-78
        // in the pre-refactor tree).
        return false;
    }
};

}  // namespace milvus::index
