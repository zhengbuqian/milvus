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
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <geos_c.h>

// The boost::geometry R-tree ENGINE, composed by the spatial family's builder
// and reader. Was `index/RTreeIndexWrapper.{h,cpp}`.
//
// See 01-scalar-index.md §3 principle 2: engines are composed, never inherited.
//
// WHY IT IS SPLIT IN TWO HERE. Today `RTreeIndexWrapper` takes a
// `bool is_build_mode` at construction and EVERY method asserts on it:
//   add_geometry              AssertInfo(is_build_mode_)   RTreeIndexWrapper.cpp:63
//   bulk_load_from_field_data AssertInfo(is_build_mode_)   RTreeIndexWrapper.cpp:112
//   finish                    AssertInfo(is_build_mode_)   RTreeIndexWrapper.cpp:183
//   load                      AssertInfo(!is_build_mode_)  RTreeIndexWrapper.cpp:224
// A runtime flag whose only job is to keep two disjoint method sets apart IS a
// builder/reader split that has not been written down. Writing it down deletes
// four assertions and makes the misuse unrepresentable.
//
// The proto enum is gone from both classes: `query_candidates` took
// `proto::plan::GISFunctionFilterExpr_GISOp` (`RTreeIndexWrapper.h:96`), which
// README §5 rule 2 forbids below the adapter layer.

namespace milvus::index {

namespace rtree_detail {
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

using Point = bg::model::point<double, 2, bg::cs::cartesian>;
using Box = bg::model::box<Point>;
// (minimum bounding rectangle, row offset)
using Value = std::pair<Box, int64_t>;
using RTree = bgi::rtree<Value, bgi::rstar<16>>;
}  // namespace rtree_detail

// Build side. One-shot, single-threaded from the builder's point of view.
class RTreeBuildEngine {
 public:
    explicit RTreeBuildEngine(std::string index_path);

    ~RTreeBuildEngine();

    // WKB in, MBR + row offset out. Rows whose WKB fails to parse are SKIPPED
    // while the offset still advances (RTreeIndexWrapper.cpp:134-151) — that
    // silent skip is load-bearing for offset alignment; keep it, and keep it
    // documented.
    void
    AddGeometry(const uint8_t* wkb, size_t len, int64_t row_offset);

    // Bulk packing algorithm; much faster than repeated insert
    // (RTreeIndexWrapper.cpp:165-166).
    void
    BulkLoad(std::vector<rtree_detail::Value> values);

    // Writes `<index_path>.bgi` and `<index_path>.meta.json`.
    void
    Finish();

    int64_t
    Count() const;

    const std::string&
    IndexPath() const;

 private:
    rtree_detail::RTree rtree_;
    std::vector<rtree_detail::Value> values_;
    std::string index_path_;
    bool finished_{false};
    uint32_t dimension_{2};
};

// Query side. Immutable after `Load`, concurrently readable — which is what
// §5 requires of everything a reader holds.
class RTreeQueryEngine {
 public:
    explicit RTreeQueryEngine(std::string index_path);

    ~RTreeQueryEngine();

    void
    Load();

    // MBR coarse filter. The exact relation is exec's job (§5.6).
    void
    QueryCandidates(const GEOSGeometry* query_geom,
                    GEOSContextHandle_t ctx,
                    std::vector<int64_t>& candidate_offsets) const;

    int64_t
    Count() const;

    int64_t
    ByteSize() const;

 private:
    rtree_detail::RTree rtree_;
    std::vector<rtree_detail::Value> values_;
    std::string index_path_;
    uint32_t dimension_{2};
};

}  // namespace milvus::index
