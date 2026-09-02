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

#include "index/scalar/marisa/MarisaIndexReader.h"

#include <utility>

namespace milvus::index {

MarisaIndexReader::MarisaIndexReader(OpenArgs args) : data_(std::move(args)) {
}

MarisaIndexReader::~MarisaIndexReader() {
    // NOTE: `StringIndexMarisa`'s destructor (`.h:35-42`) munmaps two regions
    // it owns. Mapping is the loader's; whatever keeps the mapping alive is
    // handed to the reader as part of `OpenArgs` (§3 principle 6, §10 rule 2).
}

ReaderCaps
MarisaIndexReader::Caps() const {
    // `HasRawData()` is unconditionally true for marisa
    // (`StringIndexMarisa.h:118-121`) and reverse lookup is O(|value|) through
    // the trie, so both value bits are set.
    return ReaderCaps{.predicate = true,
                      .pattern_match = true,
                      .value_lookup = true,
                      .cheap_value_lookup = true};
}

Domain
MarisaIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
MarisaIndexReader::Count() const {
    return static_cast<int64_t>(data_.str_ids_size);
}

DataType
MarisaIndexReader::ValueType() const {
    return DataType::VARCHAR;
}

int64_t
MarisaIndexReader::MemoryUsage() const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:90-114
    // ComputeByteSize and :116-141 CalculateTotalSize).
}

ResourceUsage
MarisaIndexReader::CellByteSize() const {
    // §12.3 names marisa explicitly: a trie's serialized size and its resident
    // size differ substantially, and this family reports the FILE size today.
}

TargetBitmap
MarisaIndexReader::In(size_t n, const std::string_view* values) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:365-382).
}

TargetBitmap
MarisaIndexReader::NotIn(size_t n, const std::string_view* values) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:384-403).
}

TargetBitmap
MarisaIndexReader::Range(const std::string_view& value, CompareOp op) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:447-566,
    // both the lexicographic fast path and the slow path).
}

TargetBitmap
MarisaIndexReader::Range(const std::string_view& lo, bool lo_inc,
                         const std::string_view& hi, bool hi_inc) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:568-622).
}

std::optional<std::string>
MarisaIndexReader::Lookup(int64_t offset) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:799-811).
}

void
MarisaIndexReader::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const std::string_view*, bool valid)>&
        out) const {
    // NEW (§5.5). Sorting the offsets by key id first lets one agent walk the
    // trie once instead of once per row — the "cluster by internal layout"
    // reason this face is a callback.
}

TargetBitmap
MarisaIndexReader::PatternMatch(std::string_view pattern, PatternOp op) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:640-716), which
    // subsumes `PrefixMatch` (:624-638) — prefix is `PatternOp::PrefixMatch`,
    // not a separate entry point. The old separate method existed to serve
    // `StringIndex::Query(DatasetPtr)`'s prefix shortcut (StringIndex.h:36-44),
    // which is deleted with the dataset entry point.
}

TargetBitmap
MarisaIndexReader::IsNull() const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:405-411 and the
    // helper :413-421 SetNull).
}

TargetBitmap
MarisaIndexReader::IsNotNull() const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:434-445 and
    // :423-432 ResetNull).
}

size_t
MarisaIndexReader::LookupKeyId(std::string_view value) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:775-785).
}

std::vector<size_t>
MarisaIndexReader::PrefixMatchKeyIds(std::string_view prefix) const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:787-798).
}

bool
MarisaIndexReader::InLexicographicOrder() const {
    // TODO: move existing logic here (see StringIndexMarisa.cpp:813-823).
}

}  // namespace milvus::index
