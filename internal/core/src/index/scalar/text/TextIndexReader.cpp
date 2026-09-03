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

#include "index/scalar/text/TextIndexReader.h"

namespace milvus::index {

TextIndexReader::TextIndexReader(
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    int64_t count,
    bool mmap_enabled)
    : engine_(std::move(engine)), count_(count), mmap_enabled_(mmap_enabled) {
    // TODO: move existing logic here (see TextMatchIndex.cpp:370 CreateReader,
    // which today installs the set-bitset callback on the wrapper).
}

TextIndexReader::~TextIndexReader() = default;

ReaderCaps
TextIndexReader::Caps() const {
    // Must equal what `TextIndexLoader::DeriveCaps` computed from metadata —
    // that equality is the §4.1 consistency assertion, not a lookup path.
    return ReaderCaps{.text_match = true};
}

Domain
TextIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
TextIndexReader::Count() const {
    return count_;
}

DataType
TextIndexReader::ValueType() const {
    return DataType::VARCHAR;
}

int64_t
TextIndexReader::MemoryUsage() const {
    // TODO: move existing logic here (see InvertedIndexTantivy's
    // ComputeByteSize; the engine reports `index_size_bytes()`).
}

ResourceUsage
TextIndexReader::CellByteSize() const {
    // TODO: move existing logic here (see TextMatchIndex.h:119-152
    // `TextMatchIndexHolder`, which charges the cache with
    // `ResourceUsage(ByteSize(), 0)` or `(0, ByteSize())` depending on mmap).
    //
    // NOTE — THE HOLDER ITSELF DOES NOT COME ALONG. `TextMatchIndexHolder`
    // calls `cachinglayer::Manager::ChargeLoadedResource` from inside index/,
    // which §10 rule 5 forbids (accounting and pinning are the load-side
    // translator's). The reader only REPORTS its size; segcore charges it.
    //
    // OPEN QUESTION §12.3: text and FMIndex report MEASURED RESIDENT MEMORY
    // here while every other family reports the UNCOMPRESSED FILE SIZE. The
    // unit of this number is undefined today and must be defined before the
    // artifact pipeline sinks to L1. Do not "fix" it locally per family.
}

TargetBitmap
TextIndexReader::PrepareBitset() const {
    // TODO: move existing logic here (see TextMatchIndex.cpp:383-391).
}

TargetBitmap
TextIndexReader::MatchQuery(std::string_view query,
                            uint32_t min_should_match) const {
    // TODO: move existing logic here (see TextMatchIndex.cpp:392-400).
    // Signature change: `const std::string&` -> `std::string_view` (§5.1's
    // view decision applies to every INPUT side).
}

TargetBitmap
TextIndexReader::PhraseMatchQuery(std::string_view query, uint32_t slop) const {
    // TODO: move existing logic here (see TextMatchIndex.cpp:401-409).
}

TargetBitmap
TextIndexReader::FuzzyMatchQuery(std::string_view query,
                                 uint32_t max_edit_distance) const {
    // TODO: move existing logic here (see TextMatchIndex.cpp:410-419).
}

}  // namespace milvus::index
