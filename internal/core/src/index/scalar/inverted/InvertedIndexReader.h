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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "tantivy-wrapper.h"

namespace milvus::index {

class InvertedIndexDirectory;

template <typename Derived, bool Enabled>
class InvertedPatternReader {};

template <typename Derived>
class InvertedPatternReader<Derived, true> : public PatternMatchReader {
 public:
    bool
    ShouldUseForOp(PatternOp op, std::string_view pattern) const override {
        return static_cast<const Derived*>(this)->ShouldUseForOpImpl(op,
                                                                     pattern);
    }

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override {
        return static_cast<const Derived*>(this)->PatternMatchImpl(pattern, op);
    }
};

template <typename T>
class InvertedIndexReader final
    : public IndexReaderBase,
      public ScalarPredicateReader<T>,
      public InvertedPatternReader<InvertedIndexReader<T>,
                                   std::is_same_v<T, std::string_view>>,
      public NullReader {
 public:
    InvertedIndexReader(
        std::shared_ptr<InvertedIndexDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        DataType value_type,
        bool nested,
        bool mmap,
        size_t engine_bytes,
        size_t engine_path_bytes);

    ~InvertedIndexReader() override;

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    template <typename Derived, bool Enabled>
    friend class InvertedPatternReader;

    bool
    ShouldUseForOpImpl(PatternOp op, std::string_view pattern) const;

    TargetBitmap
    PatternMatchImpl(std::string_view pattern, PatternOp op) const;

    TargetBitmap
    PatternQuery(std::string_view pattern) const;

    // The engine is destroyed before the directory owner, so mapped files stay
    // alive through the Tantivy reader's entire lifetime.
    std::shared_ptr<InvertedIndexDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::shared_ptr<const std::vector<size_t>> null_offsets_;
    DataType value_type_{DataType::NONE};
    bool nested_{false};
    bool mmap_{false};
    // Exact staged file bytes on mmap, or the file payload bytes copied into
    // Tantivy's RamDirectory. Tantivy exposes no measurement for its reader,
    // hash-map, Arc, or allocator overhead; that narrow engine accounting gap
    // is not replaced with an admission estimate here.
    size_t engine_bytes_{0};
    // Heap bytes for the path string copied into TantivyIndexWrapper. The
    // wrapper exposes no accessor, so its source directory measures the copy
    // before heap-mode staging is released.
    size_t engine_path_bytes_{0};
    uint32_t count_{0};
};

}  // namespace milvus::index
