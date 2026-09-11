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

#include <memory>
#include <utility>

#include "cachinglayer/CacheSlot.h"
#include "index/contracts/query/IndexReader.h"

namespace milvus::segcore {

class IndexInventory;

// Retains one sealed cache accessor and exposes only the reader borrowed from
// its uniquely owned cell. Keep the pin alive while using reader-owned views.
class IndexPin {
 public:
    IndexPin() = default;

    IndexPin(const IndexPin&) = delete;
    IndexPin&
    operator=(const IndexPin&) = delete;

    IndexPin(IndexPin&& other) noexcept
        : accessor_(std::move(other.accessor_)),
          reader_(std::exchange(other.reader_, nullptr)) {
        other.accessor_.reset();
    }

    IndexPin&
    operator=(IndexPin&& other) noexcept {
        if (this != &other) {
            reader_ = nullptr;
            accessor_.reset();
            accessor_ = std::move(other.accessor_);
            reader_ = std::exchange(other.reader_, nullptr);
            other.accessor_.reset();
        }
        return *this;
    }

    // Empty means the entry or interface is absent, allowing a column
    // fallback. Cache loading failures are not absence and must propagate.
    bool
    empty() const {
        return reader_ == nullptr;
    }

    explicit operator bool() const {
        return reader_ != nullptr;
    }

    const index::IndexReaderBase*
    get() const {
        return reader_;
    }

    const index::IndexReaderBase*
    operator->() const {
        return reader_;
    }

    // Deferred iterators receive the same accessor control block. The reader
    // remains uniquely owned by its cache cell.
    std::shared_ptr<void>
    IntoSharedLifetime() && {
        reader_ = nullptr;
        auto accessor = std::move(accessor_);
        accessor_.reset();
        return std::shared_ptr<void>(std::move(accessor));
    }

 private:
    friend class IndexInventory;

    IndexPin(std::shared_ptr<
                 cachinglayer::CellAccessor<index::IndexReaderBase>> accessor,
             const index::IndexReaderBase* reader)
        : accessor_(std::move(accessor)), reader_(reader) {
    }

    std::shared_ptr<cachinglayer::CellAccessor<index::IndexReaderBase>>
        accessor_;
    const index::IndexReaderBase* reader_{nullptr};
};

}  // namespace milvus::segcore
