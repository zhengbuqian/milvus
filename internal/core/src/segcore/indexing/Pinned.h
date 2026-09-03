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

#include <any>
#include <utility>

// The RAII handle segcore hands out for a pinned index reader interface.
//
// See core_refactor/01-scalar-index.md §4.3 and core_refactor/README.md §5
// rule 4 (the cachinglayer propagation scope: `PinWrapper` / `CacheSlot` may
// appear only inside columnar-format and segcore; everything crossing outwards
// is an RAII handle `Pinned<T>` or a cursor).
//
// SHAPE (§4.3's table): "two pointers" — a type-erased keep-alive plus a typed
// raw pointer. The keep-alive is `std::any` on purpose: it holds the
// `CellAccessor` that the cache slot's `PinCells()` returned, but its TYPE does
// not appear in this header, so no consumer of `Pinned<ReaderT>` transitively
// includes cachinglayer.
//
// WHAT THIS IS NOT: it is not a per-query proxy object. §4.3 is explicit —
// "the query interface is not a proxy created per query, it IS an interface
// view of the index object itself". The implementation instance is created once
// by `Loader::Open()` and lives until cachinglayer evicts it. `Pinned<ReaderT>`
// is a stack RAII wrapper acquired ONCE PER EXPRESSION NODE (not per batch, not
// per row) that keeps that long-lived object resident and carries the
// already-cast reader pointer.

namespace milvus::segcore {

template <typename ReaderT>
class Pinned {
 public:
    Pinned() = default;

    Pinned(std::any keep_alive, const ReaderT* reader)
        : keep_alive_(std::move(keep_alive)), reader_(reader) {
    }

    // Empty means "no such reader interface on this field" — the caller falls
    // back to a column scan. §3 principle 3: absence is expressed by the type /
    // an empty handle, NEVER by `ThrowInfo(Unsupported)`.
    bool
    empty() const {
        return reader_ == nullptr;
    }

    explicit operator bool() const {
        return reader_ != nullptr;
    }

    const ReaderT*
    get() const {
        return reader_;
    }

    const ReaderT*
    operator->() const {
        return reader_;
    }

 private:
    std::any keep_alive_;
    const ReaderT* reader_{nullptr};
};

}  // namespace milvus::segcore
