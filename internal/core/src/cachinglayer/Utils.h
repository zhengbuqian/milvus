// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <atomic>
#include <cstdint>

#include "folly/executors/InlineExecutor.h"
#include <folly/futures/Future.h>

namespace milvus::cachinglayer {

using uid_t = int64_t;
using cid_t = int64_t;

// TODO(tiered storage 4): this is a temporary function to get the result of a future
// by running it on the inline executor. We don't need this once we are fully async.
template <typename T>
T
SemiInlineGet(folly::SemiFuture<T>&& future) {
    return std::move(future).via(&folly::InlineExecutor::instance()).get();
}

struct ResourceUsage {
    int64_t memory_bytes{0};
    int64_t file_bytes{0};

    ResourceUsage() noexcept : memory_bytes(0), file_bytes(0) {
    }
    ResourceUsage(int64_t mem, int64_t file) noexcept
        : memory_bytes(mem), file_bytes(file) {
    }

    ResourceUsage
    operator+(const ResourceUsage& rhs) const {
        return ResourceUsage(memory_bytes + rhs.memory_bytes,
                             file_bytes + rhs.file_bytes);
    }

    void operator+=(const ResourceUsage& rhs) {
        memory_bytes += rhs.memory_bytes;
        file_bytes += rhs.file_bytes;
    }

    ResourceUsage
    operator-(const ResourceUsage& rhs) const {
        return ResourceUsage(memory_bytes - rhs.memory_bytes,
                             file_bytes - rhs.file_bytes);
    }

    void operator-=(const ResourceUsage& rhs) {
        memory_bytes -= rhs.memory_bytes;
        file_bytes -= rhs.file_bytes;
    }

    bool
    operator==(const ResourceUsage& rhs) const {
        return memory_bytes == rhs.memory_bytes && file_bytes == rhs.file_bytes;
    }

    bool
    operator!=(const ResourceUsage& rhs) const {
        return !(*this == rhs);
    }
};

inline bool
operator>(const ResourceUsage& lhs, const ResourceUsage& rhs) {
    return lhs.memory_bytes > rhs.memory_bytes &&
           lhs.file_bytes > rhs.file_bytes;
}

inline bool
operator>=(const ResourceUsage& lhs, const ResourceUsage& rhs) {
    return lhs.memory_bytes >= rhs.memory_bytes &&
           lhs.file_bytes >= rhs.file_bytes;
}

inline bool
operator<(const ResourceUsage& lhs, const ResourceUsage& rhs) {
    return lhs.memory_bytes < rhs.memory_bytes &&
           lhs.file_bytes < rhs.file_bytes;
}

inline bool
operator<=(const ResourceUsage& lhs, const ResourceUsage& rhs) {
    return lhs.memory_bytes <= rhs.memory_bytes &&
           lhs.file_bytes <= rhs.file_bytes;
}

inline void operator+=(std::atomic<ResourceUsage>& atomic_lhs, const ResourceUsage& rhs) {
    ResourceUsage current = atomic_lhs.load();
    ResourceUsage new_value;
    do {
        new_value = current;
        new_value += rhs;
    } while (!atomic_lhs.compare_exchange_weak(current, new_value));
}

inline void operator-=(std::atomic<ResourceUsage>& atomic_lhs, const ResourceUsage& rhs) {
    ResourceUsage current = atomic_lhs.load();
    ResourceUsage new_value;
    do {
        new_value = current;
        new_value -= rhs;
    } while (!atomic_lhs.compare_exchange_weak(current, new_value));
}

}  // namespace milvus::cachinglayer
