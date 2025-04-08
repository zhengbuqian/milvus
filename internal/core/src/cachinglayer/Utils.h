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

#include <cstdint>

#include "folly/executors/InlineExecutor.h"
#include <folly/futures/Future.h>

namespace milvus::cachinglayer {

using uid_t = int64_t;
using cid_t = int64_t;

enum class StorageType {
    MEMORY = 0,  // including Anonymous mmap
    FILE_MMAP = 1,
    FILE = 2,

    // must be the last one, value will be the number of storage types
    COUNT,
};

// TODO(tiered storage 4): this is a temporary function to get the result of a future
// by running it on the inline executor.
template <typename T>
T
SemiInlineGet(folly::SemiFuture<T>&& future) {
    return std::move(future).via(&folly::InlineExecutor::instance()).get();
}

}  // namespace milvus::cachinglayer
