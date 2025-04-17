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

#include <memory>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/Utils.h"

namespace milvus::cachinglayer {

// TODO(tiered storage 2): add config system, include config for the overall caching layer, and for individual cache slots.
// TODO(tiered storage 2): 加 monitoring
// TODO(tiered storage 2): 综合管理资源用量，reserve需要reserve所有类型的资源
class Manager {
 public:
    static Manager&
    GetInstance();

    // This function is not thread safe, must be called before any CacheSlot is created.
    // Once created, CacheSlot is out of Manager's control, thus this function can't check
    // the above condition. Calling this function in such a case is UB.
    // TODO(tiered storage 2): 需要考虑是否需要支持动态更新，可以通过每一个cache slot销毁时的callback来实现感知是否为空。
    static void
    ConfigureTieredStorage(bool enabled_globally, int64_t memory_limit_bytes, int64_t disk_limit_bytes);

    Manager(const Manager&) = delete;
    Manager&
    operator=(const Manager&) = delete;
    Manager(Manager&&) = delete;
    Manager&
    operator=(Manager&&) = delete;

    ~Manager() = default;

    template <typename CellT>
    std::shared_ptr<CacheSlot<CellT>>
    CreateCacheSlot(std::unique_ptr<Translator<CellT>> translator) {
        return std::make_shared<CacheSlot<CellT>>(
            std::move(translator), dlist_.get());
    }

    // memory overhead for managing all cache slots/cells/translators/policies.
    size_t
    memory_overhead() const;

 private:
    friend void ConfigureTieredStorage(bool enabled_globally, int64_t memory_limit_bytes, int64_t disk_limit_bytes);

    Manager() = default;  // Private constructor

    std::unique_ptr<internal::DList> dlist_{nullptr};
    bool enable_global_tiered_storage_{false};
    int64_t memory_limit_bytes_{0};
    int64_t disk_limit_bytes_{0};
};  // class Manager

}  // namespace milvus::cachinglayer
