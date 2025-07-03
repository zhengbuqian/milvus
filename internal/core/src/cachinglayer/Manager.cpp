// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License
#include "cachinglayer/Manager.h"

#include <memory>
#include <shared_mutex>
#include <unordered_set>

#include "cachinglayer/CacheSlotBase.h"
#include "cachinglayer/Utils.h"
#include "log/Log.h"
#include "pb/cgo_msg.pb.h"

namespace milvus::cachinglayer {

Manager&
Manager::GetInstance() {
    static Manager instance;
    return instance;
}

void
Manager::ConfigureTieredStorage(CacheWarmupPolicies warmup_policies,
                                CacheLimit cache_limit,
                                bool evictionEnabled,
                                EvictionConfig eviction_config) {
    static std::once_flag once;
    std::call_once(once, [&]() {
        Manager& manager = GetInstance();
        manager.warmup_policies_ = warmup_policies;
        manager.cache_limit_ = cache_limit;
        manager.evictionEnabled_ = evictionEnabled;

        if (!evictionEnabled) {
            LOG_INFO(
                "Tiered Storage manager is configured with disabled eviction");
            return;
        }

        ResourceUsage max{cache_limit.memory_max_bytes,
                          cache_limit.disk_max_bytes};
        ResourceUsage low_watermark{cache_limit.memory_low_watermark_bytes,
                                    cache_limit.disk_low_watermark_bytes};
        ResourceUsage high_watermark{cache_limit.memory_high_watermark_bytes,
                                     cache_limit.disk_high_watermark_bytes};

        AssertInfo(
            low_watermark.GEZero(),
            "Milvus Caching Layer: low watermark must be greater than 0");
        AssertInfo((high_watermark - low_watermark).GEZero(),
                   "Milvus Caching Layer: high watermark must be greater than "
                   "low watermark");
        AssertInfo(
            (max - high_watermark).GEZero(),
            "Milvus Caching Layer: max must be greater than high watermark");

        manager.dlist_ = std::make_unique<internal::DList>(
            max, low_watermark, high_watermark, eviction_config);

        LOG_INFO(
            "Configured Tiered Storage manager with memory watermark: low {} "
            "bytes ({:.2} GB), high {} bytes ({:.2} GB), max {} bytes "
            "({:.2} GB), disk watermark: low "
            "{} bytes ({:.2} GB), high {} bytes ({:.2} GB), max {} bytes "
            "({:.2} GB), cache touch "
            "window: {} ms, eviction interval: {} ms",
            low_watermark.memory_bytes,
            low_watermark.memory_bytes / (1024.0 * 1024.0 * 1024.0),
            high_watermark.memory_bytes,
            high_watermark.memory_bytes / (1024.0 * 1024.0 * 1024.0),
            max.memory_bytes,
            max.memory_bytes / (1024.0 * 1024.0 * 1024.0),
            low_watermark.file_bytes,
            low_watermark.file_bytes / (1024.0 * 1024.0 * 1024.0),
            high_watermark.file_bytes,
            high_watermark.file_bytes / (1024.0 * 1024.0 * 1024.0),
            max.file_bytes,
            max.file_bytes / (1024.0 * 1024.0 * 1024.0),
            eviction_config.cache_touch_window.count(),
            eviction_config.eviction_interval.count());
    });
}

size_t
Manager::memory_overhead() const {
    // TODO(tiered storage 2): calculate memory overhead
    return 0;
}

void
Manager::registerCacheSlot(const std::string& key, std::weak_ptr<CacheSlotBase> slot) {
    std::unique_lock<std::shared_mutex> lock(slots_mutex_);
    registered_slots_[key] = slot;
}

void
Manager::unregisterCacheSlot(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(slots_mutex_);
    registered_slots_.erase(key);
}

milvus::proto::cgo::SegmentListResponse
Manager::listSegments() const {
    std::shared_lock<std::shared_mutex> lock(slots_mutex_);
    std::unordered_set<int64_t> segment_ids;
    
    for (const auto& [key, slot] : registered_slots_) {
        if (auto locked_slot = slot.lock()) {
            int64_t seg_id = extractSegmentId(key);
            if (seg_id != -1) {
                segment_ids.insert(seg_id);
            }
        }
    }
    
    milvus::proto::cgo::SegmentListResponse response;
    for (auto seg_id : segment_ids) {
        response.add_segment_ids(seg_id);
    }
    return response;
}

milvus::proto::cgo::CacheSlotListResponse
Manager::getCacheSlotsBySegmentId(int64_t segment_id) const {
    std::shared_lock<std::shared_mutex> lock(slots_mutex_);
    milvus::proto::cgo::CacheSlotListResponse response;
    
    for (const auto& [key, slot] : registered_slots_) {
        if (auto locked_slot = slot.lock()) {
            int64_t seg_id = extractSegmentId(key);
            if (seg_id == segment_id) {
                auto slot_info = locked_slot->get_usage_proto();
                *response.add_cache_slots() = slot_info;
            }
        }
    }
    
    return response;
}

const std::function<void(const std::string&)>&
Manager::getUnregisterFunction() {
    static const std::function<void(const std::string&)> unregister_fn = 
        [](const std::string& key) {
            Manager::GetInstance().unregisterCacheSlot(key);
        };
    return unregister_fn;
}

// Global function to get unregister function (for CacheSlot.h)
const std::function<void(const std::string&)>&
getManagerUnregisterFunction() {
    return Manager::getUnregisterFunction();
}


}  // namespace milvus::cachinglayer
