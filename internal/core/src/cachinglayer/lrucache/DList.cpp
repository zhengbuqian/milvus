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
#include "cachinglayer/lrucache/DList.h"

#include <mutex>
#include <vector>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "cachinglayer/Utils.h"
#include "cachinglayer/lrucache/ListNode.h"

namespace milvus::cachinglayer::internal {

bool
DList::reserveMemory(const ResourceUsage& size) {
    std::unique_lock<std::mutex> list_lock(list_mtx_);
    auto used = used_memory_.load();
    if (used + size <= max_memory_) {
        used_memory_ += size;
        return true;
    }
    auto size_to_evict = tryEvict(size);
    auto added = size - size_to_evict;
    used_memory_ += added;
    return true;
}


ResourceUsage
DList::tryEvict(const ResourceUsage& expected_slack) {
    auto current_slack = max_memory_ - used_memory_;

    std::vector<ListNode*> to_evict;
    // items are evicted because they are not used for a while, thus it should be ok to lock them
    // a little bit longer.
    std::vector<std::unique_lock<std::shared_mutex>> item_locks;

    ResourceUsage size_to_evict;

    // whether or not evicting this item would help. If we are short on memory, 
    // we evict only items that have memory usage.
    auto would_help = [&](const ResourceUsage& size) -> bool {
        auto need_memory = expected_slack.memory_bytes > (size_to_evict.memory_bytes + current_slack.memory_bytes);
        auto need_disk = expected_slack.file_bytes > (size_to_evict.file_bytes + current_slack.file_bytes);
        return (need_memory && size.memory_bytes > 0) || (need_disk && size.file_bytes > 0);
    };

    for (auto it = tail_; it != nullptr; it = it->prev_) {
        if (!would_help(it->size())) {
            continue;
        }
        // use try_to_lock to avoid dead lock by failing immediately if the ListNode lock is already held.
        auto& lock = item_locks.emplace_back(it->mtx_, std::try_to_lock);
        // if lock failed, it means this ListNode will be used again, so we don't evict it anymore.
        if (lock.owns_lock() && it->pin_count_ == 0) {
            to_evict.push_back(it);
            size_to_evict += it->size();
            if (current_slack + size_to_evict >= expected_slack) {
                break;
            }
        } else {
            // if we grabbed the lock only to find that the ListNode is pinned; or if we failed to lock
            // the ListNode, we do not evict this ListNode.
            item_locks.pop_back();
        }
    }
    if (current_slack + size_to_evict < expected_slack) {
        // eviction failed, insufficient memory even if we evicted all unpinned items.
        return ResourceUsage();
    }
    for (auto* list_node : to_evict) {
        list_node->clear_data();
        popItem(list_node);
    }
    return size_to_evict;
}

void
DList::UpdateLimit(const ResourceUsage& new_limit) {
    if (!new_limit.GreaterThanZero()) {
        throw std::invalid_argument("Milvus Caching Layer: memory and disk usage limit must be greater than 0");
    }
    std::unique_lock<std::mutex> list_lock(list_mtx_);
    auto used = used_memory_.load();
    if (new_limit < used) {
        // positive means amount owed
        auto deficit = used - new_limit;
        auto size_to_evict = tryEvict(deficit);
        AssertInfo(size_to_evict >= deficit, "Milvus Caching Layer: tryEvict returned less than expected");
        used_memory_ -= size_to_evict;
    }
    max_memory_ = new_limit;
    AssertInfo(used_memory_ <= max_memory_, "Milvus Caching Layer: used memory is greater than max memory");
}

void
DList::releaseMemoryWhenLoadFailed(const ResourceUsage& size) {
    // safe to substract on atomic without lock
    used_memory_ -= size;
}

void
DList::touchItem(ListNode* list_node) {
    std::lock_guard<std::mutex> list_lock(list_mtx_);
    popItem(list_node);
    pushHead(list_node);
}

void
DList::pushHead(ListNode* list_node) {
    if (head_ == nullptr) {
        head_ = list_node;
        tail_ = list_node;
    } else {
        list_node->prev_ = head_;
        head_->next_ = list_node;
        head_ = list_node;
    }
}

void
DList::popItem(ListNode* list_node) {
    if (list_node->prev_ == nullptr && list_node->next_ == nullptr &&
        list_node != head_) {
        // list_node is not in the list
        return;
    }
    if (head_ == tail_) {
        // Assert(head_ == list_node);
        head_ = tail_ = nullptr;
        list_node->prev_ = list_node->next_ = nullptr;
    } else if (head_ == list_node) {
        head_ = list_node->prev_;
        head_->next_ = nullptr;
        list_node->prev_ = nullptr;
    } else if (tail_ == list_node) {
        tail_ = list_node->next_;
        tail_->prev_ = nullptr;
        list_node->next_ = nullptr;
    } else {
        list_node->prev_->next_ = list_node->next_;
        list_node->next_->prev_ = list_node->prev_;
        list_node->prev_ = list_node->next_ = nullptr;
    }
}

bool
DList::IsEmpty() const {
    std::lock_guard<std::mutex> list_lock(list_mtx_);
    return head_ == nullptr;
}

}  // namespace milvus::cachinglayer::internal
