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

#include <cstddef>
#include <exception>
#include <memory>
#include <type_traits>
#include <vector>
#include <any>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/Synchronized.h>

#include "cachinglayer/EvictionManager.h"
#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Types.h"
#include "log/Log.h"

namespace milvus::cachinglayer {

template <typename CellT>
class CellAccessor;

// - The action of pinning cells is not started until the returned SemiFuture is scheduled on an executor.
// - Once the future is scheduled, CacheSlot must live until both the future is ready.
// - The returned CellAccessor stores a shared_ptr of CacheSlot, thus will keep CacheSlot alive.
template <typename CellT>
class CacheSlot final : public std::enable_shared_from_this<CacheSlot<CellT>> {
 public:
    // TODO(tiered storage 2): may want to request a different sizing method, and allow return a struct of usage of different resources.
    static_assert(
        std::is_same_v<size_t, decltype(std::declval<CellT>().CellByteSize())>,
        "CellT must have a CellByteSize() method that returns a size_t "
        "representing the memory consumption of the cell");

    // TODO(tiered storage 1): 添加配置：允许配置为总是加载所有cell且不evict
    CacheSlot(std::unique_ptr<Translator<CellT>> translator,
              EvictionManager* eviction_manager)
        : cells_(translator->num_cells()),
          translator_(std::move(translator)),
          em_(eviction_manager),
          load_delay_ms_(2) {
        // em_->register_slot(slot_id(), cells_.size());
        for (cid_t i = 0; i < translator_->num_cells(); ++i) {
            new (&cells_[i])
                CacheCell(this, i, translator_->estimated_byte_size_of_cell(i));
        }
    }

    CacheSlot(const CacheSlot&) = delete;
    CacheSlot&
    operator=(const CacheSlot&) = delete;
    CacheSlot(CacheSlot&&) = delete;
    CacheSlot&
    operator=(CacheSlot&&) = delete;

    folly::SemiFuture<std::shared_ptr<CellAccessor<CellT>>>
    PinCells(std::vector<uid_t> uids) {
        return folly::makeSemiFuture().deferValue([this,
                                                   uids =
                                                       std::vector<uid_t>(uids),
                                                   count =
                                                       uids.size()](auto&&) {
            BitsetType bitset(cells_.size());
            std::vector<cid_t> involved_cids;
            involved_cids.reserve(cells_.size());
            for (size_t i = 0; i < count; ++i) {
                auto uid = uids[i];
                auto cid = translator_->cell_id_of(uid);
                if (cid >= cells_.size()) {
                    return folly::makeSemiFuture<
                        std::shared_ptr<CellAccessor<CellT>>>(
                        folly::make_exception_wrapper<std::invalid_argument>(
                            fmt::format(
                                "CacheSlot {}: translator returned cell_id {} "
                                "for uid {} which is out of range",
                                translator_->key(),
                                cid,
                                uid)));
                }
                if (!bitset[cid]) {
                    bitset[cid] = true;
                    involved_cids.push_back(cid);
                }
            }
            // TODO(tiered storage 1): cell_[cid].pin() 需要返回一个 std::pair<bool/*need_loading*/, ListNode::NodePin>
            // 然后RunLoad() 需要根据 need_loading 来决定是否需要 load，而不是加延迟
            std::vector<folly::SemiFuture<internal::ListNode::NodePin>> futures;
            futures.reserve(involved_cids.size());
            for (auto cid : involved_cids) {
                futures.push_back(cells_[cid].pin());
            }
            return folly::collect(futures).deferValue(
                [this](auto&& pins) mutable {
                    // or else unpinning is done in CellAccessor's destructor.
                    return std::make_shared<CellAccessor<CellT>>(
                        this->shared_from_this(), std::move(pins));
                });
        });
    }

    size_t
    num_cells() const {
        return translator_->num_cells();
    }

    size_t
    size_of_cell(cid_t cid) const {
        return translator_->estimated_byte_size_of_cell(cid);
    }

    Meta*
    meta() {
        return translator_->meta();
    }

    ~CacheSlot() {
        // em_->unregister_slot(slot_id(), cells_.size());
    }

 private:
    friend class CellAccessor<CellT>;
    friend class EvictionManager;

    cid_t
    cell_id_of(uid_t uid) const {
        return translator_->cell_id_of(uid);
    }

    // When called, RunLoad uses translator to load all NOT_LOADED cells in load_queue_.
    folly::SemiFuture<folly::Unit>
    RunLoad() {
        return folly::makeSemiFuture().deferValue([this](auto&&)
                                                      -> folly::SemiFuture<
                                                          folly::Unit> {
            // TODO(tiered storage 4): should use folly::SemiFuture::delayed(std::chrono::milliseconds(load_delay_ms_)),
            // but this requires folly::Init, thus using std::this_thread::sleep_for for now.
            std::this_thread::sleep_for(
                std::chrono::milliseconds(load_delay_ms_));
            std::unique_ptr<folly::SharedPromise<folly::Unit>>
                batch_load_promise_ptr{nullptr};
            std::vector<cid_t> copy_load_queue;
            BitsetType bitset(cells_.size());

            {
                auto load_queue = load_queue_.wlock();
                AssertInfo(!load_queue->empty(), "Load queue is empty");
                copy_load_queue.assign(load_queue->begin(), load_queue->end());
                load_queue->clear();
                batch_load_promise_ptr = std::move(batch_load_promise_);
            }
            for (auto cid : copy_load_queue) {
                bitset[cid] = true;
            }
            // after unlock, other load requests can be scheduled.
            try {
                auto results = translator_->get_cells(copy_load_queue);
                for (auto& result : results) {
                    cells_[result.first].set_cell(std::move(result.second), bitset[result.first]);
                }
                batch_load_promise_ptr->setValue(folly::Unit());
            } catch (std::exception& e) {
                LOG_ERROR(
                    fmt::format("CacheSlot {}: Error loading cells, reason: {}",
                                translator_->key(),
                                e.what()));
                batch_load_promise_ptr->setException(e);
                return folly::makeSemiFuture<folly::Unit>(e);
            }
            return folly::Unit();
        });
    }

    struct CacheCell : internal::ListNode {
     public:
        CacheCell() = default;
        CacheCell(CacheSlot<CellT>* slot, cid_t cid, size_t size)
            : internal::ListNode(slot->em_->dlist()),
              slot_(slot),
              cid_(cid),
              size_(size) {
        }
        ~CacheCell() {
            if (state_ == State::LOADING) {
                LOG_ERROR("CacheSlot {} Cell id {} destroyed while loading",
                          slot_->translator_->key(),
                          cid_);
            }
        }

        CellT*
        cell() {
            return cell_.get();
        }

        // Be careful that even though only a single thread can request loading a cell,
        // it is still possible that multiple threads call set_cell() concurrently.
        // For example, 2 RunLoad() calls tries to download cell 4 and 6, and both decided
        // to also download cell 5, if they finished at the same time, they will call set_cell()
        // of cell 5 concurrently.
        void
        set_cell(std::unique_ptr<CellT> cell, bool requesting_thread) {
            // translator may return more than requested, thus we need insert those
            // extra cells into the dlist and mark them as LOADED.
            mark_loaded([this, cell = std::move(cell)]() mutable {
                cell_ = std::move(cell);
            }, requesting_thread);
        }

     protected:
        folly::SemiFuture<folly::Unit>
        load() override {
            auto load_queue = slot_->load_queue_.wlock();
            bool is_first = load_queue->empty();
            load_queue->push_back(cid_);
            if (!is_first) {
                return slot_->batch_load_promise_->getSemiFuture();
            }
            slot_->batch_load_promise_ =
                std::make_unique<folly::SharedPromise<folly::Unit>>();
            return slot_->RunLoad();
        }
        size_t
        size() override {
            return size_;
        }
        void
        unload() override {
            cell_ = nullptr;
        }
        const std::string&
        key() const override {
            return slot_->translator_->key();
        }
        const cid_t&
        cid() const override {
            return cid_;
        }

     private:
        CacheSlot<CellT>* slot_{nullptr};
        cid_t cid_{0};
        std::unique_ptr<CellT> cell_{nullptr};
        size_t size_{0};
    };
    // Each CacheCell's cid_t is its index in vector
    // Once initialized, cells_ should never be resized.
    std::vector<CacheCell> cells_;

    const std::unique_ptr<Translator<CellT>> translator_;

    // The first thread that adds a cid to load_queue_ will schedule a load in load_delay_ms_ and create a
    // batch_load_promise_. Access of batch_load_promise_ must be protected by wlock of load_queue_.
    folly::Synchronized<std::vector<cid_t>> load_queue_;
    const size_t load_delay_ms_;
    // Fulfilled when a batch load is finished.
    std::unique_ptr<folly::SharedPromise<folly::Unit>> batch_load_promise_{
        nullptr};

    // - CacheSlot is registered when created, and unregistered when destroyed.
    // - Each CellT is pinned when attempting to be loaded(no matter already loaded or not), and
    //   unpinned when: 1. load failed; 2. load succeeded and CellAccessor destroyed.
    // - Each CellT is marked as inserted when load succeeds.
    EvictionManager* em_;
};

// - A thin wrapper for accessing cells in a CacheSlot, does not own any logic of loading/eviction, etc.
// - When this class is created, the cells are loaded and pinned. Destroying this class will unpin the cells.
// - Accessing cells through this class does not incur any lock overhead.
// - Accessing cells that are not pinned by this CellAccessor is undefined behavior.
template <typename CellT>
class CellAccessor {
 public:
    CellAccessor(std::shared_ptr<CacheSlot<CellT>> slot,
                 std::vector<internal::ListNode::NodePin> pins)
        : slot_(std::move(slot)), pins_(std::move(pins)) {
    }

    CellT*
    get_cell_of(uid_t uid) {
        auto cid = slot_->cell_id_of(uid);
        return slot_->cells_[cid].cell();
    }

 private:
    std::vector<internal::ListNode::NodePin> pins_;
    std::shared_ptr<CacheSlot<CellT>> slot_;
};

// TODO(tiered storage 2): this class is a temp solution. Later we should modify all usage of this class
// to use folly::SemiFuture instead. Instead of early loading and pinning, we should delay the loading until
// the actual user of the content.
// Current impl requires the T type to be movable/copyable. Might want to relax this.
template <typename T>
class PinWrapper {
 public:
    PinWrapper() = default;
    PinWrapper(std::any raii, T&& content)
        : raii_(std::move(raii)), content_(std::move(content)) {
    }

    PinWrapper(std::any raii, const T& content)
        : raii_(std::move(raii)), content_(content) {
    }

    // For those that does not need a pin. eg: growing segment, views that actually copies the data, etc.
    PinWrapper(T&& content) : raii_(nullptr), content_(std::move(content)) {}
    PinWrapper(const T& content) : raii_(nullptr), content_(content) {}


    PinWrapper(PinWrapper&& other) noexcept
        : raii_(std::move(other.raii_)), content_(std::move(other.content_)) {
    }

    PinWrapper(const PinWrapper& other)
        : raii_(other.raii_), content_(other.content_) {
    }

    PinWrapper& operator=(PinWrapper&& other) noexcept {
        if (this != &other) {
            raii_ = std::move(other.raii_);
            content_ = std::move(other.content_);
        }
        return *this;
    }

    PinWrapper& operator=(const PinWrapper& other) {
        if (this != &other) {
            raii_ = other.raii_;
            content_ = other.content_;
        }
        return *this;
    }

    T&
    get() {
        return content_;
    }

    template <typename T2, typename Fn>
    PinWrapper<T2> transform(Fn&& transformer) && {
        T2 transformed = transformer(std::move(content_));
        return PinWrapper<T2>(std::move(raii_), std::move(transformed));
    }

 private:
    // CellAccessor is templated on CellT, we don't want to enforce that in this class.
    std::any raii_{nullptr};
    T content_;
};

}  // namespace milvus::cachinglayer
