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
#include "cachinglayer/lrucache/ListNode.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include <fmt/core.h>
#include <folly/ExceptionWrapper.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "cachinglayer/lrucache/DList.h"
#include "common/EasyAssert.h"

namespace milvus::cachinglayer::internal {

ListNode::Pin::Pin(ListNode* node) : node_(node) {
}

ListNode::Pin::~Pin() {
    if (node_) {
        node_->unpin();
    }
}

ListNode::Pin::Pin(Pin&& other) : Pin(other.node_) {
    other.node_ = nullptr;
}

ListNode::Pin&
ListNode::Pin::operator=(Pin&& other) {
    node_ = other.node_;
    other.node_ = nullptr;
    return *this;
}

ListNode::ListNode(DList* dlist)
    : last_touch_(std::chrono::steady_clock::now() -
                  2 * dlist->touch_config_.refresh_window),
      dlist_(dlist) {
}

ListNode::~ListNode() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dlist_->popItem(this);
}

folly::SemiFuture<ListNode::Pin>
ListNode::pin() {
    return folly::makeSemiFuture().deferValue([this](auto&&) {
        // must be called with lock acquired, and state must not be NOT_LOADED.
        auto read_op = [this]() -> folly::SemiFuture<Pin> {
            AssertInfo(
                state_ != State::NOT_LOADED,
                "Programming error: read_op called on a {} cell",
                state_to_string(state_));
            if (state_ == State::ERROR) {
                return folly::makeSemiFuture<ListNode::Pin>(error_);
            }
            pin_count_++;
            if (state_ == State::LOADED) {
                return Pin(this);
            }
            return load_promise_->getSemiFuture().deferValue(
                [this](auto&&) { return Pin(this); });
        };
        {
            std::shared_lock<std::shared_mutex> lock(mtx_);
            if (state_ != State::NOT_LOADED) {
                return read_op();
            }
        }
        std::unique_lock<std::shared_mutex> lock(mtx_);
        if (state_ != State::NOT_LOADED) {
            return read_op();
        }
        // need to load.
        load_promise_ = std::make_unique<folly::SharedPromise<folly::Unit>>();
        state_ = State::LOADING;
        lock.unlock();

        if (!dlist_->reserveMemory(size())) {
            lock.lock();
            state_ = State::ERROR;
            // TODO(tiered storage 2): better error handling.
            error_ = folly::make_exception_wrapper<std::runtime_error>(
                fmt::format("Failed to load {}:{} due to insufficient resource",
                            key(),
                            cid()));
            load_promise_->setException(error_);
            load_promise_ = nullptr;
            return folly::makeSemiFuture<Pin>(error_);
        }

        return load()
            .deferValue([this](auto&&) {
                return folly::makeSemiFuture<Pin>(this);
            })
            .deferError([this](folly::exception_wrapper&& e) {
                dlist_->releaseMemoryWhenLoadFailed(size());
                std::unique_lock<std::shared_mutex> lock(mtx_);
                state_ = State::ERROR;
                error_ = folly::make_exception_wrapper<std::runtime_error>(
                    fmt::format("Failed to load {}:{} due to error: {}",
                                key(),
                                cid(),
                                e.what()));
                load_promise_->setException(error_);
                load_promise_ = nullptr;
                return folly::makeSemiFuture<Pin>(error_);
            });
    });
}

std::string
ListNode::state_to_string(State state) {
    switch (state) {
        case State::NOT_LOADED: return "NOT_LOADED";
        case State::LOADING: return "LOADING";
        case State::LOADED: return "LOADED";
        case State::ERROR: return "ERROR";
    }
    throw std::invalid_argument("Invalid state");
}

void
ListNode::unpin() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    AssertInfo(state_ == State::LOADED || state_ == State::ERROR,
               "Programming error: unpin() called on a {} cell",
               state_to_string(state_));
    if (state_ == State::ERROR) {
        return;
    }
    if (pin_count_.fetch_sub(1) == 1) {
        touch();
    }
}

void
ListNode::touch() {
    auto now = std::chrono::steady_clock::now();
    if (now - last_touch_ > dlist_->touch_config_.refresh_window) {
        dlist_->touchItem(this);
        last_touch_ = now;
    }
}

void
ListNode::clear_data() {
    unload();
    state_ = State::NOT_LOADED;
}

}  // namespace milvus::cachinglayer::internal
