#pragma once

#include <gmock/gmock.h>

#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer/Utils.h"

namespace milvus::cachinglayer::internal {

class MockListNode : public ListNode {
public:
    MockListNode(DList* dlist,
                 ResourceUsage size,
                 const std::string& key = "mock_key",
                 cid_t cid = 0)
        : ListNode(dlist, size), mock_key_(key), mock_cid_(cid) {
        ON_CALL(*this, clear_data).WillByDefault([this]() {
             // Default clear_data calls unload() by default in base, mimic if needed
             unload();
             state_ = State::NOT_LOADED;
         });
    }

    MOCK_METHOD(void, clear_data, (), (override));

    const std::string& key() const override { return mock_key_; }
    const cid_t& cid() const override { return mock_cid_; }

    // Directly manipulate state for test setup (Use carefully!)
    void test_set_state(State new_state) {
         std::unique_lock lock(mtx_);
         state_ = new_state;
    }
    State test_get_state() {
         std::shared_lock lock(mtx_);
         return state_;
    }

    void test_set_pin_count(int count) {
        pin_count_.store(count);
    }
    int test_get_pin_count() const { return pin_count_.load(); }

    // Expose mutex for lock testing
    std::shared_mutex& test_get_mutex() { return mtx_; }

    ListNode* test_get_prev() const { return prev_; }
    ListNode* test_get_next() const { return next_; }

private:
    friend class DListTest;
    friend class DListTestFriend;
    std::string mock_key_;
    cid_t mock_cid_;
};

} // namespace milvus::cachinglayer::internal