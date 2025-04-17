#include <memory>

#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/EvictionManager.h"
#include "cachinglayer/cachinglayer_test_utils.h"

namespace milvus::cachinglayer::test {

class CacheSlotTest : public ::testing::Test {
 protected:
    void SetUp() override {
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
        eviction_manager_ = std::make_unique<EvictionManager>(1024 * 1024);  // 1MB limit
        translator_ = std::make_unique<MockTranslator>(10, "test_slot");
        cache_slot_ = std::make_unique<CacheSlot<MockCell>>(
            std::move(translator_), eviction_manager_.get());
    }

    void TearDown() override {
        cache_slot_.reset();
        eviction_manager_.reset();
        executor_.reset();
    }

    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::unique_ptr<EvictionManager> eviction_manager_;
    std::unique_ptr<MockTranslator> translator_;
    std::unique_ptr<CacheSlot<MockCell>> cache_slot_;
};

TEST_F(CacheSlotTest, BasicPinCells) {
    std::vector<uid_t> uids = {0, 1, 2};
    auto future = cache_slot_->PinCells(uids)
                      .via(executor_.get())
                      .get();

    ASSERT_NE(future, nullptr);
    auto accessor = std::move(future);

    // Test that we can access the cells
    for (uid_t uid : uids) {
        auto cid = translator_->cell_id_of(uid);
        auto cell = accessor->get_cell_of(uid);
        ASSERT_NE(cell, nullptr);
        EXPECT_EQ(cell->size(), 100);  // Default size from MockTranslator
    }
}

TEST_F(CacheSlotTest, InvalidUid) {
    std::vector<uid_t> uids = {100};  // This will map to cid 0 but is invalid
    auto future = cache_slot_->PinCells(uids.data(), uids.size())
                      .via(executor_.get())
                      .get();

    ASSERT_NE(future, nullptr);
    auto accessor = std::move(future);

    // Should still be able to access the cell even with invalid uid
    auto cell = accessor->get_cell_of(uids[0]);
    ASSERT_NE(cell, nullptr);
    EXPECT_EQ(cell->size(), 100);
}

TEST_F(CacheSlotTest, MultiplePins) {
    std::vector<uid_t> uids1 = {0, 1};
    std::vector<uid_t> uids2 = {1, 2};  // Overlap with uids1

    auto future1 = cache_slot_->PinCells(uids1.data(), uids1.size())
                       .via(executor_.get())
                       .get();
    auto future2 = cache_slot_->PinCells(uids2.data(), uids2.size())
                       .via(executor_.get())
                       .get();

    ASSERT_NE(future1, nullptr);
    ASSERT_NE(future2, nullptr);

    auto accessor1 = std::move(future1);
    auto accessor2 = std::move(future2);

    // Both accessors should be able to access the overlapping cell
    auto cell1 = accessor1->get_cell_of(1);
    auto cell2 = accessor2->get_cell_of(1);

    ASSERT_NE(cell1, nullptr);
    ASSERT_NE(cell2, nullptr);
    EXPECT_EQ(cell1->size(), cell2->size());
}

}  // namespace milvus::cachinglayer::test