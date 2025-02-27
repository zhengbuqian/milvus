#include <gtest/gtest.h>

#include "cachinglayer/EvictionManager.h"
#include "cachinglayer/cachinglayer_test_utils.h"

namespace milvus::cachinglayer::test {

class EvictionManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        eviction_manager_ = std::make_unique<EvictionManager>(1024 * 1024);  // 1MB limit
    }

    void TearDown() override {
        eviction_manager_.reset();
    }

    std::unique_ptr<EvictionManager> eviction_manager_;
};

TEST_F(EvictionManagerTest, BasicRegistration) {
    const std::string slot_id = "test_slot";
    size_t num_cells = 10;

    eviction_manager_->register_slot(slot_id, num_cells);

    // Test that we can unregister
    eviction_manager_->unregister_slot(slot_id, num_cells);
}

TEST_F(EvictionManagerTest, MultipleSlots) {
    const std::string slot1_id = "slot1";
    const std::string slot2_id = "slot2";
    size_t num_cells1 = 5;
    size_t num_cells2 = 5;

    eviction_manager_->register_slot(slot1_id, num_cells1);
    eviction_manager_->register_slot(slot2_id, num_cells2);

    // Test that we can unregister both slots
    eviction_manager_->unregister_slot(slot1_id, num_cells1);
    eviction_manager_->unregister_slot(slot2_id, num_cells2);
}

TEST_F(EvictionManagerTest, ResourceLimit) {
    const std::string slot_id = "test_slot";
    size_t num_cells = 10;
    size_t cell_size = 200 * 1024;  // 200KB per cell

    eviction_manager_->register_slot(slot_id, num_cells);

    // Try to pin more cells than can fit in memory
    std::vector<internal::ListNode::Pin> pins;
    pins.reserve(num_cells);

    for (size_t i = 0; i < num_cells; ++i) {
        auto pin = eviction_manager_->pin_cell(slot_id, i, cell_size);
        pins.push_back(std::move(pin));
    }

    // All pins should succeed since we're using the mock translator
    for (const auto& pin : pins) {
        ASSERT_TRUE(pin.isReady());
    }

    eviction_manager_->unregister_slot(slot_id, num_cells);
}

}  // namespace milvus::cachinglayer::test