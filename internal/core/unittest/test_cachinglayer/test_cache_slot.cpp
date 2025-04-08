#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/futures/Future.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>
#include <utility>
#include <unordered_map>
#include <unordered_set>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer_test_utils.h"

using namespace milvus::cachinglayer;
using namespace milvus::cachinglayer::internal;
using cl_uid_t = milvus::cachinglayer::uid_t;

struct TestCell {
    int data;
    cid_t cid;

    TestCell(int d, cid_t id) : data(d), cid(id) {
    }

    size_t
    CellByteSize() const {
        return sizeof(data) + sizeof(cid);
    }
};

class MockTranslator : public Translator<TestCell> {
 public:
    MockTranslator(std::vector<std::pair<cid_t, int64_t>> cell_sizes,
                   std::unordered_map<cl_uid_t, cid_t> uid_to_cid_map,
                   const std::string& key,
                   StorageType storage_type)
        : uid_to_cid_map_(std::move(uid_to_cid_map)),
          key_(key),
          meta_(storage_type) {
        cid_set_.reserve(cell_sizes.size());
        cell_sizes_.reserve(cell_sizes.size());
        for (const auto& pair : cell_sizes) {
            cid_t cid = pair.first;
            int64_t size = pair.second;
            cid_set_.insert(cid);
            cell_sizes_[cid] = size;
            cid_load_delay_ms_[cid] = 0;
        }
        num_unique_cids_ = cid_set_.size();
    }

    size_t
    num_cells() const override {
        return num_unique_cids_;
    }

    cid_t
    cell_id_of(cl_uid_t uid) const override {
        auto it = uid_to_cid_map_.find(uid);
        if (it != uid_to_cid_map_.end()) {
            if (cid_set_.count(it->second)) {
                return it->second;
            }
        }
        return static_cast<cid_t>(num_unique_cids_);
    }

    ResourceUsage
    estimated_byte_size_of_cell(cid_t cid) const override {
        auto it = cell_sizes_.find(cid);
        if (it != cell_sizes_.end()) {
            return ResourceUsage{it->second, 0};
        }
        return ResourceUsage{1, 0};
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<TestCell>>>
    get_cells(const std::vector<cid_t>& cids) override {
        get_cells_call_count_++;
        requested_cids_.push_back(cids);

        if (load_should_throw_) {
            throw std::runtime_error("Simulated load error");
        }

        if (load_delay_ms_ > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(load_delay_ms_));
        }

        std::vector<std::pair<cid_t, std::unique_ptr<TestCell>>> result;
        for (cid_t cid : cids) {
            auto delay_it = cid_load_delay_ms_.find(cid);
            if (delay_it != cid_load_delay_ms_.end() && delay_it->second > 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(delay_it->second));
            }

            result.emplace_back(
                cid,
                std::make_unique<TestCell>(static_cast<int>(cid * 10), cid));
        }
        for (cid_t extra_cid : return_extra_cids_) {
            bool already_present = false;
            for (const auto& p : result) {
                if (p.first == extra_cid) {
                    already_present = true;
                    break;
                }
            }
            if (!already_present) {
                result.emplace_back(
                    extra_cid,
                    std::make_unique<TestCell>(static_cast<int>(extra_cid * 10),
                                               extra_cid));
            }
        }
        return result;
    }

    void
    SetLoadDelay(int ms) {
        load_delay_ms_ = ms;
    }
    void
    SetCidLoadDelay(const std::unordered_map<cid_t, int>& delays) {
        for (const auto& pair : delays) {
            cid_load_delay_ms_[pair.first] = pair.second;
        }
    }
    void
    SetShouldThrow(bool should_throw) {
        load_should_throw_ = should_throw;
    }
    void
    SetExtraReturnCids(const std::vector<cid_t>& cids) {
        return_extra_cids_ = cids;
    }
    int
    GetCellsCallCount() const {
        return get_cells_call_count_;
    }
    const std::vector<std::vector<cid_t>>&
    GetRequestedCids() const {
        return requested_cids_;
    }
    void
    ResetCounters() {
        get_cells_call_count_ = 0;
        requested_cids_.clear();
    }

 private:
    std::unordered_map<cl_uid_t, cid_t> uid_to_cid_map_;
    std::unordered_map<cid_t, int64_t> cell_sizes_;
    std::unordered_set<cid_t> cid_set_;
    size_t num_unique_cids_;
    std::string key_;
    Meta meta_;

    int load_delay_ms_ = 0;
    std::unordered_map<cid_t, int> cid_load_delay_ms_;
    bool load_should_throw_ = false;
    std::vector<cid_t> return_extra_cids_;
    std::atomic<int> get_cells_call_count_ = 0;
    std::vector<std::vector<cid_t>> requested_cids_;
};

class CacheSlotTest : public ::testing::Test {
 protected:
    std::unique_ptr<DList> dlist_;
    MockTranslator* translator_ = nullptr;
    std::shared_ptr<CacheSlot<TestCell>> cache_slot_;

    std::vector<std::pair<cid_t, int64_t>> cell_sizes_ = {
        {0, 50}, {1, 150}, {2, 100}, {3, 200}, {4, 75}};
    std::unordered_map<cl_uid_t, cid_t> uid_to_cid_map_ = {{10, 0},
                                                           {11, 0},
                                                           {20, 1},
                                                           {30, 2},
                                                           {31, 2},
                                                           {32, 2},
                                                           {40, 3},
                                                           {50, 4},
                                                           {51, 4}};

    size_t NUM_UNIQUE_CIDS = 5;
    int64_t TOTAL_CELL_SIZE_BYTES = 50 + 150 + 100 + 200 + 75;
    int64_t MEMORY_LIMIT = TOTAL_CELL_SIZE_BYTES * 2;
    static constexpr int64_t DISK_LIMIT = 0;
    const std::string SLOT_KEY = "test_slot";

    void
    SetUp() override {
        dlist_ = std::make_unique<DList>(
            ResourceUsage{MEMORY_LIMIT, DISK_LIMIT}, DList::TouchConfig{});

        auto temp_translator_uptr = std::make_unique<MockTranslator>(
            cell_sizes_, uid_to_cid_map_, SLOT_KEY, StorageType::MEMORY);
        translator_ = temp_translator_uptr.get();
        cache_slot_ = std::make_shared<CacheSlot<TestCell>>(
            std::move(temp_translator_uptr), dlist_.get());
    }

    void
    TearDown() override {
        cache_slot_.reset();
        dlist_.reset();
    }
};

TEST_F(CacheSlotTest, Initialization) {
    ASSERT_EQ(cache_slot_->num_cells(), NUM_UNIQUE_CIDS);
}

TEST_F(CacheSlotTest, PinSingleCellSuccess) {
    cl_uid_t target_uid = 30;
    cid_t expected_cid = 2;
    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(expected_cid);

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells({target_uid});
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0].size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* cell = accessor->get_cell_of(target_uid);
    ASSERT_NE(cell, nullptr);
    EXPECT_EQ(cell->cid, expected_cid);
    EXPECT_EQ(cell->data, expected_cid * 10);

    TestCell* cell_by_index = accessor->get_ith_cell(expected_cid);
    ASSERT_EQ(cell, cell_by_index);
}

TEST_F(CacheSlotTest, PinMultipleCellsSuccess) {
    std::vector<cl_uid_t> target_uids = {10, 40, 51};
    std::vector<cid_t> expected_cids = {0, 3, 4};
    std::sort(expected_cids.begin(), expected_cids.end());
    ResourceUsage expected_total_size;
    for (cid_t cid : expected_cids) {
        expected_total_size += translator_->estimated_byte_size_of_cell(cid);
    }

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells(target_uids);
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    auto requested = translator_->GetRequestedCids()[0];
    std::sort(requested.begin(), requested.end());
    ASSERT_EQ(requested.size(), expected_cids.size());
    EXPECT_EQ(requested, expected_cids);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_total_size);

    for (cl_uid_t uid : target_uids) {
        cid_t cid = uid_to_cid_map_.at(uid);
        TestCell* cell = accessor->get_cell_of(uid);
        ASSERT_NE(cell, nullptr);
        EXPECT_EQ(cell->cid, cid);
        EXPECT_EQ(cell->data, cid * 10);
    }
}

TEST_F(CacheSlotTest, PinMultipleUidsMappingToSameCid) {
    std::vector<cl_uid_t> target_uids = {30, 50, 31, 51, 32};
    std::vector<cid_t> expected_unique_cids = {2, 4};
    std::sort(expected_unique_cids.begin(), expected_unique_cids.end());
    ResourceUsage expected_total_size;
    for (cid_t cid : expected_unique_cids) {
        expected_total_size += translator_->estimated_byte_size_of_cell(cid);
    }

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells(target_uids);
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    auto requested = translator_->GetRequestedCids()[0];
    std::sort(requested.begin(), requested.end());
    ASSERT_EQ(requested.size(), expected_unique_cids.size());
    EXPECT_EQ(requested, expected_unique_cids);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_total_size);

    TestCell* cell2_uid30 = accessor->get_cell_of(30);
    TestCell* cell2_uid31 = accessor->get_cell_of(31);
    TestCell* cell4_uid50 = accessor->get_cell_of(50);
    TestCell* cell4_uid51 = accessor->get_cell_of(51);
    ASSERT_NE(cell2_uid30, nullptr);
    ASSERT_NE(cell4_uid50, nullptr);
    EXPECT_EQ(cell2_uid30->cid, 2);
    EXPECT_EQ(cell4_uid50->cid, 4);
    EXPECT_EQ(cell2_uid30, cell2_uid31);
    EXPECT_EQ(cell4_uid50, cell4_uid51);
}

TEST_F(CacheSlotTest, PinInvalidUid) {
    cl_uid_t invalid_uid = 999;
    cl_uid_t valid_uid = 10;
    std::vector<cl_uid_t> target_uids = {valid_uid, invalid_uid};

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells(target_uids);

    EXPECT_THROW(
        {
            try {
                SemiInlineGet(std::move(future));
            } catch (const std::invalid_argument& e) {
                std::string error_what = e.what();
                EXPECT_TRUE(error_what.find("out of range") !=
                                std::string::npos ||
                            error_what.find("invalid") != std::string::npos);
                throw;
            }
        },
        std::invalid_argument);

    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
}

TEST_F(CacheSlotTest, LoadFailure) {
    cl_uid_t target_uid = 20;
    cid_t expected_cid = 1;

    translator_->ResetCounters();
    translator_->SetShouldThrow(true);

    auto future = cache_slot_->PinCells({target_uid});

    EXPECT_THROW(
        {
            try {
                SemiInlineGet(std::move(future));
            } catch (const std::runtime_error& e) {
                std::string error_what = e.what();
                EXPECT_TRUE(error_what.find("Simulated load error") !=
                                std::string::npos ||
                            error_what.find("Failed to load") !=
                                std::string::npos ||
                            error_what.find("Exception during Future") !=
                                std::string::npos);
                throw;
            }
        },
        std::runtime_error);

    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0].size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), ResourceUsage{});
}

TEST_F(CacheSlotTest, PinAlreadyLoadedCell) {
    cl_uid_t target_uid = 40;
    cid_t expected_cid = 3;
    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(expected_cid);

    translator_->ResetCounters();

    auto future1 = cache_slot_->PinCells({target_uid});
    auto accessor1 = SemiInlineGet(std::move(future1));
    ASSERT_NE(accessor1, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell1 = accessor1->get_cell_of(target_uid);
    ASSERT_NE(cell1, nullptr);

    translator_->ResetCounters();
    auto future2 = cache_slot_->PinCells({target_uid});
    auto accessor2 = SemiInlineGet(std::move(future2));
    ASSERT_NE(accessor2, nullptr);

    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* cell2 = accessor2->get_cell_of(target_uid);
    ASSERT_NE(cell2, nullptr);
    EXPECT_EQ(cell1, cell2);

    accessor1.reset();
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell_after_unpin = accessor2->get_cell_of(target_uid);
    ASSERT_NE(cell_after_unpin, nullptr);
    EXPECT_EQ(cell_after_unpin, cell2);
}

TEST_F(CacheSlotTest, PinAlreadyLoadedCellViaDifferentUid) {
    cl_uid_t uid1 = 30;
    cl_uid_t uid2 = 31;
    cid_t expected_cid = 2;
    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(expected_cid);

    translator_->ResetCounters();

    auto future1 = cache_slot_->PinCells({uid1});
    auto accessor1 = SemiInlineGet(std::move(future1));
    ASSERT_NE(accessor1, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell1 = accessor1->get_cell_of(uid1);
    ASSERT_NE(cell1, nullptr);
    EXPECT_EQ(cell1->cid, expected_cid);

    translator_->ResetCounters();
    auto future2 = cache_slot_->PinCells({uid2});
    auto accessor2 = SemiInlineGet(std::move(future2));
    ASSERT_NE(accessor2, nullptr);

    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* cell2 = accessor2->get_cell_of(uid2);
    ASSERT_NE(cell2, nullptr);
    EXPECT_EQ(cell2->cid, expected_cid);
    EXPECT_EQ(cell1, cell2);

    accessor1.reset();
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell_after_unpin_uid1 = accessor2->get_cell_of(uid1);
    TestCell* cell_after_unpin_uid2 = accessor2->get_cell_of(uid2);
    ASSERT_NE(cell_after_unpin_uid1, nullptr);
    ASSERT_NE(cell_after_unpin_uid2, nullptr);
    EXPECT_EQ(cell_after_unpin_uid1, cell2);
    EXPECT_EQ(cell_after_unpin_uid2, cell2);
}

TEST_F(CacheSlotTest, TranslatorReturnsExtraCells) {
    cl_uid_t requested_uid = 10;
    cid_t requested_cid = 0;
    cid_t extra_cid = 1;
    cl_uid_t extra_uid = 20;

    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(requested_cid) +
        translator_->estimated_byte_size_of_cell(extra_cid);

    translator_->ResetCounters();
    translator_->SetExtraReturnCids({extra_cid});

    auto future = cache_slot_->PinCells({requested_uid});
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0],
              std::vector<cid_t>{requested_cid});
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* requested_cell = accessor->get_cell_of(requested_uid);
    ASSERT_NE(requested_cell, nullptr);
    EXPECT_EQ(requested_cell->cid, requested_cid);

    translator_->ResetCounters();
    auto future_extra = cache_slot_->PinCells({extra_uid});
    auto accessor_extra = SemiInlineGet(std::move(future_extra));

    ASSERT_NE(accessor_extra, nullptr);
    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* extra_cell = accessor_extra->get_cell_of(extra_uid);
    ASSERT_NE(extra_cell, nullptr);
    EXPECT_EQ(extra_cell->cid, extra_cid);
}

TEST_F(CacheSlotTest, EvictionTest) {
    // Sizes: 0:50, 1:150, 2:100, 3:200
    ResourceUsage NEW_LIMIT = ResourceUsage(300, 0);
    dlist_->UpdateLimit(NEW_LIMIT);
    EXPECT_EQ(DListTestFriend::get_max_memory(*dlist_), NEW_LIMIT);

    std::vector<cl_uid_t> uids_012 = {10, 20, 30};
    std::vector<cid_t> cids_012 = {0, 1, 2};
    ResourceUsage size_012 = translator_->estimated_byte_size_of_cell(0) +
                             translator_->estimated_byte_size_of_cell(1) +
                             translator_->estimated_byte_size_of_cell(2);
    ASSERT_EQ(size_012, ResourceUsage(50 + 150 + 100, 0));

    // 1. Load cells 0, 1, 2
    translator_->ResetCounters();
    auto future1 = cache_slot_->PinCells(uids_012);
    auto accessor1 = SemiInlineGet(std::move(future1));
    ASSERT_NE(accessor1, nullptr);
    EXPECT_EQ(translator_->GetCellsCallCount(), 1);
    auto requested1 = translator_->GetRequestedCids()[0];
    std::sort(requested1.begin(), requested1.end());
    EXPECT_EQ(requested1, cids_012);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), size_012);

    // 2. Unpin 0, 1, 2
    accessor1.reset();
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_),
              size_012);  // Still in cache

    // 3. Load cell 3 (size 200), requires eviction
    cl_uid_t uid_3 = 40;
    cid_t cid_3 = 3;
    ResourceUsage size_3 = translator_->estimated_byte_size_of_cell(cid_3);
    ASSERT_EQ(size_3, ResourceUsage(200, 0));

    translator_->ResetCounters();
    auto future2 = cache_slot_->PinCells({uid_3});
    auto accessor2 = SemiInlineGet(std::move(future2));
    ASSERT_NE(accessor2, nullptr);

    EXPECT_EQ(translator_->GetCellsCallCount(),
              1);  // Load was called for cell 3
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0], std::vector<cid_t>{cid_3});

    // Verify eviction happened
    ResourceUsage used_after_evict1 = DListTestFriend::get_used_memory(*dlist_);
    EXPECT_LE(used_after_evict1.memory_bytes, NEW_LIMIT.memory_bytes);
    EXPECT_GE(used_after_evict1.memory_bytes, size_3.memory_bytes);
    EXPECT_LT(
        used_after_evict1.memory_bytes,
        size_012.memory_bytes + size_3.memory_bytes);  // Eviction occurred
}
