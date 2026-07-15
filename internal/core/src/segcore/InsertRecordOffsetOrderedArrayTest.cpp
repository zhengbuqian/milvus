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

#include <gtest/gtest.h>
#include <stdint.h>
#include <random>
#include <string>
#include <vector>

#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "segcore/InsertRecord.h"
#include "segcore/TimestampIndex.h"

using namespace milvus;
using namespace milvus::segcore;

template <typename T>
class TypedOffsetOrderedArrayTest : public testing::Test {
 public:
    void
    SetUp() override {
        er = std::default_random_engine(42);
    }

    void
    TearDown() override {
    }

 protected:
    void
    insert(T pk) {
        map_.insert(pk, offset_++);
        data_.push_back(pk);
        std::sort(data_.begin(), data_.end());
    }

    void
    seal() {
        map_.seal();
    }

    std::vector<T>
    random_generate(int num) {
        std::vector<T> res;
        for (int i = 0; i < num; i++) {
            if constexpr (std::is_same_v<std::string, T>) {
                res.push_back(std::to_string(er()));
            } else {
                res.push_back(static_cast<T>(er()));
            }
        }
        return res;
    }

 protected:
    int64_t offset_ = 0;
    std::vector<T> data_;
    milvus::segcore::OffsetOrderedArray<T> map_;
    std::default_random_engine er;
};

using TypeOfPks = testing::Types<int64_t, std::string>;
TYPED_TEST_SUITE_P(TypedOffsetOrderedArrayTest);

TYPED_TEST_P(TypedOffsetOrderedArrayTest, find_first) {
    // not sealed.
    ASSERT_ANY_THROW(this->map_.find_first(Unlimited, {}));

    // insert 10 entities.
    int num = 10;
    auto data = this->random_generate(num);
    for (const auto& x : data) {
        this->insert(x);
    }

    // seal.
    this->seal();

    // all is satisfied.
    {
        BitsetType all(num);
        BitsetTypeView all_view(all.data(), num);
        {
            auto [offsets, has_more_res] =
                this->map_.find_first(num / 2, all_view);
            ASSERT_EQ(num / 2, offsets.size());
            ASSERT_TRUE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
        {
            auto [offsets, has_more_res] =
                this->map_.find_first(Unlimited, all_view);
            ASSERT_EQ(num, offsets.size());
            ASSERT_FALSE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
    }
    {
        // corner case, segment offset exceeds the size of bitset.
        BitsetType all_minus_1(num - 1);
        BitsetTypeView all_minus_1_view(all_minus_1.data(), num - 1);
        {
            auto [offsets, has_more_res] =
                this->map_.find_first(num / 2, all_minus_1_view);
            ASSERT_EQ(num / 2, offsets.size());
            ASSERT_TRUE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
        {
            auto [offsets, has_more_res] =
                this->map_.find_first(Unlimited, all_minus_1_view);
            ASSERT_EQ(all_minus_1.size(), offsets.size());
            ASSERT_FALSE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
    }
    {
        // none is satisfied.
        BitsetType none(num);
        none.set();
        BitsetTypeView none_view(none.data(), num);
        auto result_pair = this->map_.find_first(num / 2, none_view);
        ASSERT_EQ(0, result_pair.first.size());
        ASSERT_FALSE(result_pair.second);
        result_pair = this->map_.find_first(NoLimit, none_view);
        ASSERT_EQ(0, result_pair.first.size());
        ASSERT_FALSE(result_pair.second);
    }
}

REGISTER_TYPED_TEST_SUITE_P(TypedOffsetOrderedArrayTest, find_first);
INSTANTIATE_TYPED_TEST_SUITE_P(Prefix, TypedOffsetOrderedArrayTest, TypeOfPks);

// =====================================================================
// VirtualPKOffsetMap tests
// =====================================================================

class VirtualPKOffsetMapTest : public testing::Test {
 protected:
    static constexpr int64_t kSegmentID = 0x123456789ABCDEF0LL;
    static constexpr int64_t kTruncatedSegID = kSegmentID & 0xFFFFFFFF;
    static constexpr int64_t kNumRows = 100;

    VirtualPKOffsetMap map_{kSegmentID, kNumRows};

    // Build a virtual PK from offset
    int64_t
    vpk(int64_t offset) const {
        return (kTruncatedSegID << 32) | (offset & 0xFFFFFFFF);
    }
};

TEST_F(VirtualPKOffsetMapTest, ContainAndFind) {
    // Valid PKs
    EXPECT_TRUE(map_.contain(PkType(vpk(0))));
    EXPECT_TRUE(map_.contain(PkType(vpk(50))));
    EXPECT_TRUE(map_.contain(PkType(vpk(99))));

    // Out of range
    EXPECT_FALSE(map_.contain(PkType(vpk(100))));
    EXPECT_FALSE(map_.contain(PkType(vpk(1000))));

    // Wrong segment ID
    int64_t wrong_seg = ((kTruncatedSegID + 1) << 32) | 0;
    EXPECT_FALSE(map_.contain(PkType(wrong_seg)));

    // Find returns correct offset
    auto result = map_.find(PkType(vpk(42)));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], 42);

    // Find out-of-range returns empty
    result = map_.find(PkType(vpk(100)));
    EXPECT_TRUE(result.empty());
}

TEST_F(VirtualPKOffsetMapTest, FindRange) {
    int64_t num = kNumRows;
    // Equal
    {
        BitsetType bitset(num);
        BitsetTypeView view(bitset.data(), num);
        map_.find_range(
            PkType(vpk(50)), proto::plan::OpType::Equal, view, [](int64_t) {
                return true;
            });
        EXPECT_TRUE(view[50]);
        // Only offset 50 should be set
        int count = 0;
        for (int64_t i = 0; i < num; i++) {
            if (view[i])
                count++;
        }
        EXPECT_EQ(count, 1);
    }
    // GreaterEqual
    {
        BitsetType bitset(num);
        BitsetTypeView view(bitset.data(), num);
        map_.find_range(PkType(vpk(95)),
                        proto::plan::OpType::GreaterEqual,
                        view,
                        [](int64_t) { return true; });
        for (int64_t i = 95; i < num; i++) {
            EXPECT_TRUE(view[i]) << "offset " << i;
        }
        for (int64_t i = 0; i < 95; i++) {
            EXPECT_FALSE(view[i]) << "offset " << i;
        }
    }
    // LessThan
    {
        BitsetType bitset(num);
        BitsetTypeView view(bitset.data(), num);
        map_.find_range(
            PkType(vpk(5)), proto::plan::OpType::LessThan, view, [](int64_t) {
                return true;
            });
        for (int64_t i = 0; i < 5; i++) {
            EXPECT_TRUE(view[i]) << "offset " << i;
        }
        for (int64_t i = 5; i < num; i++) {
            EXPECT_FALSE(view[i]) << "offset " << i;
        }
    }
}

TEST_F(VirtualPKOffsetMapTest, FindFirstN) {
    int64_t num = kNumRows;
    // All pass
    {
        BitsetType bitset(num);
        bitset.reset();  // 0 = pass
        BitsetTypeView view(bitset.data(), num);
        auto [offsets, has_more] = map_.find_first_n(10, view);
        ASSERT_EQ(offsets.size(), 10);
        // Should be sorted by PK order = offset order for virtual PKs
        for (int64_t i = 0; i < 10; i++) {
            EXPECT_EQ(offsets[i], i);
        }
        EXPECT_TRUE(has_more);
    }
    // None pass
    {
        BitsetType bitset(num);
        bitset.set();  // 1 = filtered out
        BitsetTypeView view(bitset.data(), num);
        auto [offsets, has_more] = map_.find_first_n(10, view);
        EXPECT_EQ(offsets.size(), 0);
        EXPECT_FALSE(has_more);
    }
}

TEST_F(VirtualPKOffsetMapTest, EmptyAndSeal) {
    EXPECT_FALSE(map_.empty());
    EXPECT_EQ(map_.memory_size(), sizeof(VirtualPKOffsetMap));

    // seal and insert are no-ops
    map_.seal();
    map_.insert(PkType(vpk(0)), 0);
    EXPECT_FALSE(map_.empty());

    // zero-row map
    VirtualPKOffsetMap empty_map(kSegmentID, 0);
    EXPECT_TRUE(empty_map.empty());
}
