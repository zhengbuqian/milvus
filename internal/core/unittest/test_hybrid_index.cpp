// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
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
#include <functional>
#include <boost/filesystem.hpp>
#include <unordered_set>
#include <memory>

#include "common/Tracer.h"
#include "index/BitmapIndex.h"
#include "index/HybridScalarIndex.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "indexbuilder/IndexFactory.h"
#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "index/Meta.h"
#include "pb/schema.pb.h"

using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

template <typename T>
static std::vector<T>
GenerateData(const size_t size, const size_t cardinality) {
    std::vector<T> result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(rand() % cardinality);
    }
    return result;
}

template <>
std::vector<bool>
GenerateData<bool>(const size_t size, const size_t cardinality) {
    std::vector<bool> result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(rand() % 2 == 0);
    }
    return result;
}

template <>
std::vector<std::string>
GenerateData<std::string>(const size_t size, const size_t cardinality) {
    std::vector<std::string> result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(std::to_string(rand() % cardinality));
    }
    return result;
}

template <typename T>
class HybridIndexTestV1 : public testing::Test {
 protected:
    void
    Init(int64_t collection_id,
         int64_t partition_id,
         int64_t segment_id,
         int64_t field_id,
         int64_t index_build_id,
         int64_t index_version) {
        proto::schema::FieldSchema field_schema;
        field_schema.set_nullable(nullable_);
        if (has_default_value_) {
            auto default_value = field_schema.mutable_default_value();
            if constexpr (std::is_same_v<int8_t, T> ||
                          std::is_same_v<int16_t, T> ||
                          std::is_same_v<int32_t, T>) {
                default_value->set_int_data(10);
            } else if constexpr (std::is_same_v<int64_t, T>) {
                default_value->set_long_data(10);
            } else if constexpr (std::is_same_v<float, T>) {
                default_value->set_float_data(10);
            } else if constexpr (std::is_same_v<double, T>) {
                default_value->set_double_data(10);
            } else if constexpr (std::is_same_v<std::string, T>) {
                default_value->set_string_data("10");
            }
        }
        if constexpr (std::is_same_v<int8_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int8);
        } else if constexpr (std::is_same_v<int16_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int16);
        } else if constexpr (std::is_same_v<int32_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int32);
        } else if constexpr (std::is_same_v<int64_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int64);
        } else if constexpr (std::is_same_v<float, T>) {
            field_schema.set_data_type(proto::schema::DataType::Float);
        } else if constexpr (std::is_same_v<double, T>) {
            field_schema.set_data_type(proto::schema::DataType::Double);
        } else if constexpr (std::is_same_v<std::string, T>) {
            field_schema.set_data_type(proto::schema::DataType::String);
        }
        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        std::vector<T> data_gen;
        data_gen = GenerateData<T>(nb_, cardinality_);
        for (auto x : data_gen) {
            data_.push_back(x);
        }

        auto field_data = storage::CreateFieldData(type_, nullable_);
        if (nullable_) {
            valid_data_.reserve(nb_);
            uint8_t* ptr = new uint8_t[(nb_ + 7) / 8];
            for (int i = 0; i < nb_; i++) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                if (i % 2 == 0) {
                    valid_data_.push_back(true);
                    ptr[byteIndex] |= (1 << bitIndex);
                } else {
                    valid_data_.push_back(false);
                    ptr[byteIndex] &= ~(1 << bitIndex);
                }
            }
            field_data->FillFieldData(data_.data(), ptr, data_.size(), 0);
            delete[] ptr;
        } else {
            field_data->FillFieldData(data_.data(), data_.size());
        }
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    "/tmp/test_hybrid/",
                                    collection_id,
                                    partition_id,
                                    segment_id,
                                    field_id,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager_);
        std::vector<std::string> index_files;

        Config config;
        config["index_type"] = milvus::index::HYBRID_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        config["bitmap_cardinality_limit"] = "1000";
        config[INDEX_NUM_ROWS_KEY] = nb_;
        if (has_lack_binlog_row_) {
            config[INDEX_NUM_ROWS_KEY] = nb_ + lack_binlog_row_;
        }

        {
            auto build_index =
                indexbuilder::IndexFactory::GetInstance().CreateIndex(
                    type_, config, ctx);
            build_index->Build();

            auto create_index_result = build_index->Upload();
            auto memSize = create_index_result->GetMemSize();
            auto serializedSize = create_index_result->GetSerializedSize();
            ASSERT_GT(memSize, 0);
            ASSERT_GT(serializedSize, 0);
            index_files = create_index_result->GetIndexFiles();
        }

        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::HYBRID_INDEX_TYPE;
        index_info.field_type = type_;

        config["index_files"] = index_files;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        ctx.set_for_loading_index(true);
        index_ =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index_->Load(milvus::tracer::TraceContext{}, config);
    }

    virtual void
    SetParam() {
        nb_ = 10000;
        cardinality_ = 30;
        nullable_ = false;
        index_version_ = 1001;
        index_build_id_ = 1001;
    }
    void
    SetUp() override {
        SetParam();

        if constexpr (std::is_same_v<T, int8_t>) {
            type_ = DataType::INT8;
        } else if constexpr (std::is_same_v<T, int16_t>) {
            type_ = DataType::INT16;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            type_ = DataType::INT32;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            type_ = DataType::INT64;
        } else if constexpr (std::is_same_v<T, std::string>) {
            type_ = DataType::VARCHAR;
        }
        int64_t collection_id = 1;
        int64_t partition_id = 2;
        int64_t segment_id = 3;
        int64_t field_id = 101;
        std::string root_path = "/tmp/test-bitmap-index";

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        Init(collection_id,
             partition_id,
             segment_id,
             field_id,
             index_build_id_,
             index_version_);
    }

    virtual ~HybridIndexTestV1() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

 public:
    void
    TestInFunc() {
        boost::container::vector<T> test_data;
        std::unordered_set<T> s;
        size_t nq = 10;
        for (size_t i = 0; i < nq; i++) {
            test_data.push_back(data_[i]);
            s.insert(data_[i]);
        }
        auto index_ptr =
            dynamic_cast<index::HybridScalarIndex<T>*>(index_.get());
        auto bitset = index_ptr->In(test_data.size(), test_data.data());
        size_t start = 0;
        if (has_lack_binlog_row_) {
            for (int i = 0; i < lack_binlog_row_; i++) {
                if (!has_default_value_) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    if constexpr (std::is_same_v<std::string, T>) {
                        ASSERT_EQ(bitset[i], s.find("10") != s.end());
                    } else {
                        ASSERT_EQ(bitset[i], s.find(10) != s.end());
                    }
                }
            }
            start += lack_binlog_row_;
        }
        for (size_t i = start; i < bitset.size(); i++) {
            if (nullable_ && !valid_data_[i - start]) {
                ASSERT_EQ(bitset[i], false);
            } else {
                ASSERT_EQ(bitset[i], s.find(data_[i - start]) != s.end());
            }
        }
    }

    void
    TestNotInFunc() {
        boost::container::vector<T> test_data;
        std::unordered_set<T> s;
        size_t nq = 10;
        for (size_t i = 0; i < nq; i++) {
            test_data.push_back(data_[i]);
            s.insert(data_[i]);
        }
        auto index_ptr =
            dynamic_cast<index::HybridScalarIndex<T>*>(index_.get());
        auto bitset = index_ptr->NotIn(test_data.size(), test_data.data());
        size_t start = 0;
        if (has_lack_binlog_row_) {
            for (int i = 0; i < lack_binlog_row_; i++) {
                if (!has_default_value_) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    if constexpr (std::is_same_v<std::string, T>) {
                        ASSERT_EQ(bitset[i], s.find("10") == s.end());
                    } else {
                        ASSERT_EQ(bitset[i], s.find(10) == s.end());
                    }
                }
            }
            start += lack_binlog_row_;
        }
        for (size_t i = start; i < bitset.size(); i++) {
            if (nullable_ && !valid_data_[i - start]) {
                ASSERT_EQ(bitset[i], false);
            } else {
                ASSERT_NE(bitset[i], s.find(data_[i - start]) != s.end());
            }
        }
    }

    void
    TestIsNullFunc() {
        auto index_ptr =
            dynamic_cast<index::HybridScalarIndex<T>*>(index_.get());
        auto bitset = index_ptr->IsNull();
        size_t start = 0;
        if (has_lack_binlog_row_) {
            for (int i = 0; i < lack_binlog_row_; i++) {
                if (has_default_value_) {
                    ASSERT_EQ(bitset[i], false);
                } else {
                    ASSERT_EQ(bitset[i], true);
                }
            }
            start += lack_binlog_row_;
        }
        for (size_t i = start; i < bitset.size(); i++) {
            if (nullable_ && !valid_data_[i - start]) {
                ASSERT_EQ(bitset[i], true);
            } else {
                ASSERT_EQ(bitset[i], false);
            }
        }
    }

    void
    TestIsNotNullFunc() {
        auto index_ptr =
            dynamic_cast<index::HybridScalarIndex<T>*>(index_.get());
        auto bitset = index_ptr->IsNotNull();
        size_t start = 0;
        if (has_lack_binlog_row_) {
            for (int i = 0; i < lack_binlog_row_; i++) {
                if (has_default_value_) {
                    ASSERT_EQ(bitset[i], true);
                } else {
                    ASSERT_EQ(bitset[i], false);
                }
            }
            start += lack_binlog_row_;
        }
        for (size_t i = start; i < bitset.size(); i++) {
            if (nullable_ && !valid_data_[i - start]) {
                ASSERT_EQ(bitset[i], false);
            } else {
                ASSERT_EQ(bitset[i], true);
            }
        }
    }

    void
    TestCompareValueFunc() {
        if constexpr (!std::is_same_v<T, std::string>) {
            using RefFunc = std::function<bool(int64_t)>;
            std::vector<std::tuple<T, OpType, RefFunc, bool>> test_cases{
                {10,
                 OpType::GreaterThan,
                 [&](int64_t i) -> bool { return data_[i] > 10; },
                 false},
                {10,
                 OpType::GreaterEqual,
                 [&](int64_t i) -> bool { return data_[i] >= 10; },
                 true},
                {10,
                 OpType::LessThan,
                 [&](int64_t i) -> bool { return data_[i] < 10; },
                 false},
                {10,
                 OpType::LessEqual,
                 [&](int64_t i) -> bool { return data_[i] <= 10; },
                 true},
            };
            for (const auto& [test_value, op, ref, default_value_res] :
                 test_cases) {
                auto index_ptr =
                    dynamic_cast<index::HybridScalarIndex<T>*>(index_.get());
                auto bitset = index_ptr->Range(test_value, op);
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row_; i++) {
                        if (has_default_value_) {
                            ASSERT_EQ(bitset[i], default_value_res);
                        } else {
                            ASSERT_EQ(bitset[i], false);
                        }
                    }
                    start += lack_binlog_row_;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = ref(i - start);
                    if (nullable_ && !valid_data_[i - start]) {
                        ASSERT_EQ(ans, false)
                            << "op: " << op << ", @" << i << ", ans: " << ans
                            << ", ref: " << should;
                    } else {
                        ASSERT_EQ(ans, should)
                            << "op: " << op << ", @" << i << ", ans: " << ans
                            << ", ref: " << should;
                    }
                }
            }
        }
    }

    void
    TestRangeCompareFunc() {
        if constexpr (!std::is_same_v<T, std::string>) {
            using RefFunc = std::function<bool(int64_t)>;
            struct TestParam {
                int64_t lower_val;
                int64_t upper_val;
                bool lower_inclusive;
                bool upper_inclusive;
                RefFunc ref;
                bool default_value_res;
            };
            std::vector<TestParam> test_cases = {
                {
                    10,
                    30,
                    false,
                    false,
                    [&](int64_t i) { return 10 < data_[i] && data_[i] < 30; },
                    false,
                },
                {
                    10,
                    30,
                    true,
                    false,
                    [&](int64_t i) { return 10 <= data_[i] && data_[i] < 30; },
                    true,
                },
                {
                    10,
                    30,
                    true,
                    true,
                    [&](int64_t i) { return 10 <= data_[i] && data_[i] <= 30; },
                    true,
                },
                {
                    10,
                    30,
                    false,
                    true,
                    [&](int64_t i) { return 10 < data_[i] && data_[i] <= 30; },
                    false,
                }};

            for (const auto& test_case : test_cases) {
                auto index_ptr =
                    dynamic_cast<index::HybridScalarIndex<T>*>(index_.get());
                auto bitset = index_ptr->Range(test_case.lower_val,
                                               test_case.lower_inclusive,
                                               test_case.upper_val,
                                               test_case.upper_inclusive);
                size_t start = 0;
                if (has_lack_binlog_row_) {
                    for (int i = 0; i < lack_binlog_row_; i++) {
                        if (has_default_value_) {
                            ASSERT_EQ(bitset[i], test_case.default_value_res);
                        } else {
                            ASSERT_EQ(bitset[i], false);
                        }
                    }
                    start += lack_binlog_row_;
                }
                for (size_t i = start; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = test_case.ref(i - start);
                    if (nullable_ && !valid_data_[i - start]) {
                        ASSERT_EQ(ans, false)
                            << "lower:" << test_case.lower_val
                            << "upper:" << test_case.upper_val << ", @" << i
                            << ", ans: " << ans << ", ref: " << false;
                    } else {
                        ASSERT_EQ(ans, should)
                            << "lower:" << test_case.lower_val
                            << "upper:" << test_case.upper_val << ", @" << i
                            << ", ans: " << ans << ", ref: " << should;
                    }
                }
            }
        }
    }

 public:
    IndexBasePtr index_;
    DataType type_;
    size_t nb_;
    size_t cardinality_;
    boost::container::vector<T> data_;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
    bool nullable_;
    FixedVector<bool> valid_data_;
    int index_build_id_;
    int index_version_;
    bool has_default_value_{false};
    bool has_lack_binlog_row_{false};
    size_t lack_binlog_row_{100};
};

TYPED_TEST_SUITE_P(HybridIndexTestV1);

TYPED_TEST_P(HybridIndexTestV1, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(HybridIndexTestV1, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(HybridIndexTestV1, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(HybridIndexTestV1, IsNullFuncTest) {
    this->TestIsNullFunc();
}

TYPED_TEST_P(HybridIndexTestV1, IsNotNullFuncTest) {
    this->TestIsNotNullFunc();
}

TYPED_TEST_P(HybridIndexTestV1, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(HybridIndexTestV1, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

using BitmapType =
    testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(HybridIndexTestV1,
                            CountFuncTest,
                            INFuncTest,
                            IsNullFuncTest,
                            IsNotNullFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(HybridIndexE2ECheck_LowCardinality,
                               HybridIndexTestV1,
                               BitmapType);

template <typename T>
class HybridIndexTestV2 : public HybridIndexTestV1<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 2000;
        this->nullable_ = false;
        this->index_version_ = 1002;
        this->index_build_id_ = 1002;
    }

    virtual ~HybridIndexTestV2() {
    }
};

TYPED_TEST_SUITE_P(HybridIndexTestV2);

TYPED_TEST_P(HybridIndexTestV2, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(HybridIndexTestV2, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(HybridIndexTestV2, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(HybridIndexTestV2, IsNullFuncTest) {
    this->TestIsNullFunc();
}

TYPED_TEST_P(HybridIndexTestV2, IsNotNullFuncTest) {
    this->TestIsNotNullFunc();
}

TYPED_TEST_P(HybridIndexTestV2, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(HybridIndexTestV2, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

template <typename T>
class HybridIndexTestNullable : public HybridIndexTestV1<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 2000;
        this->nullable_ = true;
        this->index_version_ = 1003;
        this->index_build_id_ = 1003;
    }

    virtual ~HybridIndexTestNullable() {
    }
};

TYPED_TEST_SUITE_P(HybridIndexTestNullable);

TYPED_TEST_P(HybridIndexTestNullable, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(HybridIndexTestNullable, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(HybridIndexTestNullable, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(HybridIndexTestNullable, IsNullFuncTest) {
    this->TestIsNullFunc();
}

TYPED_TEST_P(HybridIndexTestNullable, IsNotNullFuncTest) {
    this->TestIsNotNullFunc();
}

TYPED_TEST_P(HybridIndexTestNullable, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(HybridIndexTestNullable, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

template <typename T>
class HybridIndexTestV3 : public HybridIndexTestV1<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 2000;
        this->nullable_ = true;
        this->index_version_ = 1003;
        this->index_build_id_ = 1003;
        this->has_default_value_ = false;
        this->has_lack_binlog_row_ = true;
        this->lack_binlog_row_ = 100;
    }

    virtual ~HybridIndexTestV3() {
    }
};

TYPED_TEST_SUITE_P(HybridIndexTestV3);

TYPED_TEST_P(HybridIndexTestV3, CountFuncTest) {
    auto count = this->index_->Count();
    if (this->has_lack_binlog_row_) {
        EXPECT_EQ(count, this->nb_ + this->lack_binlog_row_);
    } else {
        EXPECT_EQ(count, this->nb_);
    }
}

TYPED_TEST_P(HybridIndexTestV3, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(HybridIndexTestV3, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(HybridIndexTestV3, IsNullFuncTest) {
    this->TestIsNullFunc();
}

TYPED_TEST_P(HybridIndexTestV3, IsNotNullFuncTest) {
    this->TestIsNotNullFunc();
}

TYPED_TEST_P(HybridIndexTestV3, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(HybridIndexTestV3, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

template <typename T>
class HybridIndexTestV4 : public HybridIndexTestV1<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 2000;
        this->nullable_ = true;
        this->index_version_ = 1003;
        this->index_build_id_ = 1003;
        this->has_default_value_ = true;
        this->has_lack_binlog_row_ = true;
        this->lack_binlog_row_ = 100;
    }

    virtual ~HybridIndexTestV4() {
    }
};

TYPED_TEST_SUITE_P(HybridIndexTestV4);

TYPED_TEST_P(HybridIndexTestV4, CountFuncTest) {
    auto count = this->index_->Count();
    if (this->has_lack_binlog_row_) {
        EXPECT_EQ(count, this->nb_ + this->lack_binlog_row_);
    } else {
        EXPECT_EQ(count, this->nb_);
    }
}

TYPED_TEST_P(HybridIndexTestV4, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(HybridIndexTestV4, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(HybridIndexTestV4, IsNullFuncTest) {
    this->TestIsNullFunc();
}

TYPED_TEST_P(HybridIndexTestV4, IsNotNullFuncTest) {
    this->TestIsNotNullFunc();
}

TYPED_TEST_P(HybridIndexTestV4, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(HybridIndexTestV4, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

using BitmapType =
    testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(HybridIndexTestV2,
                            CountFuncTest,
                            INFuncTest,
                            IsNullFuncTest,
                            IsNotNullFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

REGISTER_TYPED_TEST_SUITE_P(HybridIndexTestNullable,
                            CountFuncTest,
                            INFuncTest,
                            IsNullFuncTest,
                            IsNotNullFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

REGISTER_TYPED_TEST_SUITE_P(HybridIndexTestV3,
                            CountFuncTest,
                            INFuncTest,
                            IsNullFuncTest,
                            IsNotNullFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

REGISTER_TYPED_TEST_SUITE_P(HybridIndexTestV4,
                            CountFuncTest,
                            INFuncTest,
                            IsNullFuncTest,
                            IsNotNullFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(HybridIndexE2ECheck_HighCardinality,
                               HybridIndexTestV2,
                               BitmapType);

INSTANTIATE_TYPED_TEST_SUITE_P(HybridIndexE2ECheck_Nullable,
                               HybridIndexTestNullable,
                               BitmapType);

INSTANTIATE_TYPED_TEST_SUITE_P(HybridIndexE2ECheck_HasLackNullBinlog,
                               HybridIndexTestV3,
                               BitmapType);

INSTANTIATE_TYPED_TEST_SUITE_P(HybridIndexE2ECheck_HasLackDefaultValueBinlog,
                               HybridIndexTestV4,
                               BitmapType);
