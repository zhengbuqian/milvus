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

#include <boost/container/vector.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <simdjson.h>
#include <stddef.h>
#include <cstdint>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/NgramInvertedIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "common/RegexQuery.h"
#include "index/Utils.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "query/Utils.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/load_index_c.h"
#include "segcore/segment_c.h"
#include "simdjson/padded_string.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/Constants.h"
#include "test_utils/storage_test_utils.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::exec;

std::unique_ptr<index::NgramInvertedIndex>
CreateNgramIndexForCanHandleLiteral(uintptr_t min_gram = 2,
                                    uintptr_t max_gram = 4) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id.get(),
                                                      DataType::VARCHAR,
                                                      DataType::NONE,
                                                      false);
    auto index_meta = gen_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    auto storage_config = gen_local_storage_config(TestLocalPath);
    auto cm = CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);

    auto ngram_params = index::NgramParams{
        .loading_index = true, .min_gram = min_gram, .max_gram = max_gram};
    return std::make_unique<index::NgramInvertedIndex>(ctx, ngram_params);
}

FieldDataPtr
MakeNullableStringFieldData(const std::vector<std::string>& values,
                            const std::vector<bool>& valid) {
    auto field_data = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, true, 1, values.size());
    std::vector<uint8_t> valid_bytes((values.size() + 7) / 8, 0);
    for (size_t i = 0; i < valid.size(); ++i) {
        if (valid[i]) {
            valid_bytes[i / 8] |= 1U << (i % 8);
        }
    }
    field_data->FillFieldData(
        values.data(), valid_bytes.data(), values.size(), 0);
    return field_data;
}

std::unique_ptr<index::NgramInvertedIndex>
CreateNgramIndexForBatchBuild(DataType data_type,
                              bool nullable,
                              const std::string& nested_path = "",
                              std::optional<index::NgramParams> params =
                                  std::nullopt) {
    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        static_cast<proto::schema::DataType>(data_type));
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(100);
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(nullable);
    file_manager_ctx.fieldDataMeta.field_id = 100;

    auto ngram_params =
        params.value_or(index::NgramParams{false, 2, 3});
    if (data_type == DataType::JSON) {
        return std::make_unique<index::NgramInvertedIndex>(
            file_manager_ctx, ngram_params, nested_path);
    }
    return std::make_unique<index::NgramInvertedIndex>(file_manager_ctx,
                                                       ngram_params);
}

uint64_t
ReplayLimitAfterBatches(size_t applied_batches) {
    constexpr uint64_t kRowsPerBatch = 512;
    constexpr uint64_t kBytesPerValue = 64;
    constexpr uint64_t kOccurrencesPerValue = 62 + 61;
    constexpr uint64_t kTermBytesPerValue = 62 * 3 + 61 * 4;
    constexpr uint64_t kPerRowEstimate =
        16 + kOccurrencesPerValue * 128 + kTermBytesPerValue;
    constexpr uint64_t kBase = 4 * 1024 * 1024;
    constexpr uint64_t kReserve = 64 * 1024 * 1024;
    const uint64_t batch_growth = kRowsPerBatch * kPerRowEstimate;
    return kReserve + kBase + applied_batches * batch_growth +
           batch_growth / 2;
}

std::vector<std::string>
MakeReplayRows(size_t count) {
    std::vector<std::string> values;
    values.reserve(count);
    for (size_t row = 0; row < count; ++row) {
        auto prefix = fmt::format("row-{:08x}-", row);
        std::string value;
        value.reserve(64);
        while (value.size() < 64) {
            value.append(prefix);
        }
        value.resize(64);
        values.push_back(std::move(value));
    }
    auto& marker = values.at(777);
    marker.assign("unique-replay-marker");
    marker.resize(64, 'z');
    return values;
}

FieldDataPtr
MakeJsonStringFieldData(const std::vector<std::string>& values) {
    arrow::BinaryBuilder builder;
    for (const auto& value : values) {
        const auto status = builder.Append(
            value.data(), static_cast<int32_t>(value.size()));
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }
    std::shared_ptr<arrow::Array> json_array;
    const auto status = builder.Finish(&json_array);
    if (!status.ok()) {
        throw std::runtime_error(status.ToString());
    }
    auto field_data =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    field_data->FillFieldData(json_array);
    return field_data;
}

constexpr size_t kHighDistinctRowBytes = 64;
constexpr uint64_t kHighDistinctBaseSeed = 0x4e4752414d5f4d45ULL;
constexpr uint64_t kSplitMixGamma = 0x9e3779b97f4a7c15ULL;
constexpr std::string_view kHighDistinctAlphabet =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr std::string_view kHighDistinctMarker =
    "unique-high-distinct-marker-q7v9";

uint64_t
NextSplitMix64(uint64_t& state) {
    state += kSplitMixGamma;
    auto value = state;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

std::vector<std::string>
MakeHighDistinctRows(size_t count) {
    std::vector<std::string> values;
    values.reserve(count);
    for (size_t row = 0; row < count; ++row) {
        uint64_t state =
            kHighDistinctBaseSeed ^ static_cast<uint64_t>(row) * kSplitMixGamma;
        std::string value;
        value.reserve(kHighDistinctRowBytes);
        for (size_t offset = 0; offset < kHighDistinctRowBytes; ++offset) {
            value.push_back(kHighDistinctAlphabet[
                NextSplitMix64(state) % kHighDistinctAlphabet.size()]);
        }
        values.push_back(std::move(value));
    }
    auto& marker = values.at(777);
    marker.assign(kHighDistinctMarker);
    marker.resize(kHighDistinctRowBytes, '-');
    return values;
}

const char*
NgramBackendName(index::NgramBuildBackend backend) {
    return backend == index::NgramBuildBackend::Direct ? "direct" : "regular";
}

TargetBitmap
QueryNgramPostings(index::NgramInvertedIndex& ngram_index,
                   const std::string& literal);

void
RunHighDistinctBenchmark(size_t row_count,
                         index::NgramBuildMode mode,
                         uint64_t soft_limit_bytes,
                         index::NgramBuildBackend expected_initial,
                         index::NgramBuildBackend expected_final,
                         size_t expected_replay_count) {
    const auto values = MakeHighDistinctRows(row_count);
    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(values.data(), row_count);
    auto params = index::NgramParams{
        .loading_index = false,
        .min_gram = 3,
        .max_gram = 4,
        .build_mode = mode,
        .direct_soft_limit_bytes = soft_limit_bytes,
    };
    auto ngram_index = CreateNgramIndexForBatchBuild(
        DataType::VARCHAR, false, "", params);

    const auto begin = std::chrono::steady_clock::now();
    ngram_index->BuildWithFieldData({field_data});
    ngram_index->finish();
    const auto build_ms =
        std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - begin)
            .count();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    const auto& stats = ngram_index->GetBuildStats();
    EXPECT_EQ(stats.initial_backend, expected_initial);
    EXPECT_EQ(stats.final_backend, expected_final);
    EXPECT_EQ(stats.replay_count, expected_replay_count);
    if (expected_final == index::NgramBuildBackend::Direct) {
        EXPECT_EQ(stats.direct_rows_applied, row_count);
    } else if (expected_initial == index::NgramBuildBackend::Regular) {
        EXPECT_EQ(stats.direct_rows_applied, 0);
    } else {
        EXPECT_GT(stats.direct_rows_applied, 0);
        EXPECT_LT(stats.direct_rows_applied, row_count);
    }
    if (mode == index::NgramBuildMode::Auto) {
        if (expected_initial == index::NgramBuildBackend::Direct) {
            EXPECT_LE(stats.preflight_required_bytes, soft_limit_bytes);
        } else {
            EXPECT_GT(stats.preflight_required_bytes, soft_limit_bytes);
        }
    } else {
        EXPECT_EQ(stats.preflight_required_bytes, 0);
    }
    EXPECT_EQ(ngram_index->Count(), row_count);
    const auto marker =
        QueryNgramPostings(*ngram_index, std::string(kHighDistinctMarker));
    EXPECT_EQ(marker.count(), 1);
    EXPECT_TRUE(marker[777]);

    std::cout << fmt::format(
                     "NGRAM_HIGH_DISTINCT_BENCH rows={} build_ms={:.3f} "
                     "initial_backend={} final_backend={} replay_count={} "
                     "direct_rows_applied={} preflight_required_bytes={} "
                     "soft_limit_bytes={} count={} marker_count={}",
                     row_count,
                     build_ms,
                     NgramBackendName(stats.initial_backend),
                     NgramBackendName(stats.final_backend),
                     stats.replay_count,
                     stats.direct_rows_applied,
                     stats.preflight_required_bytes,
                     soft_limit_bytes,
                     ngram_index->Count(),
                     marker.count())
              << std::endl;
}

TargetBitmap
QueryNgramPostings(index::NgramInvertedIndex& ngram_index,
                   const std::string& literal) {
    TargetBitmap candidates(ngram_index.Count(), true);
    ngram_index.ExecutePhase1(
        literal, proto::plan::OpType::InnerMatch, candidates);
    return candidates;
}

TEST(NgramIndex, RawDataBuildUsesNgramPostings) {
    const std::vector<std::string> values = {"alpha", "alphabet", "omega"};
    proto::schema::StringArray serialized_values;
    for (const auto& value : values) {
        serialized_values.add_data(value);
    }
    std::string serialized;
    ASSERT_TRUE(serialized_values.SerializeToString(&serialized));
    auto ngram_index =
        CreateNgramIndexForBatchBuild(DataType::VARCHAR, false);

    ngram_index->BuildWithRawDataForUT(serialized.size(), serialized.data());

    ASSERT_EQ(ngram_index->Count(), values.size());
    const auto alpha = QueryNgramPostings(*ngram_index, "alp");
    ASSERT_EQ(alpha.count(), 2);
    EXPECT_TRUE(alpha[0]);
    EXPECT_TRUE(alpha[1]);
    EXPECT_FALSE(alpha[2]);
}

TEST(NgramIndex, BatchBuildPreservesNullableStringRowsAcrossChunks) {
    auto batch0 =
        MakeNullableStringFieldData({"alpha", "", "beta"}, {true, false, true});
    auto batch1 = MakeNullableStringFieldData({"", "gamma", "", ""},
                                              {false, true, true, false});

    auto ngram_index = CreateNgramIndexForBatchBuild(DataType::VARCHAR, true);
    ngram_index->BuildWithFieldData({batch0, batch1});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    ASSERT_EQ(ngram_index->Count(), 7);
    auto nulls = ngram_index->IsNull();
    ASSERT_EQ(nulls.size(), 7);
    EXPECT_FALSE(nulls[0]);
    EXPECT_TRUE(nulls[1]);
    EXPECT_FALSE(nulls[2]);
    EXPECT_TRUE(nulls[3]);
    EXPECT_FALSE(nulls[4]);
    EXPECT_FALSE(nulls[5]);
    EXPECT_TRUE(nulls[6]);

    auto alpha = QueryNgramPostings(*ngram_index, "al");
    EXPECT_EQ(alpha.count(), 1);
    EXPECT_TRUE(alpha[0]);
    auto beta = QueryNgramPostings(*ngram_index, "be");
    EXPECT_EQ(beta.count(), 1);
    EXPECT_TRUE(beta[2]);
    auto gamma = QueryNgramPostings(*ngram_index, "ga");
    EXPECT_EQ(gamma.count(), 1);
    EXPECT_TRUE(gamma[4]);
}

TEST(NgramIndex, BatchWrapperAddsMixedRowsWithExplicitIds) {
    auto path = (boost::filesystem::path(TestLocalPath) /
                 boost::filesystem::unique_path("ngram-batch-%%%%-%%%%"))
                    .string();
    boost::filesystem::create_directories(path);

    {
        milvus::tantivy::TantivyIndexWrapper wrapper(
            "field",
            path.c_str(),
            uintptr_t{2},
            uintptr_t{3},
            uintptr_t{1},
            uintptr_t{15 * 1024 * 1024});
        const std::string alpha = "alpha";
        const std::string omega = "omega";
        std::vector<milvus::tantivy::TantivyIndexWrapper::NgramRowView> rows = {
            {reinterpret_cast<const uint8_t*>(alpha.data()),
             alpha.size(),
             0,
             true},
            {nullptr, 0, 1, false},
            {nullptr, 0, 2, true},
            {reinterpret_cast<const uint8_t*>(omega.data()),
             omega.size(),
             3,
             true},
        };

        wrapper.add_ngram_batch(rows.data(), rows.size());
        wrapper.finish();
        wrapper.create_reader(milvus::index::SetBitsetSealed);

        ASSERT_EQ(wrapper.count(), rows.size());
        TargetBitmap alpha_hits(rows.size());
        wrapper.ngram_term_posting_list("al", &alpha_hits);
        EXPECT_EQ(alpha_hits.count(), 1);
        EXPECT_TRUE(alpha_hits[0]);
        TargetBitmap omega_hits(rows.size());
        wrapper.ngram_term_posting_list("om", &omega_hits);
        EXPECT_EQ(omega_hits.count(), 1);
        EXPECT_TRUE(omega_hits[3]);
    }

    boost::filesystem::remove_all(path);
}

TEST(NgramIndex, BatchBuildPreservesRowsAcrossBatchBoundary) {
    constexpr size_t kRowCount = 513;
    std::vector<std::string> values(kRowCount, "filler");
    std::vector<bool> valid(kRowCount, true);
    values[510] = "before-boundary";
    valid[511] = false;
    values[512] = "after-boundary";

    auto field_data = MakeNullableStringFieldData(values, valid);
    auto ngram_index = CreateNgramIndexForBatchBuild(DataType::VARCHAR, true);
    ngram_index->BuildWithFieldData({field_data});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    ASSERT_EQ(ngram_index->Count(), kRowCount);
    auto nulls = ngram_index->IsNull();
    ASSERT_EQ(nulls.size(), kRowCount);
    EXPECT_TRUE(nulls[511]);

    auto before = QueryNgramPostings(*ngram_index, "before");
    EXPECT_EQ(before.count(), 1);
    EXPECT_TRUE(before[510]);
    auto after = QueryNgramPostings(*ngram_index, "after");
    EXPECT_EQ(after.count(), 1);
    EXPECT_TRUE(after[512]);
}

TEST(NgramIndex, DirectMemoryGateReplaysFromRowZeroAtBatchProgressBoundaries) {
    constexpr size_t kRowCount = 4 * 512;
    const auto values = MakeReplayRows(kRowCount);
    std::vector<bool> valid(kRowCount, true);
    for (size_t row = 0; row < kRowCount; row += 257) {
        valid[row] = false;
    }
    const auto field_data = MakeNullableStringFieldData(values, valid);

    for (size_t applied_batches : {size_t{1}, size_t{2}, size_t{3}}) {
        SCOPED_TRACE(fmt::format("applied_batches={}", applied_batches));
        auto params = index::NgramParams{
            .loading_index = false,
            .min_gram = 3,
            .max_gram = 4,
            .build_mode = index::NgramBuildMode::ForceDirect,
            .direct_soft_limit_bytes =
                ReplayLimitAfterBatches(applied_batches),
        };
        auto ngram_index = CreateNgramIndexForBatchBuild(
            DataType::VARCHAR, true, "", params);

        ngram_index->BuildWithFieldData({field_data});
        ngram_index->finish();
        ngram_index->create_reader(milvus::index::SetBitsetSealed);

        const auto& stats = ngram_index->GetBuildStats();
        EXPECT_EQ(stats.initial_backend, index::NgramBuildBackend::Direct);
        EXPECT_EQ(stats.final_backend, index::NgramBuildBackend::Regular);
        EXPECT_EQ(stats.replay_count, 1);
        EXPECT_EQ(stats.direct_rows_applied, applied_batches * 512);
        EXPECT_EQ(ngram_index->GetAvgRowSize(), 64);
        EXPECT_EQ(ngram_index->Count(), kRowCount);

        const auto nulls = ngram_index->IsNull();
        ASSERT_EQ(nulls.size(), kRowCount);
        for (size_t row = 0; row < kRowCount; ++row) {
            EXPECT_EQ(nulls[row], !valid[row]) << "row " << row;
        }
        const auto marker = QueryNgramPostings(*ngram_index, "replay-marker");
        ASSERT_EQ(marker.count(), 1);
        EXPECT_TRUE(marker[777]);
    }
}

TEST(NgramIndex, AutoAccountsForTokenizerScratchWhenNoNgramsAreEmitted) {
    constexpr size_t kValueBytes = 64 * 1024;
    constexpr uintptr_t kGramWidth = kValueBytes + 1;
    constexpr uint64_t kSoftLimitBytes = uint64_t{69} * 1024 * 1024;
    const std::vector<std::string> values(1, std::string(kValueBytes, 'x'));
    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(values.data(), values.size());
    auto params = index::NgramParams{
        .loading_index = false,
        .min_gram = kGramWidth,
        .max_gram = kGramWidth,
        .build_mode = index::NgramBuildMode::Auto,
        .direct_soft_limit_bytes = kSoftLimitBytes,
    };
    auto ngram_index = CreateNgramIndexForBatchBuild(
        DataType::VARCHAR, false, "", params);

    ngram_index->BuildWithFieldData({field_data});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    const auto& stats = ngram_index->GetBuildStats();
    EXPECT_EQ(stats.initial_backend, index::NgramBuildBackend::Regular);
    EXPECT_EQ(stats.final_backend, index::NgramBuildBackend::Regular);
    EXPECT_EQ(stats.replay_count, 0);
    EXPECT_GT(stats.preflight_required_bytes, kSoftLimitBytes);
    EXPECT_EQ(ngram_index->Count(), values.size());
}

TEST(NgramIndex, DirectMemoryGateReplaysJsonFromRowZeroAtBatchProgressBoundaries) {
    constexpr size_t kRowCount = 4 * 512;
    const auto values = MakeReplayRows(kRowCount);
    std::vector<std::string> json_rows;
    json_rows.reserve(values.size());
    for (const auto& value : values) {
        json_rows.push_back(fmt::format(R"({{"a":"{}"}})", value));
    }
    const auto field_data = MakeJsonStringFieldData(json_rows);

    for (size_t applied_batches : {size_t{1}, size_t{2}, size_t{3}}) {
        SCOPED_TRACE(fmt::format("applied_batches={}", applied_batches));
        auto params = index::NgramParams{
            .loading_index = false,
            .min_gram = 3,
            .max_gram = 4,
            .build_mode = index::NgramBuildMode::ForceDirect,
            .direct_soft_limit_bytes =
                ReplayLimitAfterBatches(applied_batches),
        };
        auto ngram_index = CreateNgramIndexForBatchBuild(
            DataType::JSON, false, "/a", params);

        ngram_index->BuildWithFieldData({field_data});
        ngram_index->finish();
        ngram_index->create_reader(milvus::index::SetBitsetSealed);

        const auto& stats = ngram_index->GetBuildStats();
        EXPECT_EQ(stats.initial_backend, index::NgramBuildBackend::Direct);
        EXPECT_EQ(stats.final_backend, index::NgramBuildBackend::Regular);
        EXPECT_EQ(stats.replay_count, 1);
        EXPECT_EQ(stats.direct_rows_applied, applied_batches * 512);
        EXPECT_EQ(stats.preflight_required_bytes, 0);
        EXPECT_EQ(ngram_index->GetAvgRowSize(), 64);
        EXPECT_EQ(ngram_index->Count(), kRowCount);
        const auto marker = QueryNgramPostings(*ngram_index, "replay-marker");
        ASSERT_EQ(marker.count(), 1);
        EXPECT_TRUE(marker[777]);
    }
}

TEST(NgramIndex, InvalidUtf8IsNotConvertedToMemoryReplay) {
    constexpr size_t kRowCount = 2 * 512;
    auto values = MakeReplayRows(kRowCount);
    values[512].assign(kHighDistinctRowBytes, 'x');
    values[512][0] = static_cast<char>(0xff);
    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(values.data(), kRowCount);
    auto params = index::NgramParams{
        .loading_index = false,
        .min_gram = 3,
        .max_gram = 4,
        .build_mode = index::NgramBuildMode::ForceDirect,
        .direct_soft_limit_bytes = ReplayLimitAfterBatches(1),
    };
    auto ngram_index = CreateNgramIndexForBatchBuild(
        DataType::VARCHAR, false, "", params);

    try {
        ngram_index->BuildWithFieldData({field_data});
        FAIL() << "expected invalid UTF-8 to fail the NGRAM build";
    } catch (const SegcoreError& error) {
        EXPECT_NE(std::string(error.what()).find("invalid UTF-8 at row 512"),
                  std::string::npos);
    }

    const auto& stats = ngram_index->GetBuildStats();
    EXPECT_EQ(stats.initial_backend, index::NgramBuildBackend::Direct);
    EXPECT_EQ(stats.final_backend, index::NgramBuildBackend::Direct);
    EXPECT_EQ(stats.replay_count, 0);
    EXPECT_EQ(stats.direct_rows_applied, 512);
}

TEST(NgramIndex, DISABLED_HighDistinctAutoAllowsDirectAt50K) {
    RunHighDistinctBenchmark(
        50'000,
        index::NgramBuildMode::Auto,
        index::DEFAULT_NGRAM_DIRECT_SOFT_LIMIT_BYTES,
        index::NgramBuildBackend::Direct,
        index::NgramBuildBackend::Direct,
        0);
}

TEST(NgramIndex, DISABLED_HighDistinctAutoRejectsDirectAt100K) {
    RunHighDistinctBenchmark(
        100'000,
        index::NgramBuildMode::Auto,
        index::DEFAULT_NGRAM_DIRECT_SOFT_LIMIT_BYTES,
        index::NgramBuildBackend::Regular,
        index::NgramBuildBackend::Regular,
        0);
}

TEST(NgramIndex, DISABLED_HighDistinctForceDirectReplaysAt50K) {
    RunHighDistinctBenchmark(50'000,
                             index::NgramBuildMode::ForceDirect,
                             uint64_t{768} * 1024 * 1024,
                             index::NgramBuildBackend::Direct,
                             index::NgramBuildBackend::Regular,
                             1);
}

TEST(NgramIndex, BatchBuildPreservesEveryJsonRowId) {
    const std::vector<std::optional<std::string>> json_rows = {
        R"({"a":"alpha"})",
        std::nullopt,
        R"({"a":null})",
        R"({"b":"missing"})",
        R"({"a":123})",
        R"({"a":""})",
        R"({"a":"omega"})",
        R"({"b":"trailing"})",
    };
    arrow::BinaryBuilder builder;
    for (const auto& row : json_rows) {
        if (!row.has_value()) {
            ASSERT_TRUE(builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(
                builder.Append(row->data(), static_cast<int32_t>(row->size()))
                    .ok());
        }
    }
    std::shared_ptr<arrow::Array> json_array;
    ASSERT_TRUE(builder.Finish(&json_array).ok());
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    json_field->FillFieldData(json_array);

    auto ngram_index =
        CreateNgramIndexForBatchBuild(DataType::JSON, true, "/a");
    ngram_index->BuildWithFieldData({json_field});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    ASSERT_EQ(ngram_index->Count(), json_rows.size());
    auto nulls = ngram_index->IsNull();
    ASSERT_EQ(nulls.size(), json_rows.size());
    for (size_t i = 0; i < json_rows.size(); ++i) {
        EXPECT_EQ(nulls[i], i == 1) << "row " << i;
    }

    auto alpha = QueryNgramPostings(*ngram_index, "al");
    EXPECT_EQ(alpha.count(), 1);
    EXPECT_TRUE(alpha[0]);
    auto omega = QueryNgramPostings(*ngram_index, "om");
    EXPECT_EQ(omega.count(), 1);
    EXPECT_TRUE(omega[6]);
}

void
test_ngram_with_data(const boost::container::vector<std::string>& data,
                     const std::string& literal,
                     proto::plan::OpType op_type,
                     const std::vector<bool>& expected_result,
                     bool forward_to_br = false) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;
    int64_t index_id = 5000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id.get(),
                                                      DataType::VARCHAR,
                                                      DataType::NONE,
                                                      false);
    auto index_meta = gen_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    std::string root_path = TestLocalPath;
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    size_t nb = data.size();

    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                          partition_id,
                                                          segment_id,
                                                          field_id.get(),
                                                          {field_data},
                                                          cm);
    segment->LoadFieldData(field_data_info);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}{}/{}/{}/{}/{}",
                           TestLocalPath,
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id.get(),
                           log_id);
    };

    auto log_path = get_binlog_path(0);

    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    auto index_size = 0;
    {
        Config config;
        config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        auto ngram_params = index::NgramParams{false, 2, 4};
        auto index =
            std::make_shared<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Build(config);

        auto create_index_result = index->UploadUnified({});
        auto memSize = create_index_result->GetMemSize();
        index_size = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(index_size, 0);
        index_files = create_index_result->GetIndexFiles();
    }

    {
        Config config;
        config[milvus::index::INDEX_FILES] = index_files;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;

        auto ngram_params = index::NgramParams{true, 2, 4};
        auto index =
            std::make_unique<index::NgramInvertedIndex>(ctx, ngram_params);
        index->LoadUnified(config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);

        exec::SegmentExpr segment_expr(std::move(std::vector<exec::ExprPtr>{}),
                                       "SegmentExpr",
                                       nullptr,
                                       segment.get(),
                                       field_id,
                                       {},
                                       DataType::VARCHAR,
                                       nb,
                                       8192,
                                       0);
        if (op_type != proto::plan::OpType::Equal) {
            std::optional<TargetBitmap> bitset_opt =
                index->ExecuteQueryForUT(literal, op_type, &segment_expr);
            if (forward_to_br) {
                ASSERT_TRUE(!bitset_opt.has_value());
            } else {
                auto bitset = std::move(bitset_opt.value());
                for (size_t i = 0; i < nb; i++) {
                    ASSERT_EQ(bitset[i], expected_result[i]);
                }
            }
        }
    }

    {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
            {milvus::index::MIN_GRAM, "2"},
            {milvus::index::MAX_GRAM, "4"},
            {milvus::LOAD_PRIORITY, "HIGH"},
            {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"},
        };
        milvus::segcore::LoadIndexInfo load_index_info{};
        load_index_info.collection_id = collection_id;
        load_index_info.partition_id = partition_id;
        load_index_info.segment_id = segment_id;
        load_index_info.field_id = field_id.get();
        load_index_info.field_type = DataType::VARCHAR;
        load_index_info.enable_mmap = true;
        load_index_info.mmap_dir_path = TestLocalPath + "mmap";
        load_index_info.index_id = index_id;
        load_index_info.index_build_id = index_build_id;
        load_index_info.index_version = index_version;
        load_index_info.index_params = index_params;
        load_index_info.index_files = index_files;
        load_index_info.schema = field_meta.field_schema;
        load_index_info.index_size = index_size;

        uint8_t trace_id[16] = {0};
        uint8_t span_id[8] = {0};
        trace_id[0] = 1;
        span_id[0] = 2;
        CTraceContext trace{};
        trace.traceID = trace_id;
        trace.spanID = span_id;
        trace.traceFlags = 0;
        auto cload_index_info = static_cast<CLoadIndexInfo>(&load_index_info);
        AppendIndexV2(trace, cload_index_info);
        segment->LoadIndex(load_index_info);

        auto unary_range_expr = test::GenUnaryRangeExpr(op_type, literal);
        auto column_info = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);
        auto parser = ProtoParser(schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        BitsetType final;
        final = ExecuteQueryExpr(parsed, segment.get(), nb, MAX_TIMESTAMP);
        for (size_t i = 0; i < nb; i++) {
            if (final[i] != expected_result[i]) {
                std::cout << "final[" << i << "] = " << final[i]
                          << ", expected_result[" << i
                          << "] = " << expected_result[i] << std::endl;
            }
            ASSERT_EQ(final[i], expected_result[i]);
        }
    }
}

TEST(NgramIndex, TestNgramWikiEpisode) {
    boost::container::vector<std::string> data;
    data.push_back(
        "'Indira Davelba Murillo Alvarado (Tegucigalpa, "
        "the youngest of eight siblings. She attended primary school at the "
        "Escuela 14 de Julio, and her secondary studies at the Instituto "
        "school called \"Indi del Bosque\", where she taught the children of "
        "Honduran women'");
    data.push_back(
        "Richmond Green Secondary School is a public secondary school in "
        "Richmond Hill, Ontario, Canada.");
    data.push_back(
        "The Gymnasium in 2002 Gymnasium Philippinum or Philippinum High "
        "School is an almost 500-year-old secondary school in Marburg, Hesse, "
        "Germany.");
    data.push_back(
        "Sir Winston Churchill Secondary School is a Canadian secondary school "
        "located in St. Catharines, Ontario.");
    data.push_back("Sir Winston Churchill Secondary School");

    // within min-max_gram
    {
        // equal, all should fail
        std::vector<bool> expected_result{false, false, false, false, false};
        test_ngram_with_data(
            data, "ary", proto::plan::OpType::Equal, expected_result);

        // inner match
        expected_result = {true, true, true, true, true};
        test_ngram_with_data(
            data, "ary", proto::plan::OpType::InnerMatch, expected_result);

        expected_result = {false, true, false, true, true};
        test_ngram_with_data(
            data, "y S", proto::plan::OpType::InnerMatch, expected_result);

        expected_result = {true, true, true, true, false};
        test_ngram_with_data(
            data, "y s", proto::plan::OpType::InnerMatch, expected_result);

        // prefix
        expected_result = {false, false, false, true, true};
        test_ngram_with_data(
            data, "Sir", proto::plan::OpType::PrefixMatch, expected_result);

        // postfix
        expected_result = {false, false, false, false, true};
        test_ngram_with_data(
            data, "ool", proto::plan::OpType::PostfixMatch, expected_result);

        // match
        expected_result = {true, false, false, false, false};
        test_ngram_with_data(
            data, "%Alv%y s%", proto::plan::OpType::Match, expected_result);
    }

    // exceeds max_gram
    {
        // inner match
        std::vector<bool> expected_result{false, true, true, true, false};
        test_ngram_with_data(data,
                             "secondary school",
                             proto::plan::OpType::InnerMatch,
                             expected_result);

        // prefix
        expected_result = {false, false, false, true, true};
        test_ngram_with_data(data,
                             "Sir Winston",
                             proto::plan::OpType::PrefixMatch,
                             expected_result);

        // postfix
        expected_result = {false, false, true, false, false};
        test_ngram_with_data(data,
                             "Germany.",
                             proto::plan::OpType::PostfixMatch,
                             expected_result);

        // match
        expected_result = {true, true, true, true, false};
        test_ngram_with_data(data,
                             "%secondary%school%",
                             proto::plan::OpType::Match,
                             expected_result);
    }
}

TEST(NgramIndex, TestNgramSimple) {
    boost::container::vector<std::string> data(10000,
                                               "elementary school secondary");

    // all can be hit by ngram tantivy but will be filterred out by the second phase
    test_ngram_with_data(data,
                         "secondary school",
                         proto::plan::OpType::InnerMatch,
                         std::vector<bool>(10000, false));

    test_ngram_with_data(data,
                         "ele",
                         proto::plan::OpType::PrefixMatch,
                         std::vector<bool>(10000, true));

    test_ngram_with_data(data,
                         "%ary%sec%",
                         proto::plan::OpType::Match,
                         std::vector<bool>(10000, true));

    // should be forwarded to brute force
    test_ngram_with_data(data,
                         "%ary%s%",
                         proto::plan::OpType::Match,
                         std::vector<bool>(10000, true),
                         true);

    test_ngram_with_data(data,
                         "ary",
                         proto::plan::OpType::PostfixMatch,
                         std::vector<bool>(10000, true));
}

// Test that ngram index should only be used for like operations
// (Match, InnerMatch, PrefixMatch, PostfixMatch)
// and NOT for other operations (Equal, NotEqual, In, NotIn, etc.)
// Issue: https://github.com/milvus-io/milvus/issues/44020
TEST(NgramIndex, TestNonLikeExpressionsWithNgram) {
    boost::container::vector<std::string> data = {"apple",
                                                  "banana",
                                                  "cherry",
                                                  "date",
                                                  "elderberry",
                                                  "fig",
                                                  "grape",
                                                  "honeydew",
                                                  "kiwi",
                                                  "lemon"};

    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;
    int64_t index_id = 5000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id.get(),
                                                      DataType::VARCHAR,
                                                      DataType::NONE,
                                                      false);
    auto index_meta = gen_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    std::string root_path = TestLocalPath;
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    size_t nb = data.size();

    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                          partition_id,
                                                          segment_id,
                                                          field_id.get(),
                                                          {field_data},
                                                          cm);
    segment->LoadFieldData(field_data_info);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}{}/{}/{}/{}/{}",
                           TestLocalPath,
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id.get(),
                           log_id);
    };

    auto log_path = get_binlog_path(0);

    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    // Build ngram index
    {
        Config config;
        config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        auto ngram_params = index::NgramParams{false, 2, 4};
        auto index =
            std::make_shared<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Build(config);

        auto create_index_result = index->UploadUnified({});
        index_files = create_index_result->GetIndexFiles();
    }

    // Load index and test
    {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
            {milvus::index::MIN_GRAM, "2"},
            {milvus::index::MAX_GRAM, "4"},
            {milvus::LOAD_PRIORITY, "HIGH"},
            {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"},
        };
        milvus::segcore::LoadIndexInfo load_index_info{};
        load_index_info.collection_id = collection_id;
        load_index_info.partition_id = partition_id;
        load_index_info.segment_id = segment_id;
        load_index_info.field_id = field_id.get();
        load_index_info.field_type = DataType::VARCHAR;
        load_index_info.enable_mmap = true;
        load_index_info.mmap_dir_path = TestLocalPath + "mmap";
        load_index_info.index_id = index_id;
        load_index_info.index_build_id = index_build_id;
        load_index_info.index_version = index_version;
        load_index_info.index_params = index_params;
        load_index_info.index_files = index_files;
        load_index_info.schema = field_meta.field_schema;
        load_index_info.index_size = 1024 * 1024 * 1024;

        uint8_t trace_id[16] = {0};
        uint8_t span_id[8] = {0};
        trace_id[0] = 1;
        span_id[0] = 2;
        CTraceContext trace{};
        trace.traceID = trace_id;
        trace.spanID = span_id;
        trace.traceFlags = 0;
        auto cload_index_info = static_cast<CLoadIndexInfo>(&load_index_info);
        AppendIndexV2(trace, cload_index_info);
        segment->LoadIndex(load_index_info);

        // Test: TermFilterExpr (IN operator)
        {
            std::vector<proto::plan::GenericValue> values;
            proto::plan::GenericValue val1;
            val1.set_string_val("apple");
            values.push_back(val1);
            proto::plan::GenericValue val2;
            val2.set_string_val("banana");
            values.push_back(val2);
            proto::plan::GenericValue val3;
            val3.set_string_val("cherry");
            values.push_back(val3);

            auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR), values);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, term_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Only apple, banana, cherry should match
            for (size_t i = 0; i < nb; i++) {
                if (i < 3) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: UnaryRangeExpr with Equal operator
        {
            auto unary_range_expr =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr->set_allocated_column_info(column_info);
            auto expr = test::GenExpr();
            expr->set_allocated_unary_range_expr(unary_range_expr);
            auto parser = ProtoParser(schema);
            auto typed_expr = parser.ParseExprs(*expr);
            auto parsed = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, typed_expr);
            BitsetType final =
                ExecuteQueryExpr(parsed, segment.get(), nb, MAX_TIMESTAMP);
            // Only apple should match (exact match)
            for (size_t i = 0; i < nb; i++) {
                if (i == 0) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: BinaryRangeFilterExpr
        {
            proto::plan::GenericValue lower_val;
            lower_val.set_string_val("cherry");
            proto::plan::GenericValue upper_val;
            upper_val.set_string_val("grape");

            auto binary_range_expr =
                std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
                    milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                    lower_val,
                    upper_val,
                    true,
                    true);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, binary_range_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Strings between "cherry" and "grape" inclusive: cherry, date, elderberry, fig, grape
            for (size_t i = 0; i < nb; i++) {
                if (i >= 2 && i <= 6) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: LogicalBinaryExpr with AND
        {
            // Create Equal expression
            auto unary_range_expr1 =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info1 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr1->set_allocated_column_info(column_info1);
            auto expr1 = test::GenExpr();
            expr1->set_allocated_unary_range_expr(unary_range_expr1);
            auto parser1 = ProtoParser(schema);
            auto typed_expr1 = parser1.ParseExprs(*expr1);

            // Create NotEqual expression
            auto unary_range_expr2 = test::GenUnaryRangeExpr(
                proto::plan::OpType::NotEqual, "banana");
            auto column_info2 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr2->set_allocated_column_info(column_info2);
            auto expr2 = test::GenExpr();
            expr2->set_allocated_unary_range_expr(unary_range_expr2);
            auto parser2 = ProtoParser(schema);
            auto typed_expr2 = parser2.ParseExprs(*expr2);

            // Create LogicalBinaryExpr with AND
            auto logical_and_expr =
                std::make_shared<milvus::expr::LogicalBinaryExpr>(
                    milvus::expr::LogicalBinaryExpr::OpType::And,
                    typed_expr1,
                    typed_expr2);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical_and_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Only apple should match (apple == "apple" AND apple != "banana")
            for (size_t i = 0; i < nb; i++) {
                if (i == 0) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: LogicalUnaryExpr with NOT
        {
            // Create Equal expression
            auto unary_range_expr =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr->set_allocated_column_info(column_info);
            auto expr = test::GenExpr();
            expr->set_allocated_unary_range_expr(unary_range_expr);
            auto parser = ProtoParser(schema);
            auto typed_expr = parser.ParseExprs(*expr);

            // Create LogicalUnaryExpr with NOT
            auto logical_not_expr =
                std::make_shared<milvus::expr::LogicalUnaryExpr>(
                    milvus::expr::LogicalUnaryExpr::OpType::LogicalNot,
                    typed_expr);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical_not_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // All except apple should match (NOT (field == "apple"))
            for (size_t i = 0; i < nb; i++) {
                if (i != 0) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: LogicalBinaryExpr with OR
        {
            // Create Equal expression
            auto unary_range_expr1 =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info1 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr1->set_allocated_column_info(column_info1);
            auto expr1 = test::GenExpr();
            expr1->set_allocated_unary_range_expr(unary_range_expr1);
            auto parser1 = ProtoParser(schema);
            auto typed_expr1 = parser1.ParseExprs(*expr1);

            // Create Equal expression for "banana"
            auto unary_range_expr2 =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "banana");
            auto column_info2 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr2->set_allocated_column_info(column_info2);
            auto expr2 = test::GenExpr();
            expr2->set_allocated_unary_range_expr(unary_range_expr2);
            auto parser2 = ProtoParser(schema);
            auto typed_expr2 = parser2.ParseExprs(*expr2);

            // Create LogicalBinaryExpr with OR
            auto logical_or_expr =
                std::make_shared<milvus::expr::LogicalBinaryExpr>(
                    milvus::expr::LogicalBinaryExpr::OpType::Or,
                    typed_expr1,
                    typed_expr2);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical_or_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Apple and banana should match (apple == "apple" OR field == "banana")
            for (size_t i = 0; i < nb; i++) {
                if (i == 0 || i == 1) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: NullExpr with IS_NULL
        {
            auto null_expr = std::make_shared<milvus::expr::NullExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                proto::plan::NullExpr_NullOp_IsNull);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, null_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // None should match since we have no null values
            for (size_t i = 0; i < nb; i++) {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }

        // Test: NullExpr with IS_NOT_NULL
        {
            auto null_expr = std::make_shared<milvus::expr::NullExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                proto::plan::NullExpr_NullOp_IsNotNull);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, null_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // All should match since we have no null values
            for (size_t i = 0; i < nb; i++) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            }
        }

        // // Test: ExistsExpr
        // {
        //     auto exists_expr = std::make_shared<milvus::expr::ExistsExpr>(
        //         milvus::expr::ColumnInfo(field_id, DataType::VARCHAR));
        //     auto plan = std::make_shared<plan::FilterBitsNode>(
        //         DEFAULT_PLANNODE_ID, exists_expr);

        //     BitsetType final = ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        //     // All should match since the field exists for all rows
        //     for (size_t i = 0; i < nb; i++) {
        //         ASSERT_TRUE(final[i]) << "Expected true at index " << i;
        //     }
        // }

        // Test: AlwaysTrueExpr
        {
            auto always_true_expr =
                std::make_shared<milvus::expr::AlwaysTrueExpr>();
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, always_true_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // All should match
            for (size_t i = 0; i < nb; i++) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            }
        }
    }
}

TEST(NgramIndex, TestNgramJson) {
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"({"a": "Milvus project"})",
        R"({"a": "Zilliz cloud"})",
        R"({"a": "Query Node"})",
        R"({"a": "Data Node"})",
        R"({"a": [1, 2, 3]})",
        R"({"a": {"b": 1}})",
        R"({"a": 1001})",
        R"({"a": true})",
        R"({"a": "Milvus", "b": "Zilliz cloud"})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index::INVERTED_INDEX_TYPE;
    create_index_info.json_cast_type = JsonCastType::FromString("VARCHAR");
    create_index_info.json_path = json_path;
    create_index_info.ngram_params =
        std::optional<index::NgramParams>{index::NgramParams{false, 2, 3}};
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        create_index_info, file_manager_ctx);

    auto ngram_index = std::unique_ptr<index::NgramInvertedIndex>(
        static_cast<index::NgramInvertedIndex*>(inv_index.release()));

    std::vector<milvus::Json> jsons;
    for (auto& json : json_raw_data) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);
    ngram_index->BuildWithFieldData({json_field});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.cache_index =
        CreateTestCacheIndex("", std::move(ngram_index));

    std::map<std::string, std::string> index_params{
        {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
        {milvus::index::MIN_GRAM, "2"},
        {milvus::index::MAX_GRAM, "3"},
        {milvus::LOAD_PRIORITY, "HIGH"},
        {JSON_PATH, json_path},
        {JSON_CAST_TYPE, "VARCHAR"}};
    load_index_info.index_params = index_params;

    segment->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    std::vector<std::tuple<proto::plan::GenericValue,
                           std::vector<int64_t>,
                           proto::plan::OpType>>
        test_cases;
    proto::plan::GenericValue value;
    value.set_string_val("liz");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{}, proto::plan::OpType::Equal));

    value.set_string_val("nothing");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{}, proto::plan::OpType::InnerMatch));

    value.set_string_val("il");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{1, 2, 9}, proto::plan::OpType::InnerMatch));

    value.set_string_val("lliz");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{2}, proto::plan::OpType::InnerMatch));

    value.set_string_val("Zi");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{2}, proto::plan::OpType::PrefixMatch));

    value.set_string_val("Zilliz");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{2}, proto::plan::OpType::PrefixMatch));

    value.set_string_val("de");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{3, 4}, proto::plan::OpType::PostfixMatch));

    value.set_string_val("Node");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{3, 4}, proto::plan::OpType::PostfixMatch));

    value.set_string_val("%ery%ode%");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{3}, proto::plan::OpType::Match));

    for (auto& test_case : test_cases) {
        auto value = std::get<0>(test_case);
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            std::get<2>(test_case),
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
        auto expect_result = std::get<1>(test_case);
        EXPECT_EQ(result.count(), expect_result.size());
        for (auto& id : expect_result) {
            EXPECT_TRUE(result[id]);
        }
    }
}

// Test that ngram index should only be used for like operations on JSON fields
// and NOT for other operations (Equal, NotEqual, In, etc.)
TEST(NgramIndex, TestJsonNonLikeExpressionsWithNgram) {
    std::vector<std::string> json_raw_data = {R"({"name": "apple"})",
                                              R"({"name": "banana"})",
                                              R"({"name": "cherry"})",
                                              R"({"name": "date"})",
                                              R"({"name": "elderberry"})",
                                              R"({"name": "fig"})",
                                              R"({"name": "grape"})",
                                              R"({"name": "honeydew"})",
                                              R"({"name": "kiwi"})",
                                              R"({"name": "lemon"})"};

    auto json_path = "/name";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index::INVERTED_INDEX_TYPE;
    create_index_info.json_cast_type = JsonCastType::FromString("VARCHAR");
    create_index_info.json_path = json_path;
    create_index_info.ngram_params =
        std::optional<index::NgramParams>{index::NgramParams{false, 2, 4}};
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        create_index_info, file_manager_ctx);

    auto ngram_index = std::unique_ptr<index::NgramInvertedIndex>(
        static_cast<index::NgramInvertedIndex*>(inv_index.release()));

    std::vector<milvus::Json> jsons;
    for (auto& json : json_raw_data) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);
    ngram_index->BuildWithFieldData({json_field});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.cache_index =
        CreateTestCacheIndex("", std::move(ngram_index));

    std::map<std::string, std::string> index_params{
        {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
        {milvus::index::MIN_GRAM, "2"},
        {milvus::index::MAX_GRAM, "4"},
        {milvus::LOAD_PRIORITY, "HIGH"},
        {JSON_PATH, json_path},
        {JSON_CAST_TYPE, "VARCHAR"}};
    load_index_info.index_params = index_params;

    segment->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    size_t nb = json_raw_data.size();

    // Test: JSON Equal operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only first record should match (exact match for "apple")
        EXPECT_EQ(result.count(), 1);
        EXPECT_TRUE(result[0]);
    }

    // Test: JSON NotEqual operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::NotEqual,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All except first record should match
        EXPECT_EQ(result.count(), 9);
        EXPECT_FALSE(result[0]);
        for (size_t i = 1; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON GreaterThan operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("fig");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Records with names > "fig": grape, honeydew, kiwi, lemon
        EXPECT_EQ(result.count(), 4);
        for (size_t i = 6; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON LessThan operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("date");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Records with names < "date": apple, banana, cherry
        EXPECT_EQ(result.count(), 3);
        for (size_t i = 0; i < 3; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON TermFilterExpr (IN operation)
    {
        std::vector<proto::plan::GenericValue> values;
        proto::plan::GenericValue val1, val2, val3;
        val1.set_string_val("apple");
        val2.set_string_val("cherry");
        val3.set_string_val("grape");
        values.push_back(val1);
        values.push_back(val2);
        values.push_back(val3);

        auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            values);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           term_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only apple, cherry, grape should match
        EXPECT_EQ(result.count(), 3);
        EXPECT_TRUE(result[0]);  // apple
        EXPECT_TRUE(result[2]);  // cherry
        EXPECT_TRUE(result[6]);  // grape
    }

    // Test: JSON BinaryRangeFilterExpr
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_string_val("cherry");
        proto::plan::GenericValue upper_val;
        upper_val.set_string_val("grape");

        auto binary_range_expr =
            std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, {"name"}, true),
                lower_val,
                upper_val,
                true,
                true);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           binary_range_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Strings between "cherry" and "grape" inclusive: cherry, date, elderberry, fig, grape
        EXPECT_EQ(result.count(), 5);
        for (size_t i = 2; i <= 6; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON NullExpr IS_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::NullExpr_NullOp_IsNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // None should match since all have non-null names
        EXPECT_EQ(result.count(), 0);
    }

    // Test: JSON NullExpr IS_NOT_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::NullExpr_NullOp_IsNotNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All should match since all have non-null names
        EXPECT_EQ(result.count(), 10);
        for (size_t i = 0; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON ExistsExpr
    {
        auto exists_expr = std::make_shared<milvus::expr::ExistsExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true));
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           exists_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All should match since all have the "name" field
        EXPECT_EQ(result.count(), 10);
        for (size_t i = 0; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON LogicalBinaryExpr with AND
    {
        // Create Equal expression for "apple"
        proto::plan::GenericValue val1;
        val1.set_string_val("apple");
        auto expr1 = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            val1,
            std::vector<proto::plan::GenericValue>{});

        // Create NotEqual expression for "banana"
        proto::plan::GenericValue val2;
        val2.set_string_val("banana");
        auto expr2 = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::NotEqual,
            val2,
            std::vector<proto::plan::GenericValue>{});

        // Create LogicalBinaryExpr with AND
        auto logical_and_expr =
            std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_and_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only apple should match (name == "apple" AND name != "banana")
        EXPECT_EQ(result.count(), 1);
        EXPECT_TRUE(result[0]);
    }

    // Test: JSON LogicalUnaryExpr with NOT
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto equal_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        // Create LogicalUnaryExpr with NOT
        auto logical_not_expr =
            std::make_shared<milvus::expr::LogicalUnaryExpr>(
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, equal_expr);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_not_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All except apple should match (NOT (name == "apple"))
        EXPECT_EQ(result.count(), 9);
        EXPECT_FALSE(result[0]);
        for (size_t i = 1; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }
}

// ============== Ngram Index Pattern Matching Consistency Tests ==============
// These tests verify that ngram index pattern matching produces the same results
// as RE2 and LikePatternMatcher. Ngram index uses a two-phase approach:
// 1. Phase 1: Use ngram matching to get candidate rows (may have false positives)
// 2. Phase 2: Use LikePatternMatcher to filter candidates (exact matching)
// The final result must be consistent with direct RE2/LikePatternMatcher matching.

TEST(NgramPatternMatchConsistency, MatchersMustAgree) {
    // Test data - strings that are long enough for ngram matching (min_gram=2)
    boost::container::vector<std::string> test_data = {
        "hello",      "hello world",  "world hello", "say hello there",
        "helloworld", "worldhello",   "testing",     "tested",
        "tester",     "test case",    "application", "apple pie",
        "pineapple",  "banana split", "aaaa",        "aaa",
        "aaab",       "abaa",         "abab",        "ababab",
        "xaax",       "xaaax",
    };

    // Patterns to test - all have segments >= 2 chars for ngram
    std::vector<std::pair<std::string, proto::plan::OpType>> test_cases = {
        // PrefixMatch tests
        {"hello", proto::plan::OpType::PrefixMatch},
        {"test", proto::plan::OpType::PrefixMatch},
        {"app", proto::plan::OpType::PrefixMatch},
        {"ab", proto::plan::OpType::PrefixMatch},

        // PostfixMatch tests
        {"world", proto::plan::OpType::PostfixMatch},
        {"ing", proto::plan::OpType::PostfixMatch},
        {"ple", proto::plan::OpType::PostfixMatch},
        {"ab", proto::plan::OpType::PostfixMatch},

        // InnerMatch tests
        {"ello", proto::plan::OpType::InnerMatch},
        {"test", proto::plan::OpType::InnerMatch},
        {"app", proto::plan::OpType::InnerMatch},
        {"aa", proto::plan::OpType::InnerMatch},
        {"ab", proto::plan::OpType::InnerMatch},

        // Match (LIKE pattern) tests - patterns with segments >= 2 chars
        {"hello%", proto::plan::OpType::Match},
        {"%world", proto::plan::OpType::Match},
        {"%ello%", proto::plan::OpType::Match},
        {"test%ing", proto::plan::OpType::Match},
        {"%aa%aa%", proto::plan::OpType::Match},  // Overlapping pattern
        {"ab%ab", proto::plan::OpType::Match},
        {"%ab%ab%", proto::plan::OpType::Match},
    };

    for (const auto& [pattern, op_type] : test_cases) {
        // Compute expected results using RE2 and LikePatternMatcher
        std::vector<bool> re2_results;
        std::vector<bool> like_results;

        PatternMatchTranslator translator;
        std::string like_pattern;

        // Convert to LIKE pattern based on op_type
        switch (op_type) {
            case proto::plan::OpType::PrefixMatch:
                like_pattern = pattern + "%";
                break;
            case proto::plan::OpType::PostfixMatch:
                like_pattern = "%" + pattern;
                break;
            case proto::plan::OpType::InnerMatch:
                like_pattern = "%" + pattern + "%";
                break;
            case proto::plan::OpType::Match:
                like_pattern = pattern;
                break;
            default:
                continue;
        }

        auto regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(like_pattern);

        for (const auto& data : test_data) {
            re2_results.push_back(re2_matcher(data));
            like_results.push_back(like_matcher(data));
        }

        // Verify RE2 and LikePatternMatcher agree
        for (size_t i = 0; i < test_data.size(); i++) {
            EXPECT_EQ(re2_results[i], like_results[i])
                << "RE2/LikePatternMatcher mismatch for ngram test:\n"
                << "  pattern=\"" << pattern
                << "\", op_type=" << static_cast<int>(op_type) << "\n"
                << "  like_pattern=\"" << like_pattern << "\"\n"
                << "  data=\"" << test_data[i] << "\"\n"
                << "  RE2=" << re2_results[i] << ", Like=" << like_results[i];
        }

        // Test with ngram index
        test_ngram_with_data(test_data, pattern, op_type, like_results);
    }
}

TEST(NgramPatternMatchConsistency, OverlappingPatterns) {
    // Test data specifically for overlapping patterns
    boost::container::vector<std::string> test_data = {
        "aa",
        "aaa",
        "aaaa",
        "aaaaa",
        "aaaaaa",
        "ab",
        "aba",
        "abab",
        "ababab",
        "abba",
        "aab",
        "baa",
        "xaax",
        "xaaax",
        "xaaaax",
        "abcabc",
        "abcabcabc",
    };

    // Overlapping patterns with segments >= 2 chars
    std::vector<std::string> patterns = {
        "%aa%aa%",     // Two overlapping "aa"
        "%aa%aa%aa%",  // Three overlapping "aa"
        "%ab%ab%",     // Two "ab"
        "%ab%ab%ab%",  // Three "ab"
        "aa%aa",       // Prefix and suffix both "aa"
        "ab%ab",       // Prefix and suffix both "ab"
        "%abc%abc%",   // Two "abc"
    };

    for (const auto& pattern : patterns) {
        // Compute expected results
        std::vector<bool> expected_results;

        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            // RE2 and LikePatternMatcher must agree
            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher mismatch for overlapping pattern:\n"
                << "  pattern=\"" << pattern << "\"\n"
                << "  data=\"" << data << "\"\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            expected_results.push_back(like_result);
        }

        // Test with ngram index
        test_ngram_with_data(
            test_data, pattern, proto::plan::OpType::Match, expected_results);
    }
}

TEST(NgramPatternMatchConsistency, UTF8Patterns) {
    // UTF-8 test data with strings long enough for ngram
    boost::container::vector<std::string> test_data = {
        "café latte",    // 2-byte UTF-8
        "hello café",    // Mixed
        "你好世界",      // 3-byte UTF-8 (Chinese)
        "test你好test",  // Mixed ASCII and Chinese
        "emoji😀test",    // 4-byte UTF-8 (emoji)
        "normal text",
        "café café",  // Repeated UTF-8
        "你好你好",   // Repeated Chinese
    };

    // Patterns with UTF-8 characters (segments must be >= 2 chars)
    std::vector<std::pair<std::string, proto::plan::OpType>> test_cases = {
        {"café", proto::plan::OpType::PrefixMatch},
        {"café", proto::plan::OpType::InnerMatch},
        {"你好", proto::plan::OpType::PrefixMatch},
        {"你好", proto::plan::OpType::InnerMatch},
        {"%café%", proto::plan::OpType::Match},
        {"%你好%", proto::plan::OpType::Match},
        {"%café%café%", proto::plan::OpType::Match},  // Overlapping UTF-8
        {"%你好%你好%", proto::plan::OpType::Match},  // Overlapping Chinese
    };

    for (const auto& [pattern, op_type] : test_cases) {
        // Compute expected results
        std::vector<bool> expected_results;

        PatternMatchTranslator translator;
        std::string like_pattern;

        switch (op_type) {
            case proto::plan::OpType::PrefixMatch:
                like_pattern = pattern + "%";
                break;
            case proto::plan::OpType::PostfixMatch:
                like_pattern = "%" + pattern;
                break;
            case proto::plan::OpType::InnerMatch:
                like_pattern = "%" + pattern + "%";
                break;
            case proto::plan::OpType::Match:
                like_pattern = pattern;
                break;
            default:
                continue;
        }

        auto regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(like_pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher UTF-8 mismatch:\n"
                << "  pattern=\"" << pattern << "\"\n"
                << "  data=\"" << data << "\"\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            expected_results.push_back(like_result);
        }

        // Test with ngram index
        test_ngram_with_data(test_data, pattern, op_type, expected_results);
    }
}

TEST(NgramPatternMatchConsistency, CanHandleLiteralUsesUtf8CharacterCount) {
    auto ngram_index = CreateNgramIndexForCanHandleLiteral(2, 4);

    const std::string single_chinese = "测";
    const std::string two_chinese = "测试";
    ASSERT_EQ(single_chinese.size(), 3);
    ASSERT_EQ(Utf8CharCount(single_chinese.data(), single_chinese.size()), 1);
    ASSERT_EQ(Utf8CharCount(two_chinese.data(), two_chinese.size()), 2);

    struct TestCase {
        std::string literal;
        proto::plan::OpType op_type;
        bool expected_can_handle;
    };

    std::vector<TestCase> test_cases = {
        {single_chinese, proto::plan::OpType::InnerMatch, false},
        {single_chinese, proto::plan::OpType::PrefixMatch, false},
        {single_chinese, proto::plan::OpType::PostfixMatch, false},
        {"%" + single_chinese + "%", proto::plan::OpType::Match, false},
        {single_chinese, proto::plan::OpType::RegexMatch, false},
        {two_chinese, proto::plan::OpType::InnerMatch, true},
        {"%" + two_chinese + "%", proto::plan::OpType::Match, true},
        {two_chinese, proto::plan::OpType::RegexMatch, true},
    };

    for (const auto& test_case : test_cases) {
        EXPECT_EQ(
            ngram_index->CanHandleLiteral(test_case.literal, test_case.op_type),
            test_case.expected_can_handle)
            << "literal=\"" << test_case.literal << "\""
            << ", op_type=" << static_cast<int>(test_case.op_type)
            << ", byte_len=" << test_case.literal.size() << ", utf8_char_count="
            << Utf8CharCount(test_case.literal.data(),
                             test_case.literal.size());
    }
}

TEST(NgramPatternMatchConsistency, EscapeSequences) {
    // Test data with special characters
    boost::container::vector<std::string> test_data = {
        "100% complete",
        "50% off sale",
        "file_name.txt",
        "path\\to\\file",
        "normal text",
        "%percent%",
        "_underscore_",
        "test\\escape",
    };

    // Patterns with escape sequences (segments >= 2 chars)
    std::vector<std::string> patterns = {
        "%100\\%%",   // Contains literal %
        "%file\\_%",  // Contains literal _
        "%\\\\%",     // Contains backslash
    };

    for (const auto& pattern : patterns) {
        std::vector<bool> expected_results;

        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher escape mismatch:\n"
                << "  pattern=\"" << pattern << "\"\n"
                << "  data=\"" << data << "\"\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            expected_results.push_back(like_result);
        }

        // Test with ngram index.
        // Pattern %\\\\% has literal segment "\" (1 byte) < min_gram(2),
        // so the ngram index correctly cannot handle it and forwards to
        // brute-force matching.
        bool forward_to_br = (pattern == "%\\\\%");
        test_ngram_with_data(test_data,
                             pattern,
                             proto::plan::OpType::Match,
                             expected_results,
                             forward_to_br);
    }
}

// ============== Performance Benchmark: Ngram vs Tantivy vs Brute-Force ==============
// Run with: --gtest_filter="*NgramBenchmark*"

TEST(NgramBenchmark, NgramVsTantivyVsBruteForce) {
    // Generate random strings
    const size_t N = 10000;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    std::uniform_int_distribution<int> len_dist(40, 120);
    boost::container::vector<std::string> data;
    data.reserve(N);
    for (size_t i = 0; i < N; i++) {
        size_t len = len_dist(rng);
        std::string s;
        s.reserve(len);
        for (size_t j = 0; j < len; j++) s += static_cast<char>(char_dist(rng));
        data.push_back(std::move(s));
    }

    // --- Ngram Index Setup ---
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id.get(),
                                                      DataType::VARCHAR,
                                                      DataType::NONE,
                                                      false);
    auto index_meta = gen_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    std::string root_path = "/tmp/test-ngram-bench/";
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                          partition_id,
                                                          segment_id,
                                                          field_id.get(),
                                                          {field_data},
                                                          cm);
    segment->LoadFieldData(field_data_info);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto log_path = fmt::format("{}/{}/{}/{}/{}",
                                collection_id,
                                partition_id,
                                segment_id,
                                field_id.get(),
                                0);
    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    // Build ngram index
    {
        Config config;
        config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        auto ngram_params = index::NgramParams{
            .loading_index = false, .min_gram = 2, .max_gram = 4};
        auto index =
            std::make_shared<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Build(config);
        auto result = index->UploadUnified({});
        index_files = result->GetIndexFiles();
    }

    // Load ngram index
    Config load_config;
    load_config[milvus::index::INDEX_FILES] = index_files;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;
    auto load_ngram_params =
        index::NgramParams{.loading_index = true, .min_gram = 2, .max_gram = 4};
    auto ngram_index =
        std::make_unique<index::NgramInvertedIndex>(ctx, load_ngram_params);
    ngram_index->LoadUnified(load_config);

    // Build Tantivy index for comparison
    std::vector<std::string> data_vec(data.begin(), data.end());
    auto tantivy_index =
        std::make_unique<index::InvertedIndexTantivy<std::string>>();
    tantivy_index->BuildWithRawDataForUT(
        data_vec.size(), data_vec.data(), Config());

    // --- Benchmark Patterns ---
    struct BenchPattern {
        std::string name;
        std::string term;
        std::string like_pattern;
        proto::plan::OpType op_type;
    };

    std::vector<BenchPattern> bench_patterns = {
        {"LIKE: %ab%cd%ef%",
         "%ab%cd%ef%",
         "%ab%cd%ef%",
         proto::plan::OpType::Match},
        {"LIKE: %ab%cd%", "%ab%cd%", "%ab%cd%", proto::plan::OpType::Match},
        {"LIKE: abc%xyz%", "abc%xyz%", "abc%xyz%", proto::plan::OpType::Match},
        {"PREFIX: abc", "abc", "abc%", proto::plan::OpType::PrefixMatch},
        {"INNER: hello", "hello", "%hello%", proto::plan::OpType::InnerMatch},
        {"SUFFIX: xyz", "xyz", "%xyz", proto::plan::OpType::PostfixMatch},
    };

    const int W = 3, I = 5;
    std::cout << "\n====== Ngram vs Tantivy vs Brute-Force (" << N
              << " strings, avg 80 bytes) ======\n";

    for (const auto& bp : bench_patterns) {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(bp.like_pattern);
        RegexMatcher re2(regex_pattern);
        LikePatternMatcher like(bp.like_pattern);

        // RE2 brute-force
        double re2_us;
        int64_t re2_cnt;
        {
            volatile int64_t total = 0;
            for (int w = 0; w < W; w++)
                for (const auto& s : data_vec) total += re2(s) ? 1 : 0;
            total = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < I; i++)
                for (const auto& s : data_vec) total += re2(s) ? 1 : 0;
            re2_us = std::chrono::duration<double, std::micro>(
                         std::chrono::high_resolution_clock::now() - t0)
                         .count() /
                     I;
            re2_cnt = total / I;
        }

        // LikePatternMatcher brute-force
        double like_us;
        int64_t like_cnt;
        {
            volatile int64_t total = 0;
            for (int w = 0; w < W; w++)
                for (const auto& s : data_vec) total += like(s) ? 1 : 0;
            total = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < I; i++)
                for (const auto& s : data_vec) total += like(s) ? 1 : 0;
            like_us = std::chrono::duration<double, std::micro>(
                          std::chrono::high_resolution_clock::now() - t0)
                          .count() /
                      I;
            like_cnt = total / I;
        }

        // Tantivy index
        double tantivy_us;
        int64_t tantivy_cnt;
        {
            volatile int64_t total = 0;
            for (int w = 0; w < W; w++) {
                auto r = tantivy_index->PatternMatch(bp.term, bp.op_type);
                total += r.count();
            }
            total = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < I; i++) {
                auto r = tantivy_index->PatternMatch(bp.term, bp.op_type);
                total += r.count();
            }
            tantivy_us = std::chrono::duration<double, std::micro>(
                             std::chrono::high_resolution_clock::now() - t0)
                             .count() /
                         I;
            tantivy_cnt = total / I;
        }

        // Ngram index
        double ngram_us;
        int64_t ngram_cnt;
        {
            volatile int64_t total = 0;
            for (int w = 0; w < W; w++) {
                exec::SegmentExpr se(std::move(std::vector<exec::ExprPtr>{}),
                                     "SegmentExpr",
                                     nullptr,
                                     segment.get(),
                                     field_id,
                                     {},
                                     DataType::VARCHAR,
                                     N,
                                     8192,
                                     0);
                auto result =
                    ngram_index->ExecuteQueryForUT(bp.term, bp.op_type, &se);
                if (result.has_value())
                    total += result.value().count();
            }
            total = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < I; i++) {
                exec::SegmentExpr se(std::move(std::vector<exec::ExprPtr>{}),
                                     "SegmentExpr",
                                     nullptr,
                                     segment.get(),
                                     field_id,
                                     {},
                                     DataType::VARCHAR,
                                     N,
                                     8192,
                                     0);
                auto result =
                    ngram_index->ExecuteQueryForUT(bp.term, bp.op_type, &se);
                if (result.has_value())
                    total += result.value().count();
            }
            ngram_us = std::chrono::duration<double, std::micro>(
                           std::chrono::high_resolution_clock::now() - t0)
                           .count() /
                       I;
            ngram_cnt = total / I;
        }

        // Print results
        std::cout << "\n  " << bp.name << "\n"
                  << "  " << std::string(65, '-') << "\n"
                  << std::left << "  " << std::setw(30) << "Matcher"
                  << std::right << std::setw(15) << "total(us)" << std::setw(12)
                  << "matches"
                  << "\n"
                  << "  " << std::string(65, '-') << "\n";
        auto row = [](const std::string& n, double us, int64_t m) {
            std::cout << "  " << std::left << std::setw(30) << n << std::right
                      << std::setw(12) << std::fixed << std::setprecision(0)
                      << us << " us" << std::setw(12) << m << "\n";
        };
        // Measure Phase 1 filtering ratio
        int64_t phase1_cnt = -1;
        {
            auto total_count = static_cast<size_t>(ngram_index->Count());
            if (ngram_index->CanHandleLiteral(bp.term, bp.op_type)) {
                TargetBitmap candidates(total_count, true);
                ngram_index->ExecutePhase1(bp.term, bp.op_type, candidates);
                phase1_cnt = candidates.count();
            }
        }

        row("RE2 (brute-force)", re2_us, re2_cnt);
        row("LikePatternMatcher", like_us, like_cnt);
        row("Tantivy (index)", tantivy_us, tantivy_cnt);
        row("Ngram (index+verify)", ngram_us, ngram_cnt);
        if (phase1_cnt >= 0) {
            std::cout << "  >> Phase1 filter: " << N << " -> " << phase1_cnt
                      << " candidates (" << std::fixed << std::setprecision(1)
                      << (100.0 * phase1_cnt / N) << "% remain, "
                      << std::setprecision(1) << (100.0 * (N - phase1_cnt) / N)
                      << "% filtered)\n";
        } else {
            std::cout << "  >> Ngram can't handle (literal < min_gram)\n";
        }
    }
}

// Benchmark focusing on ngram filtering effectiveness and worst cases
TEST(NgramBenchmark, NgramFilteringEffectiveness) {
    // Generate data with controlled characteristics
    const size_t N = 10000;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    std::uniform_int_distribution<int> len_dist(40, 120);
    boost::container::vector<std::string> data;
    data.reserve(N);
    for (size_t i = 0; i < N; i++) {
        size_t len = len_dist(rng);
        std::string s;
        s.reserve(len);
        for (size_t j = 0; j < len; j++) s += static_cast<char>(char_dist(rng));
        data.push_back(std::move(s));
    }

    // --- Reuse same ngram index setup ---
    int64_t collection_id = 1, partition_id = 2, segment_id = 3;
    int64_t index_build_id = 5000, index_version = 5000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram2", DataType::VARCHAR);

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id.get(),
                                                      DataType::VARCHAR,
                                                      DataType::NONE,
                                                      false);
    auto index_meta = gen_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    std::string root_path = "/tmp/test-ngram-filter/";
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                          partition_id,
                                                          segment_id,
                                                          field_id.get(),
                                                          {field_data},
                                                          cm);
    segment->LoadFieldData(field_data_info);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto log_path = fmt::format("{}/{}/{}/{}/{}",
                                collection_id,
                                partition_id,
                                segment_id,
                                field_id.get(),
                                0);
    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    {
        Config config;
        config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        auto ngram_params = index::NgramParams{
            .loading_index = false, .min_gram = 2, .max_gram = 4};
        auto index =
            std::make_shared<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Build(config);
        auto result = index->UploadUnified({});
        index_files = result->GetIndexFiles();
    }

    Config load_config;
    load_config[milvus::index::INDEX_FILES] = index_files;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;
    auto load_ngram_params =
        index::NgramParams{.loading_index = true, .min_gram = 2, .max_gram = 4};
    auto ngram_index =
        std::make_unique<index::NgramInvertedIndex>(ctx, load_ngram_params);
    ngram_index->LoadUnified(load_config);

    std::vector<std::string> data_vec(data.begin(), data.end());

    // Count expected matches for each pattern using brute-force
    auto count_matches = [&](const std::string& like_pattern) -> int64_t {
        LikePatternMatcher matcher(like_pattern);
        int64_t cnt = 0;
        for (const auto& s : data_vec)
            if (matcher(s))
                cnt++;
        return cnt;
    };

    struct FilterCase {
        std::string name;
        std::string term;          // for ngram index
        std::string like_pattern;  // for brute-force
        proto::plan::OpType op_type;
        std::string reason;  // why filtering is good or bad
    };

    // min_gram=2, max_gram=4, 26 chars, random strings of len 40-120

    std::vector<FilterCase> cases = {
        // ===== Good cases: ngram filters well =====
        {"LIKE %xyz%def%ghi%",
         "%xyz%def%ghi%",
         "%xyz%def%ghi%",
         proto::plan::OpType::Match,
         "3 rare trigrams => excellent filtering"},

        {"LIKE %abcdef%",
         "%abcdef%",
         "%abcdef%",
         proto::plan::OpType::Match,
         "6-char literal => many ngram terms => good"},

        {"PREFIX: qzx",
         "qzx",
         "qzx%",
         proto::plan::OpType::PrefixMatch,
         "Rare trigram prefix"},

        // ===== Bad cases: ngram can't handle =====
        {"LIKE %a%b%c%",
         "%a%b%c%",
         "%a%b%c%",
         proto::plan::OpType::Match,
         "All segments 1-char < min_gram=2 => FALLBACK"},

        {"LIKE %a%bc%",
         "%a%bc%",
         "%a%bc%",
         proto::plan::OpType::Match,
         "Segment 'a' is 1-char < min_gram=2 => FALLBACK"},

        {"LIKE _%_%_%",
         "_%_%_%",
         "_%_%_%",
         proto::plan::OpType::Match,
         "Only underscores, no literals => FALLBACK"},

        {"SUFFIX: a",
         "a",
         "%a",
         proto::plan::OpType::PostfixMatch,
         "1-char literal < min_gram=2 => FALLBACK"},

        // ===== Weak cases: ngram handles but filters poorly =====
        {"LIKE %ab%",
         "%ab%",
         "%ab%",
         proto::plan::OpType::Match,
         "2-char literal => only 1 bigram => weak filter"},

        {"INNER: ab",
         "ab",
         "%ab%",
         proto::plan::OpType::InnerMatch,
         "Common bigram 'ab' => very high posting list"},

        {"INNER: th",
         "th",
         "%th%",
         proto::plan::OpType::InnerMatch,
         "Common bigram => poor selectivity"},

        {"LIKE %ab%cd%",
         "%ab%cd%",
         "%ab%cd%",
         proto::plan::OpType::Match,
         "Two common bigrams => moderate filter"},

        // ===== Control: strong filtering =====
        {"INNER: qzxw",
         "qzxw",
         "%qzxw%",
         proto::plan::OpType::InnerMatch,
         "Very rare 4-gram => near-zero candidates"},

        {"LIKE %mnop%qrst%",
         "%mnop%qrst%",
         "%mnop%qrst%",
         proto::plan::OpType::Match,
         "Two 4-char rare literals => strong filter"},
    };

    std::cout << "\n====== Ngram Filtering Effectiveness (N=" << N
              << ", min_gram=2, max_gram=4) ======\n"
              << "\n  " << std::left << std::setw(28) << "Pattern" << std::right
              << std::setw(8) << "Handle?" << std::setw(10) << "Phase1"
              << std::setw(10) << "Final" << std::setw(10) << "Filter%"
              << std::setw(12) << "Ngram(us)" << std::setw(12) << "Like(us)"
              << "  Reason\n"
              << "  " << std::string(100, '-') << "\n";

    const int W = 3, I = 5;
    for (const auto& fc : cases) {
        bool can_handle = ngram_index->CanHandleLiteral(fc.term, fc.op_type);
        int64_t final_matches = count_matches(fc.like_pattern);

        int64_t phase1_cnt = -1;
        if (can_handle) {
            auto total_count = static_cast<size_t>(ngram_index->Count());
            TargetBitmap candidates(total_count, true);
            ngram_index->ExecutePhase1(fc.term, fc.op_type, candidates);
            phase1_cnt = candidates.count();
        }

        // Benchmark: Ngram full query (Phase1 + Phase2)
        double ngram_us = 0;
        if (can_handle) {
            for (int w = 0; w < W; w++) {
                exec::SegmentExpr se(std::move(std::vector<exec::ExprPtr>{}),
                                     "SegmentExpr",
                                     nullptr,
                                     segment.get(),
                                     field_id,
                                     {},
                                     DataType::VARCHAR,
                                     N,
                                     8192,
                                     0);
                ngram_index->ExecuteQueryForUT(fc.term, fc.op_type, &se);
            }
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < I; i++) {
                exec::SegmentExpr se(std::move(std::vector<exec::ExprPtr>{}),
                                     "SegmentExpr",
                                     nullptr,
                                     segment.get(),
                                     field_id,
                                     {},
                                     DataType::VARCHAR,
                                     N,
                                     8192,
                                     0);
                ngram_index->ExecuteQueryForUT(fc.term, fc.op_type, &se);
            }
            ngram_us = std::chrono::duration<double, std::micro>(
                           std::chrono::high_resolution_clock::now() - t0)
                           .count() /
                       I;
        }

        // Benchmark: LikePatternMatcher brute-force
        double like_us;
        {
            LikePatternMatcher matcher(fc.like_pattern);
            for (int w = 0; w < W; w++)
                for (const auto& s : data_vec) matcher(s);
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < I; i++)
                for (const auto& s : data_vec) matcher(s);
            like_us = std::chrono::duration<double, std::micro>(
                          std::chrono::high_resolution_clock::now() - t0)
                          .count() /
                      I;
        }

        // Print row
        std::cout << "  " << std::left << std::setw(28) << fc.name
                  << std::right;
        if (can_handle) {
            double filter_pct = 100.0 * (N - phase1_cnt) / N;
            std::cout << std::setw(8) << "YES" << std::setw(10) << phase1_cnt
                      << std::setw(10) << final_matches << std::fixed
                      << std::setprecision(1) << std::setw(9) << filter_pct
                      << "%" << std::setw(11) << std::setprecision(0)
                      << ngram_us << "us" << std::setw(11) << like_us << "us"
                      << "  " << fc.reason << "\n";
        } else {
            std::cout << std::setw(8) << "NO" << std::setw(10) << "N/A"
                      << std::setw(10) << final_matches << std::setw(10)
                      << "N/A" << std::setw(12) << "FALLBACK" << std::setw(11)
                      << std::fixed << std::setprecision(0) << like_us << "us"
                      << "  " << fc.reason << "\n";
        }
    }
}
