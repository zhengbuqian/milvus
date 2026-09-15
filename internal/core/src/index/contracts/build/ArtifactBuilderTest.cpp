// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/Families.h"
#include "index/contracts/query/NullReader.h"
#include "index/contracts/query/ScalarPredicateReader.h"
#include "index/test_utils/CaseTestDriver.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"
#include "index/test_utils/TestArtifactIO.h"

namespace milvus::index::test {
namespace {

template <typename T>
ScalarTestData<T>
SafeScalarData(bool nullable) {
    ScalarTestData<T> data(
        {static_cast<T>(1), static_cast<T>(2), static_cast<T>(3)});
    if (nullable) {
        data.validity.reset(1);
    } else {
        data.validity_present = false;
    }
    data.batch_sizes = {0, 1, 0, 2, 0};
    return data;
}

template <>
ScalarTestData<bool>
SafeScalarData<bool>(bool nullable) {
    ScalarTestData<bool> data({false, true, false});
    if (nullable) {
        data.validity.reset(1);
    } else {
        data.validity_present = false;
    }
    data.batch_sizes = {0, 1, 0, 2, 0};
    return data;
}

template <>
ScalarTestData<std::string_view>
SafeScalarData<std::string_view>(bool nullable) {
    ScalarTestData<std::string_view> data({"alpha", "beta", "gamma"});
    if (nullable) {
        data.validity.reset(1);
    } else {
        data.validity_present = false;
    }
    data.batch_sizes = {0, 1, 0, 2, 0};
    return data;
}

void
ExpectSerializedArtifact(const storage::Artifact& artifact) {
    TestArtifactData persisted;
    TestArtifactSink sink(persisted);
    artifact.Serialize(sink);
    const auto stats = sink.Finish();
    EXPECT_FALSE(persisted.entries.empty());
    EXPECT_EQ(stats.Files().size(), persisted.entries.size());
    EXPECT_GE(stats.MemSize(), 0);
    int64_t total = 0;
    for (const auto& file : stats.Files()) {
        ASSERT_TRUE(persisted.entries.contains(file.file_name));
        EXPECT_EQ(
            file.file_size,
            static_cast<int64_t>(persisted.entries.at(file.file_name).size()));
        total += file.file_size;
    }
    EXPECT_EQ(stats.MemSize(), total);
}

template <typename T>
void
RunScalarArtifactLifecycle(const ReaderBackend& backend) {
    EXPECT_TRUE(backend.InputSpec().side_inputs.empty());

    storage::ArtifactPtr artifact;
    {
        auto input_data = SafeScalarData<T>(backend.Nullable());
        const ScalarTestInput<T> input(input_data);
        artifact = backend.Build(input.View(),
                                 {.row_count = input_data.values.size()});
    }
    ASSERT_NE(artifact, nullptr);
    ASSERT_NO_FATAL_FAILURE(ExpectSerializedArtifact(*artifact));

    auto reader = backend.Open(std::move(artifact), {.row_count = 3});
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 3);
    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
    const auto* null_reader = dynamic_cast<const NullReader*>(reader.get());
    ASSERT_NE(null_reader, nullptr);
    const auto nulls = null_reader->IsNull();
    ASSERT_EQ(nulls.size(), 3);
    EXPECT_EQ(nulls[0], false);
    EXPECT_EQ(nulls[1], backend.Nullable());
    EXPECT_EQ(nulls[2], false);
}

bool
IsOrdinaryLifecycleFamily(const ReaderBackend& backend) {
    return backend.Family() == families::kBitmap ||
           backend.Family() == families::kSort ||
           backend.Family() == families::kInverted ||
           backend.Family() == families::kHybrid ||
           backend.Family() == families::kMarisa;
}

template <typename T>
void
AddScalarLifecycleCases(std::vector<FilterParam>& cases) {
    auto backends = ScalarReaderBackends().For<T>(&ReaderCaps::predicate);
    std::erase_if(backends, [](const auto& backend) {
        return backend.MmapRequested() || !IsOrdinaryLifecycleFamily(backend);
    });
    if constexpr (std::is_same_v<T, std::string_view>) {
        backends.push_back(ScalarReaderBackends().Get<T>("FmIndexVarchar"));
        backends.push_back(
            ScalarReaderBackends().Get<T>("FmIndexVarcharNonNull"));
        backends.push_back(ScalarReaderBackends().Get<T>("TextVarcharV7Heap"));
        backends.push_back(
            ScalarReaderBackends().Get<T>("TextVarcharV7HeapNonNull"));
    }
    if (backends.empty()) {
        throw std::logic_error("no scalar artifact lifecycle backends");
    }
    for (auto& backend : backends) {
        cases.push_back({
            .name = "Scalar_" + backend.Name(),
            .run = [backend = std::move(
                        backend)] { RunScalarArtifactLifecycle<T>(backend); },
        });
    }
}

template <typename T>
T
ArrayMembershipKey() {
    return static_cast<T>(2);
}

template <>
std::string_view
ArrayMembershipKey<std::string_view>() {
    return "b";
}

template <typename T>
T
ArrayRangeKey() {
    return static_cast<T>(3);
}

template <>
std::string_view
ArrayRangeKey<std::string_view>() {
    return "c";
}

template <typename T>
void
RunArrayRowsLifecycle(const ReaderBackend& backend,
                      const ScalarDataSet<ArrayView>& dataset) {
    EXPECT_TRUE(backend.InputSpec().side_inputs.empty());
    auto expected = dataset.make_data();
    storage::ArtifactPtr artifact;
    {
        auto input_data = dataset.make_data();
        const ScalarTestInput<ArrayView> input(input_data);
        artifact = backend.Build(input.View(),
                                 {.row_count = input_data.values.size()});
    }
    ASSERT_NE(artifact, nullptr);
    ASSERT_NO_FATAL_FAILURE(ExpectSerializedArtifact(*artifact));

    auto reader = backend.Open(std::move(artifact),
                               {.row_count = expected.values.size()});
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), expected.values.size());
    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
    EXPECT_EQ(reader->ValueType(), detail::ScalarTestType<T>());

    const auto* null_reader = dynamic_cast<const NullReader*>(reader.get());
    ASSERT_NE(null_reader, nullptr);
    const auto nulls = null_reader->IsNull();
    const auto not_nulls = null_reader->IsNotNull();
    ASSERT_EQ(nulls.size(), 4);
    ASSERT_EQ(not_nulls.size(), 4);
    for (size_t row = 0; row < 4; ++row) {
        EXPECT_EQ(nulls[row], !expected.validity[row]);
        EXPECT_EQ(not_nulls[row], expected.validity[row]);
    }
    EXPECT_FALSE(nulls[1]) << "a valid empty ARRAY is not NULL";
    EXPECT_TRUE(not_nulls[1]);

    const auto* predicate =
        dynamic_cast<const ScalarPredicateReader<T>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const auto member = ArrayMembershipKey<T>();
    const auto in = predicate->In(1, &member);
    ASSERT_EQ(in.size(), 4);
    EXPECT_TRUE(in[0]);
    EXPECT_FALSE(in[1]);
    if constexpr (std::is_same_v<T, bool>) {
        EXPECT_EQ(in[2], !dataset.requires_nullable);
    } else {
        EXPECT_FALSE(in[2]);
    }
    EXPECT_TRUE(in[3]);

    const auto range =
        predicate->Range(ArrayRangeKey<T>(), CompareOp::GreaterEqual);
    ASSERT_EQ(range.size(), 4);
    if constexpr (std::is_same_v<T, bool>) {
        EXPECT_TRUE(range[0]);
    } else {
        EXPECT_FALSE(range[0]);
    }
    EXPECT_FALSE(range[1]);
    EXPECT_EQ(range[2], !dataset.requires_nullable);
    EXPECT_TRUE(range[3]);
}

template <typename T>
void
AddArrayRowsCases(std::vector<FilterParam>& cases, std::string_view type_name) {
    const auto add_dataset = [&](std::string suffix) {
        const auto dataset_name =
            "ArrayRows" + std::string(type_name) + std::move(suffix);
        const auto& dataset = ScalarDataSets().Get<ArrayView>(dataset_name);
        auto backends = ScalarReaderBackends().ForInput<ArrayView>(
            BackendInputShape::ArrayRows,
            Domain::Row,
            &ReaderCaps::predicate,
            dataset.requires_nullable,
            detail::ScalarTestType<T>());
        if (backends.empty()) {
            throw std::logic_error(dataset_name + ": no ARRAY row backend");
        }
        const auto* dataset_ptr = &dataset;
        for (auto& backend : backends) {
            cases.push_back({
                .name = "ArrayRows_" + backend.Name() + "_" + dataset_name,
                .run =
                    [backend = std::move(backend), dataset_ptr] {
                        RunArrayRowsLifecycle<T>(backend, *dataset_ptr);
                    },
            });
        }
    };
    add_dataset("Nullable");
    add_dataset("AllValid");
}

const std::vector<FilterParam>&
ArtifactLifecycleCases() {
    static const auto cases = [] {
        std::vector<FilterParam> result;
        AddScalarLifecycleCases<bool>(result);
        AddScalarLifecycleCases<int8_t>(result);
        AddScalarLifecycleCases<int16_t>(result);
        AddScalarLifecycleCases<int32_t>(result);
        AddScalarLifecycleCases<int64_t>(result);
        AddScalarLifecycleCases<float>(result);
        AddScalarLifecycleCases<double>(result);
        AddScalarLifecycleCases<std::string_view>(result);

        AddArrayRowsCases<bool>(result, "Bool");
        AddArrayRowsCases<int8_t>(result, "Int8");
        AddArrayRowsCases<int16_t>(result, "Int16");
        AddArrayRowsCases<int32_t>(result, "Int32");
        AddArrayRowsCases<int64_t>(result, "Int64");
        AddArrayRowsCases<float>(result, "Float");
        AddArrayRowsCases<double>(result, "Double");
        AddArrayRowsCases<std::string_view>(result, "Varchar");
        return result;
    }();
    return cases;
}

const IndexTestCases&
ArtifactBuildFailureCases() {
    static const auto cases = [] {
        IndexTestCases result;
        result.Add(IndexTestCase<std::string_view>{
            .name = "NonNullableRejectsNullInput",
            .dataset = "NullVsEmptyString",
            .backends = {"FmIndexVarcharNonNull"},
            .body =
                BuildFails{
                    .expected_error = ErrorCode::DataFormatBroken,
                    .allow_nullability_mismatch = true,
                },
        });
        return result;
    }();
    return cases;
}

class ArtifactBuilderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(ArtifactBuilderTest, ArtifactAndReaderOutliveBorrowedInput) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarAndArrayBuilders,
                         ArtifactBuilderTest,
                         ::testing::ValuesIn(ArtifactLifecycleCases()),
                         FilterParamName);

class ArtifactBuilderFailureTest
    : public ::testing::TestWithParam<FilterParam> {};

TEST_P(ArtifactBuilderFailureTest, RejectsInvalidInput) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarBuilders,
                         ArtifactBuilderFailureTest,
                         ::testing::ValuesIn(ArtifactBuildFailureCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
