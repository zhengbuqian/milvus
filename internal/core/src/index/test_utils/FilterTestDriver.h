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

#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {

// For small, manually specified answers. The result bitmap is allocated only
// during execution. Large/custom answers can supply an expected callback instead.
inline auto
ManualHits(std::vector<size_t> offsets) {
    return [offsets = std::move(offsets)](const auto& data, const auto&) {
        return Hits(data.values.size(), offsets);
    };
}

template <typename Op>
struct FilterCase {
    std::string name;
    std::string dataset;
    typename Op::Args args;
    // Empty selects every capability-matching backend. Focused routing cases
    // may name one or more families without duplicating load profiles.
    std::vector<std::string> families;
    // Used only by SelectiveAndRun routing cases. Ordinary result cases leave
    // this empty and still execute the direct query.
    std::optional<bool> expected_should_use;
    // Query-only failure expectation. Build, load, cast, and routing checks
    // remain outside the exception boundary.
    std::optional<ErrorCode> expected_error;
    // Empty callback selects Op::Oracle. ManualHits({}) is a nonempty callback
    // producing an all-zero bitmap, so it never falls back to the oracle.
    std::function<TargetBitmap(const ScalarTestData<typename Op::ValueType>&,
                               const typename Op::Args&)>
        expected;
};

template <typename Op>
TargetBitmap
FilterGroundTruth(const FilterCase<Op>& test_case,
                  const ScalarTestData<typename Op::ValueType>& data) {
    if (test_case.expected) {
        return test_case.expected(data, test_case.args);
    }
    if constexpr (requires { Op::Oracle(data, test_case.args); }) {
        return Op::Oracle(data, test_case.args);
    } else {
        // Complex operators may deliberately have no reference algorithm.
        throw std::logic_error(test_case.name +
                               ": expected result is required");
    }
}

// One exact filter combination per invocation. Combinations are
// expanded during registration; this driver never loops over combinations.
template <typename Op>
void
RunFilterCase(const ReaderBackend& backend, const FilterCase<Op>& test_case) {
    using T = typename Op::ValueType;
    const auto& dataset = ScalarDataSets().Get<T>(test_case.dataset);
    auto data = dataset.make_data();
    ASSERT_EQ(dataset.requires_nullable, ScalarTestHasNulls(data))
        << "dataset nullability descriptor disagrees with generated rows";
    ASSERT_EQ(dataset.input_shape, BackendInputShape::Scalar);
    ASSERT_EQ(dataset.domain, data.domain);
    if (!backend.Nullable()) {
        ASSERT_EQ(data.validity.count(), data.values.size())
            << "non-nullable backend received null test rows";
    }
    const ScalarTestInput<T> input(data);

    auto reader = backend.Create(
        input.View(),
        {.row_count = data.values.size(), .values = data.metadata});
    ASSERT_NE(reader, nullptr);
    ASSERT_EQ(reader->CoordDomain(), dataset.domain);
    ASSERT_EQ(reader->CoordDomain(), backend.ExpectedDomain());
    ASSERT_EQ(reader->Count(), data.values.size());
    EXPECT_TRUE(reader->Caps().*Op::kCapability);
    EXPECT_TRUE(backend.Supports(Op::kCapability));
    const auto* contract =
        dynamic_cast<const typename Op::Reader*>(reader.get());
    ASSERT_NE(contract, nullptr);

    auto expected_error = test_case.expected_error;
    if constexpr (requires {
                      Op::QueryPolicy(backend, test_case.args);
                      Op::ShouldUse(*contract, test_case.args);
                  }) {
        const auto policy = Op::QueryPolicy(backend, test_case.args);
        const auto should_use = Op::ShouldUse(*contract, test_case.args);
        switch (policy) {
            case PatternQueryPolicy::UseAndRun:
                EXPECT_TRUE(should_use);
                break;
            case PatternQueryPolicy::DeclineButRun:
                EXPECT_FALSE(should_use);
                break;
            case PatternQueryPolicy::SelectiveAndRun:
                if (test_case.expected_should_use.has_value()) {
                    EXPECT_EQ(should_use, *test_case.expected_should_use);
                }
                break;
            case PatternQueryPolicy::Unsupported:
                EXPECT_FALSE(should_use);
                expected_error = ErrorCode::Unsupported;
                break;
        }
    }

    if (expected_error.has_value()) {
        try {
            static_cast<void>(Op::Run(*contract, test_case.args));
            ADD_FAILURE() << "query expected an error but succeeded";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), *expected_error);
        } catch (const std::exception& error) {
            ADD_FAILURE() << "query threw a non-SegcoreError: " << error.what();
        } catch (...) {
            ADD_FAILURE() << "query threw a non-SegcoreError";
        }
        return;
    }

    const auto expected = FilterGroundTruth<Op>(test_case, data);
    ASSERT_EQ(expected.size(), data.values.size());
    auto actual = Op::Run(*contract, test_case.args);
    ExpectBitmap(actual, expected);
}

// GTest sees one uniform parameter type. The callback retains a typed case and
// backend configuration, never generated data, an expected bitmap, or a Reader.
struct FilterParam {
    std::string name;
    std::function<void()> run;
};

inline std::string
FilterParamName(const ::testing::TestParamInfo<FilterParam>& info) {
    return info.param.name;
}

class FilterCases {
 public:
    template <typename Op>
    void
    Add(FilterCase<Op> test_case) {
        using T = typename Op::ValueType;
        // Resolve the descriptor to catch typos; never invoke its generator here.
        const auto& dataset = ScalarDataSets().Get<T>(test_case.dataset);
        auto backends = ScalarReaderBackends().For<T>(
            Op::kCapability, dataset.requires_nullable);
        if (!test_case.families.empty()) {
            std::erase_if(backends, [&](const auto& backend) {
                return std::find(test_case.families.begin(),
                                 test_case.families.end(),
                                 backend.Family()) == test_case.families.end();
            });
        }
        if (backends.empty()) {
            throw std::logic_error(test_case.name +
                                   ": no matching reader backend");
        }
        for (auto& backend : backends) {
            auto name =
                backend.Name() + "_" + test_case.dataset + "_" + test_case.name;
            for (const auto& existing : cases_) {
                if (existing.name == name) {
                    throw std::logic_error(name + ": duplicate filter case");
                }
            }
            cases_.push_back({
                .name = std::move(name),
                .run = [backend = std::move(backend),
                        test_case] { RunFilterCase<Op>(backend, test_case); },
            });
        }
    }

    const std::vector<FilterParam>&
    All() const {
        return cases_;
    }

 private:
    std::vector<FilterParam> cases_;
};

}  // namespace milvus::index::test
