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
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "index/test_utils/FilterTestDriver.h"

namespace milvus::index::test {

inline void
ExpectReaderBase(const ReaderBackend& backend,
                 const IndexReaderBase& reader,
                 size_t expected_count) {
    ASSERT_EQ(reader.Count(), expected_count);
    EXPECT_EQ(reader.CoordDomain(), backend.ExpectedDomain());
    EXPECT_EQ(reader.ValueType(), backend.ExpectedValueType());

    const auto caps = reader.Caps();
    EXPECT_EQ(caps.nested, reader.CoordDomain() == Domain::Element);
    EXPECT_FALSE(caps.cheap_value_lookup && !caps.value_lookup);

    const auto memory = reader.MemoryUsage();
    const auto usage = reader.CellByteSize();
    EXPECT_GE(memory, 0);
    EXPECT_GE(usage.memory_bytes, 0);
    EXPECT_GE(usage.file_bytes, 0);
    EXPECT_EQ(reader.MemoryUsage(), memory);
    const auto repeated_usage = reader.CellByteSize();
    EXPECT_EQ(repeated_usage.memory_bytes, usage.memory_bytes);
    EXPECT_EQ(repeated_usage.file_bytes, usage.file_bytes);
}

template <typename T>
struct ReaderObservationCase {
    std::string name;
    std::string dataset;
    BackendInputShape input_shape{BackendInputShape::Scalar};
    Domain domain{Domain::Row};
    bool ReaderCaps::*capability{nullptr};
    std::optional<DataType> logical_value_type;
    // Empty selects all matching profiles. Otherwise these are exact profile
    // names, including their load-mode and nullability suffixes.
    std::vector<std::string> backends;
    // Applied after type, shape, domain, nullability, and capability selection.
    // Use configured parameters instead of parsing profile names.
    std::function<bool(const ReaderBackend&)> select_backend;
    std::function<void(
        const ReaderBackend&, const ScalarTestData<T>&, IndexReaderBasePtr&)>
        observe;
};

template <typename T>
void
RunReaderObservation(const ReaderBackend& backend,
                     const ScalarDataSet<T>& dataset,
                     const ReaderObservationCase<T>& test_case) {
    auto expected = dataset.make_data();
    ASSERT_EQ(dataset.requires_nullable, ScalarTestHasNulls(expected));
    if (!backend.Nullable()) {
        ASSERT_FALSE(ScalarTestHasNulls(expected));
    }
    ASSERT_EQ(expected.domain, dataset.domain);
    ASSERT_EQ(test_case.domain, dataset.domain);

    IndexReaderBasePtr reader;
    {
        // Generate a second, independent owner. In particular, strings must not
        // share the storage later used by the expectation callback.
        auto input_data = dataset.make_data();
        ASSERT_EQ(input_data.values.size(), expected.values.size());
        ASSERT_EQ(input_data.domain, expected.domain);
        ASSERT_EQ(input_data.metadata, expected.metadata);
        const ScalarTestInput<T> input(input_data);
        reader = backend.Create(input.View(),
                                {.row_count = input_data.values.size(),
                                 .values = input_data.metadata});
    }

    ASSERT_NE(reader, nullptr);
    ASSERT_NO_FATAL_FAILURE(
        ExpectReaderBase(backend, *reader, expected.values.size()));
    test_case.observe(backend, expected, reader);
}

class ReaderObservationCases {
 public:
    template <typename T>
    void
    Add(ReaderObservationCase<T> test_case) {
        if (test_case.name.empty() || !test_case.observe) {
            throw std::logic_error(
                "reader observation requires a name and callback");
        }
        const auto& dataset = ScalarDataSets().Get<T>(test_case.dataset);
        if (dataset.input_shape != test_case.input_shape ||
            dataset.domain != test_case.domain) {
            throw std::logic_error(test_case.name +
                                   ": dataset shape/domain mismatch");
        }
        if (dataset.logical_value_type.has_value() &&
            test_case.logical_value_type.has_value() &&
            !ScalarValueTypesMatch(*dataset.logical_value_type,
                                   *test_case.logical_value_type)) {
            throw std::logic_error(test_case.name +
                                   ": dataset logical type mismatch");
        }
        const auto logical_type = test_case.logical_value_type.has_value()
                                      ? test_case.logical_value_type
                                      : dataset.logical_value_type;
        auto backends =
            test_case.capability == nullptr
                ? ScalarReaderBackends().AllInput<T>(test_case.input_shape,
                                                     test_case.domain,
                                                     dataset.requires_nullable,
                                                     logical_type)
                : ScalarReaderBackends().ForInput<T>(test_case.input_shape,
                                                     test_case.domain,
                                                     test_case.capability,
                                                     dataset.requires_nullable,
                                                     logical_type);
        if (!test_case.backends.empty()) {
            std::erase_if(backends, [&](const auto& backend) {
                return std::find(test_case.backends.begin(),
                                 test_case.backends.end(),
                                 backend.Name()) == test_case.backends.end();
            });
        }
        if (test_case.select_backend) {
            std::erase_if(backends, [&](const auto& backend) {
                return !test_case.select_backend(backend);
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
                    throw std::logic_error(name +
                                           ": duplicate reader observation");
                }
            }
            const auto* dataset_ptr = &dataset;
            cases_.push_back({
                .name = std::move(name),
                .run =
                    [backend = std::move(backend), test_case, dataset_ptr] {
                        RunReaderObservation<T>(
                            backend, *dataset_ptr, test_case);
                    },
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
