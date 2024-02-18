// Copyright (C) 2019-2024 Zilliz. All rights reserved.
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
#include <random>

#include "common/Utils.h"

#include "query/SearchBruteForce.h"
#include "test_utils/Constants.h"
#include "test_utils/Distance.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::query;

namespace {

std::vector<int>
Ref(const knowhere::sparse::SparseRow<float>* base,
    const knowhere::sparse::SparseRow<float>& query,
    int nb,
    int topk,
    const knowhere::MetricType& metric) {
    std::vector<std::tuple<float, int>> res;
    for (int i = 0; i < nb; i++) {
        auto& row = base[i];
        auto distance = row.dot(query);
        res.emplace_back(-distance, i);
    }
    std::sort(res.begin(), res.end());
    std::vector<int> offsets;
    for (int i = 0; i < topk; i++) {
        auto [distance, offset] = res[i];
        offsets.push_back(offset);
    }
    return offsets;
}

void
AssertMatch(const std::vector<int>& expected, const int64_t* actual) {
    for (int i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], actual[i]);
    }
}

bool
is_supported_sparse_float_metric(const std::string& metric) {
    return milvus::IsMetricType(metric, knowhere::metric::IP);
}

}  // namespace

class TestSparseFloatSearchBruteForce : public ::testing::Test {
 public:
    void
    Run(int nb,
        int nq,
        int topk,
        const knowhere::MetricType& metric_type) {
        auto bitset = std::make_shared<BitsetType>();
        bitset->resize(nb);
        auto bitset_view = BitsetView(*bitset);

        auto base = milvus::segcore::GenerateRandomSparseFloatVector(nb);
        auto query = milvus::segcore::GenerateRandomSparseFloatVector(nq);

        dataset::SearchDataset dataset{
            metric_type, nq, topk, -1, kTestSparseDim, query.get()};
        if (!is_supported_sparse_float_metric(metric_type)) {
            ASSERT_ANY_THROW(BruteForceSearch(dataset,
                                              base.get(),
                                              nb,
                                              knowhere::Json(),
                                              bitset_view,
                                              DataType::VECTOR_SPARSE_FLOAT));
            return;
        }
        auto result = BruteForceSearch(dataset,
                                       base.get(),
                                       nb,
                                       knowhere::Json(),
                                       bitset_view,
                                       DataType::VECTOR_SPARSE_FLOAT);
        for (int i = 0; i < nq; i++) {
            auto ref = Ref(base.get(),
                           *(query.get() + i),
                           nb,
                           topk,
                           metric_type);
            auto ans = result.get_seg_offsets() + i * topk;
            AssertMatch(ref, ans);
        }
    }
};

TEST_F(TestSparseFloatSearchBruteForce, NotSupported) {
    Run(100, 10, 5, "L2");
    Run(100, 10, 5, "l2");
    Run(100, 10, 5, "lxxx");
}

TEST_F(TestSparseFloatSearchBruteForce, IP) {
    Run(100, 10, 5, "IP");
    Run(100, 10, 5, "ip");
}

