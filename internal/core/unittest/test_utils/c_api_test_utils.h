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

#include <boost/format.hpp>
#include <chrono>
#include <iostream>
#include <unordered_set>

#include "common/Types.h"
#include "common/type_c.h"
#include "pb/plan.pb.h"
#include "segcore/Collection.h"
#include "segcore/Reduce.h"
#include "segcore/reduce_c.h"
#include "segcore/segment_c.h"
#include "DataGen.h"
#include "PbHelper.h"
#include "c_api_test_utils.h"
#include "indexbuilder_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

std::string
generate_max_float_query_data(int all_nq, int max_float_nq) {
    assert(max_float_nq <= all_nq);
    namespace ser = milvus::proto::common;
    int dim = DIM;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
    for (int i = 0; i < all_nq; ++i) {
        std::vector<float> vec;
        if (i < max_float_nq) {
            for (int d = 0; d < dim; ++d) {
                vec.push_back(std::numeric_limits<float>::max());
            }
        } else {
            for (int d = 0; d < dim; ++d) {
                vec.push_back(1);
            }
        }
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();
    return blob;
}
std::string
generate_query_data(int nq) {
    namespace ser = milvus::proto::common;
    std::default_random_engine e(67);
    int dim = DIM;
    std::normal_distribution<double> dis(0.0, 1.0);
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
    for (int i = 0; i < nq; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();
    return blob;
}
void
CheckSearchResultDuplicate(const std::vector<CSearchResult>& results) {
    auto nq = ((SearchResult*)results[0])->total_nq_;

    std::unordered_set<PkType> pk_set;
    std::unordered_set<GroupByValueType> group_by_val_set;
    for (int qi = 0; qi < nq; qi++) {
        pk_set.clear();
        group_by_val_set.clear();
        for (size_t i = 0; i < results.size(); i++) {
            auto search_result = (SearchResult*)results[i];
            ASSERT_EQ(nq, search_result->total_nq_);
            auto topk_beg = search_result->topk_per_nq_prefix_sum_[qi];
            auto topk_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
            for (size_t ki = topk_beg; ki < topk_end; ki++) {
                ASSERT_NE(search_result->seg_offsets_[ki], INVALID_SEG_OFFSET);
                auto ret = pk_set.insert(search_result->primary_keys_[ki]);
                ASSERT_TRUE(ret.second);

                if (search_result->group_by_values_.size() > ki) {
                    auto group_by_val = search_result->group_by_values_[ki];
                    ASSERT_TRUE(group_by_val_set.count(group_by_val) == 0);
                    group_by_val_set.insert(group_by_val);
                }
            }
        }
    }
}
}  // namespace
