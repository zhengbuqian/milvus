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
#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include "common/ArrayOffsets.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "index/IndexFactory.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "simdjson/padded_string.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

constexpr int64_t kRLSBenchmarkBaseRows = 1'010'000;
constexpr int kRLSBenchmarkUserCount = 26;
constexpr std::array<int64_t, 6> kRLSHeavySharedUsersCapacities = {
    100, 1000, 5000, 10000, 50000, 100000};
constexpr std::array<int64_t, 6> kRLSSharedUserDocCounts = {
    100, 1000, 5000, 10000, 50000, 100000};

enum class RLSGrantStyle {
    Owner,
    SharedUser,
    Department,
    Overlap,
    None,
};

struct RLSUserCase {
    std::string user;
    std::string category;
    int64_t base_count;
    RLSGrantStyle style;
};

const std::vector<RLSUserCase>&
RLSBenchmarkCases() {
    static const std::vector<RLSUserCase> cases = {
        {"u00", "owner_only", 50000, RLSGrantStyle::Owner},
        {"u01", "shared_user_only", 50000, RLSGrantStyle::SharedUser},
        {"u02", "department_only", 50000, RLSGrantStyle::Department},
        {"u03", "overlap_owner_user_department", 50000, RLSGrantStyle::Overlap},
        {"u04", "broad_250k", 250000, RLSGrantStyle::Department},
        {"u05", "medium_100k", 100000, RLSGrantStyle::SharedUser},
        {"u06", "sparse_10k", 10000, RLSGrantStyle::Owner},
        {"u07", "ultra_sparse_1k", 1000, RLSGrantStyle::Department},
        {"u08", "mixed_25k", 25000, RLSGrantStyle::Owner},
        {"u09", "mixed_25k", 25000, RLSGrantStyle::SharedUser},
        {"u10", "mixed_25k", 25000, RLSGrantStyle::Department},
        {"u11", "mixed_25k", 25000, RLSGrantStyle::Overlap},
        {"u12", "mixed_25k", 25000, RLSGrantStyle::Owner},
        {"u13", "mixed_25k", 25000, RLSGrantStyle::SharedUser},
        {"u14", "mixed_25k", 25000, RLSGrantStyle::Department},
        {"u15", "mixed_25k", 25000, RLSGrantStyle::Overlap},
        {"u16", "mixed_25k", 25000, RLSGrantStyle::Owner},
        {"u17", "mixed_25k", 25000, RLSGrantStyle::SharedUser},
        {"u18", "mixed_25k", 25000, RLSGrantStyle::Department},
        {"u19", "no_access", 0, RLSGrantStyle::None},
        {"u20", "shared_user_docs_100", 100, RLSGrantStyle::SharedUser},
        {"u21", "shared_user_docs_1k", 1000, RLSGrantStyle::SharedUser},
        {"u22", "shared_user_docs_5k", 5000, RLSGrantStyle::SharedUser},
        {"u23", "shared_user_docs_10k", 10000, RLSGrantStyle::SharedUser},
        {"u24", "shared_user_docs_50k", 50000, RLSGrantStyle::SharedUser},
        {"u25", "shared_user_docs_100k", 100000, RLSGrantStyle::SharedUser},
    };
    return cases;
}

proto::plan::GenericValue
StringValue(const std::string& value) {
    proto::plan::GenericValue generic_value;
    generic_value.set_string_val(value);
    return generic_value;
}

proto::plan::GenericValue
BoolValue(bool value) {
    proto::plan::GenericValue generic_value;
    generic_value.set_bool_val(value);
    return generic_value;
}

int
UserIndex(const std::string& user) {
    return std::stoi(user.substr(1));
}

std::string
DepartmentForUser(const std::string& user) {
    return "d" + user.substr(1);
}

std::shared_ptr<plan::FilterBitsNode>
BuildRLSCountPlan(FieldId owner_fid,
                  FieldId shared_users_fid,
                  FieldId shared_departments_fid,
                  const std::string& user) {
    auto user_value = StringValue(user);
    auto department_value = StringValue(DepartmentForUser(user));

    auto owner_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(owner_fid, DataType::VARCHAR),
        proto::plan::OpType::Equal,
        user_value);
    auto shared_users_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(
            shared_users_fid, DataType::ARRAY, DataType::VARCHAR),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{user_value});
    auto shared_departments_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(
            shared_departments_fid, DataType::ARRAY, DataType::VARCHAR),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{department_value});

    auto array_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::Or,
        shared_users_expr,
        shared_departments_expr);
    auto rls_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::Or, owner_expr, array_expr);
    return std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, rls_expr);
}

std::shared_ptr<plan::FilterBitsNode>
BuildBoolCountPlan(FieldId bool_fid) {
    auto bool_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(bool_fid, DataType::BOOL),
        proto::plan::OpType::Equal,
        BoolValue(true));
    return std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, bool_expr);
}

double
MedianMs(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

std::pair<size_t, double>
RunCountPlan(const std::shared_ptr<plan::FilterBitsNode>& plan,
             const SegmentInternalInterface* segment,
             int64_t row_count) {
    auto start = std::chrono::steady_clock::now();
    auto bitset = ExecuteQueryExpr(plan, segment, row_count, MAX_TIMESTAMP);
    auto finish = std::chrono::steady_clock::now();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(finish - start).count();
    return {bitset.count(), elapsed_ms};
}

FieldDataPtr
BuildFieldData(const GeneratedData& raw_data, FieldId field_id) {
    auto data = raw_data.get_col(field_id);
    return CreateFieldDataFromDataArray(raw_data.raw_->num_rows(),
                                        data.get(),
                                        raw_data.schema_->operator[](field_id));
}

void
LoadStringSortIndex(SegmentSealed* segment,
                    FieldId field_id,
                    const FieldDataPtr& field_data,
                    bool nested) {
    auto index = std::make_unique<index::StringIndexSort>(
        storage::FileManagerContext(), nested);
    index->BuildWithFieldData({field_data});

    LoadIndexInfo info{};
    info.field_id = field_id.get();
    info.field_type = DataType::VARCHAR;
    info.index_params = GenIndexParams(index.get());
    info.cache_index = CreateTestCacheIndex(
        fmt::format("stl_sort_{}", field_id.get()), std::move(index));
    segment->LoadIndex(info);
}

void
LoadBoolSortIndex(SegmentSealed* segment,
                  FieldId field_id,
                  const FieldDataPtr& field_data) {
    auto index = index::CreateScalarIndexSort<bool>();
    index->BuildWithFieldData({field_data});

    LoadIndexInfo info{};
    info.field_id = field_id.get();
    info.field_type = DataType::BOOL;
    info.index_params = GenIndexParams(index.get());
    info.cache_index = CreateTestCacheIndex(
        fmt::format("stl_sort_{}", field_id.get()), std::move(index));
    segment->LoadIndex(info);
}

std::unique_ptr<index::StringIndexSort>
BuildStringSortIndex(const FieldDataPtr& field_data, bool nested) {
    auto index = std::make_unique<index::StringIndexSort>(
        storage::FileManagerContext(), nested);
    index->BuildWithFieldData({field_data});
    return index;
}

struct RLSBenchmarkFixture {
    RLSBenchmarkFixture() {
        raw_data.raw_ = nullptr;
    }

    std::shared_ptr<Schema> schema;
    FieldId id_fid;
    FieldId owner_fid;
    FieldId shared_users_fid;
    FieldId shared_departments_fid;
    std::array<FieldId, kRLSBenchmarkUserCount> bool_fids;
    int64_t row_count = 0;
    std::array<int64_t, kRLSBenchmarkUserCount> expected_counts{};
    std::array<int64_t, kRLSBenchmarkUserCount> heavy_shared_users_capacity{};
    std::array<int64_t, kRLSBenchmarkUserCount> shared_user_doc_count{};
    GeneratedData raw_data;
    std::unique_ptr<SegmentSealed> segment;
    std::unique_ptr<index::StringIndexSort> owner_index;
    std::unique_ptr<index::StringIndexSort> shared_users_index;
    std::unique_ptr<index::StringIndexSort> shared_departments_index;
};

RLSBenchmarkFixture
BuildRLSBenchmarkFixture() {
    RLSBenchmarkFixture fixture;
    fixture.schema = std::make_shared<Schema>();
    fixture.id_fid = fixture.schema->AddDebugField("id", DataType::INT64);
    fixture.owner_fid =
        fixture.schema->AddDebugField("owner", DataType::VARCHAR);
    fixture.shared_users_fid = fixture.schema->AddDebugField(
        "shared_users[elements]", DataType::ARRAY, DataType::VARCHAR);
    fixture.shared_departments_fid = fixture.schema->AddDebugField(
        "shared_departments[elements]", DataType::ARRAY, DataType::VARCHAR);

    for (int i = 0; i < kRLSBenchmarkUserCount; ++i) {
        fixture.bool_fids[i] = fixture.schema->AddDebugField(
            fmt::format("can_access_u{:02d}", i), DataType::BOOL);
    }
    fixture.schema->set_primary_field_id(fixture.id_fid);

    const auto& cases = RLSBenchmarkCases();
    fixture.row_count =
        kRLSBenchmarkBaseRows +
        static_cast<int64_t>(kRLSHeavySharedUsersCapacities.size());

    for (const auto& test_case : cases) {
        fixture.expected_counts[UserIndex(test_case.user)] =
            test_case.base_count;
    }
    for (int i = 0; i < kRLSHeavySharedUsersCapacities.size(); ++i) {
        auto user_index = 8 + i;
        fixture.expected_counts[user_index] += 1;
        fixture.heavy_shared_users_capacity[user_index] =
            kRLSHeavySharedUsersCapacities[i];
    }
    for (int i = 0; i < kRLSSharedUserDocCounts.size(); ++i) {
        fixture.shared_user_doc_count[20 + i] = kRLSSharedUserDocCounts[i];
    }

    std::array<int64_t, kRLSBenchmarkUserCount> case_starts{};
    int64_t cursor = 0;
    for (int i = 0; i < cases.size(); ++i) {
        case_starts[i] = cursor;
        cursor += cases[i].base_count;
    }
    EXPECT_LE(cursor, kRLSBenchmarkBaseRows);

    auto grant_access = [](const RLSUserCase& test_case,
                           std::string& owner,
                           ScalarFieldProto& shared_users,
                           ScalarFieldProto& shared_departments) {
        switch (test_case.style) {
            case RLSGrantStyle::Owner:
                owner = test_case.user;
                break;
            case RLSGrantStyle::SharedUser:
                shared_users.mutable_string_data()->add_data(test_case.user);
                break;
            case RLSGrantStyle::Department:
                shared_departments.mutable_string_data()->add_data(
                    DepartmentForUser(test_case.user));
                break;
            case RLSGrantStyle::Overlap:
                owner = test_case.user;
                shared_users.mutable_string_data()->add_data(test_case.user);
                shared_departments.mutable_string_data()->add_data(
                    DepartmentForUser(test_case.user));
                break;
            case RLSGrantStyle::None:
                break;
        }
    };

    auto find_base_case = [&](int64_t row_id) -> const RLSUserCase* {
        for (int i = 0; i < cases.size(); ++i) {
            if (row_id >= case_starts[i] &&
                row_id < case_starts[i] + cases[i].base_count) {
                return &cases[i];
            }
        }
        return nullptr;
    };

    std::vector<int64_t> ids(fixture.row_count);
    std::vector<idx_t> row_ids(fixture.row_count);
    std::vector<Timestamp> timestamps(fixture.row_count);
    std::vector<std::string> owners(fixture.row_count, "external_owner");
    std::vector<ScalarFieldProto> shared_users(fixture.row_count);
    std::vector<ScalarFieldProto> shared_departments(fixture.row_count);
    std::array<FixedVector<bool>, kRLSBenchmarkUserCount> can_access;
    for (auto& access : can_access) {
        access.resize(fixture.row_count, false);
    }

    for (int64_t row = 0; row < fixture.row_count; ++row) {
        ids[row] = row;
        row_ids[row] = row;
        timestamps[row] = row;

        if (row < kRLSBenchmarkBaseRows) {
            auto* test_case = find_base_case(row);
            if (test_case != nullptr) {
                grant_access(*test_case,
                             owners[row],
                             shared_users[row],
                             shared_departments[row]);
                can_access[UserIndex(test_case->user)][row] = true;
            }
            continue;
        }

        auto heavy_index = row - kRLSBenchmarkBaseRows;
        const auto& test_case = cases[8 + heavy_index];
        auto capacity = kRLSHeavySharedUsersCapacities[heavy_index];
        auto* user_data = shared_users[row].mutable_string_data()->mutable_data();
        user_data->Reserve(capacity);
        for (int64_t i = 0; i < capacity - 1; ++i) {
            *user_data->Add() = fmt::format("hu{}_{}", capacity, i);
        }
        *user_data->Add() = test_case.user;
        can_access[UserIndex(test_case.user)][row] = true;
    }

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto add_col = [&](FieldId field_id, const auto& data) {
        auto array = milvus::segcore::CreateDataArrayFrom(
            data.data(), nullptr, fixture.row_count, fixture.schema->operator[](field_id));
        insert_data->mutable_fields_data()->AddAllocated(array.release());
    };
    add_col(fixture.id_fid, ids);
    add_col(fixture.owner_fid, owners);
    add_col(fixture.shared_users_fid, shared_users);
    add_col(fixture.shared_departments_fid, shared_departments);
    for (int i = 0; i < kRLSBenchmarkUserCount; ++i) {
        add_col(fixture.bool_fids[i], can_access[i]);
    }
    insert_data->set_num_rows(fixture.row_count);

    fixture.raw_data.row_ids_ = std::move(row_ids);
    fixture.raw_data.timestamps_ = std::move(timestamps);
    fixture.raw_data.raw_ = insert_data.release();
    fixture.raw_data.schema_ = fixture.schema;

    fixture.segment =
        CreateSealedWithFieldDataLoaded(fixture.schema, fixture.raw_data);

    auto owner_field_data = BuildFieldData(fixture.raw_data, fixture.owner_fid);
    auto shared_users_field_data =
        BuildFieldData(fixture.raw_data, fixture.shared_users_fid);
    auto shared_departments_field_data =
        BuildFieldData(fixture.raw_data, fixture.shared_departments_fid);

    fixture.owner_index = BuildStringSortIndex(owner_field_data, false);
    fixture.shared_users_index =
        BuildStringSortIndex(shared_users_field_data, true);
    fixture.shared_departments_index =
        BuildStringSortIndex(shared_departments_field_data, true);

    for (int i = 0; i < kRLSBenchmarkUserCount; ++i) {
        LoadBoolSortIndex(fixture.segment.get(),
                          fixture.bool_fids[i],
                          BuildFieldData(fixture.raw_data, fixture.bool_fids[i]));
    }

    return fixture;
}

const std::vector<int32_t>&
RowStarts(const std::shared_ptr<const IArrayOffsets>& offsets) {
    auto sealed_offsets =
        std::dynamic_pointer_cast<const ArrayOffsetsSealed>(offsets);
    AssertInfo(sealed_offsets != nullptr, "POC benchmark expects sealed offsets");
    return sealed_offsets->row_to_element_start_;
}

bool
BitsetsEqual(const TargetBitmap& left, const TargetBitmap& right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (size_t i = 0; i < left.size(); ++i) {
        if (left[i] != right[i]) {
            return false;
        }
    }
    return true;
}

TargetBitmap
ProjectAnyByRowRangeScan(const TargetBitmap& element_bitset,
                         const std::vector<int32_t>& row_starts,
                         int64_t row_count) {
    TargetBitmap row_bitset(row_count, false);
    for (int64_t row_id = 0; row_id < row_count; row_id++) {
        auto elem_start = row_starts[row_id];
        auto elem_end = row_starts[row_id + 1];
        for (int32_t elem_id = elem_start; elem_id < elem_end; elem_id++) {
            if (element_bitset[elem_id]) {
                row_bitset.set(row_id);
                break;
            }
        }
    }
    return row_bitset;
}

TargetBitmap
ProjectAnyBySetBitCursor(const TargetBitmap& element_bitset,
                         const std::vector<int32_t>& row_starts,
                         int64_t row_count) {
    TargetBitmap row_bitset(row_count, false);
    int64_t row_id = 0;
    auto elem_id_opt = element_bitset.find_first();
    while (elem_id_opt.has_value()) {
        auto elem_id = static_cast<int64_t>(elem_id_opt.value());
        while (row_id < row_count && row_starts[row_id + 1] <= elem_id) {
            row_id++;
        }
        if (row_id >= row_count) {
            break;
        }
        row_bitset.set(row_id);
        elem_id_opt = element_bitset.find_next(elem_id);
    }
    return row_bitset;
}

void
SearchAnyDirectToRows(const index::StringIndexSort& sort_index,
                      const std::string& value,
                      const std::vector<int32_t>& row_starts,
                      int64_t row_count,
                      TargetBitmap& row_bitset) {
    auto* memory_impl =
        dynamic_cast<index::StringIndexSortMemoryImpl*>(sort_index.impl_.get());
    AssertInfo(memory_impl != nullptr,
               "POC benchmark expects in-memory StringIndexSort");

    auto value_index = memory_impl->FindValueIndex(value);
    if (value_index == std::numeric_limits<size_t>::max()) {
        return;
    }

    int64_t row_id = 0;
    const auto& posting_list = memory_impl->posting_lists_[value_index];
    for (auto elem_id_value : posting_list) {
        auto elem_id = static_cast<int64_t>(elem_id_value);
        while (row_id < row_count && row_starts[row_id + 1] <= elem_id) {
            row_id++;
        }
        if (row_id >= row_count) {
            break;
        }
        row_bitset.set(row_id);
    }
}

TargetBitmap
RunRLSProjectionCurrent(const RLSBenchmarkFixture& fixture,
                        const std::string& user) {
    auto result = fixture.owner_index->In(1, &user);
    auto shared_user_offsets =
        RowStarts(fixture.segment->GetArrayOffsets(fixture.shared_users_fid));
    auto shared_department_offsets = RowStarts(
        fixture.segment->GetArrayOffsets(fixture.shared_departments_fid));
    auto department = DepartmentForUser(user);

    auto shared_users_element_bitset =
        fixture.shared_users_index->In(1, &user);
    result |= ProjectAnyByRowRangeScan(
        shared_users_element_bitset, shared_user_offsets, fixture.row_count);

    auto shared_departments_element_bitset =
        fixture.shared_departments_index->In(1, &department);
    result |= ProjectAnyByRowRangeScan(shared_departments_element_bitset,
                                       shared_department_offsets,
                                       fixture.row_count);
    return result;
}

TargetBitmap
RunRLSProjectionSetBitCursor(const RLSBenchmarkFixture& fixture,
                             const std::string& user) {
    auto result = fixture.owner_index->In(1, &user);
    auto shared_user_offsets =
        RowStarts(fixture.segment->GetArrayOffsets(fixture.shared_users_fid));
    auto shared_department_offsets = RowStarts(
        fixture.segment->GetArrayOffsets(fixture.shared_departments_fid));
    auto department = DepartmentForUser(user);

    auto shared_users_element_bitset =
        fixture.shared_users_index->In(1, &user);
    result |= ProjectAnyBySetBitCursor(
        shared_users_element_bitset, shared_user_offsets, fixture.row_count);

    auto shared_departments_element_bitset =
        fixture.shared_departments_index->In(1, &department);
    result |= ProjectAnyBySetBitCursor(shared_departments_element_bitset,
                                       shared_department_offsets,
                                       fixture.row_count);
    return result;
}

TargetBitmap
RunRLSProjectionDirect(const RLSBenchmarkFixture& fixture,
                       const std::string& user) {
    auto result = fixture.owner_index->In(1, &user);
    auto shared_user_offsets =
        RowStarts(fixture.segment->GetArrayOffsets(fixture.shared_users_fid));
    auto shared_department_offsets = RowStarts(
        fixture.segment->GetArrayOffsets(fixture.shared_departments_fid));
    auto department = DepartmentForUser(user);

    SearchAnyDirectToRows(*fixture.shared_users_index,
                          user,
                          shared_user_offsets,
                          fixture.row_count,
                          result);
    SearchAnyDirectToRows(*fixture.shared_departments_index,
                          department,
                          shared_department_offsets,
                          fixture.row_count,
                          result);
    return result;
}

template <typename Func>
std::pair<TargetBitmap, double>
TimeProjection(Func&& func) {
    auto start = std::chrono::steady_clock::now();
    auto bitset = func();
    auto finish = std::chrono::steady_clock::now();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(finish - start).count();
    return {std::move(bitset), elapsed_ms};
}

}  // namespace

TEST(Expr, TestArrayRange) {
    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // binary_range_expr: 1 < long_array[0] < 10000
            {"1 < long_array[0] < 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 < val && val < 10000;
             }},
            // binary_range_expr: 1 < long_array[1024] < 10000
            {"1 < long_array[1024] < 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 < val && val < 10000;
             }},
            // binary_range_expr: 1 <= long_array[0] < 10000
            {"1 <= long_array[0] < 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 <= val && val < 10000;
             }},
            // binary_range_expr: 1 <= long_array[1024] < 10000
            {"1 <= long_array[1024] < 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 <= val && val < 10000;
             }},
            // binary_range_expr: 1 < long_array[0] <= 10000
            {"1 < long_array[0] <= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 < val && val <= 10000;
             }},
            // binary_range_expr: 1 < long_array[1024] <= 10000
            {"1 < long_array[1024] <= 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 < val && val <= 10000;
             }},
            // binary_range_expr: 1 <= long_array[0] <= 10000
            {"1 <= long_array[0] <= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 <= val && val <= 10000;
             }},
            // binary_range_expr: 1 <= long_array[1024] <= 10000
            {"1 <= long_array[1024] <= 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 <= val && val <= 10000;
             }},
            // binary_range_expr: "aaa" <= string_array[0] <= "zzz"
            {R"("aaa" <= string_array[0] <= "zzz")",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return "aaa" <= val && val <= "zzz";
             }},
            // binary_range_expr: 1.1 <= double_array[0] <= 2048.12
            {"1.1 <= double_array[0] <= 2048.12",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return 1.1 <= val && val <= 2048.12;
             }},
            // unary_range_expr: long_array[0] >= 10000
            {"long_array[0] >= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val >= 10000;
             }},
            // unary_range_expr: long_array[0] > 2000
            {"long_array[0] > 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val > 2000;
             }},
            // unary_range_expr: long_array[0] <= 2000
            {"long_array[0] <= 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val <= 2000;
             }},
            // unary_range_expr: long_array[0] < 2000
            {"long_array[0] < 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val < 2000;
             }},
            // unary_range_expr: long_array[0] == 2000
            {"long_array[0] == 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val == 2000;
             }},
            // unary_range_expr: long_array[0] != 2000
            {"long_array[0] != 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val != 2000;
             }},
            // unary_range_expr: bool_array[0] == false
            {"bool_array[0] == false",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data<bool>(0);
                 return !val;
             }},
            // unary_range_expr: string_array[0] == "abc"
            {R"(string_array[0] == "abc")",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return val == "abc";
             }},
            // unary_range_expr: double_array[0] == 2.2
            {"double_array[0] == 2.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val == 2.2;
             }},
            // unary_range_expr: double_array[1024] == 2.2
            {"double_array[1024] == 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val == 2.2;
             }},
            // unary_range_expr: double_array[1024] != 2.2
            {"double_array[1024] != 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val != 2.2;
             }},
            // unary_range_expr: double_array[1024] >= 2.2
            {"double_array[1024] >= 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val >= 2.2;
             }},
            // unary_range_expr: double_array[1024] > 2.2
            {"double_array[1024] > 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val > 2.2;
             }},
            // unary_range_expr: double_array[1024] <= 2.2
            {"double_array[1024] <= 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val <= 2.2;
             }},
            // unary_range_expr: double_array[1024] < 2.2
            {"double_array[1024] < 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val < 2.2;
             }},

        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto float_array_fid =
        schema->AddDebugField("double_array", DataType::ARRAY, DataType::FLOAT);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);

        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST(Expr, TestArrayEqual) {
    std::vector<
        std::tuple<std::string, std::function<bool(std::vector<int64_t>)>>>
        testcases = {
            // unary_range_expr: long_array == [1, 2, 3]
            {"long_array == [1, 2, 3]",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return false;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return false;
                     }
                 }
                 return true;
             }},
            // unary_range_expr: long_array != [1, 2, 3]
            {"long_array != [1, 2, 3]",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return true;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return true;
                     }
                 }
                 return false;
             }},
        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarFieldProto> long_array_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(long_array_col[i]);
            std::vector<int64_t> array_values(array.length());
            for (int j = 0; j < array.length(); ++j) {
                array_values.push_back(array.get_data<int64_t>(j));
            }
            auto ref = ref_func(array_values);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST(Expr, TestArrayNullExpr) {
    std::vector<std::tuple<std::string, std::function<bool(bool)>>> testcases =
        {
            // null_expr: long_array is null
            {"long_array is null", [](bool v) { return !v; }},
        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarFieldProto> long_array_col;
    int num_iters = 1;
    FixedVector<bool> valid_data;

    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        auto new_valid_col = raw_data.get_col_valid(long_array_fid);
        valid_data.insert(
            valid_data.end(), new_valid_col.begin(), new_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);
        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto valid = valid_data[i];
            auto ref = ref_func(valid);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, PraseArrayContainsExpr) {
    // Test expressions for array_contains operations
    std::vector<const char*> exprs{
        // json_contains_expr: array_contains(array, 1)
        "array_contains(array, 1)",
        // json_contains_expr: array_contains_all(array, [1, 2, 3])
        "array_contains_all(array, [1, 2, 3])",
        // json_contains_expr: array_contains_any(array, [1, 2, 3])
        "array_contains_any(array, [1, 2, 3])",
    };

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddField(FieldName("array"),
                     FieldId(101),
                     DataType::ARRAY,
                     DataType::INT64,
                     false);
    ScopedSchemaHandle schema_handle(*schema);

    for (auto& expr : exprs) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    }
}

TEST(Expr, RLSCountBenchmarkCompareBoolField) {
    auto schema = std::make_shared<Schema>();
    auto id_fid = schema->AddDebugField("id", DataType::INT64);
    auto owner_fid = schema->AddDebugField("owner", DataType::VARCHAR);
    auto shared_users_fid = schema->AddDebugField(
        "shared_users[elements]", DataType::ARRAY, DataType::VARCHAR);
    auto shared_departments_fid = schema->AddDebugField(
        "shared_departments[elements]", DataType::ARRAY, DataType::VARCHAR);

    std::array<FieldId, kRLSBenchmarkUserCount> bool_fids;
    for (int i = 0; i < kRLSBenchmarkUserCount; ++i) {
        bool_fids[i] = schema->AddDebugField(
            fmt::format("can_access_u{:02d}", i), DataType::BOOL);
    }
    schema->set_primary_field_id(id_fid);

    const auto& cases = RLSBenchmarkCases();
    const auto row_count = kRLSBenchmarkBaseRows +
                           static_cast<int64_t>(
                               kRLSHeavySharedUsersCapacities.size());

    std::array<int64_t, kRLSBenchmarkUserCount> expected_counts{};
    std::array<int64_t, kRLSBenchmarkUserCount> heavy_shared_users_capacity{};
    std::array<int64_t, kRLSBenchmarkUserCount> shared_user_doc_count{};
    for (const auto& test_case : cases) {
        expected_counts[UserIndex(test_case.user)] = test_case.base_count;
    }
    for (int i = 0; i < kRLSHeavySharedUsersCapacities.size(); ++i) {
        auto user_index = 8 + i;
        expected_counts[user_index] += 1;
        heavy_shared_users_capacity[user_index] =
            kRLSHeavySharedUsersCapacities[i];
    }
    for (int i = 0; i < kRLSSharedUserDocCounts.size(); ++i) {
        shared_user_doc_count[20 + i] = kRLSSharedUserDocCounts[i];
    }

    std::array<int64_t, kRLSBenchmarkUserCount> case_starts{};
    int64_t cursor = 0;
    for (int i = 0; i < cases.size(); ++i) {
        case_starts[i] = cursor;
        cursor += cases[i].base_count;
    }
    ASSERT_LE(cursor, kRLSBenchmarkBaseRows);

    auto grant_access = [](const RLSUserCase& test_case,
                           std::string& owner,
                           ScalarFieldProto& shared_users,
                           ScalarFieldProto& shared_departments) {
        switch (test_case.style) {
            case RLSGrantStyle::Owner:
                owner = test_case.user;
                break;
            case RLSGrantStyle::SharedUser:
                shared_users.mutable_string_data()->add_data(test_case.user);
                break;
            case RLSGrantStyle::Department:
                shared_departments.mutable_string_data()->add_data(
                    DepartmentForUser(test_case.user));
                break;
            case RLSGrantStyle::Overlap:
                owner = test_case.user;
                shared_users.mutable_string_data()->add_data(test_case.user);
                shared_departments.mutable_string_data()->add_data(
                    DepartmentForUser(test_case.user));
                break;
            case RLSGrantStyle::None:
                break;
        }
    };

    auto find_base_case = [&](int64_t row_id) -> const RLSUserCase* {
        for (int i = 0; i < cases.size(); ++i) {
            if (row_id >= case_starts[i] &&
                row_id < case_starts[i] + cases[i].base_count) {
                return &cases[i];
            }
        }
        return nullptr;
    };

    std::vector<int64_t> ids(row_count);
    std::vector<idx_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count);
    std::vector<std::string> owners(row_count, "external_owner");
    std::vector<ScalarFieldProto> shared_users(row_count);
    std::vector<ScalarFieldProto> shared_departments(row_count);
    std::array<FixedVector<bool>, kRLSBenchmarkUserCount> can_access;
    for (auto& access : can_access) {
        access.resize(row_count, false);
    }

    for (int64_t row = 0; row < row_count; ++row) {
        ids[row] = row;
        row_ids[row] = row;
        timestamps[row] = row;

        if (row < kRLSBenchmarkBaseRows) {
            auto* test_case = find_base_case(row);
            if (test_case != nullptr) {
                grant_access(*test_case,
                             owners[row],
                             shared_users[row],
                             shared_departments[row]);
                can_access[UserIndex(test_case->user)][row] = true;
            }
            continue;
        }

        auto heavy_index = row - kRLSBenchmarkBaseRows;
        const auto& test_case = cases[8 + heavy_index];
        auto capacity = kRLSHeavySharedUsersCapacities[heavy_index];
        auto* user_data = shared_users[row].mutable_string_data()->mutable_data();
        user_data->Reserve(capacity);
        for (int64_t i = 0; i < capacity - 1; ++i) {
            *user_data->Add() = fmt::format("hu{}_{}", capacity, i);
        }
        *user_data->Add() = test_case.user;
        can_access[UserIndex(test_case.user)][row] = true;
    }

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto add_col = [&](FieldId field_id, const auto& data) {
        auto array = milvus::segcore::CreateDataArrayFrom(
            data.data(), nullptr, row_count, schema->operator[](field_id));
        insert_data->mutable_fields_data()->AddAllocated(array.release());
    };
    add_col(id_fid, ids);
    add_col(owner_fid, owners);
    add_col(shared_users_fid, shared_users);
    add_col(shared_departments_fid, shared_departments);
    for (int i = 0; i < kRLSBenchmarkUserCount; ++i) {
        add_col(bool_fids[i], can_access[i]);
    }
    insert_data->set_num_rows(row_count);

    GeneratedData raw_data;
    raw_data.row_ids_ = std::move(row_ids);
    raw_data.timestamps_ = std::move(timestamps);
    raw_data.raw_ = insert_data.release();
    raw_data.schema_ = schema;

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    LoadStringSortIndex(seg.get(), owner_fid, BuildFieldData(raw_data, owner_fid), false);
    LoadStringSortIndex(
        seg.get(), shared_users_fid, BuildFieldData(raw_data, shared_users_fid), true);
    LoadStringSortIndex(seg.get(),
                        shared_departments_fid,
                        BuildFieldData(raw_data, shared_departments_fid),
                        true);
    for (int i = 0; i < kRLSBenchmarkUserCount; ++i) {
        LoadBoolSortIndex(
            seg.get(), bool_fids[i], BuildFieldData(raw_data, bool_fids[i]));
    }

    constexpr int warmup_runs = 1;
    constexpr int benchmark_runs = 3;
    std::cout << "RLS count(*) benchmark rows=" << row_count
              << " base_rows=" << kRLSBenchmarkBaseRows
              << " warmups=" << warmup_runs << " runs=" << benchmark_runs
              << std::endl;
    for (const auto& test_case : cases) {
        auto user_index = UserIndex(test_case.user);
        auto rls_plan = BuildRLSCountPlan(
            owner_fid, shared_users_fid, shared_departments_fid, test_case.user);
        auto bool_plan = BuildBoolCountPlan(bool_fids[user_index]);

        for (int i = 0; i < warmup_runs; ++i) {
            auto [rls_count, rls_ms] =
                RunCountPlan(rls_plan, seg.get(), row_count);
            auto [bool_count, bool_ms] =
                RunCountPlan(bool_plan, seg.get(), row_count);
            ASSERT_EQ(rls_count, expected_counts[user_index]);
            ASSERT_EQ(bool_count, expected_counts[user_index]);
        }

        std::vector<double> rls_ms;
        std::vector<double> bool_ms;
        for (int i = 0; i < benchmark_runs; ++i) {
            auto [rls_count, rls_elapsed] =
                RunCountPlan(rls_plan, seg.get(), row_count);
            auto [bool_count, bool_elapsed] =
                RunCountPlan(bool_plan, seg.get(), row_count);
            ASSERT_EQ(rls_count, expected_counts[user_index]);
            ASSERT_EQ(bool_count, expected_counts[user_index]);
            rls_ms.push_back(rls_elapsed);
            bool_ms.push_back(bool_elapsed);
        }

        auto rls_p50 = MedianMs(rls_ms);
        auto bool_p50 = MedianMs(bool_ms);
        auto ratio = bool_p50 > 0 ? rls_p50 / bool_p50 : 0;
        std::cout << "RLS_COUNT_RESULT user=" << test_case.user
                  << " category=" << test_case.category
                  << " expected=" << expected_counts[user_index]
                  << " heavy_shared_users_capacity="
                  << heavy_shared_users_capacity[user_index]
                  << " shared_user_doc_count="
                  << shared_user_doc_count[user_index]
                  << " rls_p50_ms=" << rls_p50
                  << " bool_p50_ms=" << bool_p50 << " ratio=" << ratio
                  << std::endl;
    }
}

TEST(Expr, RLSCountBenchmarkArrayProjectionPOC) {
    auto fixture = BuildRLSBenchmarkFixture();
    const auto& cases = RLSBenchmarkCases();

    constexpr int warmup_runs = 1;
    constexpr int benchmark_runs = 3;
    std::cout << "RLS projection POC rows=" << fixture.row_count
              << " base_rows=" << kRLSBenchmarkBaseRows
              << " warmups=" << warmup_runs << " runs=" << benchmark_runs
              << std::endl;

    for (const auto& test_case : cases) {
        auto user_index = UserIndex(test_case.user);
        auto bool_plan = BuildBoolCountPlan(fixture.bool_fids[user_index]);

        auto validate = [&](const TargetBitmap& current,
                            const TargetBitmap& setbit,
                            const TargetBitmap& direct,
                            size_t bool_count) {
            ASSERT_TRUE(BitsetsEqual(current, setbit));
            ASSERT_TRUE(BitsetsEqual(current, direct));
            ASSERT_EQ(current.count(), fixture.expected_counts[user_index]);
            ASSERT_EQ(setbit.count(), fixture.expected_counts[user_index]);
            ASSERT_EQ(direct.count(), fixture.expected_counts[user_index]);
            ASSERT_EQ(bool_count, fixture.expected_counts[user_index]);
        };

        for (int i = 0; i < warmup_runs; ++i) {
            auto [current_bits, current_ms] = TimeProjection([&] {
                return RunRLSProjectionCurrent(fixture, test_case.user);
            });
            auto [setbit_bits, setbit_ms] = TimeProjection([&] {
                return RunRLSProjectionSetBitCursor(fixture, test_case.user);
            });
            auto [direct_bits, direct_ms] = TimeProjection([&] {
                return RunRLSProjectionDirect(fixture, test_case.user);
            });
            auto [bool_count, bool_ms] =
                RunCountPlan(bool_plan, fixture.segment.get(), fixture.row_count);
            validate(current_bits, setbit_bits, direct_bits, bool_count);
        }

        std::vector<double> current_ms;
        std::vector<double> setbit_ms;
        std::vector<double> direct_ms;
        std::vector<double> bool_ms;
        size_t current_count = 0;
        size_t setbit_count = 0;
        size_t direct_count = 0;
        size_t bool_count = 0;

        for (int i = 0; i < benchmark_runs; ++i) {
            auto [current_bits, current_elapsed] = TimeProjection([&] {
                return RunRLSProjectionCurrent(fixture, test_case.user);
            });
            auto [setbit_bits, setbit_elapsed] = TimeProjection([&] {
                return RunRLSProjectionSetBitCursor(fixture, test_case.user);
            });
            auto [direct_bits, direct_elapsed] = TimeProjection([&] {
                return RunRLSProjectionDirect(fixture, test_case.user);
            });
            auto [bool_result_count, bool_elapsed] =
                RunCountPlan(bool_plan, fixture.segment.get(), fixture.row_count);

            validate(current_bits, setbit_bits, direct_bits, bool_result_count);
            current_count = current_bits.count();
            setbit_count = setbit_bits.count();
            direct_count = direct_bits.count();
            bool_count = bool_result_count;
            current_ms.push_back(current_elapsed);
            setbit_ms.push_back(setbit_elapsed);
            direct_ms.push_back(direct_elapsed);
            bool_ms.push_back(bool_elapsed);
        }

        auto current_p50 = MedianMs(current_ms);
        auto setbit_p50 = MedianMs(setbit_ms);
        auto direct_p50 = MedianMs(direct_ms);
        auto bool_p50 = MedianMs(bool_ms);
        std::cout << "RLS_PROJECTION_POC_RESULT user=" << test_case.user
                  << " category=" << test_case.category
                  << " expected=" << fixture.expected_counts[user_index]
                  << " heavy_shared_users_capacity="
                  << fixture.heavy_shared_users_capacity[user_index]
                  << " shared_user_doc_count="
                  << fixture.shared_user_doc_count[user_index]
                  << " current_count=" << current_count
                  << " setbit_count=" << setbit_count
                  << " direct_count=" << direct_count
                  << " bool_count=" << bool_count
                  << " model_a_current_p50_ms=" << current_p50
                  << " model_a_setbit_p50_ms=" << setbit_p50
                  << " model_b_direct_p50_ms=" << direct_p50
                  << " bool_p50_ms=" << bool_p50 << std::endl;
    }
}

template <typename T>
struct ArrayTestcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
};

TEST(Expr, TestArrayContains) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col =
            raw_data.get_col<ScalarFieldProto>(int_array_fid);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarFieldProto>(double_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<ArrayTestcase<bool>> bool_testcases{{{true, true}, {}},
                                                    {{false, false}, {}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_bool_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(bool_array_fid, DataType::ARRAY, DataType::BOOL),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["bool"][i]);
            std::vector<bool> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<bool>(j));
            }
            ASSERT_EQ(ans, check(res)) << "@" << i;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res)) << "@" << i;
            }
        }
    }

    std::vector<ArrayTestcase<double>> double_testcases{
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_float_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                double_array_fid, DataType::ARRAY, DataType::DOUBLE),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["double"][i]);
            std::vector<double> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<double>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<float>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_float_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(float_array_fid, DataType::ARRAY, DataType::FLOAT),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["float"][i]);
            std::vector<float> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<float>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<ArrayTestcase<int64_t>> testcases{
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_int64_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(int_array_fid, DataType::ARRAY, DataType::INT8),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["int"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_int64_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<ArrayTestcase<std::string>> testcases_string = {
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_string_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, DataType::VARCHAR),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            std::vector<std::string_view> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<std::string_view>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

TEST(Expr, TestArrayContainsEmptyValues) {
    auto schema = std::make_shared<Schema>();
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(schema->AddDebugField("id", DataType::INT64));
    std::vector<FieldId> fields = {
        int_array_fid,
        long_array_fid,
        bool_array_fid,
        float_array_fid,
        double_array_fid,
        string_array_fid,
    };

    auto dummy_seg = CreateGrowingSegment(schema, empty_index_meta);

    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        dummy_seg->PreInsert(N);
        dummy_seg->Insert(iter * N,
                          N,
                          raw_data.row_ids_.data(),
                          raw_data.timestamps_.data(),
                          raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(dummy_seg.get());
    std::vector<proto::plan::GenericValue> empty_values;

    for (auto field_id : fields) {
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(field_id, DataType::ARRAY),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            empty_values);

        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);
        for (int i = 0; i < N * num_iters; ++i) {
            ASSERT_EQ(final[i], false);
        }
    }
}

TEST(Expr, TestArrayBinaryArith) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col =
            raw_data.get_col<ScalarFieldProto>(int_array_fid);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarFieldProto>(double_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // int_array[0] + 2 == 5
            {"int_array[0] + 2 == 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 == 5;
             }},
            // int_array[0] + 2 != 5
            {"int_array[0] + 2 != 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 != 5;
             }},
            // int_array[0] + 2 > 5
            {"int_array[0] + 2 > 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 > 5;
             }},
            // int_array[0] + 2 >= 5
            {"int_array[0] + 2 >= 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 >= 5;
             }},
            // int_array[0] + 2 < 5
            {"int_array[0] + 2 < 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 < 5;
             }},
            // int_array[0] + 2 <= 5
            {"int_array[0] + 2 <= 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 <= 5;
             }},
            // long_array[0] - 1 == 144
            {"long_array[0] - 1 == 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 == 144;
             }},
            // long_array[0] - 1 != 144
            {"long_array[0] - 1 != 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 != 144;
             }},
            // long_array[0] - 1 > 144
            {"long_array[0] - 1 > 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 > 144;
             }},
            // long_array[0] - 1 >= 144
            {"long_array[0] - 1 >= 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 >= 144;
             }},
            // long_array[0] - 1 < 144
            {"long_array[0] - 1 < 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 < 144;
             }},
            // long_array[0] - 1 <= 144
            {"long_array[0] - 1 <= 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 <= 144;
             }},
            // float_array[0] + 2.2 == 133.2
            {"float_array[0] + 2.2 == 133.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val + 2.2 == 133.2;
             }},
            // float_array[0] + 2.2 != 133.2
            {"float_array[0] + 2.2 != 133.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val + 2.2 != 133.2;
             }},
            // double_array[0] - 11.1 == 125.7
            {"double_array[0] - 11.1 == 125.7",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val - 11.1 == 125.7;
             }},
            // double_array[0] - 11.1 != 125.7
            {"double_array[0] - 11.1 != 125.7",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val - 11.1 != 125.7;
             }},
            // long_array[0] * 2 == 8
            {"long_array[0] * 2 == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 == 8;
             }},
            // long_array[0] * 2 != 20
            {"long_array[0] * 2 != 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 != 20;
             }},
            // long_array[0] * 2 > 20
            {"long_array[0] * 2 > 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 > 20;
             }},
            // long_array[0] * 2 >= 20
            {"long_array[0] * 2 >= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 >= 20;
             }},
            // long_array[0] * 2 < 20
            {"long_array[0] * 2 < 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 < 20;
             }},
            // long_array[0] * 2 <= 20
            {"long_array[0] * 2 <= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 <= 20;
             }},
            // long_array[0] / 2 == 8
            {"long_array[0] / 2 == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 == 8;
             }},
            // long_array[0] / 2 != 20
            {"long_array[0] / 2 != 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 != 20;
             }},
            // long_array[0] / 2 > 20
            {"long_array[0] / 2 > 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 > 20;
             }},
            // long_array[0] / 2 >= 20
            {"long_array[0] / 2 >= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 >= 20;
             }},
            // long_array[0] / 2 < 20
            {"long_array[0] / 2 < 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 < 20;
             }},
            // long_array[0] / 2 <= 20
            {"long_array[0] / 2 <= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 <= 20;
             }},
            // long_array[0] % 3 == 0
            {"long_array[0] % 3 == 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 == 0;
             }},
            // long_array[0] % 3 != 2
            {"long_array[0] % 3 != 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 != 2;
             }},
            // long_array[0] % 3 > 2
            {"long_array[0] % 3 > 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 > 2;
             }},
            // long_array[0] % 3 >= 2
            {"long_array[0] % 3 >= 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 >= 2;
             }},
            // long_array[0] % 3 < 2
            {"long_array[0] % 3 < 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 < 2;
             }},
            // long_array[0] % 3 <= 2
            {"long_array[0] % 3 <= 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 <= 2;
             }},
            // float_array[1024] + 2.2 == 133.2
            {"float_array[1024] + 2.2 == 133.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val + 2.2 == 133.2;
             }},
            // float_array[1024] + 2.2 != 133.2
            {"float_array[1024] + 2.2 != 133.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val + 2.2 != 133.2;
             }},
            // double_array[1024] - 11.1 == 125.7
            {"double_array[1024] - 11.1 == 125.7",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val - 11.1 == 125.7;
             }},
            // double_array[1024] - 11.1 != 125.7
            {"double_array[1024] - 11.1 != 125.7",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val - 11.1 != 125.7;
             }},
            // long_array[1024] * 2 == 8
            {"long_array[1024] * 2 == 8",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val * 2 == 8;
             }},
            // long_array[1024] * 2 != 20
            {"long_array[1024] * 2 != 20",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val * 2 != 20;
             }},
            // long_array[1024] / 2 == 8
            {"long_array[1024] / 2 == 8",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val / 2 == 8;
             }},
            // long_array[1024] / 2 != 20
            {"long_array[1024] / 2 != 20",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val / 2 != 20;
             }},
            // long_array[1024] % 3 == 0
            {"long_array[1024] % 3 == 0",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val % 3 == 0;
             }},
            // long_array[1024] % 3 != 2
            {"long_array[1024] % 3 != 2",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val % 3 != 2;
             }},
            // array_length(int_array) == 10
            {"array_length(int_array) == 10",
             "int",
             [](milvus::Array& array) { return array.length() == 10; }},
            // array_length(int_array) != 8
            {"array_length(int_array) != 8",
             "int",
             [](milvus::Array& array) { return array.length() != 8; }},
            // array_length(int_array) > 8
            {"array_length(int_array) > 8",
             "int",
             [](milvus::Array& array) { return array.length() > 8; }},
            // array_length(int_array) >= 8
            {"array_length(int_array) >= 8",
             "int",
             [](milvus::Array& array) { return array.length() >= 8; }},
            // array_length(int_array) < 8
            {"array_length(int_array) < 8",
             "int",
             [](milvus::Array& array) { return array.length() < 8; }},
            // array_length(int_array) <= 8
            {"array_length(int_array) <= 8",
             "int",
             [](milvus::Array& array) { return array.length() <= 8; }},
        };

    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

template <typename T>
struct UnaryRangeTestcase {
    milvus::OpType op_type;
    T value;
    std::vector<std::string> nested_path;
    std::function<bool(milvus::Array&)> check_func;
};

TEST(Expr, TestArrayStringMatch) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<UnaryRangeTestcase<std::string>> prefix_testcases{
        {OpType::PrefixMatch,
         "abc",
         {"0"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data<std::string_view>(0), "abc");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data<std::string_view>(1), "def");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1024"},
         [](milvus::Array& array) {
             if (array.length() <= 1024) {
                 return false;
             }
             return PrefixMatch(array.get_data<std::string_view>(1024), "def");
         }},
    };
    //vector_anns:<field_id:201 predicates:<unary_range_expr:<column_info:<field_id:131 data_type:Array nested_path:"0" element_type:VarChar > op:PrefixMatch value:<string_val:"abc" > > > query_info:<> placeholder_tag:"$0" >
    for (auto& testcase : prefix_testcases) {
        proto::plan::GenericValue value;
        value.set_string_val(testcase.value);
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, testcase.nested_path),
            testcase.op_type,
            value,
            std::vector<proto::plan::GenericValue>{});
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], testcase.check_func(array));
            }
        }
    }
}

TEST(Expr, TestArrayInTerm) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // term_expr: long_array[0] in [1, 2, 3]
            {"long_array[0] in [1, 2, 3]",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val == 1 || val == 2 || val == 3;
             }},
            // term_expr: long_array[0] in [] (empty list)
            {"long_array[0] in []",
             "long",
             [](milvus::Array& array) { return false; }},
            // term_expr: bool_array[0] in [false, false]
            {"bool_array[0] in [false, false]",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data<bool>(0);
                 return !val;
             }},
            // term_expr: bool_array[0] in [] (empty list)
            {"bool_array[0] in []",
             "bool",
             [](milvus::Array& array) { return false; }},
            // term_expr: float_array[0] in [1.23, 124.31]
            {"float_array[0] in [1.23, 124.31]",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val == 1.23 || val == 124.31;
             }},
            // term_expr: float_array[0] in [] (empty list)
            {"float_array[0] in []",
             "float",
             [](milvus::Array& array) { return false; }},
            // term_expr: string_array[0] in ["abc", "idhgf1s"]
            {R"(string_array[0] in ["abc", "idhgf1s"])",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return val == "abc" || val == "idhgf1s";
             }},
            // term_expr: string_array[0] in [] (empty list)
            {R"(string_array[0] in [])",
             "string",
             [](milvus::Array& array) { return false; }},
            // term_expr: string_array[1024] in ["abc", "idhgf1s"]
            {R"(string_array[1024] in ["abc", "idhgf1s"])",
             "string",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<std::string_view>(1024);
                 return val == "abc" || val == "idhgf1s";
             }},
        };

    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            ASSERT_EQ(ans, ref_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref_func(array));
            }
        }
    }
}

TEST(Expr, TestTermInArray) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    struct TermTestCases {
        std::vector<int64_t> values;
        std::vector<std::string> nested_path;
        std::function<bool(milvus::Array&)> check_func;
    };
    std::vector<TermTestCases> testcases = {
        {{100},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data<int64_t>(i);
                 if (val == 100) {
                     return true;
                 }
             }
             return false;
         }},
        {{1024},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data<int64_t>(i);
                 if (val == 1024) {
                     return true;
                 }
             }
             return false;
         }},
    };

    for (auto& testcase : testcases) {
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.values) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.emplace_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                long_array_fid, DataType::ARRAY, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], testcase.check_func(array));
            }
        }
    }
}
