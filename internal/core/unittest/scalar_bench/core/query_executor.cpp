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

#include "query_executor.h"
#include <google/protobuf/text_format.h>
#include <sys/resource.h>
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "expr_parser_client.h"
#include "query/PlanProto.h"

namespace milvus::scalar_bench {

QueryExecutor::QueryExecutor(SchemaPtr schema) : schema_(schema) {
    AssertInfo(schema != nullptr, "Schema cannot be null");
}

QueryResult
QueryExecutor::ExecuteQueryExpr(
    SegmentInterface* segment,
    const std::string& expr,
    bool is_count,
    int64_t limit) {
    QueryResult result;
    result.total_rows = segment->get_row_count();

    try {
        auto plan = BuildPlanFromExpr(expr, is_count, limit);

        auto start = std::chrono::high_resolution_clock::now();
        auto initial_memory = MeasureMemoryUsage();

        auto retrieve_result = segment->Retrieve(
            nullptr,  // RetrieveContext
            plan.get(),
            MAX_TIMESTAMP,
            limit > 0 ? limit : DEFAULT_MAX_OUTPUT_SIZE,
            false  // ignore_non_pk
        );

        auto end = std::chrono::high_resolution_clock::now();
        result.execution_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
        auto final_memory = MeasureMemoryUsage();
        result.memory_used_bytes = final_memory - initial_memory;

        if (retrieve_result) {
            if (is_count) {
                result.matched_rows = retrieve_result->fields_data(0).scalars().long_data().data(0);
            } else {
                auto matched_offsets = ExtractMatchedOffsets(retrieve_result);
                result.matched_rows = matched_offsets.size();
            }
            result.selectivity = static_cast<double>(result.matched_rows) / result.total_rows;
            result.success = true;
        } else {
            result.success = false;
            result.error_message = "Query returned null result";
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = e.what();
    }
    return result;
}

std::unique_ptr<RetrievePlan>
QueryExecutor::BuildPlanFromExpr(const std::string& expr,
                                 bool is_count,
                                 int64_t limit) {
    // Build schema proto bytes
    auto schema_bytes = BuildCollectionSchemaProtoBytes(schema_);
    // Call helper
    auto& client = ExprParserClient::Instance();
    client.Start();
    std::string plan_bytes = client.ParseExprToPlanBytes(expr, schema_bytes, is_count, limit > 0 ? limit : DEFAULT_MAX_OUTPUT_SIZE);
    // Parse plan bytes
    proto::plan::PlanNode plan_pb;
    bool ok = plan_pb.ParseFromString(plan_bytes);
    AssertInfo(ok, "failed to parse plan bytes returned by helper");
    ProtoParser parser(schema_);
    return parser.CreateRetrievePlan(plan_pb);
}

int64_t
QueryExecutor::MeasureMemoryUsage() {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        return usage.ru_maxrss * 1024;  // Convert KB to bytes
    }
    return 0;
}

std::vector<int64_t>
QueryExecutor::ExtractMatchedOffsets(
    const std::unique_ptr<milvus::proto::segcore::RetrieveResults>& results) {

    std::vector<int64_t> offsets;

    if (!results) {
        return offsets;
    }

    // Extract offsets from the retrieve result
    offsets.reserve(results->offset_size());
    for (int i = 0; i < results->offset_size(); ++i) {
        offsets.push_back(results->offset(i));
    }

    return offsets;
}

} // namespace milvus::scalar_bench