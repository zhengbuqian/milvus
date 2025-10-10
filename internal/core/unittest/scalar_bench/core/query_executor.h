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

#pragma once

#include <memory>
#include <vector>
#include <string>

#include "common/Schema.h"
#include "segcore/SegmentInterface.h"
#include "query/Plan.h"

namespace milvus::scalar_bench {

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::query;

struct QueryResult {
    // Execution metrics
    double execution_time_ms;
    int64_t matched_rows;
    int64_t total_rows;
    double selectivity;

    // Memory usage
    int64_t memory_used_bytes;

    // Error info
    bool success;
    std::string error_message;
};

class QueryExecutor {
public:
    QueryExecutor(SchemaPtr schema);
    ~QueryExecutor() = default;

    // Execute query using expr string via Go helper
    QueryResult ExecuteQueryExpr(
        SegmentInterface* segment,
        const std::string& expr,
        bool is_count = true,
        int64_t limit = -1);

private:
    SchemaPtr schema_;

    // Build query plan by expr with Go helper
    std::unique_ptr<RetrievePlan> BuildPlanFromExpr(const std::string& expr,
                                                    bool is_count,
                                                    int64_t limit);

    // Measure memory usage during query execution
    int64_t MeasureMemoryUsage();

    // Extract matched offsets from retrieve results
    std::vector<int64_t> ExtractMatchedOffsets(
        const std::unique_ptr<milvus::proto::segcore::RetrieveResults>& results);
};

} // namespace milvus::scalar_bench