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

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>

#include "common/Schema.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentSealed.h"
#include "segcore/segment_c.h"
#include "common/FieldData.h"
#include "storage/InsertData.h"
#include "storage/ChunkManager.h"
#include "test_utils/storage_test_utils.h"

// Forward declarations from benchmark config
#include "../config/benchmark_config.h"
#include "common/Types.h"

namespace milvus {
namespace scalar_bench {

// Forward declarations
class SegmentData;

class SegmentWrapper {
public:
    SegmentWrapper();
    ~SegmentWrapper() = default;

    void Initialize(const DataConfig& config);

    void LoadFromSegmentData(const SegmentData& segment_data);

    std::shared_ptr<milvus::Schema> GetSchema() const { return schema_; }

    std::shared_ptr<milvus::segcore::SegmentSealed> GetSealedSegment() const {
        return sealed_segment_;
    }

    FieldId GetFieldId(const std::string& field_name) const;

    std::vector<std::string> GetFieldInsertFiles(FieldId field_id) const;

    int64_t GetRowCount() const { return row_count_; }

    int64_t GetCollectionId() const { return collection_id_; }
    int64_t GetPartitionId() const { return partition_id_; }
    int64_t GetSegmentId() const { return segment_id_; }

    void DropIndex(FieldId field_id);

private:

    void WriteBinlogThenLoad(
        const std::string& field_name,
        FieldId field_id,
        const milvus::DataArray& field_data);

    void LoadSystemFields(const SegmentData& segment_data);

private:
    std::shared_ptr<milvus::Schema> schema_;
    std::shared_ptr<milvus::segcore::SegmentSealed> sealed_segment_;
    std::shared_ptr<milvus::storage::ChunkManager> chunk_manager_;

    std::map<std::string, FieldId> field_name_to_id_;
    std::map<FieldId, std::string> field_id_to_name_;

    int64_t collection_id_;
    int64_t partition_id_;
    int64_t segment_id_;
    int64_t row_count_;

    std::unordered_map<int64_t, std::vector<std::string>> field_insert_files_;

    static int64_t next_collection_id_;
    static int64_t next_partition_id_;
    static int64_t next_segment_id_;
};

} // namespace scalar_bench
} // namespace milvus