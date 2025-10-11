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
#include <string>
#include <unordered_map>
#include <vector>

#include "index/Index.h"
#include "segment_wrapper.h"
#include "scalar_filter_benchmark.h"

namespace milvus {
namespace scalar_bench {

// 索引构建结果
struct IndexBuildResult {
    double build_time_ms;
    size_t memory_bytes;
    size_t serialized_size;
    std::string error_message;
    std::vector<std::string> index_files;
};

// 索引包装器基类
class IndexWrapper {
public:
    virtual ~IndexWrapper() = default;

    // 构建索引
    virtual IndexBuildResult Build(const SegmentWrapper& segment,
                                   const std::string& field_name,
                                   const IndexConfig& config);

    // 加载索引到Segment (提供默认实现)
    virtual void LoadToSegment(SegmentWrapper& segment,
                               const std::string& field_name);

    // 获取索引类型名称
    std::string GetTypeName() const { return spec_.name; }

    // 规格，描述差异
    struct IndexBuildSpec {
        std::string name;                 // 展示名
        std::string index_type;           // milvus::index::<TYPE>
        int64_t build_id_seed = 0;        // 用于 index_meta 区分
        int64_t version_seed = 0;         // 用于 index_meta 区分
        bool numeric_only = false;        // 是否仅支持数值类型
    };

    explicit IndexWrapper(IndexBuildSpec spec) : spec_(std::move(spec)) {}

private:
    // 构建产物：用于后续加载
    struct BuiltIndexArtifacts {
        std::vector<std::string> index_files;
        std::map<std::string, std::string> index_params;
        proto::schema::FieldSchema field_schema;  // for loading
        int64_t index_build_id{0};
        int64_t index_version{0};
    };

    // 按 field_id 缓存构建产物（index files + params）
    std::unordered_map<int64_t, BuiltIndexArtifacts> index_cache_;
    IndexBuildSpec spec_;
};

// 索引管理器
class IndexManager {
public:
    IndexManager(std::shared_ptr<milvus::storage::ChunkManager> chunk_manager);

    // 构建并加载索引 (field-specific configuration)
    IndexBuildResult BuildAndLoadIndexForField(SegmentWrapper& segment,
                                               const std::string& field_name,
                                               const FieldIndexConfig& field_config);

private:
    std::shared_ptr<milvus::storage::ChunkManager> chunk_manager_;
};

} // namespace scalar_bench
} // namespace milvus