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
#include <map>
#include <unordered_map>
#include <vector>
#include <chrono>

#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/BitmapIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/StringIndex.h"
#include "index/NgramInvertedIndex.h"
#include "storage/FileManager.h"
#include "segment_wrapper.h"
#include "scalar_filter_benchmark.h"

namespace milvus {
namespace scalar_bench {

// 索引构建结果
struct IndexBuildResult {
    bool success;
    double build_time_ms;
    size_t memory_bytes;
    size_t serialized_size;
    std::string error_message;
    std::vector<std::string> index_files;
};

// 索引包装器基类
class IndexWrapperBase {
public:
    virtual ~IndexWrapperBase() = default;

    // 构建索引
    virtual IndexBuildResult Build(const SegmentWrapper& segment,
                                    const std::string& field_name,
                                    const IndexConfig& config) = 0;

    // 加载索引到Segment (提供默认实现)
    virtual bool LoadToSegment(SegmentWrapper& segment,
                                const std::string& field_name,
                                const IndexBuildResult& build_result);

    // 获取索引类型名称
    virtual std::string GetTypeName() const = 0;

protected:
    // 缓存构建的索引对象
    std::unordered_map<int64_t, milvus::index::IndexBasePtr> index_cache_;
};

// 具体索引包装器
class BitmapIndexWrapper : public IndexWrapperBase {
public:
    IndexBuildResult Build(const SegmentWrapper& segment,
                           const std::string& field_name,
                           const IndexConfig& config) override;

    std::string GetTypeName() const override { return "BITMAP"; }
};

class InvertedIndexWrapper : public IndexWrapperBase {
public:
    IndexBuildResult Build(const SegmentWrapper& segment,
                           const std::string& field_name,
                           const IndexConfig& config) override;

    std::string GetTypeName() const override { return "INVERTED"; }
};

class STLSortIndexWrapper : public IndexWrapperBase {
public:
    IndexBuildResult Build(const SegmentWrapper& segment,
                           const std::string& field_name,
                           const IndexConfig& config) override;

    std::string GetTypeName() const override { return "STL_SORT"; }
};

// 索引工厂
class IndexWrapperFactory {
public:
    static std::unique_ptr<IndexWrapperBase> CreateIndexWrapper(ScalarIndexType type);
};

// 索引管理器
class IndexManager {
public:
    IndexManager(std::shared_ptr<milvus::storage::ChunkManager> chunk_manager);

    // 构建并加载索引
    IndexBuildResult BuildAndLoadIndex(SegmentWrapper& segment,
                                        const std::string& field_name,
                                        const IndexConfig& config);

private:
    std::shared_ptr<milvus::storage::ChunkManager> chunk_manager_;
    int64_t next_index_build_id_;
    int64_t next_index_id_;
};

} // namespace scalar_bench
} // namespace milvus