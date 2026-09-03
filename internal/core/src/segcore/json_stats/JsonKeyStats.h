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

#include <folly/ExceptionWrapper.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <any>
#include <functional>
#include <initializer_list>
#include <istream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/OpContext.h"
#include "common/Span.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/bson_view.h"
#include "common/jsmn.h"
#include "common/protobuf_utils.h"
#include "folly/FBVector.h"
#include "glog/logging.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/SkipIndex.h"
#include "segcore/json_stats/bson_inverted.h"
#include "segcore/json_stats/parquet_writer.h"
#include "segcore/json_stats/utils.h"
#include "log/Log.h"
#include "mmap/ChunkedColumnInterface.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/MemFileManagerImpl.h"

class CollectSingleJsonStatsInfoAccessor;
// Forward declaration of test accessor in global namespace for friend declaration
class TraverseJsonForBuildStatsAccessor;
class JsonStatsProjectionTestAccessor;

namespace milvus::index {

// JSON SHREDDING IS NOT AN INDEX, IT IS A COLUMN LAYOUT.
// See core_refactor/01-scalar-index.md §1 ("JSON shredding is a column layout,
// not an index") and the §8 mapping table row for `JsonKeyStats`.
//
// This class used to derive from `ScalarIndex<std::string>` purely to borrow
// the index factory / load / pin machinery; the price was a whole predicate
// surface of `ThrowInfo(NotImplemented)` overrides that no call site ever
// reached. §1's transitional treatment for refactor phase 1 is exactly two
// steps, with NO behaviour change, NO format change and NO Go-side change:
//   1. drop the inheritance clause and the dead `NotImplemented` overrides
//      (`In` / `NotIn` / `Range` x2 / `IsNull` / `IsNotNull` / `Reverse_Lookup`
//      / `Build(n, values, valid)` / `BuildWithDataset` / `BuildWithRawDataForUT`
//      / `Load(BinarySet)`), keeping `Build(config)` / `Upload(config)` /
//      `Load(TraceContext, Config)` / `Serialize` as ordinary methods because
//      those have real call sites;
//   2. `git mv index/json_stats/ -> segcore/json_stats/`, which zeroes the five
//      `index -> segcore` reverse include edges on the spot (they were all in
//      `JsonKeyStats.cpp`). `indexbuilder` now includes segcore: an L5 -> L3
//      downward edge, which is legal.
//
// WHY segcore AND NOT A CLEANER STANDALONE COMPONENT: it needs
// `ManifestGroupTranslator` (segcore) while segcore's `runtime.json_stats`
// holds it, so a standalone component would cycle unless the translator moved
// out of segcore first (refactor phase 2 / refactor phase 3 work). The
// wide-table modelling design has already ruled that the JSON layout directory
// lives in segcore permanently, so this is not throwaway placement.
//
// EXPLICITLY NOT DONE THIS PHASE (all of it waits on wide-table modelling, see
// §1's "explicitly out of scope" list):
//   - the typed sub-columns are NOT promoted to first-class columnar-format
//     objects (they are already `ManifestGroupTranslator` ->
//     `ChunkedColumnGroup` -> `ProxyChunkColumn`, i.e. the ordinary
//     storage-v2 column construction chain, but the promotion is a separate
//     modelling step);
//   - the "optional alternative layout of a column" concept and `ColumnCaps`
//     are NOT designed;
//   - exec's JSON expression call shape is unchanged (only include paths moved);
//   - this class is NOT wired to the L1 artifact pipeline
//     (`storage::Artifact` / `storage::ArtifactLoader`, §11.2 item 1) — it keeps
//     hand-writing its own `Build` / `Serialize` / `Upload` / `Load`;
//   - NOT renamed: `JsonKeyStats` crosses into proto and the Go side.
//
// DELIBERATE LEFTOVER: the namespace stays `milvus::index` even though the file
// now lives under `segcore/`. §1 authorises exactly the two steps above and
// insists on zero call-site changes; a namespace rename is a third step it does
// not authorise, and wide-table modelling will re-name these types anyway when
// it unifies Struct/JSON shredding. Revisit together with the rename.
//
// EXIT CONDITION for this transitional placement (§1's cost-and-exit-condition
// entry): wide-table modelling chapter 6 (the query node's data representation)
// lands AND the sub-columns are promoted to columnar-format objects. Until then
// `segcore/json_stats/` is a JSON-specific data structure inside segcore, which
// runs against segcore's "shed the type special cases" direction. That is
// knowingly accepted.
class JsonKeyStats {
 public:
    explicit JsonKeyStats(
        const storage::FileManagerContext& ctx,
        bool is_load,
        int64_t json_stats_max_shredding_columns = 1024,
        double json_stats_shredding_ratio_threshold = 0.3,
        int64_t json_stats_write_batch_size = 81920,
        uint32_t tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION);

    ~JsonKeyStats();

 public:
    // ---- Build / persist / load ---------------------------------------------
    // These four are the ONLY members of the former `ScalarIndex<std::string>`
    // surface that had real call sites, so §1 keeps them as ordinary methods
    // (no `override`, nothing to override any more):
    //   Build(config)            <- indexbuilder/index_c.cpp:485 (BuildJsonKeyIndex)
    //   Upload(config)           <- indexbuilder/index_c.cpp:486
    //   Load(TraceContext, ...)  <- ChunkedSegmentSealedImpl::BuildJsonKeyStatsIndex
    //   Serialize(config)        <- internal to the build path
    //
    // NOT wired to `storage::Artifact` / `storage::ArtifactLoader` this phase —
    // §1's "explicitly out of scope" list keeps the hand-written pipeline until
    // wide-table modelling lands (§12.2 also notes refactor phase 1 ends with
    // index as the pipeline's only real consumer, so the second consumer cannot
    // be validated yet).
    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas, bool nullable);

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {});

    void
    Build(const Config& config = {});

    BinarySet
    Serialize(const Config& config);

    IndexStatsPtr
    Upload(const Config& config = {});

    // ---- Self-description ----------------------------------------------------
    int64_t
    Count() const {
        return num_rows_;
    }

    // DELETED WITH THE INHERITANCE CLAUSE (§1, the "only coupling point"
    // table): every one of these was a `ThrowInfo(NotImplemented)` shell
    // reachable from no call site.
    //   Load(BinarySet, Config), BuildWithDataset, BuildWithRawDataForUT,
    //   Build(n, values, valid), In, NotIn, Range x2, IsNull, IsNotNull,
    //   Reverse_Lookup, HasRawData, Size, GetIndexType
    // (`GetIndexType()` returned `ScalarIndexType::JSONSTATS` and had ZERO call
    // sites — it existed only to satisfy the base class, and keeping it would
    // have kept `index/ScalarIndex.h` in this header's include set for nothing.)
    // The query surface of shredding is `ExecutorForShreddingData` /
    // `ExecuteForSharedData` below, which exec calls on the CONCRETE type via
    // `segment->GetJsonStats()` — never through a virtual dispatch.

 public:
    PinWrapper<BsonInvertedIndex*>
    GetBsonIndex(milvus::OpContext* op_ctx) const {
        if (bson_index_cache_slot_ == nullptr) {
            return PinWrapper<BsonInvertedIndex*>(nullptr);
        }
        auto ca = SemiInlineGet(bson_index_cache_slot_->PinCells(op_ctx, {0}));
        auto index = ca->get_cell_of(0);
        return PinWrapper<BsonInvertedIndex*>(std::move(ca), index);
    }

    void
    ExecuteForSharedData(
        milvus::OpContext* op_ctx,
        PinWrapper<BsonInvertedIndex*>& bson_index_cache,
        const std::string& path,
        std::function<void(BsonView bson, uint32_t row_id, uint32_t offset)>
            func) {
        if (bson_index_cache.get() == nullptr) {
            bson_index_cache = GetBsonIndex(op_ctx);
        }
        if (bson_index_cache.get() == nullptr || shared_column_ == nullptr) {
            return;
        }
        bson_index_cache.get()->TermQuery(
            path,
            [this, &func, op_ctx](const uint32_t* row_id_array,
                                  const uint32_t* offset_array,
                                  const int64_t array_len) {
                shared_column_->BulkRawBsonAt(
                    op_ctx, func, row_id_array, offset_array, array_len);
            });
    }

    int64_t
    ExecutorForGettingValid(milvus::OpContext* op_ctx,
                            const std::string& path,
                            TargetBitmapView valid_res) {
        size_t processed_size = 0;
        // if path is not in shredding_columns_, return 0
        if (shredding_columns_.find(path) == shredding_columns_.end()) {
            return processed_size;
        }
        auto column = shredding_columns_[path];
        auto num_data_chunk = column->num_chunks();

        for (size_t i = 0; i < num_data_chunk; i++) {
            auto chunk_size = column->chunk_row_nums(i);
            column->ApplyValidDataInChunk(
                op_ctx, i, 0, chunk_size, valid_res + processed_size);
            processed_size += chunk_size;
        }
        AssertInfo(processed_size == valid_res.size(),
                   "Processed size {} is not equal to num_rows {}",
                   processed_size,
                   valid_res.size());
        return processed_size;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ExecutorForShreddingData(
        milvus::OpContext* op_ctx,
        // path is field_name in shredding_columns_
        const std::string& path,
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, std::string, int)>
            skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        ValTypes... values) {
        int64_t processed_size = 0;
        // if path is not in shredding_columns_, return 0
        if (shredding_columns_.find(path) == shredding_columns_.end()) {
            return processed_size;
        }
        auto column = shredding_columns_[path];
        auto num_data_chunk = column->num_chunks();
        auto num_rows = column->NumRows();

        for (size_t i = 0; i < num_data_chunk; i++) {
            auto chunk_size = column->chunk_row_nums(i);

            if (!skip_func || !skip_func(skip_index_, path, i)) {
                if constexpr (std::is_same_v<T, std::string_view>) {
                    // first is the raw data, second is valid_data
                    // use valid_data to see if raw data is null
                    auto pw = column->StringViews(op_ctx, i);
                    auto [data_vec, valid_data] = pw.get();

                    func(data_vec.data(),
                         valid_data,
                         chunk_size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                } else {
                    auto pw = column->Span(op_ctx, i);
                    auto chunk = pw.get();
                    const T* data = static_cast<const T*>(chunk.data());
                    const auto validity = chunk.validity();
                    func(data,
                         validity,
                         chunk_size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                }
            } else {
                if (column->IsNullable()) {
                    auto pw = column->GetChunk(op_ctx, i);
                    auto chunk = pw.get();
                    chunk->ApplyValidityMask(
                        0, chunk_size, res + processed_size);
                    chunk->ApplyValidityMask(
                        0, chunk_size, valid_res + processed_size);
                }
            }

            processed_size += chunk_size;
        }
        AssertInfo(processed_size == num_rows,
                   "Processed size {} is not equal to num_rows {}",
                   processed_size,
                   num_rows);
        return processed_size;
    }

    std::set<std::string>
    GetShreddingFields(const std::string& pointer) {
        std::set<std::string> fields;
        if (key_field_map_.find(pointer) != key_field_map_.end()) {
            for (const auto& field : key_field_map_[pointer]) {
                if (shred_field_data_type_map_.find(field) !=
                    shred_field_data_type_map_.end()) {
                    fields.insert(field);
                }
            }
        }
        return fields;
    }

    // return all shredding fields whose pointers start with the given prefix
    // for example, prefix "/a/b" will include fields for "/a/b" and "/a/b/..."
    std::set<std::string>
    GetShreddingFieldsWithPrefix(const std::string& prefix) {
        std::set<std::string> fields;
        for (const auto& [path, field_names] : key_field_map_) {
            if (path.size() >= prefix.size() &&
                path.compare(0, prefix.size(), prefix) == 0 &&
                (path.size() == prefix.size() || path[prefix.size()] == '/')) {
                for (const auto& field : field_names) {
                    if (shred_field_data_type_map_.find(field) !=
                        shred_field_data_type_map_.end()) {
                        fields.insert(field);
                    }
                }
            }
        }
        return fields;
    }

    std::string
    GetShreddingField(const std::string& pointer, JSONType type) {
        if (key_field_map_.find(pointer) == key_field_map_.end()) {
            return "";
        }
        for (const auto& field : key_field_map_[pointer]) {
            if (shred_field_data_type_map_.find(field) !=
                    shred_field_data_type_map_.end() &&
                shred_field_data_type_map_[field] == type) {
                return field;
            }
        }
        return "";
    }

    bool
    HasAllShreddingFields(const std::string& pointer,
                          std::initializer_list<JSONType> types) {
        if (types.size() == 0) {
            return false;
        }
        for (auto type : types) {
            if (GetShreddingField(pointer, type).empty()) {
                return false;
            }
        }
        return true;
    }

    std::set<std::string>
    GetShreddingFields(const std::string& pointer,
                       std::vector<JSONType> types) {
        std::set<std::string> fields;
        if (key_field_map_.find(pointer) == key_field_map_.end()) {
            return fields;
        }
        for (const auto& field : key_field_map_[pointer]) {
            if (shred_field_data_type_map_.find(field) !=
                    shred_field_data_type_map_.end() &&
                std::find(types.begin(),
                          types.end(),
                          shred_field_data_type_map_[field]) != types.end()) {
                fields.insert(field);
            }
        }
        return fields;
    }

    JSONType
    GetShreddingJsonType(const std::string& field_name) {
        if (shred_field_data_type_map_.find(field_name) !=
            shred_field_data_type_map_.end()) {
            return shred_field_data_type_map_[field_name];
        }
        return JSONType::UNKNOWN;
    }

 private:
    void
    CollectSingleJsonStatsInfo(const char* json_str,
                               std::map<JsonKey, KeyStatsInfo>& infos);

    std::string
    PrintKeyInfo(const std::map<JsonKey, KeyStatsInfo>& infos) {
        std::stringstream ss;
        for (const auto& [key, info] : infos) {
            ss << key.ToString() << " -> " << info.ToString() << "\t";
        }
        return ss.str();
    }

    std::map<JsonKey, KeyStatsInfo>
    CollectKeyInfo(const std::vector<FieldDataPtr>& field_datas, bool nullable);

    void
    TraverseJsonForStats(const char* json,
                         jsmntok* tokens,
                         int& index,
                         std::vector<std::string>& path,
                         std::map<JsonKey, KeyStatsInfo>& infos);

    void
    AddKeyStatsInfo(const std::vector<std::string>& paths,
                    JSONType type,
                    uint8_t* value,
                    std::map<JsonKey, KeyStatsInfo>& infos);

    std::string
    PrintJsonKeyLayoutType(const std::map<JsonKey, JsonKeyLayoutType>& infos) {
        std::stringstream ss;
        std::unordered_map<JsonKeyLayoutType, std::vector<std::string>>
            type_to_keys;
        for (const auto& [key, type] : infos) {
            type_to_keys[type].push_back(key.ToString());
        }
        for (const auto& [type, keys] : type_to_keys) {
            ss << ToString(type) << " -> [" << Join(keys, ", ") << "]\n";
        }
        return ss.str();
    }

    std::map<JsonKey, JsonKeyLayoutType>
    ClassifyJsonKeyLayoutType(const std::map<JsonKey, KeyStatsInfo>& infos);

    void
    BuildKeyStats(const std::vector<FieldDataPtr>& field_datas, bool nullable);

    void
    BuildKeyStatsForRow(const char* json_str, uint32_t row_id);

    void
    BuildKeyStatsForNullRow();

    std::string
    GetShreddingDir();

    std::string
    GetSharedKeyIndexDir();

    std::string
    GetMetaFilePath();

    void
    WriteMetaFile();

    void
    LoadMetaFile(const std::string& meta_file_path);

    void
    AddKeyStats(const std::vector<std::string>& path,
                JSONType type,
                const std::string& value,
                std::map<JsonKey, std::string>& values);

    void
    TraverseJsonForBuildStats(const char* json,
                              jsmntok* tokens,
                              int& index,
                              std::vector<std::string>& path,
                              std::map<JsonKey, std::string>& values);

    bool
    IsBoolean(const std::string& str) {
        return str == "true" || str == "false";
    }

    bool
    IsInt8(const std::string& str) {
        std::istringstream iss(str);
        int8_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int8_t>::min() &&
               num <= std::numeric_limits<int8_t>::max();
    }

    bool
    IsInt16(const std::string& str) {
        std::istringstream iss(str);
        int16_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int16_t>::min() &&
               num <= std::numeric_limits<int16_t>::max();
    }

    bool
    IsInt32(const std::string& str) {
        std::istringstream iss(str);
        int64_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int32_t>::min() &&
               num <= std::numeric_limits<int32_t>::max();
    }

    bool
    IsInt64(const std::string& str) {
        std::istringstream iss(str);
        int64_t num;
        iss >> num;

        return !iss.fail() && iss.eof();
    }

    bool
    IsFloat(const std::string& str) {
        try {
            std::stof(str);
            return true;
        } catch (...) {
            return false;
        }
    }

    bool
    IsDouble(const std::string& str) {
        try {
            std::stod(str);
            return true;
        } catch (...) {
            return false;
        }
    }

    bool
    IsNull(const std::string& str) {
        return str == "null";
    }

    JSONType
    getType(const std::string& str) {
        if (IsBoolean(str)) {
            return JSONType::BOOL;
            // TODO: add int8, int16, int32 support
            // now we only support int64 for build performance
            // } else if (IsInt8(str)) {
            //     return JSONType::INT8;
            // } else if (IsInt16(str)) {
            //     return JSONType::INT16;
            // } else if (IsInt32(str)) {
            //     return JSONType::INT32;
        } else if (IsInt64(str)) {
            return JSONType::INT64;
        } else if (IsFloat(str)) {
            return JSONType::FLOAT;
        } else if (IsDouble(str)) {
            return JSONType::DOUBLE;
        } else if (IsNull(str)) {
            return JSONType::NONE;
        }
        LOG_DEBUG("unknown json type for string: {}", str);
        return JSONType::UNKNOWN;
    }

    void
    LoadShreddingData(const std::vector<std::string>& index_files,
                      const std::string& warmup_policy = "");

    void
    GetColumnSchemaFromParquet(int64_t column_group_id,
                               const std::string& file);

    void
    GetCommonMetaFromParquet(const std::string& file);

    void
    LoadColumnGroup(int64_t column_group_id,
                    const std::vector<int64_t>& file_ids,
                    const std::string& warmup_policy = "",
                    const std::string& override_prefix = "");

    void
    LoadShreddingMeta(
        std::vector<std::pair<int64_t, std::vector<int64_t>>> sorted_files,
        const std::string& override_prefix = "");

    std::string
    AddBucketName(const std::string& remote_prefix);

    void
    LoadSharedKeyIndex(const std::vector<std::string>& shared_key_index_files,
                       bool enable_mmap,
                       int64_t index_size,
                       const std::string& warmup_policy = "");

 private:
    proto::schema::FieldSchema schema_;
    int64_t segment_id_;
    int64_t field_id_;
    mutable std::mutex mtx_;
    int64_t num_rows_{0};
    bool is_built_ = false;
    std::string path_;
    milvus::storage::FileManagerContext file_manager_context_;
    milvus::storage::ChunkManagerPtr rcm_;
    std::shared_ptr<milvus::storage::MemFileManagerImpl> mem_file_manager_;
    std::shared_ptr<milvus::storage::DiskFileManagerImpl> disk_file_manager_;
    int64_t max_shredding_columns_;
    double shredding_ratio_threshold_;
    int64_t write_batch_size_;

    std::map<JsonKey, JsonKeyLayoutType> key_types_;
    std::set<JsonKey> shared_keys_;
    std::set<JsonKey> column_keys_;
    std::shared_ptr<JsonStatsParquetWriter> parquet_writer_;
    std::shared_ptr<BsonInvertedIndex> bson_inverted_index_;
    std::string shard_;
    // cache slot for bson inverted index when using translator
    std::shared_ptr<milvus::cachinglayer::CacheSlot<BsonInvertedIndex>>
        bson_index_cache_slot_;

    milvus::proto::common::LoadPriority load_priority_;
    // some meta cache for searching
    // json_path -> [json_path_int, json_path_string, ...], only for shredding columns
    std::unordered_map<std::string, std::set<std::string>> key_field_map_;
    // field_name -> data_type, such as json_path_int -> JSONType::INT64, only for real shredding columns
    std::unordered_map<std::string, JSONType> shred_field_data_type_map_;
    // field_name -> field_id, such as json_path_int -> 1001
    std::unordered_map<std::string, int64_t> field_name_to_id_map_;
    // field_id -> field_name, such as 1001 -> json_path_int
    std::unordered_map<int64_t, std::string> field_id_to_name_map_;
    // field_name vector, the sequece is the same as the order of files
    std::vector<std::string> field_names_;
    // field_name -> column
    mutable std::unordered_map<std::string,
                               std::shared_ptr<milvus::ChunkedColumnInterface>>
        shredding_columns_;
    std::string mmap_filepath_;

    std::string shared_column_field_name_;
    std::shared_ptr<milvus::ChunkedColumnInterface> shared_column_;
    // DUPLICATED CONCEPT — MERGE WHEN THE SUB-COLUMNS ARE PROMOTED.
    // `SkipIndex` is the column's zone-map statistic and has already been ruled
    // to belong to columnar-format (core_refactor/01-scalar-index.md §1
    // exclusions; PR #51504 wires it in as `CellSkipPredicate` during scan
    // planning, which settles the ownership). This class keeps a SECOND COPY of
    // it to skip chunks in `ExecutorForShreddingData` — the same concept stored
    // once per component. README's columnar-format entry lists merging the two
    // as connected item (1) of the shredding migration; it is work parallel to
    // refactor phase 1 and NOT done here.
    SkipIndex skip_index_;

    // Meta file for storing layout type map and other metadata
    JsonStatsMeta json_stats_meta_;
    int64_t meta_file_size_{0};

    // Friend accessor for unit tests to call private methods safely.
    friend class ::TraverseJsonForBuildStatsAccessor;
    friend class ::CollectSingleJsonStatsInfoAccessor;
    friend class ::JsonStatsProjectionTestAccessor;
};

}  // namespace milvus::index
