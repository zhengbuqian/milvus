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

#include <glog/logging.h>
#include <string.h>
#include <cstdint>
#include <exception>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "filemanager/InputStream.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "segcore/json_stats/JsonKeyStats.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/type_c.h"
#include "knowhere/binaryset.h"
#include "log/Log.h"
#include "monitor/scope_metric.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/schema.pb.h"
#include "storage/FileManager.h"
#include "storage/PluginLoader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"
#include "storage/plugin/PluginInterface.h"

using namespace milvus;

namespace {

struct CIndexHandle {
    explicit CIndexHandle(
        std::unique_ptr<milvus::indexbuilder::BuildSession> native_session)
        : session(std::move(native_session)) {
    }

    std::unique_ptr<milvus::indexbuilder::BuildSession> session;
};

CIndexHandle&
RequireHandle(CIndex index) {
    AssertInfo(index != nullptr, "passed index handle was null");
    return *reinterpret_cast<CIndexHandle*>(index);
}

milvus::indexbuilder::BuildSession&
RequireSession(CIndex index) {
    auto& handle = RequireHandle(index);
    AssertInfo(handle.session != nullptr, "native index session was null");
    return *handle.session;
}

std::unique_ptr<CIndexHandle>
MakeNativeHandle(std::unique_ptr<milvus::indexbuilder::BuildSession> session) {
    AssertInfo(session != nullptr, "cannot publish a null native session");
    return std::make_unique<CIndexHandle>(std::move(session));
}

milvus::indexbuilder::PhysicalBinarySet
ToPhysicalBuffers(const knowhere::BinarySet& source) {
    milvus::indexbuilder::PhysicalBinarySet result;
    for (const auto& [name, binary] : source.binary_map_) {
        AssertInfo(binary != nullptr,
                   "BinarySet entry {} has a null descriptor",
                   name);
        AssertInfo(
            binary->size >= 0, "BinarySet entry {} has a negative size", name);
        const auto size = static_cast<uint64_t>(binary->size);
        AssertInfo(
            size <= static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
            "BinarySet entry {} exceeds size_t",
            name);
        result.emplace(name,
                       milvus::indexbuilder::PhysicalBinaryEntry{
                           binary->data, static_cast<size_t>(size)});
    }
    return result;
}

std::unique_ptr<knowhere::BinarySet>
ToBinarySet(const milvus::indexbuilder::PhysicalBinarySet& source) {
    auto result = std::make_unique<knowhere::BinarySet>();
    for (const auto& [name, buffer] : source) {
        AssertInfo(buffer.size <=
                       static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                   "physical entry {} exceeds BinarySet size domain",
                   name);
        result->Append(name, buffer.data, static_cast<int64_t>(buffer.size));
    }
    return result;
}

CStatus
SuccessStatus() {
    CStatus status{};
    status.error_code = Success;
    status.error_msg = "";
    return status;
}

void
WriteArtifactStats(const milvus::storage::ArtifactStats& stats,
                   ProtoLayoutInterface result) {
    AssertInfo(result != nullptr, "index stats output was null");
    auto proto = milvus::indexbuilder::AdaptArtifactStats(stats);
    auto* layout = reinterpret_cast<milvus::ProtoLayout*>(result);
    AssertInfo(layout->SerializeAndHoldProto(proto),
               "failed to serialize index artifact stats");
}

void
BuildDirectVector(CIndex index, milvus::indexbuilder::DirectVectorInput input) {
    auto& session = RequireSession(index);
    AssertInfo(session.IsDirect(),
               "production index session cannot consume direct vector data");
    AssertInfo(session.SourceType() == input.source_type,
               "direct vector entry for type {} received session type {}",
               input.source_type,
               session.SourceType());
    input.configured_dim = session.DirectDimension();
    session.BuildDirect(
        milvus::indexbuilder::AdaptDirectVectorFieldData(input));
}

}  // namespace

CStatus
CreateIndexForUT(enum CDataType dtype,
                 const char* serialized_type_params,
                 const char* serialized_index_params,
                 CIndex* res_index) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(res_index, "failed to create index, passed index was null");

        milvus::proto::indexcgo::TypeParams type_params;
        milvus::proto::indexcgo::IndexParams index_params;
        milvus::index::ParseFromString(type_params, serialized_type_params);
        milvus::index::ParseFromString(index_params, serialized_index_params);

        const auto data_type = milvus::DataType(dtype);
        auto prepared = milvus::indexbuilder::AdaptDirectBuild(
            data_type, type_params, index_params);
        auto handle = MakeNativeHandle(
            std::make_unique<milvus::indexbuilder::BuildSession>(
                prepared.source_type, std::move(prepared.adapted)));

        *res_index = handle.release();
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

milvus::storage::StorageConfig
get_storage_config(const milvus::proto::indexcgo::StorageConfig& config) {
    auto storage_config = milvus::storage::StorageConfig();
    storage_config.address = std::string(config.address());
    storage_config.bucket_name = std::string(config.bucket_name());
    storage_config.access_key_id = std::string(config.access_keyid());
    storage_config.access_key_value = std::string(config.secret_access_key());
    storage_config.root_path = std::string(config.root_path());
    storage_config.storage_type = std::string(config.storage_type());
    storage_config.cloud_provider = std::string(config.cloud_provider());
    storage_config.iam_endpoint = std::string(config.iamendpoint());
    storage_config.useSSL = config.usessl();
    storage_config.sslCACert = config.sslcacert();
    storage_config.useIAM = config.useiam();
    storage_config.region = config.region();
    storage_config.useVirtualHost = config.use_virtual_host();
    storage_config.requestTimeoutMs = config.request_timeout_ms();
    storage_config.gcp_credential_json =
        std::string(config.gcpcredentialjson());
    storage_config.max_connections = config.max_connections();
    storage_config.tls_min_version = std::string(config.ssl_tls_min_version());
    storage_config.use_crc32c_checksum = config.use_crc32c_checksum();
    return storage_config;
}

milvus::OptFieldT
get_opt_field(const ::google::protobuf::RepeatedPtrField<
              milvus::proto::indexcgo::OptionalFieldInfo>& field_infos) {
    milvus::OptFieldT opt_fields_map;
    for (const auto& field_info : field_infos) {
        auto field_id = field_info.fieldid();
        auto it = opt_fields_map.find(field_id);
        if (it == opt_fields_map.end()) {
            it = opt_fields_map
                     .emplace(field_id,
                              std::make_tuple(field_info.field_name(),
                                              static_cast<milvus::DataType>(
                                                  field_info.field_type()),
                                              static_cast<milvus::DataType>(
                                                  field_info.element_type()),
                                              std::vector<std::string>{}))
                     .first;
        }
        for (const auto& str : field_info.data_paths()) {
            std::get<3>(it->second).emplace_back(str);
        }
    }

    return opt_fields_map;
}

milvus::SegmentInsertFiles
get_segment_insert_files(
    const milvus::proto::indexcgo::SegmentInsertFiles& segment_insert_files) {
    milvus::SegmentInsertFiles files;
    for (const auto& column_group_files :
         segment_insert_files.field_insert_files()) {
        std::vector<std::string> paths;
        paths.reserve(column_group_files.file_paths().size());
        for (const auto& path : column_group_files.file_paths()) {
            paths.push_back(path);
        }
        files.emplace_back(std::move(paths));
    }
    return files;
}

milvus::storage::StorageColumnMapping
get_storage_column_mapping(
    const milvus::proto::schema::FieldSchema& field_schema,
    bool is_milvus_table) {
    auto physical_mapping =
        milvus::ResolvePhysicalColumnMapping(is_milvus_table, field_schema);
    milvus::storage::StorageColumnMapping mapping;
    mapping.schema_column_name = physical_mapping.schema_column_name;
    mapping.storage_column_name = physical_mapping.storage_column_name;
    mapping.is_external_column = physical_mapping.is_external_column;
    return mapping;
}

milvus::storage::StorageColumnMapping
get_storage_column_mapping(
    const milvus::proto::indexcgo::OptionalFieldInfo& field_info,
    bool is_milvus_table) {
    milvus::storage::StorageColumnMapping mapping;
    mapping.schema_column_name = field_info.field_name();
    mapping.storage_column_name = std::to_string(field_info.fieldid());
    mapping.is_external_column = is_milvus_table;
    return mapping;
}

void
configure_manifest_file_manager_context(
    milvus::storage::FileManagerContext& file_manager_context,
    const milvus::proto::indexcgo::BuildIndexInfo& build_index_info,
    const milvus::storage::StorageConfig& storage_config) {
    if (build_index_info.manifest().empty()) {
        return;
    }

    auto loon_properties = MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config));
    if (!build_index_info.external_source().empty()) {
        InjectExternalSpecProperties(*loon_properties,
                                     build_index_info.collectionid(),
                                     build_index_info.external_source(),
                                     build_index_info.external_spec());
    }
    // Widen the per-round read window for index-build manifest reads when
    // configured. With loon's 32MB default each prefetch round admits a
    // single 64MB-class row group, so the whole raw-data download degrades
    // to one S3 range read at a time; a wider window lets one round span
    // multiple row groups whose column chunks are prefetched in parallel
    // on the arrow IO thread pool.
    milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
        .ApplyIndexBuildReadWindow(*loon_properties);
    file_manager_context.set_loon_ffi_properties(loon_properties);

    auto is_milvus_table =
        milvus::IsMilvusTableExternalSpec(build_index_info.external_spec());
    file_manager_context.set_storage_column_mapping(
        build_index_info.field_schema().fieldid(),
        get_storage_column_mapping(build_index_info.field_schema(),
                                   is_milvus_table));
    for (const auto& field_info : build_index_info.opt_fields()) {
        file_manager_context.set_storage_column_mapping(
            field_info.fieldid(),
            get_storage_column_mapping(field_info, is_milvus_table));
    }
}

milvus::Config
get_config(std::unique_ptr<milvus::proto::indexcgo::BuildIndexInfo>& info) {
    milvus::Config config;
    for (auto i = 0; i < info->index_params().size(); ++i) {
        const auto& param = info->index_params(i);
        config[param.key()] = param.value();
    }

    for (auto i = 0; i < info->type_params().size(); ++i) {
        const auto& param = info->type_params(i);
        config[param.key()] = param.value();
    }

    config[INSERT_FILES_KEY] = info->insert_files();
    if (info->opt_fields().size()) {
        config[VEC_OPT_FIELDS] = get_opt_field(info->opt_fields());
    }
    if (info->partition_key_isolation()) {
        config[PARTITION_KEY_ISOLATION_KEY] = info->partition_key_isolation();
    }
    config[INDEX_NUM_ROWS_KEY] = info->num_rows();
    config[STORAGE_VERSION_KEY] = info->storage_version();
    if (info->storage_version() == STORAGE_V2 ||
        info->storage_version() == STORAGE_V3) {
        config[SEGMENT_INSERT_FILES_KEY] =
            get_segment_insert_files(info->segment_insert_files());
        config[SEGMENT_MANIFEST_KEY] = info->manifest();
    }
    config[DIM_KEY] = info->dim();
    config[DATA_TYPE_KEY] = info->field_schema().data_type();
    config[ELEMENT_TYPE_KEY] = info->field_schema().element_type();
    if (!info->stats_base_path().empty()) {
        config[STATS_BASE_PATH_KEY] = info->stats_base_path();
    }

    if (!info->analyzer_extra_info().empty()) {
        config["analyzer_extra_info"] = info->analyzer_extra_info();
    }

    return config;
}

CStatus
CreateIndex(CIndex* res_index,
            const uint8_t* serialized_build_index_info,
            const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(res_index != nullptr, "index output handle was null");
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        const auto field_type =
            static_cast<DataType>(build_index_info->field_schema().data_type());
        const auto purpose =
            milvus::IsVectorDataType(field_type)
                ? milvus::indexbuilder::BuildPurpose::VectorIndex
                : milvus::indexbuilder::BuildPurpose::ScalarIndex;
        auto prepared = milvus::indexbuilder::AdaptBuildIndexInfo(
            *build_index_info, purpose);
        auto session = std::make_unique<milvus::indexbuilder::BuildSession>(
            std::move(prepared.request),
            std::move(prepared.file_manager_context));
        session->BuildProduction();
        *res_index = MakeNativeHandle(std::move(session)).release();
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

// JSON shredding: NOT an index build, a column-layout build
// (core_refactor/01-scalar-index.md §1). Refactor phase 1 changed only this
// file's include path — `segcore/json_stats/JsonKeyStats.h` instead of
// `index/json_stats/JsonKeyStats.h` — which makes this an L5 -> L3 edge, and
// legal. Everything else here is deliberately untouched:
//   - it never went through `index::IndexFactory` (the `make_unique` below is
//     the whole construction path), so nothing had to be unhooked;
//   - it is NOT wired to the L1 artifact pipeline this phase (§1's "explicitly
//     out of scope" list):
//     `Build(config)` / `Upload(config)` stay hand-written until wide-table
//     modelling settles the sub-column representation.
CStatus
BuildJsonKeyIndex(ProtoLayoutInterface result,
                  const uint8_t* serialized_build_index_info,
                  const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(result != nullptr, "index stats output was null");
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshall build index info failed");

        auto field_type = static_cast<milvus::DataType>(
            build_index_info->field_schema().data_type());

        auto storage_config =
            get_storage_config(build_index_info->storage_config());
        auto config = get_config(build_index_info);

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            build_index_info->collectionid(),
            build_index_info->partitionid(),
            build_index_info->segmentid(),
            build_index_info->field_schema().fieldid(),
            build_index_info->field_schema()};

        milvus::storage::IndexMeta index_meta{
            build_index_info->segmentid(),
            build_index_info->field_schema().fieldid(),
            build_index_info->buildid(),
            build_index_info->index_version(),
            "",
            build_index_info->field_schema().name(),
            field_type,
            build_index_info->dim(),
        };

        auto scalar_index_engine_version =
            build_index_info->current_scalar_index_version();
        config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] =
            scalar_index_engine_version;
        auto tantivy_index_version =
            scalar_index_engine_version <= 1
                ? milvus::index::TANTIVY_INDEX_MINIMUM_VERSION
                : milvus::index::TANTIVY_INDEX_LATEST_VERSION;
        config[milvus::index::TANTIVY_INDEX_VERSION] = tantivy_index_version;

        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);
        auto fs = milvus::storage::InitArrowFileSystem(storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager, fs);
        fileManagerContext.set_stats_base_path(
            build_index_info->stats_base_path());

        configure_manifest_file_manager_context(
            fileManagerContext, *build_index_info, storage_config);

        if (build_index_info->has_storage_plugin_context()) {
            fileManagerContext.set_plugin_context(
                milvus::storage::PluginLoader::GetInstance()
                    .registerCipherPluginContext(
                        build_index_info->storage_plugin_context()
                            .encryption_zone_id(),
                        build_index_info->storage_plugin_context()
                            .collection_id(),
                        build_index_info->storage_plugin_context()
                            .encryption_key()));
        }

        auto field_schema =
            FieldMeta::ParseFrom(build_index_info->field_schema());
        auto index = std::make_unique<index::JsonKeyStats>(
            fileManagerContext,
            false,
            build_index_info->json_stats_max_shredding_columns(),
            build_index_info->json_stats_shredding_ratio_threshold(),
            build_index_info->json_stats_write_batch_size(),
            tantivy_index_version);
        index->Build(config);
        WriteArtifactStats(index->Upload(config), result);
        return SuccessStatus();
    } catch (SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
BuildTextIndex(ProtoLayoutInterface result,
               const uint8_t* serialized_build_index_info,
               const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(result != nullptr, "index stats output was null");
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        auto prepared = milvus::indexbuilder::AdaptBuildIndexInfo(
            *build_index_info, milvus::indexbuilder::BuildPurpose::TextIndex);
        milvus::indexbuilder::BuildSession session(
            std::move(prepared.request),
            std::move(prepared.file_manager_context));
        session.BuildProduction();
        WriteArtifactStats(session.Publish(), result);
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
DeleteIndex(CIndex index) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto* handle = &RequireHandle(index);
        delete handle;
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildFloatVecIndex(CIndex index,
                   int64_t float_value_num,
                   const float* vectors) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_FLOAT,
                           .payload_count = float_value_num,
                           .payload = vectors});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildFloatVecIndexWithValidData(CIndex index,
                                int64_t float_value_num,
                                const float* vectors,
                                const bool* valid_data,
                                int64_t valid_data_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_FLOAT,
                           .payload_count = float_value_num,
                           .payload = vectors,
                           .has_validity = true,
                           .valid_data = valid_data,
                           .logical_rows = valid_data_len});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildFloat16VecIndex(CIndex index,
                     int64_t float16_value_num,
                     const uint8_t* vectors) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_FLOAT16,
                           .payload_count = float16_value_num,
                           .payload = vectors});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildFloat16VecIndexWithValidData(CIndex index,
                                  int64_t float16_value_num,
                                  const uint8_t* vectors,
                                  const bool* valid_data,
                                  int64_t valid_data_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_FLOAT16,
                           .payload_count = float16_value_num,
                           .payload = vectors,
                           .has_validity = true,
                           .valid_data = valid_data,
                           .logical_rows = valid_data_len});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildBFloat16VecIndex(CIndex index,
                      int64_t bfloat16_value_num,
                      const uint8_t* vectors) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_BFLOAT16,
                           .payload_count = bfloat16_value_num,
                           .payload = vectors});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildBFloat16VecIndexWithValidData(CIndex index,
                                   int64_t bfloat16_value_num,
                                   const uint8_t* vectors,
                                   const bool* valid_data,
                                   int64_t valid_data_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_BFLOAT16,
                           .payload_count = bfloat16_value_num,
                           .payload = vectors,
                           .has_validity = true,
                           .valid_data = valid_data,
                           .logical_rows = valid_data_len});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_BINARY,
                           .payload_count = data_size,
                           .payload = vectors});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildBinaryVecIndexWithValidData(CIndex index,
                                 int64_t data_size,
                                 const uint8_t* vectors,
                                 const bool* valid_data,
                                 int64_t valid_data_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_BINARY,
                           .payload_count = data_size,
                           .payload = vectors,
                           .has_validity = true,
                           .valid_data = valid_data,
                           .logical_rows = valid_data_len});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildSparseFloatVecIndex(CIndex index,
                         int64_t row_num,
                         int64_t dim,
                         const uint8_t* vectors) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_SPARSE_U32_F32,
                           .payload_count = row_num,
                           .payload = vectors,
                           .sparse_dim = dim});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildSparseFloatVecIndexWithValidData(CIndex index,
                                      int64_t row_num,
                                      int64_t dim,
                                      const uint8_t* vectors,
                                      const bool* valid_data,
                                      int64_t valid_data_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_SPARSE_U32_F32,
                           .payload_count = row_num,
                           .payload = vectors,
                           .has_validity = true,
                           .valid_data = valid_data,
                           .logical_rows = valid_data_len,
                           .sparse_dim = dim});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildInt8VecIndex(CIndex index, int64_t int8_value_num, const int8_t* vectors) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_INT8,
                           .payload_count = int8_value_num,
                           .payload = vectors});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
BuildInt8VecIndexWithValidData(CIndex index,
                               int64_t int8_value_num,
                               const int8_t* vectors,
                               const bool* valid_data,
                               int64_t valid_data_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        BuildDirectVector(index,
                          {.source_type = DataType::VECTOR_INT8,
                           .payload_count = int8_value_num,
                           .payload = vectors,
                           .has_validity = true,
                           .valid_data = valid_data,
                           .logical_rows = valid_data_len});
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

// field_data:
//  1, serialized proto::schema::BoolArray, if type is bool;
//  2, serialized proto::schema::StringArray, if type is string;
//  3, raw pointer, if type is of fundamental except bool type;
// TODO: optimize here if necessary.
CStatus
BuildScalarIndex(CIndex c_index, int64_t size, const void* field_data) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto& session = RequireSession(c_index);
        AssertInfo(session.IsDirect(),
                   "production scalar/text session cannot consume a direct "
                   "scalar payload");
        session.BuildDirect(milvus::indexbuilder::AdaptDirectScalarFieldData(
            session.SourceType(), size, field_data));
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
SerializeIndexToBinarySet(CIndex index, CBinarySet* c_binary_set) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(c_binary_set != nullptr,
                   "BinarySet output pointer was null");
        auto binary = ToBinarySet(RequireSession(index).SerializePhysical());
        *c_binary_set = binary.release();
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
LoadIndexFromBinarySet(CIndex index, CBinarySet c_binary_set) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(c_binary_set != nullptr, "BinarySet input pointer was null");
        auto binary_set = reinterpret_cast<knowhere::BinarySet*>(c_binary_set);
        RequireSession(index).LoadPhysical(ToPhysicalBuffers(*binary_set));
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
CleanLocalData(CIndex index) {
    SCOPE_CGO_CALL_METRIC();

    try {
        RequireSession(index).CleanLocalData();
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}

CStatus
SerializeIndexAndUpLoad(CIndex index, ProtoLayoutInterface result) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(result != nullptr, "index stats output was null");
        WriteArtifactStats(RequireSession(index).Publish(), result);
        return SuccessStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(&error);
    }
}
