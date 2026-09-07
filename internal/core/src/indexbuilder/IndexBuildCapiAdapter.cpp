// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/IndexBuildCapiAdapter.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/Utils.h"
#include "knowhere/version.h"
#include "knowhere/sparse_utils.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/PluginLoader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace milvus::indexbuilder {
namespace {

storage::StorageConfig
AdaptStorageConfig(const proto::indexcgo::StorageConfig& config) {
    storage::StorageConfig result;
    result.address = config.address();
    result.bucket_name = config.bucket_name();
    result.access_key_id = config.access_keyid();
    result.access_key_value = config.secret_access_key();
    result.root_path = config.root_path();
    result.storage_type = config.storage_type();
    result.cloud_provider = config.cloud_provider();
    result.iam_endpoint = config.iamendpoint();
    result.useSSL = config.usessl();
    result.sslCACert = config.sslcacert();
    result.useIAM = config.useiam();
    result.region = config.region();
    result.useVirtualHost = config.use_virtual_host();
    result.requestTimeoutMs = config.request_timeout_ms();
    result.gcp_credential_json = config.gcpcredentialjson();
    result.max_connections = config.max_connections();
    result.tls_min_version = config.ssl_tls_min_version();
    result.use_crc32c_checksum = config.use_crc32c_checksum();
    return result;
}

OptFieldT
AdaptOptionalFields(const google::protobuf::RepeatedPtrField<
                    proto::indexcgo::OptionalFieldInfo>& fields) {
    OptFieldT result;
    for (const auto& field : fields) {
        const auto field_id = field.fieldid();
        auto [it, inserted] = result.emplace(
            field_id,
            std::make_tuple(field.field_name(),
                            static_cast<DataType>(field.field_type()),
                            static_cast<DataType>(field.element_type()),
                            std::vector<std::string>{}));
        if (!inserted) {
            AssertInfo(std::get<0>(it->second) == field.field_name() &&
                           std::get<1>(it->second) ==
                               static_cast<DataType>(field.field_type()) &&
                           std::get<2>(it->second) ==
                               static_cast<DataType>(field.element_type()),
                       "optional field {} has conflicting metadata",
                       field_id);
        }
        auto& paths = std::get<3>(it->second);
        const auto added = static_cast<size_t>(field.data_paths_size());
        AssertInfo(added <= std::numeric_limits<size_t>::max() - paths.size(),
                   "optional field {} path count overflows size_t",
                   field_id);
        paths.reserve(paths.size() + added);
        for (const auto& path : field.data_paths()) {
            paths.push_back(path);
        }
    }
    return result;
}

std::vector<std::vector<std::string>>
AdaptSegmentFiles(const proto::indexcgo::SegmentInsertFiles& files) {
    std::vector<std::vector<std::string>> result;
    result.reserve(static_cast<size_t>(files.field_insert_files_size()));
    for (const auto& group : files.field_insert_files()) {
        std::vector<std::string> paths;
        paths.reserve(static_cast<size_t>(group.file_paths_size()));
        for (const auto& path : group.file_paths()) {
            paths.push_back(path);
        }
        result.push_back(std::move(paths));
    }
    return result;
}

std::vector<std::string>
AdaptInsertFiles(const google::protobuf::RepeatedPtrField<std::string>& files) {
    return {files.begin(), files.end()};
}

storage::StorageColumnMapping
AdaptStorageColumnMapping(const proto::schema::FieldSchema& field_schema,
                          bool is_milvus_table) {
    const auto mapping =
        ResolvePhysicalColumnMapping(is_milvus_table, field_schema);
    return {.schema_column_name = mapping.schema_column_name,
            .storage_column_name = mapping.storage_column_name,
            .is_external_column = mapping.is_external_column};
}

storage::StorageColumnMapping
AdaptStorageColumnMapping(const proto::indexcgo::OptionalFieldInfo& field,
                          bool is_milvus_table) {
    return {.schema_column_name = field.field_name(),
            .storage_column_name = std::to_string(field.fieldid()),
            .is_external_column = is_milvus_table};
}

int64_t
CanonicalBuildDimension(const proto::indexcgo::BuildIndexInfo& info) {
    const auto dim = info.dim();
    const auto field_type =
        static_cast<DataType>(info.field_schema().data_type());
    return IsSparseFloatVectorDataType(field_type) && dim == -1 ? 0 : dim;
}

void
ConfigureManifestContext(storage::FileManagerContext& context,
                         const proto::indexcgo::BuildIndexInfo& info,
                         const storage::StorageConfig& storage_config) {
    if (info.manifest().empty()) {
        return;
    }

    auto properties = MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config));
    if (!info.external_source().empty()) {
        InjectExternalSpecProperties(*properties,
                                     info.collectionid(),
                                     info.external_source(),
                                     info.external_spec());
    }
    storage::LoonFFIPropertiesSingleton::GetInstance()
        .ApplyIndexBuildReadWindow(*properties);
    context.set_loon_ffi_properties(std::move(properties));

    const auto is_milvus_table =
        IsMilvusTableExternalSpec(info.external_spec());
    context.set_storage_column_mapping(
        info.field_schema().fieldid(),
        AdaptStorageColumnMapping(info.field_schema(), is_milvus_table));
    for (const auto& field : info.opt_fields()) {
        context.set_storage_column_mapping(
            field.fieldid(), AdaptStorageColumnMapping(field, is_milvus_table));
    }
}

index::BuildParams
AdaptParams(const proto::indexcgo::BuildIndexInfo& info) {
    index::BuildParams params = index::BuildParams::object();
    for (const auto& param : info.index_params()) {
        params[param.key()] = param.value();
    }
    for (const auto& param : info.type_params()) {
        params[param.key()] = param.value();
    }

    if (!info.opt_fields().empty()) {
        params[VEC_OPT_FIELDS] = AdaptOptionalFields(info.opt_fields());
    }
    if (info.partition_key_isolation()) {
        params[PARTITION_KEY_ISOLATION_KEY] = true;
    }
    params[INDEX_NUM_ROWS_KEY] = info.num_rows();
    params[STORAGE_VERSION_KEY] = info.storage_version();
    params[DIM_KEY] = CanonicalBuildDimension(info);
    params[DATA_TYPE_KEY] = info.field_schema().data_type();
    params[ELEMENT_TYPE_KEY] = info.field_schema().element_type();
    if (!info.stats_base_path().empty()) {
        params[STATS_BASE_PATH_KEY] = info.stats_base_path();
    }
    if (!info.analyzer_extra_info().empty()) {
        params["analyzer_extra_info"] = info.analyzer_extra_info();
    }
    return params;
}

index::BuildParams
AdaptDirectParams(const proto::indexcgo::TypeParams& type_params,
                  const proto::indexcgo::IndexParams& index_params) {
    index::BuildParams params = index::BuildParams::object();
    for (const auto& param : type_params.params()) {
        params[param.key()] = param.value();
    }
    for (const auto& param : index_params.params()) {
        params[param.key()] = param.value();
    }
    return params;
}

int64_t
ParseDirectInt(const index::BuildParams& params,
               std::string_view key,
               int64_t fallback) {
    if (!params.contains(key)) {
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_integer()) {
        return encoded.get<int64_t>();
    }
    if (encoded.is_number_unsigned()) {
        const auto value = encoded.get<uint64_t>();
        AssertInfo(
            value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
            "direct-build parameter {} is out of int64 range",
            key);
        return static_cast<int64_t>(value);
    }
    AssertInfo(encoded.is_string(),
               "direct-build parameter {} must be an integer",
               key);
    auto text = encoded.get<std::string>();
    std::string_view digits(text);
    if (!digits.empty() && digits.front() == '+') {
        digits.remove_prefix(1);
    }
    int64_t result = 0;
    const auto [end, error] =
        std::from_chars(digits.data(), digits.data() + digits.size(), result);
    AssertInfo(!digits.empty() && error == std::errc{} &&
                   end == digits.data() + digits.size(),
               "direct-build parameter {} is not a decimal integer",
               key);
    return result;
}

DataType
DirectElementType(const index::BuildParams& params) {
    const auto element = ParseDirectInt(
        params, ELEMENT_TYPE_KEY, static_cast<int64_t>(DataType::NONE));
    const auto array_element = ParseDirectInt(
        params, "array_element_type", static_cast<int64_t>(DataType::NONE));
    AssertInfo(element == static_cast<int64_t>(DataType::NONE) ||
                   array_element == static_cast<int64_t>(DataType::NONE) ||
                   element == array_element,
               "direct-build element_type aliases conflict");
    const auto selected = element != static_cast<int64_t>(DataType::NONE)
                              ? element
                              : array_element;
    AssertInfo(selected >= std::numeric_limits<int32_t>::min() &&
                   selected <= std::numeric_limits<int32_t>::max(),
               "direct-build element type is out of range");
    return static_cast<DataType>(static_cast<int32_t>(selected));
}

bool
DirectNested(const index::BuildParams& params) {
    return index::ReadNestedConfigParam(params, "direct build").value_or(false);
}

std::string
LocalStagingParent();

std::string
DirectLocalDir(const index::BuildParams& params) {
    if (!params.contains("local_dir")) {
        return LocalStagingParent();
    }
    AssertInfo(params.at("local_dir").is_string(),
               "direct-build local_dir must be a string");
    auto path = params.at("local_dir").get<std::string>();
    return path.empty() ? LocalStagingParent() : path;
}

std::string
RequiredString(const index::BuildParams& params, std::string_view key) {
    AssertInfo(params.contains(key),
               "index-build request is missing parameter {}",
               key);
    AssertInfo(params.at(key).is_string(),
               "index-build parameter {} must be a string",
               key);
    auto value = params.at(key).get<std::string>();
    AssertInfo(!value.empty(), "index-build parameter {} is empty", key);
    return value;
}

std::string
NormalizeIndexType(index::BuildParams& params, BuildPurpose purpose) {
    if (purpose != BuildPurpose::TextIndex) {
        return RequiredString(params, index::INDEX_TYPE);
    }

    if (params.contains(index::INDEX_TYPE)) {
        const auto configured = RequiredString(params, index::INDEX_TYPE);
        AssertInfo(configured == index::INVERTED_INDEX_TYPE,
                   "text index build requires {} index type, got {}",
                   index::INVERTED_INDEX_TYPE,
                   configured);
    }
    params[index::INDEX_TYPE] = index::INVERTED_INDEX_TYPE;
    return index::INVERTED_INDEX_TYPE;
}

std::string
CanonicalJsonPath(const index::BuildParams& params, DataType field_type) {
    if (field_type != DataType::JSON) {
        return {};
    }

    const auto has_path = params.contains(JSON_PATH);
    const auto has_nested = params.contains("nested_path");
    auto read = [&](std::string_view key) {
        AssertInfo(params.at(key).is_string(),
                   "JSON index parameter {} must be a string",
                   key);
        return params.at(key).get<std::string>();
    };
    const auto path = has_path ? read(JSON_PATH) : std::string{};
    const auto nested = has_nested ? read("nested_path") : std::string{};
    AssertInfo(!has_path || !has_nested || path == nested,
               "JSON path parameters {} and nested_path conflict",
               JSON_PATH);
    return has_path ? path : nested;
}

BuildSource
AdaptSource(const proto::indexcgo::BuildIndexInfo& info) {
    if (!info.manifest().empty()) {
        return ManifestBuildSource{info.manifest()};
    }
    if (info.storage_version() == STORAGE_V2 ||
        info.storage_version() == STORAGE_V3) {
        return StorageV2BuildSource{
            AdaptSegmentFiles(info.segment_insert_files())};
    }
    return V1BinlogBuildSource{AdaptInsertFiles(info.insert_files())};
}

std::string
LocalStagingParent() {
    const auto local =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    AssertInfo(local != nullptr,
               "index-build local chunk manager is not initialized");
    const auto& root = local->GetRootPath();
    AssertInfo(!root.empty(), "index-build local staging root is empty");
    return root;
}

bool
IndexNonEncoding(const index::BuildParams& params) {
    return index::GetValueFromConfigOrFallback<bool>(
        params, index::INDEX_NON_ENCODING, false);
}

void
ConfigureTextParams(index::BuildParams& params,
                    const proto::indexcgo::BuildIndexInfo& info) {
    auto field = FieldMeta::ParseFrom(info.field_schema());
    params["analyzer_name"] = "milvus_tokenizer";
    params["analyzer_params"] = field.get_analyzer_params();
    params["analyzer_extra_info"] = info.analyzer_extra_info();
}

storage::FileManagerContext
MakeFileManagerContext(const proto::indexcgo::BuildIndexInfo& info,
                       const storage::StorageConfig& storage_config,
                       const index::BuildParams& params,
                       DataType field_type) {
    storage::FieldDataMeta field_meta{info.collectionid(),
                                      info.partitionid(),
                                      info.segmentid(),
                                      info.field_schema().fieldid(),
                                      info.field_schema()};
    storage::IndexMeta index_meta{info.segmentid(),
                                  info.field_schema().fieldid(),
                                  info.buildid(),
                                  info.index_version(),
                                  "",
                                  info.field_schema().name(),
                                  field_type,
                                  params.at(DIM_KEY).get<int64_t>(),
                                  IndexNonEncoding(params),
                                  info.index_store_path_version()};

    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext context(
        field_meta, index_meta, chunk_manager, std::move(fs));
    if (!info.stats_base_path().empty()) {
        context.set_stats_base_path(info.stats_base_path());
    }
    ConfigureManifestContext(context, info, storage_config);
    if (info.has_storage_plugin_context()) {
        const auto& plugin = info.storage_plugin_context();
        context.set_plugin_context(
            storage::PluginLoader::GetInstance().registerCipherPluginContext(
                plugin.encryption_zone_id(),
                plugin.collection_id(),
                plugin.encryption_key()));
    }
    return context;
}

size_t
CheckedDirectSize(int64_t value, std::string_view label) {
    AssertInfo(value >= 0, "direct vector {} is negative: {}", label, value);
    AssertInfo(static_cast<uint64_t>(value) <=
                   static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
               "direct vector {} exceeds size_t: {}",
               label,
               value);
    return static_cast<size_t>(value);
}

size_t
CheckedDenseRowBytes(DataType source_type, int64_t dim) {
    AssertInfo(dim > 0,
               "direct vector dimension must be positive for type {}",
               source_type);
    const auto size_dim = CheckedDirectSize(dim, "dimension");
    size_t bytes_per_component = 0;
    switch (source_type) {
        case DataType::VECTOR_FLOAT:
            bytes_per_component = sizeof(float);
            break;
        case DataType::VECTOR_FLOAT16:
            bytes_per_component = sizeof(float16);
            break;
        case DataType::VECTOR_BFLOAT16:
            bytes_per_component = sizeof(bfloat16);
            break;
        case DataType::VECTOR_INT8:
            bytes_per_component = sizeof(int8_t);
            break;
        case DataType::VECTOR_BINARY:
            AssertInfo(dim % 8 == 0,
                       "direct binary vector dimension {} is not byte-aligned",
                       dim);
            return size_dim / 8;
        default:
            ThrowInfo(DataTypeInvalid,
                      "direct dense vector type {} is not supported",
                      source_type);
    }
    AssertInfo(
        size_dim <= std::numeric_limits<size_t>::max() / bytes_per_component,
        "direct vector row byte size overflows size_t");
    return size_dim * bytes_per_component;
}

size_t
DirectPhysicalRows(const DirectVectorInput& input) {
    const auto count = CheckedDirectSize(input.payload_count, "payload count");
    if (input.source_type == DataType::VECTOR_SPARSE_U32_F32) {
        AssertInfo(input.configured_dim >= 0,
                   "direct sparse configured dimension is negative: {}",
                   input.configured_dim);
        AssertInfo(input.sparse_dim >= 0,
                   "direct sparse payload dimension is negative: {}",
                   input.sparse_dim);
        return count;
    }

    const auto row_bytes =
        CheckedDenseRowBytes(input.source_type, input.configured_dim);
    size_t payload_bytes = count;
    if (input.source_type == DataType::VECTOR_FLOAT) {
        AssertInfo(count <= std::numeric_limits<size_t>::max() / sizeof(float),
                   "direct float vector payload byte size overflows size_t");
        payload_bytes = count * sizeof(float);
    } else if (input.source_type == DataType::VECTOR_INT8) {
        payload_bytes = count * sizeof(int8_t);
    }
    AssertInfo(payload_bytes <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "direct vector payload byte size exceeds int64");
    AssertInfo(payload_bytes % row_bytes == 0,
               "direct vector payload has {} bytes, not a whole number of "
               "{}-byte rows",
               payload_bytes,
               row_bytes);
    return payload_bytes / row_bytes;
}

std::vector<uint8_t>
PackDirectValidity(const DirectVectorInput& input, size_t physical_rows) {
    if (!input.has_validity) {
        AssertInfo(input.valid_data == nullptr,
                   "direct vector input supplied validity without enabling "
                   "it");
        AssertInfo(input.logical_rows == 0,
                   "direct vector input supplied a logical validity domain "
                   "without validity");
        return {};
    }

    const auto logical_rows =
        CheckedDirectSize(input.logical_rows, "logical row count");
    AssertInfo(input.valid_data != nullptr || logical_rows == 0,
               "direct vector validity is null for {} logical rows",
               logical_rows);
    AssertInfo(logical_rows <= std::numeric_limits<size_t>::max() - 7,
               "direct vector validity byte count overflows size_t");
    std::vector<uint8_t> packed((logical_rows + 7) / 8, 0);
    size_t valid_rows = 0;
    for (size_t row = 0; row < logical_rows; ++row) {
        if (!input.valid_data[row]) {
            continue;
        }
        packed[row >> 3] |=
            static_cast<uint8_t>(1U << static_cast<unsigned>(row & 7));
        ++valid_rows;
    }
    AssertInfo(valid_rows == physical_rows,
               "direct vector validity has {} rows, payload has {} physical "
               "rows",
               valid_rows,
               physical_rows);
    return packed;
}

void
ValidateDirectSparseRows(const DirectVectorInput& input, size_t physical_rows) {
    using SparseRow = knowhere::sparse::SparseRow<sparse_u32_f32::ValueType>;
    AssertInfo(
        physical_rows <= std::numeric_limits<size_t>::max() / sizeof(SparseRow),
        "direct sparse vector object storage overflows size_t");
    AssertInfo(input.payload != nullptr || physical_rows == 0,
               "direct sparse vector payload is null for {} rows",
               physical_rows);
    const auto* rows = static_cast<const SparseRow*>(input.payload);
    int64_t max_dim = 0;
    size_t total_bytes = 0;
    for (size_t row = 0; row < physical_rows; ++row) {
        const auto row_dim = static_cast<int64_t>(rows[row].dim());
        max_dim = std::max(max_dim, row_dim);
        const auto row_bytes = rows[row].data_byte_size();
        AssertInfo(
            row_bytes <= std::numeric_limits<size_t>::max() - total_bytes,
            "direct sparse vector payload byte size overflows size_t");
        total_bytes += row_bytes;
    }
    AssertInfo(
        total_bytes <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "direct sparse vector payload byte size exceeds int64");
    if (input.configured_dim > 0) {
        AssertInfo(max_dim <= input.configured_dim,
                   "direct sparse vector coordinate dimension {} exceeds "
                   "configured dimension {}",
                   max_dim,
                   input.configured_dim);
    }
    if (input.sparse_dim > 0) {
        AssertInfo(max_dim <= input.sparse_dim,
                   "direct sparse vector coordinate dimension {} exceeds "
                   "payload dimension {}",
                   max_dim,
                   input.sparse_dim);
    }
}

}  // namespace

PreparedBuild
AdaptBuildIndexInfo(const proto::indexcgo::BuildIndexInfo& info,
                    BuildPurpose purpose) {
    const auto field_type =
        static_cast<DataType>(info.field_schema().data_type());
    const bool vector_field = IsVectorDataType(field_type);
    AssertInfo((purpose == BuildPurpose::VectorIndex) == vector_field,
               "index-build purpose does not match field type {}",
               field_type);
    if (purpose == BuildPurpose::TextIndex) {
        AssertInfo(IsStringDataType(field_type),
                   "text index build requires a string field");
    }

    auto params = AdaptParams(info);
    const auto index_type = NormalizeIndexType(params, purpose);
    const auto json_path = CanonicalJsonPath(params, field_type);
    const auto is_nested =
        vector_field ? false : IsStructSubField(info.field_schema().name());
    const auto element_type =
        static_cast<DataType>(info.field_schema().element_type());

    params[index::SCALAR_INDEX_ENGINE_VERSION] =
        info.current_scalar_index_version();
    params[index::TANTIVY_INDEX_VERSION] =
        info.current_scalar_index_version() <= 1
            ? index::TANTIVY_INDEX_MINIMUM_VERSION
            : index::TANTIVY_INDEX_LATEST_VERSION;

    index::IndexTypeAdapterRequest adapter_request{
        .index_type = index_type,
        .field_type = field_type,
        .element_type = element_type,
        .index_engine_version = info.current_index_version(),
        .params = std::move(params),
        .is_nested = is_nested,
        .is_text_match = purpose == BuildPurpose::TextIndex,
    };
    auto adapted = index::AdaptIndexType(adapter_request);
    if (adapted.family == index::families::kText) {
        adapted.params["is_text_match"] = true;
        ConfigureTextParams(adapted.params, info);
    } else {
        AssertInfo(purpose != BuildPurpose::TextIndex,
                   "text build did not resolve to the text family");
    }

    const auto staging_parent = LocalStagingParent();
    BuildOutputSpec output;
    output.generation =
        !vector_field && info.current_scalar_index_version() >= 3
            ? storage::Generation::V3
            : storage::Generation::V1V2;
    output.storage_path = adapted.family == index::families::kText
                              ? storage::ArtifactStoragePath::TextLog
                              : storage::ArtifactStoragePath::Index;
    if (output.generation == storage::Generation::V3) {
        output.packed_file_name =
            index::PackedScalarIndexFileName(adapted.artifact_type);
    }

    auto source = AdaptSource(info);
    const bool legacy_binlog_source =
        std::holds_alternative<V1BinlogBuildSource>(source);
    BuildRequest request{
        .family = adapted.family,
        .params = std::move(adapted.params),
        .value_type = adapted.value_type,
        .field_id = FieldId(info.field_schema().fieldid()),
        .source = std::move(source),
        .expected_rows = info.num_rows(),
        // lack_binlog_rows describes legacy per-field binlogs and cannot
        // describe StorageV2 column groups. Columnar source handling decides
        // missing-row semantics from the field data it visits instead.
        .missing_rows =
            legacy_binlog_source ? info.lack_binlog_rows() : 0,
        .staging_parent = staging_parent,
        .output = std::move(output),
        .json_path = json_path,
    };

    auto storage_config = AdaptStorageConfig(info.storage_config());
    auto context = MakeFileManagerContext(
        info, storage_config, request.params, field_type);
    return {.request = std::move(request),
            .file_manager_context = std::move(context)};
}

PreparedDirectBuild
AdaptDirectBuild(DataType source_type,
                 const proto::indexcgo::TypeParams& type_params,
                 const proto::indexcgo::IndexParams& index_params) {
    auto params = AdaptDirectParams(type_params, index_params);
    if (source_type == DataType::VECTOR_SPARSE_U32_F32 &&
        !params.contains(DIM_KEY)) {
        // The existing sparse C entrance supplies its dimension with the
        // physical rows. Zero preserves coordinate-derived inference until
        // that direct payload arrives.
        params[DIM_KEY] = 0;
    }
    const auto index_type = RequiredString(params, index::INDEX_TYPE);
    const auto element_type = DirectElementType(params);
    const auto nested = DirectNested(params);

    const auto scalar_version =
        ParseDirectInt(params, index::SCALAR_INDEX_ENGINE_VERSION, 1);
    const auto tantivy_version =
        ParseDirectInt(params,
                       index::TANTIVY_INDEX_VERSION,
                       index::TANTIVY_INDEX_LATEST_VERSION);
    params[index::SCALAR_INDEX_ENGINE_VERSION] = scalar_version;
    params[index::TANTIVY_INDEX_VERSION] = tantivy_version;

    index::IndexTypeAdapterRequest request{
        .index_type = index_type,
        .field_type = source_type,
        .element_type = element_type,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = std::move(params),
        .is_nested = nested,
        .is_text_match = false,
    };
    auto adapted = index::AdaptIndexType(request);
    adapted.params[index::FIELD_ID] =
        ParseDirectInt(adapted.params, index::FIELD_ID, 0);
    adapted.params["local_dir"] = DirectLocalDir(adapted.params);
    adapted.params["is_nested_index"] = nested;
    return {.source_type = source_type, .adapted = std::move(adapted)};
}

FieldDataPtr
AdaptDirectScalarFieldData(DataType source_type,
                           int64_t size,
                           const void* field_data) {
    AssertInfo(size >= 0, "direct scalar input size must be non-negative");
    AssertInfo(size == 0 || field_data != nullptr,
               "direct scalar input data is null for non-empty input");

    if (source_type == DataType::BOOL) {
        AssertInfo(size <= std::numeric_limits<int>::max(),
                   "serialized bool input exceeds protobuf size limit");
        proto::schema::BoolArray values;
        AssertInfo(values.ParseFromArray(field_data, static_cast<int>(size)),
                   "failed to parse direct bool input");
        const auto rows = static_cast<int64_t>(values.data_size());
        auto batch = storage::CreateFieldData(
            source_type, DataType::NONE, false, 1, rows);
        batch->FillFieldData(values.data().data(), rows);
        return batch;
    }

    if (IsStringDataType(source_type)) {
        AssertInfo(size <= std::numeric_limits<int>::max(),
                   "serialized string input exceeds protobuf size limit");
        proto::schema::StringArray encoded;
        AssertInfo(encoded.ParseFromArray(field_data, static_cast<int>(size)),
                   "failed to parse direct string input");
        std::vector<std::string> values;
        values.reserve(static_cast<size_t>(encoded.data_size()));
        for (const auto& value : encoded.data()) {
            values.push_back(value);
        }
        const auto rows = static_cast<int64_t>(values.size());
        auto batch = storage::CreateFieldData(
            source_type, DataType::NONE, false, 1, rows);
        batch->FillFieldData(values.data(), rows);
        return batch;
    }

    if (source_type == DataType::GEOMETRY) {
        AssertInfo(
            static_cast<uint64_t>(size) <=
                static_cast<uint64_t>(std::numeric_limits<ssize_t>::max()),
            "direct geometry row count exceeds ssize_t");
        auto batch =
            std::make_shared<FieldData<std::string>>(source_type, false, size);
        FieldDataPtr field_batch = batch;
        field_batch->FillFieldData(field_data, static_cast<ssize_t>(size));
        return batch;
    }

    switch (source_type) {
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::TIMESTAMPTZ: {
            AssertInfo(
                static_cast<uint64_t>(size) <=
                    static_cast<uint64_t>(std::numeric_limits<ssize_t>::max()),
                "direct scalar row count exceeds ssize_t");
            auto batch = storage::CreateFieldData(
                source_type, DataType::NONE, false, 1, size);
            batch->FillFieldData(field_data, static_cast<ssize_t>(size));
            return batch;
        }
        default:
            ThrowInfo(DataTypeInvalid,
                      "direct scalar input type {} is not supported",
                      static_cast<int>(source_type));
    }
}

FieldDataPtr
AdaptDirectVectorFieldData(const DirectVectorInput& input) {
    AssertInfo(IsVectorDataType(input.source_type) &&
                   input.source_type != DataType::VECTOR_ARRAY,
               "direct vector input type {} is not a physical vector type",
               input.source_type);
    const auto physical_rows = DirectPhysicalRows(input);
    AssertInfo(input.payload != nullptr || physical_rows == 0,
               "direct vector payload is null for {} physical rows",
               physical_rows);
    AssertInfo(physical_rows <=
                   static_cast<size_t>(std::numeric_limits<ssize_t>::max()),
               "direct vector physical row count exceeds ssize_t");

    if (input.source_type == DataType::VECTOR_SPARSE_U32_F32) {
        ValidateDirectSparseRows(input, physical_rows);
    }
    auto packed_validity = PackDirectValidity(input, physical_rows);
    const auto logical_rows =
        input.has_validity
            ? CheckedDirectSize(input.logical_rows, "logical row count")
            : physical_rows;
    AssertInfo(logical_rows <= static_cast<size_t>(
                                   std::numeric_limits<int64_t>::max()) &&
                   logical_rows <=
                       static_cast<size_t>(std::numeric_limits<ssize_t>::max()),
               "direct vector logical row count exceeds supported range");

    auto batch = storage::CreateFieldData(input.source_type,
                                          DataType::NONE,
                                          input.has_validity,
                                          input.configured_dim,
                                          0);
    if (input.has_validity) {
        batch->FillFieldData(
            input.payload,
            packed_validity.empty() ? nullptr : packed_validity.data(),
            static_cast<ssize_t>(logical_rows),
            0);
    } else {
        batch->FillFieldData(input.payload,
                             static_cast<ssize_t>(physical_rows));
    }
    AssertInfo(
        batch->Length() == logical_rows &&
            static_cast<size_t>(batch->get_valid_rows()) == physical_rows,
        "direct vector FieldData ownership changed row counts");
    return batch;
}

proto::cgo::IndexStats
AdaptArtifactStats(const storage::ArtifactStats& stats) {
    proto::cgo::IndexStats result;
    result.set_mem_size(stats.MemSize());
    for (const auto& file : stats.Files()) {
        auto* output = result.add_serialized_index_infos();
        output->set_file_name(file.file_name);
        output->set_file_size(file.file_size);
    }
    return result;
}

}  // namespace milvus::indexbuilder
