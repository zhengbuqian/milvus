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

#include "indexbuilder/IndexBuildService.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "indexbuilder/VectorBuildDriver.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "storage/DataCodec.h"
#include "storage/Util.h"

namespace milvus::indexbuilder {
namespace {

constexpr int64_t kMissingBatchMaxRows = 64 * 1024;
constexpr size_t kMissingBatchTargetBytes = 4U << 20;

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

bool
CompatibleType(DataType left, DataType right) {
    return left == right || (IsStringType(left) && IsStringType(right)) ||
           ((left == DataType::INT64 || left == DataType::TIMESTAMPTZ) &&
            (right == DataType::INT64 || right == DataType::TIMESTAMPTZ));
}

struct JsonProjectionShape {
    DataType value_type{DataType::NONE};
    bool is_array{false};
    bool is_flat{false};
};

JsonProjectionShape
ParseJsonProjectionShape(const index::BuildParams& params) {
    if (!params.contains(JSON_CAST_TYPE) ||
        !params.at(JSON_CAST_TYPE).is_string()) {
        ThrowInfo(DataTypeInvalid,
                  "JSON index build requires string parameter {}",
                  JSON_CAST_TYPE);
    }
    const auto cast = params.at(JSON_CAST_TYPE).get<std::string>();
    if (cast == "JSON") {
        return {.value_type = DataType::JSON, .is_flat = true};
    }
    if (cast == "BOOL") {
        return {.value_type = DataType::BOOL};
    }
    if (cast == "DOUBLE") {
        return {.value_type = DataType::DOUBLE};
    }
    if (cast == "VARCHAR") {
        return {.value_type = DataType::VARCHAR};
    }
    if (cast == "ARRAY_BOOL") {
        return {.value_type = DataType::BOOL, .is_array = true};
    }
    if (cast == "ARRAY_DOUBLE") {
        return {.value_type = DataType::DOUBLE, .is_array = true};
    }
    if (cast == "ARRAY_VARCHAR") {
        return {.value_type = DataType::VARCHAR, .is_array = true};
    }
    ThrowInfo(DataTypeInvalid, "unsupported JSON cast type {}", cast);
}

int64_t
ParseInt64(const nlohmann::json& value, std::string_view key) {
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(parsed);
        }
    } else if (value.is_number_integer()) {
        return value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t parsed = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (!text.empty() && error == std::errc{} &&
            end == text.data() + text.size()) {
            return parsed;
        }
    }
    ThrowInfo(
        UnexpectedError, "normalized build parameter {} is not an int64", key);
}

bool
ParseBool(const nlohmann::json& value, std::string_view key) {
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <= 1) {
            return parsed != 0;
        }
    } else if (value.is_number_integer()) {
        const auto parsed = value.get<int64_t>();
        if (parsed == 0 || parsed == 1) {
            return parsed != 0;
        }
    } else if (value.is_string()) {
        auto text = value.get<std::string>();
        std::transform(
            text.begin(), text.end(), text.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
        if (text == "true" || text == "1") {
            return true;
        }
        if (text == "false" || text == "0") {
            return false;
        }
    }
    ThrowInfo(
        UnexpectedError, "normalized build parameter {} is not boolean", key);
}

void
ValidateOrSetInt64(index::BuildParams& params,
                   std::string_view key,
                   int64_t expected) {
    if (params.contains(key) && !params.at(key).is_null()) {
        const auto actual = ParseInt64(params.at(key), key);
        if (actual != expected) {
            ThrowInfo(UnexpectedError,
                      "normalized build parameter {}={} conflicts with {}",
                      key,
                      actual,
                      expected);
        }
    }
    params[std::string(key)] = expected;
}

void
ValidateOrSetBool(index::BuildParams& params,
                  std::string_view key,
                  bool expected) {
    if (params.contains(key) && !params.at(key).is_null()) {
        const auto actual = ParseBool(params.at(key), key);
        if (actual != expected) {
            ThrowInfo(UnexpectedError,
                      "normalized build parameter {} conflicts with field "
                      "schema",
                      key);
        }
    }
    params[std::string(key)] = expected;
}

void
ValidateOrSetString(index::BuildParams& params,
                    std::string_view key,
                    const std::string& expected) {
    if (params.contains(key) && !params.at(key).is_null()) {
        if (!params.at(key).is_string() ||
            params.at(key).get<std::string>() != expected) {
            ThrowInfo(UnexpectedError,
                      "normalized build parameter {} conflicts with request",
                      key);
        }
    }
    params[std::string(key)] = expected;
}

void
ValidateExistingString(const index::BuildParams& params,
                       std::string_view key,
                       const std::string& expected) {
    if (!params.contains(key)) {
        return;
    }
    if (!params.at(key).is_string() ||
        params.at(key).get<std::string>() != expected) {
        ThrowInfo(UnexpectedError,
                  "normalized build parameter {} conflicts with request",
                  key);
    }
}

void
NormalizeJsonPath(index::BuildParams& params, const std::string& canonical) {
    ValidateExistingString(params, JSON_PATH, canonical);
    ValidateExistingString(params, "nested_path", canonical);
    params.erase("nested_path");
    params[JSON_PATH] = canonical;
}

int64_t
ParseBinlogSequence(const std::string& path) {
    const auto slash = path.find_last_of('/');
    const auto name = std::string_view(path).substr(
        slash == std::string::npos ? 0 : slash + 1);
    int64_t sequence = 0;
    const auto [end, error] =
        std::from_chars(name.data(), name.data() + name.size(), sequence);
    if (name.empty() || error != std::errc{} ||
        end != name.data() + name.size()) {
        ThrowInfo(DataFormatBroken,
                  "insert binlog path has a non-numeric filename: {}",
                  path);
    }
    return sequence;
}

void
SortBinlogPaths(std::vector<std::string>& paths) {
    std::vector<std::pair<int64_t, std::string>> sequenced;
    sequenced.reserve(paths.size());
    for (auto& path : paths) {
        sequenced.emplace_back(ParseBinlogSequence(path), std::move(path));
    }
    std::sort(sequenced.begin(),
              sequenced.end(),
              [](const auto& left, const auto& right) {
                  return left.first < right.first;
              });
    for (size_t i = 0; i < sequenced.size(); ++i) {
        paths[i] = std::move(sequenced[i].second);
    }
}

int64_t
MissingBatchRows(size_t estimated_row_bytes, int64_t remaining) {
    estimated_row_bytes = std::max<size_t>(1, estimated_row_bytes);
    const auto bytes_limited =
        std::max<size_t>(1, kMissingBatchTargetBytes / estimated_row_bytes);
    const auto bounded = std::min<size_t>(
        static_cast<size_t>(kMissingBatchMaxRows), bytes_limited);
    return std::min<int64_t>(remaining, static_cast<int64_t>(bounded));
}

struct VectorSideInputPlan {
    FieldId field_id;
    DataType field_type{DataType::NONE};
    DataType element_type{DataType::NONE};
    std::vector<std::string> v1_files;
    storage::FieldDataMeta field_meta;
};

storage::VisitOutcome
VisitBuildField(const BuildSource& build_source,
                const std::vector<std::string>* v1_files_override,
                bool empty_v1_is_missing,
                FieldId field_id,
                DataType field_type,
                DataType element_type,
                int64_t dim,
                const storage::FieldDataMeta& field_meta,
                const storage::FileManagerContext& context,
                const storage::FieldDataVisitor& visitor,
                int64_t manifest_inflight_bytes) {
    return std::visit(
        [&](const auto& source) -> storage::VisitOutcome {
            using Source = std::decay_t<decltype(source)>;
            if constexpr (std::is_same_v<Source, V1BinlogBuildSource>) {
                auto files = v1_files_override == nullptr ? source.files
                                                          : *v1_files_override;
                if (files.empty()) {
                    return empty_v1_is_missing
                               ? storage::VisitOutcome::FieldMissing
                               : storage::VisitOutcome::Exhausted;
                }
                SortBinlogPaths(files);

                const auto slice_size = FILE_SLICE_SIZE.load();
                AssertInfo(slice_size > 0,
                           "index-build file slice size must be positive");
                const auto parallel_degree = std::max<int64_t>(
                    1, DEFAULT_FIELD_MAX_MEMORY_LIMIT / slice_size);
                for (size_t begin = 0; begin < files.size();) {
                    const auto remaining = files.size() - begin;
                    const auto count = std::min<size_t>(
                        remaining, static_cast<size_t>(parallel_degree));
                    std::vector<std::string> batch_files(
                        files.begin() + static_cast<ptrdiff_t>(begin),
                        files.begin() + static_cast<ptrdiff_t>(begin + count));
                    auto futures = storage::GetObjectData(
                        context.chunkManagerPtr.get(), batch_files);
                    std::exception_ptr first_failure;
                    bool stopped = false;
                    for (auto& future : futures) {
                        try {
                            auto codec = future.get();
                            AssertInfo(codec != nullptr,
                                       "binlog decoder returned null codec");
                            if (!first_failure && !stopped) {
                                stopped = visitor(codec->GetFieldData()) ==
                                          storage::VisitControl::Stop;
                            }
                        } catch (...) {
                            if (!first_failure) {
                                first_failure = std::current_exception();
                            }
                        }
                    }
                    if (first_failure) {
                        std::rethrow_exception(first_failure);
                    }
                    if (stopped) {
                        return storage::VisitOutcome::Stopped;
                    }
                    begin += count;
                }
                return storage::VisitOutcome::Exhausted;
            } else if constexpr (std::is_same_v<Source, StorageV2BuildSource>) {
                if (source.files.empty()) {
                    return storage::VisitOutcome::FieldMissing;
                }
                return storage::VisitFieldDataFromStorageV2(source.files,
                                                            field_id.get(),
                                                            field_type,
                                                            element_type,
                                                            dim,
                                                            context.fs,
                                                            visitor);
            } else if constexpr (std::is_same_v<Source, ManifestBuildSource>) {
                std::optional<storage::StorageColumnMapping> mapping;
                const auto mapping_it =
                    context.storage_column_mappings.find(field_id.get());
                if (mapping_it != context.storage_column_mappings.end()) {
                    mapping = mapping_it->second;
                }
                return storage::VisitFieldDataFromManifest(
                    source.manifest_path,
                    context.loon_ffi_properties,
                    field_meta,
                    field_type,
                    dim,
                    element_type,
                    std::move(mapping),
                    visitor,
                    manifest_inflight_bytes);
            }
        },
        build_source);
}

std::optional<VectorSideInputPlan>
PrepareVectorSideInputPlan(const BuildRequest& request,
                           const BuildFieldSpec& field_spec,
                           const storage::FileManagerContext& context,
                           const index::BuilderInputSpec& input_spec,
                           const BuildDriver& driver) {
    if (input_spec.side_inputs.empty()) {
        return std::nullopt;
    }
    if (field_spec.field_type == DataType::VECTOR_ARRAY) {
        ThrowInfo(Unsupported,
                  "VECTOR_ARRAY optional scalar input is not migrated");
    }
    AssertInfo(IsVectorDataType(field_spec.field_type),
               "non-vector builder declared vector optional scalar input");
    if (input_spec.side_inputs.size() != 1) {
        ThrowInfo(Unsupported,
                  "vector index build supports exactly one optional scalar "
                  "field");
    }

    const auto configured =
        index::GetValueFromConfig<OptFieldT>(request.params, VEC_OPT_FIELDS);
    AssertInfo(configured.has_value(),
               "vector builder declared side input without optional field "
               "metadata");
    if (configured->size() != 1) {
        ThrowInfo(Unsupported,
                  "vector index build supports exactly one optional scalar "
                  "field");
    }

    const auto field_id = input_spec.side_inputs.front();
    const auto it = configured->find(field_id.get());
    AssertInfo(it != configured->end(),
               "vector builder side-input field {} is absent from metadata",
               field_id.get());
    const auto& [field_name, field_type, element_type, files] = it->second;
    static_cast<void>(field_name);
    if (!IsSupportedVectorScalarInfoType(field_type) ||
        element_type != DataType::NONE) {
        ThrowInfo(Unsupported,
                  "optional scalar field {} has unsupported type {} and "
                  "element type {}",
                  field_id.get(),
                  field_type,
                  element_type);
    }

    // Family-local delivery is checked before any source I/O.
    if (input_spec.form == index::BuilderInputSpec::LocalFile) {
        ValidateVectorDiskInputDriver(driver);
    } else {
        static_cast<void>(GetVectorPrimaryLayout(driver));
    }
    return VectorSideInputPlan{
        .field_id = field_id,
        .field_type = field_type,
        .element_type = element_type,
        .v1_files = files,
        .field_meta = AdaptOptionalBuildFieldMeta(
            context, field_id, field_type, element_type)};
}

VectorScalarInfo
MaterializeVectorScalarInfo(const VectorSideInputPlan& plan,
                            const BuildRequest& request,
                            const storage::FileManagerContext& context,
                            const VectorPrimaryLayout& layout) {
    constexpr auto kMaxPhysicalRows =
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
    if (static_cast<uint64_t>(layout.PhysicalRows()) > kMaxPhysicalRows) {
        ThrowInfo(Unsupported,
                  "vector scalar-info physical row count {} exceeds uint32 "
                  "coordinate capacity",
                  layout.PhysicalRows());
    }

    VectorScalarInfoAccumulator accumulator(
        plan.field_id, plan.field_type, layout);
    const auto outcome = VisitBuildField(
        request.source,
        &plan.v1_files,
        /*empty_v1_is_missing=*/true,
        plan.field_id,
        plan.field_type,
        plan.element_type,
        /*dim=*/1,
        plan.field_meta,
        context,
        [&](FieldDataPtr batch) {
            accumulator.Add(batch);
            return storage::VisitControl::Continue;
        },
        storage::kAccumulatingInflightBytes);
    switch (outcome) {
        case storage::VisitOutcome::FieldMissing: {
            // Legacy V1 returned before inserting the field key when no paths
            // existed. V2/V3 inserted the requested key with no categories
            // after their field lookup returned no batches.
            if (std::holds_alternative<V1BinlogBuildSource>(request.source)) {
                return {};
            }
            VectorScalarInfo result;
            result.emplace(plan.field_id.get(),
                           std::vector<std::vector<uint32_t>>{});
            return result;
        }
        case storage::VisitOutcome::Exhausted:
            return std::move(accumulator).Finish();
        case storage::VisitOutcome::Stopped:
            ThrowInfo(UnexpectedError,
                      "always-continue optional scalar visitor stopped early");
    }
}

}  // namespace

BuildProduct
BuildProduct::FromArtifact(storage::ArtifactPtr artifact) {
    AssertInfo(artifact != nullptr, "index build produced a null artifact");
    return BuildProduct(Kind::Artifact, std::move(artifact));
}

BuildProduct
BuildProduct::SkippedEmpty() {
    return BuildProduct(Kind::SkippedEmpty, nullptr);
}

const storage::Artifact&
BuildProduct::GetArtifact() const {
    AssertInfo(kind_ == Kind::Artifact && artifact_ != nullptr,
               "skipped-empty build product has no artifact");
    return *artifact_;
}

IndexBuildService::IndexBuildService(
    BuildRequest request,
    const storage::FileManagerContext& file_manager_context)
    : req_(std::move(request)),
      file_manager_context_(file_manager_context),
      field_spec_(AdaptBuildField(file_manager_context_)) {
    NormalizeAndValidateRequest();
}

const BuildRequest&
IndexBuildService::Request() const noexcept {
    return req_;
}

const storage::FileManagerContext&
IndexBuildService::Context() const noexcept {
    return file_manager_context_;
}

void
IndexBuildService::NormalizeAndValidateRequest() {
    AssertInfo(file_manager_context_.Valid(),
               "index-build service requires a valid FileManagerContext");
    AssertInfo(req_.params.is_object(),
               "normalized index-build parameters must be an object");
    AssertInfo(req_.expected_rows >= 0,
               "index-build expected row count must be non-negative");
    AssertInfo(
        req_.missing_rows >= 0 && req_.missing_rows <= req_.expected_rows,
        "index-build missing row count {} is outside [0, {}]",
        req_.missing_rows,
        req_.expected_rows);
    AssertInfo(
        req_.field_id.get() == file_manager_context_.fieldDataMeta.field_id,
        "index-build field {} conflicts with FileManagerContext field "
        "{}",
        req_.field_id.get(),
        file_manager_context_.fieldDataMeta.field_id);
    AssertInfo(!req_.staging_parent.empty(),
               "index-build staging parent is not configured");

    if (req_.output.generation == storage::Generation::V3) {
        AssertInfo(!req_.output.packed_file_name.empty(),
                   "V3 index-build output requires a packed file name");
    } else {
        AssertInfo(req_.output.generation == storage::Generation::V1V2,
                   "unknown index-build output generation");
        AssertInfo(req_.output.packed_file_name.empty(),
                   "V1/V2 index-build output cannot use a packed file name");
    }

    const auto field_type = field_spec_.field_type;
    const auto element_type = field_spec_.element_type;
    AssertInfo(field_type != DataType::NONE,
               "index-build field schema has no data type");

    std::optional<JsonProjectionShape> json_projection;
    if (field_type == DataType::JSON) {
        NormalizeJsonPath(req_.params, req_.json_path);
        json_projection = ParseJsonProjectionShape(req_.params);
        if (req_.value_type != json_projection->value_type) {
            ThrowInfo(DataTypeInvalid,
                      "JSON build value type {} conflicts with its cast",
                      static_cast<int>(req_.value_type));
        }
        if (json_projection->is_flat) {
            if (req_.family != index::families::kJsonFlat) {
                ThrowInfo(DataTypeInvalid,
                          "JSON cast JSON requires the flat JSON family");
            }
        } else {
            if (req_.family == index::families::kJsonFlat ||
                req_.json_path.empty()) {
                ThrowInfo(DataTypeInvalid,
                          "typed JSON projection requires a non-empty path "
                          "and a predicate family");
            }
        }
    } else {
        AssertInfo(req_.json_path.empty(),
                   "non-JSON index build has an unexpected JSON path");
    }

    if (IsVectorDataType(field_type)) {
        AssertInfo(req_.output.generation == storage::Generation::V1V2,
                   "vector indexes have no V3 persisted format");
        AssertInfo(req_.family == index::families::kVectorMem ||
                       req_.family == index::families::kVectorDisk,
                   "vector build resolved to non-vector family {}",
                   req_.family);
        if (field_type == DataType::VECTOR_ARRAY) {
            AssertInfo(element_type != DataType::NONE &&
                           IsVectorDataType(element_type) &&
                           element_type != DataType::VECTOR_SPARSE_U32_F32 &&
                           element_type != DataType::VECTOR_ARRAY,
                       "VECTOR_ARRAY has invalid element type {}",
                       element_type);
            AssertInfo(req_.value_type == element_type,
                       "VECTOR_ARRAY build value type {} conflicts with "
                       "element type {}",
                       req_.value_type,
                       element_type);
        } else {
            AssertInfo(element_type == DataType::NONE,
                       "ordinary vector field has unexpected element type {}",
                       element_type);
            AssertInfo(req_.value_type == field_type,
                       "vector build value type {} conflicts with field type "
                       "{}",
                       req_.value_type,
                       field_type);
        }
    } else if (field_type == DataType::ARRAY) {
        AssertInfo(element_type != DataType::NONE,
                   "ARRAY index-build field schema has no element type");
        AssertInfo(req_.value_type == DataType::ARRAY ||
                       CompatibleType(req_.value_type, element_type),
                   "ARRAY index-build value type {} conflicts with schema "
                   "element type {}",
                   static_cast<int>(req_.value_type),
                   static_cast<int>(element_type));
    } else if (field_type != DataType::JSON) {
        AssertInfo(CompatibleType(req_.value_type, field_type),
                   "index-build value type {} conflicts with schema field "
                   "type {}",
                   static_cast<int>(req_.value_type),
                   static_cast<int>(field_type));
    }

    ValidateOrSetInt64(req_.params, index::FIELD_ID, req_.field_id.get());
    ValidateOrSetString(req_.params, "local_dir", req_.staging_parent);
    ValidateOrSetInt64(
        req_.params, "field_type", static_cast<int64_t>(field_type));
    ValidateOrSetInt64(
        req_.params, "value_type", static_cast<int64_t>(req_.value_type));
    if (json_projection.has_value()) {
        const bool inner_nullable =
            json_projection->is_flat || json_projection->is_array
                ? field_spec_.nullable
                : true;
        req_.params["nullable"] = inner_nullable;
        ValidateOrSetInt64(
            req_.params, "element_type", static_cast<int64_t>(DataType::NONE));
        ValidateOrSetInt64(req_.params,
                           "array_element_type",
                           static_cast<int64_t>(DataType::NONE));
    } else {
        ValidateOrSetBool(req_.params, "nullable", field_spec_.nullable);
    }
    if (field_type == DataType::ARRAY) {
        ValidateOrSetInt64(req_.params,
                           "array_element_type",
                           static_cast<int64_t>(element_type));
        if (req_.params.contains("element_type") &&
            !req_.params.at("element_type").is_null()) {
            ValidateOrSetInt64(req_.params,
                               "element_type",
                               static_cast<int64_t>(element_type));
        }
    } else if (field_type == DataType::VECTOR_ARRAY) {
        ValidateOrSetInt64(
            req_.params, "element_type", static_cast<int64_t>(element_type));
        ValidateOrSetInt64(req_.params,
                           "array_element_type",
                           static_cast<int64_t>(element_type));
    }

    std::visit(
        [&](const auto& source) {
            using Source = std::decay_t<decltype(source)>;
            if constexpr (std::is_same_v<Source, StorageV2BuildSource>) {
                if (!source.files.empty()) {
                    AssertInfo(file_manager_context_.fs != nullptr,
                               "storage-v2 index build has no Arrow file "
                               "system");
                    for (const auto& group : source.files) {
                        AssertInfo(!group.empty(),
                                   "storage-v2 index build contains an empty "
                                   "column group");
                    }
                }
            } else if constexpr (std::is_same_v<Source, ManifestBuildSource>) {
                AssertInfo(!source.manifest_path.empty(),
                           "manifest index build has an empty manifest path");
                AssertInfo(file_manager_context_.loon_ffi_properties != nullptr,
                           "manifest index build has no storage properties");
            }
        },
        req_.source);
}

FeedControl
IndexBuildService::FeedBatch(BuildDriver& driver,
                             const FieldDataPtr& batch,
                             int64_t& rows_fed) const {
    if (batch == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "index-build source produced a null field-data batch");
    }
    const auto batch_rows = batch->Length();
    if (batch_rows > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "index-build field-data row count exceeds int64");
    }
    const auto rows = static_cast<int64_t>(batch_rows);
    if (rows > req_.expected_rows - rows_fed) {
        ThrowInfo(DataFormatBroken,
                  "index-build source exceeds expected row count {}",
                  req_.expected_rows);
    }
    const auto control = driver.Feed(batch);
    rows_fed += rows;
    return control;
}

IndexBuildService::FeedOutcome
IndexBuildService::FeedMissingRows(BuildDriver& driver,
                                   int64_t& rows_fed) const {
    auto remaining = req_.missing_rows;
    while (remaining > 0) {
        const auto batch_rows = MissingBatchRows(
            field_spec_.estimated_missing_row_bytes, remaining);
        auto batch = CreateMissingFieldData(file_manager_context_, batch_rows);
        if (FeedBatch(driver, batch, rows_fed) == FeedControl::PassComplete) {
            return FeedOutcome::Stopped;
        }
        remaining -= batch_rows;
    }
    return FeedOutcome::Exhausted;
}

IndexBuildService::FeedOutcome
IndexBuildService::Feed(BuildDriver& driver, int64_t& rows_fed) const {
    if (FeedMissingRows(driver, rows_fed) == FeedOutcome::Stopped) {
        return FeedOutcome::Stopped;
    }

    const auto max_inflight_bytes =
        driver.InputSpec().form == index::BuilderInputSpec::Contiguous
            ? storage::kAccumulatingInflightBytes
            : storage::kStreamingInflightBytes;
    const auto outcome = VisitBuildField(
        req_.source,
        /*v1_files_override=*/nullptr,
        /*empty_v1_is_missing=*/false,
        req_.field_id,
        field_spec_.field_type,
        field_spec_.element_type,
        file_manager_context_.indexMeta.dim,
        file_manager_context_.fieldDataMeta,
        file_manager_context_,
        [&](FieldDataPtr batch) {
            return FeedBatch(driver, batch, rows_fed) ==
                           FeedControl::PassComplete
                       ? storage::VisitControl::Stop
                       : storage::VisitControl::Continue;
        },
        max_inflight_bytes);
    switch (outcome) {
        case storage::VisitOutcome::Stopped:
            return FeedOutcome::Stopped;
        case storage::VisitOutcome::Exhausted:
            return FeedOutcome::Exhausted;
        case storage::VisitOutcome::FieldMissing:
            // Missing rows were supplied first. The final expected-row check
            // decides whether the absent primary field is legitimate.
            return FeedOutcome::Exhausted;
    }
}

void
IndexBuildService::ValidateInputSpec(const index::BuilderInputSpec& spec,
                                     bool allow_second_pass) const {
    if (spec.needs_second_pass && !allow_second_pass) {
        ThrowInfo(UnexpectedError,
                  "selected index builder unexpectedly requires another "
                  "input pass");
    }
    if (!spec.side_inputs.empty() &&
        !IsVectorDataType(field_spec_.field_type)) {
        ThrowInfo(UnexpectedError,
                  "non-vector builder declared unsupported side input");
    }
    AssertInfo(spec.form != index::BuilderInputSpec::LocalFile ||
                   IsVectorDataType(field_spec_.field_type),
               "non-vector builder requested LocalFile materialization");
}

BuildProduct
IndexBuildService::RunToArtifact() {
    AssertInfo(!run_started_, "index-build service cannot run more than once");
    run_started_ = true;

    auto driver = MakeBuildDriver(req_.value_type, req_.family, req_.params);
    AssertInfo(driver != nullptr, "index-build driver factory returned null");
    const auto initial_spec = driver->InputSpec();
    ValidateInputSpec(initial_spec, true);
    const auto vector_side_input = PrepareVectorSideInputPlan(
        req_, field_spec_, file_manager_context_, initial_spec, *driver);

    try {
        if (initial_spec.form == index::BuilderInputSpec::LocalFile) {
            AssertInfo(!initial_spec.needs_second_pass,
                       "LocalFile vector builder cannot request two passes");
            ValidateVectorDiskInputDriver(*driver);
            VectorDiskBuildMaterializer materializer(
                req_.staging_parent,
                field_spec_.field_type,
                req_.value_type,
                file_manager_context_.indexMeta.dim,
                field_spec_.nullable,
                req_.expected_rows,
                vector_side_input.has_value());

            auto remaining = req_.missing_rows;
            while (remaining > 0) {
                const auto batch_rows = MissingBatchRows(
                    field_spec_.estimated_missing_row_bytes, remaining);
                materializer.Add(
                    CreateMissingFieldData(file_manager_context_, batch_rows));
                remaining -= batch_rows;
            }
            const auto outcome = VisitBuildField(
                req_.source,
                /*v1_files_override=*/nullptr,
                /*empty_v1_is_missing=*/false,
                req_.field_id,
                field_spec_.field_type,
                field_spec_.element_type,
                file_manager_context_.indexMeta.dim,
                file_manager_context_.fieldDataMeta,
                file_manager_context_,
                [&](FieldDataPtr batch) {
                    materializer.Add(batch);
                    return storage::VisitControl::Continue;
                },
                storage::kStreamingInflightBytes);
            AssertInfo(outcome != storage::VisitOutcome::Stopped,
                       "always-continue disk vector visitor stopped early");
            materializer.FinishPrimary();
            if (materializer.RequiresEngineBuild() &&
                vector_side_input.has_value()) {
                auto scalar_info =
                    MaterializeVectorScalarInfo(*vector_side_input,
                                                req_,
                                                file_manager_context_,
                                                materializer.PrimaryLayout());
                materializer.SetScalarInfo(std::move(scalar_info));
            }
            auto inputs = std::move(materializer).TakeInputs();
            DeliverVectorDiskInputs(*driver, std::move(inputs));
            return BuildProduct::FromArtifact(std::move(*driver).Seal());
        }

        int64_t rows_fed = 0;
        auto outcome = Feed(*driver, rows_fed);
        if (!initial_spec.needs_second_pass &&
            outcome != FeedOutcome::Exhausted) {
            ThrowInfo(UnexpectedError,
                      "one-pass builder requested an input early stop");
        }
        if (initial_spec.needs_second_pass) {
            if (outcome == FeedOutcome::Exhausted &&
                rows_fed != req_.expected_rows) {
                ThrowInfo(DataFormatBroken,
                          "index-build probe source produced {} rows, expected "
                          "{}",
                          rows_fed,
                          req_.expected_rows);
            }
            driver->FinishPass();
            const auto build_spec = driver->InputSpec();
            ValidateInputSpec(build_spec, false);
            rows_fed = 0;
            outcome = Feed(*driver, rows_fed);
            if (outcome != FeedOutcome::Exhausted) {
                ThrowInfo(UnexpectedError,
                          "selected one-pass builder requested an input early "
                          "stop");
            }
        }
        if (rows_fed != req_.expected_rows) {
            ThrowInfo(DataFormatBroken,
                      "index-build source produced {} rows, expected {}",
                      rows_fed,
                      req_.expected_rows);
        }
        if (vector_side_input.has_value()) {
            const auto& layout = GetVectorPrimaryLayout(*driver);
            auto scalar_info = MaterializeVectorScalarInfo(
                *vector_side_input, req_, file_manager_context_, layout);
            DeliverVectorScalarInfo(*driver, std::move(scalar_info));
        }
        return BuildProduct::FromArtifact(std::move(*driver).Seal());
    } catch (const SegcoreError& error) {
        if (error.get_error_code() == DataIsEmpty) {
            return BuildProduct::SkippedEmpty();
        }
        throw;
    }
}

std::unique_ptr<storage::FileSink>
IndexBuildService::MakeSink() const {
    if (req_.output.generation == storage::Generation::V3) {
        return std::make_unique<storage::V3PackedSink>(
            file_manager_context_,
            req_.output.packed_file_name,
            req_.output.storage_path);
    }
    return std::make_unique<storage::V1DiskSink>(file_manager_context_,
                                                 req_.output.storage_path);
}

storage::ArtifactStats
IndexBuildService::Publish(const BuildProduct& product) const {
    if (product.IsSkippedEmpty()) {
        return {};
    }
    auto sink = MakeSink();
    product.GetArtifact().Serialize(*sink);
    auto stats = sink->Finish();
    sink->ReleaseLocalStaging();
    return stats;
}

storage::ArtifactStats
IndexBuildService::Run() {
    auto product = RunToArtifact();
    return Publish(product);
}

}  // namespace milvus::indexbuilder
