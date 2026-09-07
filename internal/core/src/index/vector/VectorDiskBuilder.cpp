// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/vector/VectorDiskBuilder.h"

#include <algorithm>
#include <charconv>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/vector/VectorDiskArtifact.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "index/vector/VectorDiskLocalFiles.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

constexpr std::string_view kEmptyEmbListOffsets = "empty_emb_list_offsets";

struct RawHeader {
    uint32_t rows{0};
    uint32_t dim{0};
};

struct DiskValidity {
    bool present{false};
    size_t total_count{0};
    size_t valid_count{0};
    std::vector<uint8_t> bytes;
};

void
ReadExact(
    int fd, void* data, size_t size, size_t offset, const std::string& path) {
    size_t read_bytes = 0;
    while (read_bytes < size) {
        const auto result = ::pread(fd,
                                    static_cast<uint8_t*>(data) + read_bytes,
                                    size - read_bytes,
                                    static_cast<off_t>(offset + read_bytes));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            ThrowInfo(FileReadFailed,
                      "failed to read vector build input {}: {}",
                      path,
                      result == 0 ? "unexpected EOF" : std::strerror(errno));
        }
        read_bytes += static_cast<size_t>(result);
    }
}

std::vector<uint8_t>
ReadFile(const std::string& path) {
    const auto fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open vector build input {}: {}",
                  path,
                  std::strerror(errno));
    }
    struct Guard {
        int fd;
        ~Guard() {
            if (fd >= 0) {
                ::close(fd);
            }
        }
    } guard{fd};
    struct stat stat {};
    if (::fstat(fd, &stat) != 0 || stat.st_size < 0 ||
        static_cast<uint64_t>(stat.st_size) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(FileReadFailed,
                  "failed to stat vector build input {}: {}",
                  path,
                  std::strerror(errno));
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(stat.st_size));
    if (!bytes.empty()) {
        ReadExact(fd, bytes.data(), bytes.size(), 0, path);
    }
    return bytes;
}

RawHeader
ReadRawHeader(const std::string& path) {
    const auto fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open vector raw data {}: {}",
                  path,
                  std::strerror(errno));
    }
    struct Guard {
        int fd;
        ~Guard() {
            if (fd >= 0) {
                ::close(fd);
            }
        }
    } guard{fd};
    RawHeader header;
    ReadExact(fd, &header, sizeof(header), 0, path);
    return header;
}

void
WriteFile(const std::string& path, const void* data, size_t size) {
    auto fd =
        ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ThrowInfo(FileCreateFailed,
                  "failed to create vector staging file {}: {}",
                  path,
                  std::strerror(errno));
    }
    try {
        size_t written = 0;
        while (written < size) {
            const auto result =
                ::write(fd,
                        static_cast<const uint8_t*>(data) + written,
                        size - written);
            if (result < 0 && errno == EINTR) {
                continue;
            }
            if (result <= 0) {
                ThrowInfo(FileWriteFailed,
                          "failed to write vector staging file {}: {}",
                          path,
                          std::strerror(errno));
            }
            written += static_cast<size_t>(result);
        }
        if (::fsync(fd) != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to flush vector staging file {}: {}",
                      path,
                      std::strerror(errno));
        }
        const auto close_result = ::close(fd);
        const auto close_error = errno;
        fd = -1;
        if (close_result != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to close vector staging file {}: {}",
                      path,
                      std::strerror(close_error));
        }
    } catch (...) {
        if (fd >= 0) {
            ::close(fd);
        }
        ::unlink(path.c_str());
        throw;
    }
}

DiskValidity
ReadValidity(const std::string& path) {
    DiskValidity result;
    if (path.empty()) {
        return result;
    }
    result.bytes = ReadFile(path);
    if (result.bytes.size() < sizeof(uint64_t)) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data file is too small");
    }
    uint64_t wire_count = 0;
    std::memcpy(&wire_count, result.bytes.data(), sizeof(wire_count));
    result.total_count = FromValidDataCount(wire_count);
    const auto bitmap_size = GetValidDataBitmapSize(result.total_count);
    if (result.bytes.size() < sizeof(uint64_t) + bitmap_size) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data bitmap is truncated");
    }
    result.valid_count = CountValidDataBitmap(
        result.total_count, result.bytes.data() + sizeof(uint64_t));
    result.present = true;
    return result;
}

std::vector<size_t>
ReadEmbeddingOffsets(const std::string& path) {
    auto bytes = ReadFile(path);
    if (bytes.size() < sizeof(size_t)) {
        ThrowInfo(DataFormatBroken, "embedding-list offset file is too small");
    }
    size_t count = 0;
    std::memcpy(&count, bytes.data(), sizeof(count));
    if (count == 0 ||
        count > (std::numeric_limits<size_t>::max() - sizeof(size_t)) /
                    sizeof(size_t)) {
        ThrowInfo(DataFormatBroken, "embedding-list offset count is invalid");
    }
    const auto required = sizeof(size_t) + count * sizeof(size_t);
    if (bytes.size() < required) {
        ThrowInfo(DataFormatBroken, "embedding-list offset file is truncated");
    }
    std::vector<size_t> offsets(count);
    std::memcpy(
        offsets.data(), bytes.data() + sizeof(size_t), count * sizeof(size_t));
    if (offsets.front() != 0 ||
        !std::is_sorted(offsets.begin(), offsets.end())) {
        ThrowInfo(DataFormatBroken,
                  "embedding-list offsets are not a monotonic prefix sum");
    }
    return offsets;
}

std::vector<uint8_t>
EncodeEmptyEmbeddingOffsets(int64_t dim, const std::vector<size_t>& offsets) {
    AssertInfo(dim > 0, "empty embedding-list dimension must be positive");
    AssertInfo(!offsets.empty() && offsets.front() == 0 && offsets.back() == 0,
               "empty embedding-list offsets are invalid");
    constexpr size_t header_size = sizeof(int64_t) + sizeof(uint64_t);
    if (offsets.size() >
        (std::numeric_limits<size_t>::max() - header_size) / sizeof(size_t)) {
        ThrowInfo(UnexpectedError,
                  "empty embedding-list offset payload size overflows");
    }
    std::vector<uint8_t> bytes(header_size + offsets.size() * sizeof(size_t));
    auto* cursor = bytes.data();
    const auto wire_count = ToValidDataCount(offsets.size());
    std::memcpy(cursor, &dim, sizeof(dim));
    cursor += sizeof(dim);
    std::memcpy(cursor, &wire_count, sizeof(wire_count));
    cursor += sizeof(wire_count);
    std::memcpy(cursor, offsets.data(), offsets.size() * sizeof(size_t));
    return bytes;
}

int64_t
ParsePositiveInt(const nlohmann::json& value, std::string_view key) {
    int64_t parsed = 0;
    if (value.is_number_unsigned()) {
        const auto number = value.get<uint64_t>();
        if (number <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            parsed = static_cast<int64_t>(number);
        }
    } else if (value.is_number_integer()) {
        parsed = value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (error != std::errc() || end != text.data() + text.size()) {
            parsed = 0;
        }
    }
    if (parsed <= 0 || parsed > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(
            ConfigInvalid, "vector parameter {} must be a positive int32", key);
    }
    return parsed;
}

void
NormalizeDiskAnnBuildThreads(Config& config, const IndexType& index_type) {
    if (index_type != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    if (!config.contains(DISK_ANN_BUILD_THREAD_NUM)) {
        ThrowInfo(ConfigInvalid,
                  "DiskANN build requires {}",
                  DISK_ANN_BUILD_THREAD_NUM);
    }
    config[DISK_ANN_THREADS_NUM] = ParsePositiveInt(
        config.at(DISK_ANN_BUILD_THREAD_NUM), DISK_ANN_BUILD_THREAD_NUM);
}

}  // namespace

template <typename T>
VectorDiskBuilder<T>::VectorDiskBuilder(DataType elem_type,
                                        IndexType index_type,
                                        MetricType metric_type,
                                        IndexVersion version,
                                        int64_t dim,
                                        knowhere::Json build_params,
                                        std::string local_dir)
    : local_files_(VectorDiskLocalFiles::Create(local_dir)),
      file_manager_(std::make_shared<VectorDiskBuildFileManager>(local_files_)),
      version_(version),
      build_params_(std::move(build_params)) {
    auto manager = std::static_pointer_cast<milvus::FileManager>(file_manager_);
    auto pack = knowhere::Pack(std::move(manager));
    engine_ = std::make_unique<KnowhereEngine>(PhysicalVectorDataType<T>(),
                                               elem_type,
                                               std::move(index_type),
                                               std::move(metric_type),
                                               version,
                                               pack,
                                               true);
    if (dim < 0 || (dim == 0 && PhysicalVectorDataType<T>() !=
                                    DataType::VECTOR_SPARSE_U32_F32)) {
        ThrowInfo(ConfigInvalid,
                  "disk vector dimension {} is invalid for type {}",
                  dim,
                  PhysicalVectorDataType<T>());
    }
    engine_->SetDim(dim);
}

template <typename T>
void
VectorDiskBuilder<T>::EnsureOpen(const char* operation) const {
    AssertInfo(!failed_, "cannot {} a failed vector disk builder", operation);
    AssertInfo(!sealed_, "cannot {} a sealed vector disk builder", operation);
    AssertInfo(engine_ != nullptr,
               "vector disk builder engine is missing during {}",
               operation);
}

template <typename T>
BuilderInputSpec
VectorDiskBuilder<T>::InputSpec() const {
    BuilderInputSpec spec;
    spec.form = BuilderInputSpec::LocalFile;
    spec.needs_second_pass = false;
    const auto opt_fields =
        GetValueFromConfig<OptFieldT>(build_params_, VEC_OPT_FIELDS);
    const auto partition_isolation =
        GetValueFromConfig<bool>(build_params_, PARTITION_KEY_ISOLATION_KEY)
            .value_or(false);
    if (opt_fields.has_value() &&
        engine_->Raw().IsAdditionalScalarSupported(partition_isolation)) {
        spec.side_inputs.reserve(opt_fields->size());
        for (const auto& [field_id, _] : *opt_fields) {
            spec.side_inputs.emplace_back(field_id);
        }
        std::sort(spec.side_inputs.begin(), spec.side_inputs.end());
    }
    return spec;
}

template <typename T>
void
VectorDiskBuilder<T>::Add(size_t, const T*, const bool*) {
    try {
        EnsureOpen("Add");
        ThrowInfo(UnexpectedError,
                  "VectorDiskBuilder is a LocalFile-form builder; use "
                  "SetSourceFile");
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
void
VectorDiskBuilder<T>::SetSourceFile(const std::string& path) {
    try {
        EnsureOpen("set the source file on");
        AssertInfo(input_files_ == nullptr,
                   "vector disk materialized inputs were already set");
        AssertInfo(raw_data_path_.empty(),
                   "vector disk source file was already set");
        AssertInfo(!path.empty(), "vector disk source file is empty");
        raw_data_path_ = path;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
void
VectorDiskBuilder<T>::SetEmbListOffsetsFile(const std::string& path) {
    try {
        EnsureOpen("set embedding-list offsets on");
        AssertInfo(input_files_ == nullptr,
                   "vector disk materialized inputs were already set");
        AssertInfo(emb_list_offsets_path_.empty(),
                   "embedding-list offset file was already set");
        AssertInfo(!path.empty(), "embedding-list offset file is empty");
        emb_list_offsets_path_ = path;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
void
VectorDiskBuilder<T>::SetValidDataFile(const std::string& path) {
    try {
        EnsureOpen("set validity input on");
        AssertInfo(input_files_ == nullptr,
                   "vector disk materialized inputs were already set");
        AssertInfo(valid_data_path_.empty(),
                   "vector validity file was already set");
        AssertInfo(!path.empty(), "vector validity file is empty");
        valid_data_path_ = path;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
void
VectorDiskBuilder<T>::SetMaterializedInputs(
    std::shared_ptr<VectorDiskLocalFiles> owner,
    std::string raw_path,
    std::string valid_path,
    std::string offsets_path,
    std::optional<std::string> scalar_info_path) {
    try {
        EnsureOpen("set materialized inputs on");
        AssertInfo(input_files_ == nullptr && raw_data_path_.empty() &&
                       valid_data_path_.empty() &&
                       emb_list_offsets_path_.empty() &&
                       !scalar_info_path_.has_value(),
                   "vector disk inputs were already set");
        AssertInfo(owner != nullptr,
                   "vector disk materialized input owner is null");
        AssertInfo(!raw_path.empty() && owner->Owns(raw_path),
                   "vector disk raw input is outside its owned generation");
        for (const auto* path : {&valid_path, &offsets_path}) {
            AssertInfo(path->empty() || owner->Owns(*path),
                       "vector disk sidecar input is outside its owned "
                       "generation");
        }
        if (scalar_info_path.has_value() && !scalar_info_path->empty()) {
            AssertInfo(owner->Owns(*scalar_info_path),
                       "vector disk scalar input is outside its owned "
                       "generation");
        }

        input_files_ = std::move(owner);
        raw_data_path_.swap(raw_path);
        valid_data_path_.swap(valid_path);
        emb_list_offsets_path_.swap(offsets_path);
        scalar_info_path_.swap(scalar_info_path);
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
storage::ArtifactPtr
VectorDiskBuilder<T>::Seal() && {
    EnsureOpen("seal");
    sealed_ = true;
    try {
        AssertInfo(!raw_data_path_.empty(),
                   "vector disk build source file was not set");
        const auto raw = ReadRawHeader(raw_data_path_);
        if constexpr (std::is_same_v<T, sparse_u32_f32>) {
            AssertInfo(engine_->Dim() == 0 ||
                           raw.dim <= static_cast<uint64_t>(engine_->Dim()),
                       "sparse vector raw-data dimension {} exceeds build "
                       "dimension {}",
                       raw.dim,
                       engine_->Dim());
            if (raw.dim != 0) {
                engine_->SetDim(static_cast<int64_t>(raw.dim));
            }
            build_params_[DIM_KEY] = engine_->Dim();
        } else {
            AssertInfo(raw.dim == static_cast<uint64_t>(engine_->Dim()),
                       "vector raw-data dimension {} disagrees with build "
                       "dimension {}",
                       raw.dim,
                       engine_->Dim());
        }

        auto validity = ReadValidity(valid_data_path_);
        if (validity.present) {
            BuildValidDataFromBitmap(valid_data_,
                                     validity.total_count,
                                     validity.bytes.data() + sizeof(uint64_t));
        }

        const bool all_null = validity.present && validity.total_count > 0 &&
                              validity.valid_count == 0;
        const bool embedding_list = engine_->ElemType() != DataType::NONE;
        std::vector<size_t> offsets;
        if (embedding_list && !all_null) {
            AssertInfo(!emb_list_offsets_path_.empty(),
                       "embedding-list disk build is missing offsets");
            offsets = ReadEmbeddingOffsets(emb_list_offsets_path_);
            if (offsets.back() != raw.rows) {
                ThrowInfo(DataFormatBroken,
                          "embedding-list terminal offset {} disagrees with "
                          "raw vector count {}",
                          offsets.back(),
                          raw.rows);
            }
            if (validity.present &&
                validity.valid_count + 1 != offsets.size()) {
                ThrowInfo(DataFormatBroken,
                          "embedding-list offset count disagrees with valid "
                          "parent row count");
            }
        } else if (!embedding_list) {
            AssertInfo(emb_list_offsets_path_.empty(),
                       "ordinary vector disk build has embedding-list "
                       "offsets");
            if (validity.present && validity.valid_count != raw.rows) {
                ThrowInfo(DataFormatBroken,
                          "nullable valid vector count {} disagrees with raw "
                          "vector count {}",
                          validity.valid_count,
                          raw.rows);
            }
        }

        if (validity.present) {
            const auto path =
                (std::filesystem::path(file_manager_->Directory()) /
                 VALID_DATA_KEY)
                    .string();
            WriteFile(path, validity.bytes.data(), validity.bytes.size());
            file_manager_->RegisterOwnedFile(path);
        }

        const bool empty_embedding_list =
            embedding_list && !offsets.empty() && offsets.back() == 0;
        const bool publish_empty_embedding_list =
            !all_null && empty_embedding_list;

        if (all_null) {
            AssertInfo(raw.rows == 0,
                       "all-null vector build contains {} raw vectors",
                       raw.rows);
        } else if (publish_empty_embedding_list) {
            auto bytes = EncodeEmptyEmbeddingOffsets(engine_->Dim(), offsets);
            const auto path =
                (std::filesystem::path(file_manager_->Directory()) /
                 kEmptyEmbListOffsets)
                    .string();
            WriteFile(path, bytes.data(), bytes.size());
            file_manager_->RegisterOwnedFile(path);
            engine_->SetEmptyEmbListOffsets(offsets);
        } else {
            const auto required_side_inputs = InputSpec().side_inputs;
            AssertInfo(
                required_side_inputs.empty() || scalar_info_path_.has_value(),
                "vector disk build requires declared scalar input "
                "delivery");
            auto config = build_params_;
            config[EMB_LIST] = embedding_list;
            config[DISK_ANN_RAW_DATA_PATH] = raw_data_path_;
            config[DISK_ANN_PREFIX_PATH] = file_manager_->IndexPrefix();
            config.erase(VALID_DATA_PATH_KEY);
            config.erase(INSERT_FILES_KEY);
            config.erase(VEC_OPT_FIELDS);
            config.erase(VEC_OPT_FIELDS_PATH);
            if (scalar_info_path_.has_value() && !scalar_info_path_->empty()) {
                config[VEC_OPT_FIELDS_PATH] = *scalar_info_path_;
            }
            if (embedding_list) {
                config[EMB_LIST_OFFSETS_PATH] = emb_list_offsets_path_;
            } else {
                config.erase(EMB_LIST_OFFSETS_PATH);
            }
            NormalizeDiskAnnBuildThreads(config, engine_->KnowhereIndexType());

            const auto status = engine_->Raw().Build({}, config);
            file_manager_->RethrowFirstFailure();
            if (status != knowhere::Status::success) {
                ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                          "failed to build disk vector index: status {} ({})",
                          static_cast<int>(status),
                          knowhere::Status2String(status));
            }
            knowhere::BinarySet entries;
            const auto serialize_status = engine_->Raw().Serialize(entries);
            file_manager_->RethrowFirstFailure();
            if (serialize_status != knowhere::Status::success) {
                ThrowInfo(knowhere::ToSegcoreErrorCode(serialize_status),
                          "failed to serialize disk vector index: status {} "
                          "({})",
                          static_cast<int>(serialize_status),
                          knowhere::Status2String(serialize_status));
            }
            AssertInfo(entries.binary_map_.empty(),
                       "disk vector index {} unexpectedly returned in-memory "
                       "serialized entries",
                       engine_->KnowhereIndexType());
        }

        file_manager_->RethrowFirstFailure();
        auto files = file_manager_->Files();
        AssertInfo(!files.empty(),
                   "vector disk build produced no artifact files");

        auto load_params = build_params_;
        load_params.erase(DISK_ANN_RAW_DATA_PATH);
        load_params.erase(EMB_LIST_OFFSETS_PATH);
        load_params.erase(VALID_DATA_PATH_KEY);
        load_params.erase(INSERT_FILES_KEY);
        load_params.erase(VEC_OPT_FIELDS);
        load_params.erase(VEC_OPT_FIELDS_PATH);
        load_params.erase("local_dir");
        if (!publish_empty_embedding_list) {
            offsets.clear();
        }
        auto artifact = std::make_unique<VectorDiskArtifact<T>>(
            engine_->ElemType(),
            engine_->KnowhereIndexType(),
            engine_->Metric(),
            version_,
            engine_->Dim(),
            std::move(valid_data_),
            std::move(offsets),
            std::move(load_params),
            local_files_,
            std::move(files));
        engine_.reset();
        file_manager_.reset();
        input_files_.reset();
        local_files_.reset();
        raw_data_path_.clear();
        emb_list_offsets_path_.clear();
        valid_data_path_.clear();
        scalar_info_path_.reset();
        build_params_.clear();
        return artifact;
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template class VectorDiskBuilder<float>;
template class VectorDiskBuilder<float16>;
template class VectorDiskBuilder<bfloat16>;
template class VectorDiskBuilder<bin1>;
template class VectorDiskBuilder<sparse_u32_f32>;
template class VectorDiskBuilder<int8>;

}  // namespace milvus::index
