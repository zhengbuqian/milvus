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

#include "index/VectorMemIndex.h"

#include <unistd.h>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/Types.h"
#include "common/type_c.h"
#include "fmt/format.h"

#include "index/Index.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "common/EasyAssert.h"
#include "config/ConfigKnowhere.h"
#include "knowhere/factory.h"
#include "knowhere/comp/time_recorder.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/File.h"
#include "common/Slice.h"
#include "common/Tracer.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "log/Log.h"
#include "mmap/Types.h"
#include "storage/DataCodec.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/space.h"
#include "storage/Util.h"

namespace milvus::index {

template <typename T>
VectorMemIndex<T>::VectorMemIndex(
    const IndexType& index_type,
    const MetricType& metric_type,
    const IndexVersion& version,
    const storage::FileManagerContext& file_manager_context)
    : VectorIndex(index_type, metric_type) {
    AssertInfo(!is_unsupported(index_type, metric_type),
               index_type + " doesn't support metric: " + metric_type);
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
    CheckCompatible(version);
    index_ =
        knowhere::IndexFactory::Instance().Create<T>(GetIndexType(), version);
}

template <typename T>
VectorMemIndex<T>::VectorMemIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space)
    : VectorIndex(create_index_info.index_type, create_index_info.metric_type),
      space_(space),
      create_index_info_(create_index_info) {
    AssertInfo(!is_unsupported(create_index_info.index_type,
                               create_index_info.metric_type),
               create_index_info.index_type +
                   " doesn't support metric: " + create_index_info.metric_type);
    if (file_manager_context.Valid()) {
        file_manager_ = std::make_shared<storage::MemFileManagerImpl>(
            file_manager_context, file_manager_context.space_);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
    auto version = create_index_info.index_engine_version;
    CheckCompatible(version);
    index_ =
        knowhere::IndexFactory::Instance().Create<T>(GetIndexType(), version);
}

template <typename T>
BinarySet
VectorMemIndex<T>::UploadV2(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFileV2(binary_set);

    auto store_version = file_manager_->space()->GetCurrentVersion();
    std::shared_ptr<uint8_t[]> store_version_data(
        new uint8_t[sizeof(store_version)]);
    store_version_data[0] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[1] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[2] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[3] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[4] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[5] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[6] = store_version & 0x00000000000000FF;
    store_version = store_version >> 8;
    store_version_data[7] = store_version & 0x00000000000000FF;
    BinarySet ret;
    ret.Append("index_store_version", store_version_data, 8);

    return ret;
}

template <typename T>
BinarySet
VectorMemIndex<T>::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

template <typename T>
BinarySet
VectorMemIndex<T>::Serialize(const Config& config) {
    knowhere::BinarySet ret;
    auto stat = index_.Serialize(ret);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to serialize index: {}",
                  KnowhereStatusString(stat));
    Disassemble(ret);

    return ret;
}

template <typename T>
void
VectorMemIndex<T>::LoadWithoutAssemble(const BinarySet& binary_set,
                                       const Config& config) {
    auto stat = index_.Deserialize(binary_set, config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to Deserialize index: {}",
                  KnowhereStatusString(stat));
    SetDim(index_.Dim());
}

template <typename T>
void
VectorMemIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
void
VectorMemIndex<T>::LoadV2(const Config& config) {
    if (config.contains(kMmapFilepath)) {
        return LoadFromFileV2(config);
    }

    auto blobs = space_->StatisticsBlobs();
    std::unordered_set<std::string> pending_index_files;
    auto index_prefix = file_manager_->GetRemoteIndexObjectPrefixV2();
    for (auto& blob : blobs) {
        if (blob.name.rfind(index_prefix, 0) == 0) {
            pending_index_files.insert(blob.name);
        }
    }

    auto slice_meta_file = index_prefix + "/" + INDEX_FILE_SLICE_META;
    auto res = space_->GetBlobByteSize(std::string(slice_meta_file));
    std::map<std::string, FieldDataPtr> index_datas{};

    if (!res.ok() && !res.status().IsFileNotFound()) {
        PanicInfo(DataFormatBroken, "failed to read blob");
    }
    bool slice_meta_exist = res.ok();

    auto read_blob = [&](const std::string& file_name)
        -> std::unique_ptr<storage::DataCodec> {
        auto res = space_->GetBlobByteSize(file_name);
        if (!res.ok()) {
            PanicInfo(DataFormatBroken, "unable to read index blob");
        }
        auto index_blob_data =
            std::shared_ptr<uint8_t[]>(new uint8_t[res.value()]);
        auto status = space_->ReadBlob(file_name, index_blob_data.get());
        if (!status.ok()) {
            PanicInfo(DataFormatBroken, "unable to read index blob");
        }
        return storage::DeserializeFileData(index_blob_data, res.value());
    };
    if (slice_meta_exist) {
        pending_index_files.erase(slice_meta_file);
        auto slice_meta_sz = res.value();
        auto slice_meta_data =
            std::shared_ptr<uint8_t[]>(new uint8_t[slice_meta_sz]);
        auto status = space_->ReadBlob(slice_meta_file, slice_meta_data.get());
        if (!status.ok()) {
            PanicInfo(DataFormatBroken, "unable to read slice meta");
        }
        auto raw_slice_meta =
            storage::DeserializeFileData(slice_meta_data, slice_meta_sz);
        Config meta_data = Config::parse(std::string(
            static_cast<const char*>(raw_slice_meta->GetFieldData()->Data()),
            raw_slice_meta->GetFieldData()->Size()));
        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);

            auto new_field_data =
                milvus::storage::CreateFieldData(DataType::INT8, 1, total_len);
            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name =
                    index_prefix + "/" + GenSlicedFileName(prefix, i);
                auto raw_index_blob = read_blob(file_name);
                new_field_data->FillFieldData(
                    raw_index_blob->GetFieldData()->Data(),
                    raw_index_blob->GetFieldData()->Size());
                pending_index_files.erase(file_name);
            }
            AssertInfo(
                new_field_data->IsFull(),
                "index len is inconsistent after disassemble and assemble");
            index_datas[prefix] = new_field_data;
        }
    }

    if (!pending_index_files.empty()) {
        for (auto& file_name : pending_index_files) {
            auto raw_index_blob = read_blob(file_name);
            index_datas.insert({file_name, raw_index_blob->GetFieldData()});
        }
    }
    LOG_INFO("construct binary set...");
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        LOG_INFO("add index data to binary set: {}", key);
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        auto file_name = key.substr(key.find_last_of('/') + 1);
        binary_set.Append(file_name, buf, size);
    }

    LOG_INFO("load index into Knowhere...");
    LoadWithoutAssemble(binary_set, config);
    LOG_INFO("load vector index done");
}

template <typename T>
void
VectorMemIndex<T>::Load(milvus::tracer::TraceContext ctx,
                        const Config& config) {
    if (config.contains(kMmapFilepath)) {
        return LoadFromFile(config);
    }

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");

    std::unordered_set<std::string> pending_index_files(index_files->begin(),
                                                        index_files->end());

    LOG_INFO("load index files: {}", index_files.value().size());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::map<std::string, FieldDataPtr> index_datas{};

    // try to read slice meta first
    std::string slice_meta_filepath;
    for (auto& file : pending_index_files) {
        auto file_name = file.substr(file.find_last_of('/') + 1);
        if (file_name == INDEX_FILE_SLICE_META) {
            slice_meta_filepath = file;
            pending_index_files.erase(file);
            break;
        }
    }

    // start read file span with active scope
    {
        auto read_file_span =
            milvus::tracer::StartSpan("SegCoreReadIndexFile", &ctx);
        auto read_scope =
            milvus::tracer::GetTracer()->WithActiveSpan(read_file_span);
        LOG_INFO("load with slice meta: {}", !slice_meta_filepath.empty());

        if (!slice_meta_filepath
                 .empty()) {  // load with the slice meta info, then we can load batch by batch
            std::string index_file_prefix = slice_meta_filepath.substr(
                0, slice_meta_filepath.find_last_of('/') + 1);

            auto result =
                file_manager_->LoadIndexToMemory({slice_meta_filepath});
            auto raw_slice_meta = result[INDEX_FILE_SLICE_META];
            Config meta_data = Config::parse(
                std::string(static_cast<const char*>(raw_slice_meta->Data()),
                            raw_slice_meta->Size()));

            for (auto& item : meta_data[META]) {
                std::string prefix = item[NAME];
                int slice_num = item[SLICE_NUM];
                auto total_len = static_cast<size_t>(item[TOTAL_LEN]);

                auto new_field_data = milvus::storage::CreateFieldData(
                    DataType::INT8, 1, total_len);

                std::vector<std::string> batch;
                batch.reserve(slice_num);
                for (auto i = 0; i < slice_num; ++i) {
                    std::string file_name = GenSlicedFileName(prefix, i);
                    batch.push_back(index_file_prefix + file_name);
                }

                auto batch_data = file_manager_->LoadIndexToMemory(batch);
                for (const auto& file_path : batch) {
                    const std::string file_name =
                        file_path.substr(file_path.find_last_of('/') + 1);
                    AssertInfo(batch_data.find(file_name) != batch_data.end(),
                               "lost index slice data: {}",
                               file_name);
                    auto data = batch_data[file_name];
                    new_field_data->FillFieldData(data->Data(), data->Size());
                }
                for (auto& file : batch) {
                    pending_index_files.erase(file);
                }

                AssertInfo(
                    new_field_data->IsFull(),
                    "index len is inconsistent after disassemble and assemble");
                index_datas[prefix] = new_field_data;
            }
        }

        if (!pending_index_files.empty()) {
            auto result =
                file_manager_->LoadIndexToMemory(std::vector<std::string>(
                    pending_index_files.begin(), pending_index_files.end()));
            for (auto&& index_data : result) {
                index_datas.insert(std::move(index_data));
            }
        }

        read_file_span->End();
    }

    LOG_INFO("construct binary set...");
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        LOG_INFO("add index data to binary set: {}", key);
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        binary_set.Append(key, buf, size);
    }

    // start engine load index span
    auto span_load_engine =
        milvus::tracer::StartSpan("SegCoreEngineLoadIndex", &ctx);
    auto engine_scope =
        milvus::tracer::GetTracer()->WithActiveSpan(span_load_engine);
    LOG_INFO("load index into Knowhere...");
    LoadWithoutAssemble(binary_set, config);
    span_load_engine->End();
    LOG_INFO("load vector index done");
}

template <typename T>
void
VectorMemIndex<T>::BuildWithDataset(const DatasetPtr& dataset,
                                    const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    SetDim(dataset->GetDim());

    knowhere::TimeRecorder rc("BuildWithoutIds", 1);
    auto stat = index_.Build(*dataset, index_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to build index, " + KnowhereStatusString(stat));
    rc.ElapseFromBegin("Done");
    SetDim(index_.Dim());
}

template <typename T>
void
VectorMemIndex<T>::BuildV2(const Config& config) {
    auto field_name = create_index_info_.field_name;
    auto field_type = create_index_info_.field_type;
    auto dim = create_index_info_.dim;
    auto res = space_->ScanData();
    if (!res.ok()) {
        PanicInfo(IndexBuildError,
                  "failed to create scan iterator: {}",
                  res.status().ToString());
    }

    auto reader = res.value();
    std::vector<FieldDataPtr> field_datas;
    for (auto rec : *reader) {
        if (!rec.ok()) {
            PanicInfo(IndexBuildError,
                      "failed to read data: {}",
                      rec.status().ToString());
        }
        auto data = rec.ValueUnsafe();
        if (data == nullptr) {
            break;
        }
        auto total_num_rows = data->num_rows();
        auto col_data = data->GetColumnByName(field_name);
        auto field_data =
            storage::CreateFieldData(field_type, dim, total_num_rows);
        field_data->FillFieldData(col_data);
        field_datas.push_back(field_data);
    }
    int64_t total_size = 0;
    int64_t total_num_rows = 0;
    for (const auto& data : field_datas) {
        total_size += data->Size();
        total_num_rows += data->get_num_rows();
        AssertInfo(dim == 0 || dim == data->get_dim(),
                   "inconsistent dim value between field datas!");
    }

    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[total_size]);
    int64_t offset = 0;
    for (auto data : field_datas) {
        std::memcpy(buf.get() + offset, data->Data(), data->Size());
        offset += data->Size();
        data.reset();
    }
    field_datas.clear();

    Config build_config;
    build_config.update(config);
    build_config.erase("insert_files");

    auto dataset = GenDataset(total_num_rows, dim, buf.get());
    BuildWithDataset(dataset, build_config);
}

template <typename T>
void
VectorMemIndex<T>::Build(const Config& config) {
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when building in memory index");
    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());

    Config build_config;
    build_config.update(config);
    build_config.erase("insert_files");
    build_config.erase(VEC_OPT_FIELDS);
    if (GetIndexType().find("SPARSE") == std::string::npos) {
        int64_t total_size = 0;
        int64_t total_num_rows = 0;
        int64_t dim = 0;
        for (auto data : field_datas) {
            total_size += data->Size();
            total_num_rows += data->get_num_rows();
            AssertInfo(dim == 0 || dim == data->get_dim(),
                       "inconsistent dim value between field datas!");
            dim = data->get_dim();
        }

        auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[total_size]);
        int64_t offset = 0;
        for (auto data : field_datas) {
            std::memcpy(buf.get() + offset, data->Data(), data->Size());
            offset += data->Size();
            data.reset();
        }
        field_datas.clear();

        auto dataset = GenDataset(total_num_rows, dim, buf.get());
        BuildWithDataset(dataset, build_config);
    } else {
        // sparse
        int64_t total_rows = 0;
        int64_t dim = 0;
        for (auto field_data : field_datas) {
            total_rows += field_data->Length();
            dim = std::max(
                dim,
                std::dynamic_pointer_cast<FieldData<SparseFloatVector>>(
                    field_data)
                    ->Dim());
        }
        std::vector<knowhere::sparse::SparseRow<float>> vec(total_rows);
        int64_t offset = 0;
        for (auto field_data : field_datas) {
            auto ptr = static_cast<const knowhere::sparse::SparseRow<float>*>(
                field_data->Data());
            AssertInfo(ptr, "failed to cast field data to sparse rows");
            for (size_t i = 0; i < field_data->Length(); ++i) {
                // TODO(SPARSE): do not copy, add a method to force field_data
                // to give up ownership.
                vec[offset + i] = ptr[i];
            }
            offset += field_data->Length();
        }
        auto dataset = GenDataset(total_rows, dim, vec.data());
        dataset->SetIsSparse(true);
        BuildWithDataset(dataset, build_config);
    }
}

template <typename T>
void
VectorMemIndex<T>::AddWithDataset(const DatasetPtr& dataset,
                                  const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    knowhere::TimeRecorder rc("AddWithDataset", 1);
    auto stat = index_.Add(*dataset, index_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to append index, " + KnowhereStatusString(stat));
    rc.ElapseFromBegin("Done");
}

template <typename T>
std::unique_ptr<SearchResult>
VectorMemIndex<T>::Query(const DatasetPtr dataset,
                         const SearchInfo& search_info,
                         const BitsetView& bitset) {
    //    AssertInfo(GetMetricType() == search_info.metric_type_,
    //               "Metric type of field index isn't the same with search info");

    auto num_queries = dataset->GetRows();
    knowhere::Json search_conf = search_info.search_params_;
    if (search_info.group_by_field_id_.has_value()) {
        auto result = std::make_unique<SearchResult>();
        if (search_conf.contains(knowhere::indexparam::EF)) {
            search_conf[knowhere::indexparam::SEED_EF] =
                search_conf[knowhere::indexparam::EF];
        }
        try {
            knowhere::expected<
                std::vector<std::shared_ptr<knowhere::IndexNode::iterator>>>
                iterators_val =
                    index_.AnnIterator(*dataset, search_conf, bitset);
            if (iterators_val.has_value()) {
                result->iterators = iterators_val.value();
            } else {
                LOG_ERROR(
                    "Returned knowhere iterator has non-ready iterators "
                    "inside, terminate group_by operation");
                PanicInfo(ErrorCode::Unsupported,
                          "Returned knowhere iterator has non-ready iterators "
                          "inside, terminate group_by operation");
            }
        } catch (const std::runtime_error& e) {
            LOG_ERROR(
                "Caught error:{} when trying to initialize ann iterators for "
                "group_by: "
                "group_by operation will be terminated",
                e.what());
            throw e;
        }
        return result;
        //if the target index doesn't support iterators, directly return empty search result
        //and the reduce process to filter empty results
    }
    auto topk = search_info.topk_;
    // TODO :: check dim of search data
    auto final = [&] {
        search_conf[knowhere::meta::TOPK] = topk;
        search_conf[knowhere::meta::METRIC_TYPE] = GetMetricType();
        auto index_type = GetIndexType();
        if (CheckKeyInConfig(search_conf, RADIUS)) {
            if (CheckKeyInConfig(search_conf, RANGE_FILTER)) {
                CheckRangeSearchParam(search_conf[RADIUS],
                                      search_conf[RANGE_FILTER],
                                      GetMetricType());
            }
            milvus::tracer::AddEvent("start_knowhere_index_range_search");
            auto res = index_.RangeSearch(*dataset, search_conf, bitset);
            milvus::tracer::AddEvent("finish_knowhere_index_range_search");
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          "failed to range search: {}: {}",
                          KnowhereStatusString(res.error()),
                          res.what());
            }
            auto result = ReGenRangeSearchResult(
                res.value(), topk, num_queries, GetMetricType());
            milvus::tracer::AddEvent("finish_ReGenRangeSearchResult");
            return result;
        } else {
            milvus::tracer::AddEvent("start_knowhere_index_search");
            auto res = index_.Search(*dataset, search_conf, bitset);
            milvus::tracer::AddEvent("finish_knowhere_index_search");
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          "failed to search: {}: {}",
                          KnowhereStatusString(res.error()),
                          res.what());
            }
            return res.value();
        }
    }();

    auto ids = final->GetIds();
    float* distances = const_cast<float*>(final->GetDistance());
    final->SetIsOwner(true);
    auto round_decimal = search_info.round_decimal_;
    auto total_num = num_queries * topk;

    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = std::round(distances[i] * multiplier) / multiplier;
        }
    }
    auto result = std::make_unique<SearchResult>();
    result->seg_offsets_.resize(total_num);
    result->distances_.resize(total_num);
    result->total_nq_ = num_queries;
    result->unity_topK_ = topk;

    std::copy_n(ids, total_num, result->seg_offsets_.data());
    std::copy_n(distances, total_num, result->distances_.data());

    return result;
}

template <typename T>
const bool
VectorMemIndex<T>::HasRawData() const {
    return index_.HasRawData(GetMetricType());
}

template <typename T>
std::vector<uint8_t>
VectorMemIndex<T>::GetVector(const DatasetPtr dataset) const {
    auto res = index_.GetVectorByIds(*dataset);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, " + KnowhereStatusString(res.error()));
    }
    auto index_type = GetIndexType();
    auto tensor = res.value()->GetTensor();
    auto row_num = res.value()->GetRows();
    auto dim = res.value()->GetDim();
    int64_t data_size;
    if (is_in_bin_list(index_type)) {
        data_size = dim / 8 * row_num;
    } else {
        data_size = dim * row_num * sizeof(float);
    }
    std::vector<uint8_t> raw_data;
    raw_data.resize(data_size);
    memcpy(raw_data.data(), tensor, data_size);
    return raw_data;
}

template <typename T>
void
VectorMemIndex<T>::LoadFromFile(const Config& config) {
    auto filepath = GetValueFromConfig<std::string>(config, kMmapFilepath);
    AssertInfo(filepath.has_value(), "mmap filepath is empty when load index");

    std::filesystem::create_directories(
        std::filesystem::path(filepath.value()).parent_path());

    auto file = File::Open(filepath.value(), O_CREAT | O_TRUNC | O_RDWR);

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");

    std::unordered_set<std::string> pending_index_files(index_files->begin(),
                                                        index_files->end());

    LOG_INFO("load index files: {}", index_files.value().size());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    // try to read slice meta first
    std::string slice_meta_filepath;
    for (auto& file : pending_index_files) {
        auto file_name = file.substr(file.find_last_of('/') + 1);
        if (file_name == INDEX_FILE_SLICE_META) {
            slice_meta_filepath = file;
            pending_index_files.erase(file);
            break;
        }
    }

    LOG_INFO("load with slice meta: {}", !slice_meta_filepath.empty());

    if (!slice_meta_filepath
             .empty()) {  // load with the slice meta info, then we can load batch by batch
        std::string index_file_prefix = slice_meta_filepath.substr(
            0, slice_meta_filepath.find_last_of('/') + 1);
        std::vector<std::string> batch{};
        batch.reserve(parallel_degree);

        auto result = file_manager_->LoadIndexToMemory({slice_meta_filepath});
        auto raw_slice_meta = result[INDEX_FILE_SLICE_META];
        Config meta_data = Config::parse(
            std::string(static_cast<const char*>(raw_slice_meta->Data()),
                        raw_slice_meta->Size()));

        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);

            auto HandleBatch = [&](int index) {
                auto batch_data = file_manager_->LoadIndexToMemory(batch);
                for (int j = index - batch.size() + 1; j <= index; j++) {
                    std::string file_name = GenSlicedFileName(prefix, j);
                    AssertInfo(batch_data.find(file_name) != batch_data.end(),
                               "lost index slice data");
                    auto data = batch_data[file_name];
                    auto written = file.Write(data->Data(), data->Size());
                    AssertInfo(
                        written == data->Size(),
                        fmt::format("failed to write index data to disk {}: {}",
                                    filepath->data(),
                                    strerror(errno)));
                }
                for (auto& file : batch) {
                    pending_index_files.erase(file);
                }
                batch.clear();
            };

            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name = GenSlicedFileName(prefix, i);
                batch.push_back(index_file_prefix + file_name);
                if (batch.size() >= parallel_degree) {
                    HandleBatch(i);
                }
            }
            if (batch.size() > 0) {
                HandleBatch(slice_num - 1);
            }
        }
    } else {
        auto result = file_manager_->LoadIndexToMemory(std::vector<std::string>(
            pending_index_files.begin(), pending_index_files.end()));
        for (auto& [_, index_data] : result) {
            file.Write(index_data->Data(), index_data->Size());
        }
    }
    file.Close();

    LOG_INFO("load index into Knowhere...");
    auto conf = config;
    conf.erase(kMmapFilepath);
    conf[kEnableMmap] = true;
    auto stat = index_.DeserializeFromFile(filepath.value(), conf);
    if (stat != knowhere::Status::success) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to Deserialize index: {}",
                  KnowhereStatusString(stat));
    }

    auto dim = index_.Dim();
    this->SetDim(index_.Dim());

    auto ok = unlink(filepath->data());
    AssertInfo(ok == 0,
               "failed to unlink mmap index file {}: {}",
               filepath.value(),
               strerror(errno));
    LOG_INFO("load vector index done");
}

template <typename T>
void
VectorMemIndex<T>::LoadFromFileV2(const Config& config) {
    auto filepath = GetValueFromConfig<std::string>(config, kMmapFilepath);
    AssertInfo(filepath.has_value(), "mmap filepath is empty when load index");

    std::filesystem::create_directories(
        std::filesystem::path(filepath.value()).parent_path());

    auto file = File::Open(filepath.value(), O_CREAT | O_TRUNC | O_RDWR);

    auto blobs = space_->StatisticsBlobs();
    std::unordered_set<std::string> pending_index_files;
    auto index_prefix = file_manager_->GetRemoteIndexObjectPrefixV2();
    for (auto& blob : blobs) {
        if (blob.name.rfind(index_prefix, 0) == 0) {
            pending_index_files.insert(blob.name);
        }
    }

    auto slice_meta_file = index_prefix + "/" + INDEX_FILE_SLICE_META;
    auto res = space_->GetBlobByteSize(std::string(slice_meta_file));

    if (!res.ok() && !res.status().IsFileNotFound()) {
        PanicInfo(DataFormatBroken, "failed to read blob");
    }
    bool slice_meta_exist = res.ok();

    auto read_blob = [&](const std::string& file_name)
        -> std::unique_ptr<storage::DataCodec> {
        auto res = space_->GetBlobByteSize(file_name);
        if (!res.ok()) {
            PanicInfo(DataFormatBroken, "unable to read index blob");
        }
        auto index_blob_data =
            std::shared_ptr<uint8_t[]>(new uint8_t[res.value()]);
        auto status = space_->ReadBlob(file_name, index_blob_data.get());
        if (!status.ok()) {
            PanicInfo(DataFormatBroken, "unable to read index blob");
        }
        return storage::DeserializeFileData(index_blob_data, res.value());
    };
    if (slice_meta_exist) {
        pending_index_files.erase(slice_meta_file);
        auto slice_meta_sz = res.value();
        auto slice_meta_data =
            std::shared_ptr<uint8_t[]>(new uint8_t[slice_meta_sz]);
        auto status = space_->ReadBlob(slice_meta_file, slice_meta_data.get());
        if (!status.ok()) {
            PanicInfo(DataFormatBroken, "unable to read slice meta");
        }
        auto raw_slice_meta =
            storage::DeserializeFileData(slice_meta_data, slice_meta_sz);
        Config meta_data = Config::parse(std::string(
            static_cast<const char*>(raw_slice_meta->GetFieldData()->Data()),
            raw_slice_meta->GetFieldData()->Size()));
        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);

            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name =
                    index_prefix + "/" + GenSlicedFileName(prefix, i);
                auto raw_index_blob = read_blob(file_name);
                auto written =
                    file.Write(raw_index_blob->GetFieldData()->Data(),
                               raw_index_blob->GetFieldData()->Size());
                pending_index_files.erase(file_name);
            }
        }
    }

    if (!pending_index_files.empty()) {
        for (auto& file_name : pending_index_files) {
            auto raw_index_blob = read_blob(file_name);
            file.Write(raw_index_blob->GetFieldData()->Data(),
                       raw_index_blob->GetFieldData()->Size());
        }
    }
    file.Close();

    LOG_INFO("load index into Knowhere...");
    auto conf = config;
    conf.erase(kMmapFilepath);
    conf[kEnableMmap] = true;
    auto stat = index_.DeserializeFromFile(filepath.value(), conf);
    if (stat != knowhere::Status::success) {
        PanicInfo(DataFormatBroken,
                  "failed to Deserialize index: {}",
                  KnowhereStatusString(stat));
    }

    auto dim = index_.Dim();
    this->SetDim(index_.Dim());

    auto ok = unlink(filepath->data());
    AssertInfo(ok == 0,
               "failed to unlink mmap index file {}: {}",
               filepath.value(),
               strerror(errno));
    LOG_INFO("load vector index done");
}
template class VectorMemIndex<float>;
template class VectorMemIndex<uint8_t>;
template class VectorMemIndex<float16>;
template class VectorMemIndex<bfloat16>;

}  // namespace milvus::index
