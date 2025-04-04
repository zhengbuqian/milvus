#include "segcore/storagev1translator/ChunkTranslator.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/SystemProperty.h"
#include "segcore/Utils.h"
#include "storage/ThreadPools.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/Types.h"

namespace milvus::segcore::storagev1translator {

ChunkTranslator::ChunkTranslator(int64_t segment_id,
                                 FieldMeta field_meta,
                                 FieldDataInfo field_data_info,
                                 std::vector<std::string> insert_files,
                                 milvus::cachinglayer::StorageType storage_type)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_f_{}", segment_id, field_data_info.field_id)),
      storage_type_(storage_type) {
    AssertInfo(
        !SystemProperty::Instance().IsSystem(FieldId(field_data_info.field_id)),
        "ChunkTranslator not supported for system field");
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    // TODO(tiered storage 4): storagev2 should use executor to perform download.
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    pool.Submit(LoadArrowReaderFromRemote,
                insert_files,
                field_data_info.arrow_reader_channel);
    LOG_INFO("segment {} submits load field {} task to thread pool",
             segment_id_,
             field_data_info.field_id);

    storage_type_ == milvus::cachinglayer::StorageType::MEMORY
        ? load_all_chunks_in_memory(field_meta, field_data_info)
        : load_all_chunks_in_mmap(field_meta, field_data_info);
}

size_t
ChunkTranslator::num_cells() const {
    return chunks_.size();
}

milvus::cachinglayer::cid_t
ChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return 0;
}

milvus::cachinglayer::StorageType
ChunkTranslator::storage_type() const {
    return storage_type_;
}

size_t
ChunkTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    return 0;
}

const std::string&
ChunkTranslator::key() const {
    return key_;
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
ChunkTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) const {
    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
        cells;
    for (auto cid : cids) {
        AssertInfo(chunks_[cid] != nullptr,
                   "ChunkTranslator::get_cells called again on cell {} of "
                   "CacheSlot {}.",
                   cid,
                   key_);
        cells.emplace_back(cid, std::move(chunks_[cid]));
    }
    return cells;
}

void
ChunkTranslator::load_all_chunks_in_memory(
    FieldMeta& field_meta, FieldDataInfo& field_data_info) const {
    
    std::shared_ptr<milvus::ArrowDataWrapper> r;
    while (field_data_info.arrow_reader_channel->pop(r)) {
        auto chunk =
            create_chunk(field_meta,
                         IsVectorDataType(data_type) &&
                                 !IsSparseFloatVectorDataType(data_type)
                             ? field_meta.get_dim()
                             : 1,
                         r->reader);
        column->AddChunk(chunk);
    }
    AssertInfo(column->NumRows() == field_data_info.row_count,
               fmt::format("data lost while loading column {}: loaded "
                           "num rows {} but expected {}",
                           field_data_info.field_id,
                           column->NumRows(),
                           field_data_info.row_count));
    return column;
}

void
ChunkTranslator::load_all_chunks_in_mmap(FieldMeta& field_meta,
                                         FieldDataInfo& field_data_info) const {
    auto filepath = std::filesystem::path(field_data_info.mmap_dir_path) /
                    std::to_string(segment_id_) /
                    std::to_string(field_data_info.field_id);
    auto dir = filepath.parent_path();
    std::filesystem::create_directories(dir);

    auto file = File::Open(filepath.string(), O_CREAT | O_TRUNC | O_RDWR);

    auto data_type = field_meta.get_data_type();

    // write the field data to disk
    std::vector<uint64_t> indices{};
    std::vector<std::vector<uint64_t>> element_indices{};

    std::shared_ptr<milvus::ArrowDataWrapper> r;
    size_t file_offset = 0;
    std::vector<std::shared_ptr<Chunk>> chunks;
    while (field_data_info.arrow_reader_channel->pop(r)) {
        auto chunk =
            create_chunk(field_meta,
                         IsVectorDataType(data_type) &&
                                 !IsSparseFloatVectorDataType(data_type)
                             ? field_meta.get_dim()
                             : 1,
                         file,
                         file_offset,
                         r->reader);
        file_offset += chunk->Size();
        chunks.push_back(chunk);
    }
    std::unique_ptr<ChunkedColumnBase> column{};
    switch (data_type) {
        case milvus::DataType::STRING:
        case milvus::DataType::VARCHAR:
        case milvus::DataType::TEXT: {
            column = std::make_unique<ChunkedVariableColumn<std::string>>(
                field_meta, chunks);
            break;
        }
        case milvus::DataType::JSON: {
            column = std::make_unique<ChunkedVariableColumn<milvus::Json>>(
                field_meta, chunks);
            break;
        }
        case milvus::DataType::ARRAY: {
            column = std::make_unique<ChunkedArrayColumn>(field_meta, chunks);
            break;
        }
        case milvus::DataType::VECTOR_SPARSE_FLOAT: {
            column =
                std::make_unique<ChunkedSparseFloatColumn>(field_meta, chunks);
            break;
        }
        default: {
            column = std::make_unique<ChunkedColumn>(field_meta, chunks);
            break;
        }
    }
    auto ok = unlink(filepath.c_str());
    AssertInfo(ok == 0,
               fmt::format("failed to unlink mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));
    return column;
}

}  // namespace milvus::segcore::storagev1translator
