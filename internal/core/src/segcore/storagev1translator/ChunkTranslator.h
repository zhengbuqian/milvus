#pragma once

#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Chunk.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/Types.h"

namespace milvus::segcore::storagev1translator {

// This class will load all cells(Chunks) in ctor, and move them out during get_cells.
// This should be used only in storagev1(no eviction allowed), thus trying to get a
// same cell a second time will result in exception.
class ChunkTranslator : public milvus::cachinglayer::Translator<milvus::Chunk> {
 public:
    ChunkTranslator(int64_t segment_id,
                    FieldMeta field_meta,
                    FieldDataInfo field_data_info,
                    std::vector<std::string> insert_files,
                    milvus::cachinglayer::StorageType storage_type);

    size_t
    num_cells() const override;
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;
    milvus::cachinglayer::StorageType
    storage_type() const override;
    size_t
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;
    const std::string&
    key() const override;
    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
    get_cells(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override;

 private:
    void
    load_all_chunks_in_memory(FieldMeta& field_meta,
                              FieldDataInfo& field_data_info) const;

    void
    load_all_chunks_in_mmap(FieldMeta& field_meta,
                            FieldDataInfo& field_data_info) const;

    int64_t segment_id_;
    std::string key_;
    milvus::cachinglayer::StorageType storage_type_;
    std::vector<std::unique_ptr<milvus::Chunk>> chunks_;
};

}  // namespace milvus::segcore::storagev1translator
