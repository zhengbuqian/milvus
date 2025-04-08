#pragma once

#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Chunk.h"
#include "mmap/Types.h"

namespace milvus::segcore::storagev1translator {

struct CTMeta : public milvus::cachinglayer::Meta {
    std::vector<int64_t> num_rows_until_chunk_;
};

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

    ~ChunkTranslator() override;

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
        const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    // TODO: info other than get_cels() should all be in meta()
    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

 private:

    int64_t segment_id_;
    std::string key_;
    milvus::cachinglayer::StorageType storage_type_;
    std::vector<milvus::Chunk*> chunks_;
    CTMeta meta_;
};

}  // namespace milvus::segcore::storagev1translator
