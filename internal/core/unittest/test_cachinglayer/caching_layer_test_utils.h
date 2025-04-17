#pragma once

#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"

namespace milvus {

using namespace cachinglayer;

class TestChunkTranslator : public Translator<milvus::Chunk> {
 public:
    TestChunkTranslator(std::vector<int64_t> num_rows_per_chunk,
                        std::string key,
                        std::vector<std::unique_ptr<Chunk>>&& chunks)
        : Translator<milvus::Chunk>(),
          num_cells_(num_rows_per_chunk.size()),
          chunks_(std::move(chunks)) {
        meta_.num_rows_until_chunk_.reserve(num_cells_ + 1);
        meta_.num_rows_until_chunk_.push_back(0);
        int total_rows = 0;
        for (int i = 0; i < num_cells_; ++i) {
            meta_.num_rows_until_chunk_.push_back(
                meta_.num_rows_until_chunk_[i] + num_rows_per_chunk[i]);
            total_rows += num_rows_per_chunk[i];
        }
        key_ = key;
    }
    ~TestChunkTranslator() override {
    }

    size_t
    num_cells() const override {
        return num_cells_;
    }

    cid_t
    cell_id_of(uid_t uid) const override {
        return uid;
    }

    ResourceUsage
    estimated_byte_size_of_cell(cid_t cid) const override {
        return {0, 0};
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::Chunk>>>
    get_cells(const std::vector<cid_t>& cids) override {
        std::vector<std::pair<cid_t, std::unique_ptr<milvus::Chunk>>> res;
        res.reserve(cids.size());
        for (auto cid : cids) {
            AssertInfo(cid < chunks_.size() && chunks_[cid] != nullptr,
                       "TestChunkTranslator assumes no eviction.");
            res.emplace_back(cid, std::move(chunks_[cid]));
        }
        return res;
    }

 private:
    size_t num_cells_;
    segcore::storagev1translator::CTMeta meta_;
    std::string key_;
    std::vector<std::unique_ptr<Chunk>> chunks_;
};

class TestGroupChunkTranslator : public Translator<milvus::GroupChunk> {
 public:
    TestGroupChunkTranslator(std::vector<int64_t> num_rows_per_chunk,
                           std::string key,
                           std::vector<std::unique_ptr<GroupChunk>>&& chunks)
        : Translator<milvus::GroupChunk>(),
          num_cells_(num_rows_per_chunk.size()),
          chunks_(std::move(chunks)) {
        meta_.num_rows_until_chunk_.reserve(num_cells_ + 1);
        for (auto& chunk : chunks_) {
            for (auto& [field_id, chunk] : chunk->GetChunks()) {
                meta_.num_rows_until_chunk_[field_id] = num_rows_per_chunk;
            }
        }
        key_ = key;
    }
    ~TestGroupChunkTranslator() override {
    }

    size_t
    num_cells() const override {
        return num_cells_;
    }

    cid_t
    cell_id_of(uid_t uid) const override {
        return uid;
    }

    ResourceUsage
    estimated_byte_size_of_cell(cid_t cid) const override {
        return {0, 0};
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::GroupChunk>>>
    get_cells(const std::vector<cid_t>& cids) override {
        std::vector<std::pair<cid_t, std::unique_ptr<milvus::GroupChunk>>> res;
        res.reserve(cids.size());
        for (auto cid : cids) {
            AssertInfo(cid < chunks_.size() && chunks_[cid] != nullptr,
                      "TestGroupChunkTranslator assumes no eviction.");
            res.emplace_back(cid, std::move(chunks_[cid]));
        }
        return res;
    }

 private:
    size_t num_cells_;
    std::vector<std::unique_ptr<GroupChunk>> chunks_;
    std::string key_;
    segcore::storagev2translator::GroupCTMeta meta_;
};

}  // namespace milvus
