#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"

namespace milvus::cachinglayer::test {

// Mock cell class for testing
class MockCell {
 public:
    explicit MockCell(size_t size) : size_(size) {}
    size_t DataByteSize() const { return size_; }
    size_t size() const { return size_; }

 private:
    size_t size_;
};

// Mock translator for testing
class MockTranslator : public Translator<MockCell> {
 public:
    MockTranslator(size_t num_cells, const std::string& key)
        : num_cells_(num_cells), key_(key) {
        // Initialize cell sizes
        for (cid_t i = 0; i < num_cells_; ++i) {
            cell_sizes_[i] = 100;  // Each cell is 100 bytes
        }
    }

    size_t num_cells() const override { return num_cells_; }

    cid_t cell_id_of(uid_t uid) const override {
        return uid % num_cells_;  // Simple mapping for testing
    }

    StorageType storage_type() const override {
        return StorageType::MEMORY;
    }

    size_t estimated_byte_size_of_cell(cid_t cid) const override {
        return cell_sizes_.at(cid);
    }

    const std::string& key() const override { return key_; }

    std::vector<std::pair<cid_t, std::unique_ptr<MockCell>>> get_cells(
        const std::vector<cid_t>& cids) const override {
        std::vector<std::pair<cid_t, std::unique_ptr<MockCell>>> results;
        for (auto cid : cids) {
            results.emplace_back(cid, std::make_unique<MockCell>(cell_sizes_.at(cid)));
        }
        return results;
    }

    // Helper method to set cell sizes for testing
    void set_cell_size(cid_t cid, size_t size) {
        cell_sizes_[cid] = size;
    }

 private:
    size_t num_cells_;
    std::string key_;
    std::unordered_map<cid_t, size_t> cell_sizes_;
};

}  // namespace milvus::cachinglayer::test
