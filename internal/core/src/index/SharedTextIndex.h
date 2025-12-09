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

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/FieldMeta.h"
#include "common/Types.h"

namespace milvus::index {

class SharedTextIndexWriter;
class SharedTextIndexReader;

// Interface for text match operations
// This allows both TextMatchIndex and SharedTextIndexView to be used polymorphically
class ITextMatchable {
 public:
    virtual ~ITextMatchable() = default;

    virtual TargetBitmap
    MatchQuery(const std::string& query, uint32_t min_should_match) = 0;

    virtual TargetBitmap
    PhraseMatchQuery(const std::string& query, uint32_t slop) = 0;

    virtual TargetBitmap
    IsNotNull() = 0;
};

// Key for identifying a shared index instance
// Shared indexes are grouped by analyzer_params_hash only,
// allowing cross-collection sharing for fields with the same analyzer config
struct SharedIndexKey {
    size_t analyzer_params_hash;

    bool
    operator==(const SharedIndexKey& other) const {
        return analyzer_params_hash == other.analyzer_params_hash;
    }
};

struct SharedIndexKeyHash {
    size_t
    operator()(const SharedIndexKey& key) const {
        return key.analyzer_params_hash;
    }
};

// Wrapper for tantivy shared text index writer
class SharedTextIndexWriter {
 public:
    SharedTextIndexWriter(const std::string& field_name,
                          const std::string& tokenizer_name,
                          const std::string& analyzer_params,
                          size_t num_threads = 1,
                          size_t memory_budget = 50 * 1024 * 1024);

    ~SharedTextIndexWriter();

    // Add a single text document
    void
    AddText(int64_t segment_id, int64_t local_doc_id, const std::string& text);

    // Add multiple texts in batch
    void
    AddTexts(int64_t segment_id,
             const std::string* texts,
             size_t num_texts,
             int64_t offset_begin);

    // Delete all documents belonging to a segment
    void
    DeleteSegment(int64_t segment_id);

    // Commit changes
    void
    Commit();

    // Create a reader
    std::shared_ptr<SharedTextIndexReader>
    CreateReader();

    // Register tokenizer
    void
    RegisterTokenizer(const std::string& tokenizer_name,
                      const std::string& analyzer_params);

 private:
    void* writer_;  // tantivy SharedTextIndexWriter pointer
    mutable std::mutex commit_mutex_;
    std::atomic<std::chrono::high_resolution_clock::time_point>
        last_commit_time_;
};

// Wrapper for tantivy shared text index reader
class SharedTextIndexReader {
 public:
    explicit SharedTextIndexReader(void* reader);
    ~SharedTextIndexReader();

    // Reload to see latest changes
    void
    Reload();

    // Match query filtered by segment_id
    std::vector<int64_t>
    MatchQuery(int64_t segment_id, const std::string& query);

    // Match query with minimum should match
    std::vector<int64_t>
    MatchQueryWithMinimum(int64_t segment_id,
                          const std::string& query,
                          uint32_t min_should_match);

    // Phrase match query
    std::vector<int64_t>
    PhraseMatchQuery(int64_t segment_id,
                     const std::string& query,
                     uint32_t slop);

    // Register tokenizer
    void
    RegisterTokenizer(const std::string& tokenizer_name,
                      const std::string& analyzer_params);

 private:
    void* reader_;  // tantivy SharedTextIndexReader pointer
};

// Shared index instance that manages writer and reader
class SharedTextIndex {
 public:
    SharedTextIndex(const std::string& field_name,
                    const std::string& tokenizer_name,
                    const std::string& analyzer_params,
                    int64_t commit_interval_ms = 200);

    ~SharedTextIndex() = default;

    // Register a segment to use this shared index
    void
    RegisterSegment(int64_t segment_id);

    // Unregister a segment (cleanup when segment is sealed/dropped)
    void
    UnregisterSegment(int64_t segment_id);

    // Add texts for a segment
    void
    AddTexts(int64_t segment_id,
             const std::string* texts,
             const bool* valids,
             size_t n,
             int64_t offset_begin);

    // Match query for a specific segment
    TargetBitmap
    MatchQuery(int64_t segment_id,
               const std::string& query,
               uint32_t min_should_match,
               int64_t row_count);

    // Phrase match query for a specific segment
    TargetBitmap
    PhraseMatchQuery(int64_t segment_id,
                     const std::string& query,
                     uint32_t slop,
                     int64_t row_count);

    // Commit if needed
    void
    TryCommit();

    // Force commit
    void
    Commit();

    // Reload reader
    void
    Reload();

    // Get number of registered segments
    size_t
    GetSegmentCount() const;

 private:
    bool
    ShouldCommit() const;

    std::shared_ptr<SharedTextIndexReader>
    GetOrCreateReader();

 private:
    std::unique_ptr<SharedTextIndexWriter> writer_;
    std::shared_ptr<SharedTextIndexReader> reader_;
    mutable std::shared_mutex mutex_;

    std::unordered_set<int64_t> registered_segments_;
    std::string tokenizer_name_;
    std::string analyzer_params_;

    std::atomic<std::chrono::high_resolution_clock::time_point>
        last_commit_time_;
    int64_t commit_interval_ms_;
};

// View of SharedTextIndex bound to a specific segment
// This allows SharedTextIndex to be used like TextMatchIndex in query path
class SharedTextIndexView : public ITextMatchable {
 public:
    SharedTextIndexView(std::shared_ptr<SharedTextIndex> index,
                        int64_t segment_id,
                        int64_t row_count)
        : index_(std::move(index)),
          segment_id_(segment_id),
          row_count_(row_count) {
    }

    TargetBitmap
    MatchQuery(const std::string& query, uint32_t min_should_match) override {
        return index_->MatchQuery(
            segment_id_, query, min_should_match, row_count_);
    }

    TargetBitmap
    PhraseMatchQuery(const std::string& query, uint32_t slop) override {
        return index_->PhraseMatchQuery(segment_id_, query, slop, row_count_);
    }

    TargetBitmap
    IsNotNull() override {
        // For shared index, we don't track null separately per segment
        // Return all true (all valid) as nulls are handled during AddTexts
        TargetBitmap result(row_count_);
        result.set();
        return result;
    }

    void
    SetRowCount(int64_t row_count) {
        row_count_ = row_count;
    }

 private:
    std::shared_ptr<SharedTextIndex> index_;
    int64_t segment_id_;
    int64_t row_count_;
};

// Global manager for shared text indexes
class SharedTextIndexManager {
 public:
    static SharedTextIndexManager&
    Instance();

    // Get or create a shared index for the given key
    std::shared_ptr<SharedTextIndex>
    GetOrCreate(const SharedIndexKey& key,
                const std::string& field_name,
                const std::string& tokenizer_name,
                const std::string& analyzer_params);

    // Release a shared index when no segments are using it
    void
    TryRelease(const SharedIndexKey& key);

    // Get statistics
    size_t
    GetIndexCount() const;

 private:
    SharedTextIndexManager() = default;
    ~SharedTextIndexManager() = default;

    SharedTextIndexManager(const SharedTextIndexManager&) = delete;
    SharedTextIndexManager&
    operator=(const SharedTextIndexManager&) = delete;

 private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<SharedIndexKey,
                       std::shared_ptr<SharedTextIndex>,
                       SharedIndexKeyHash>
        indexes_;
};

}  // namespace milvus::index
