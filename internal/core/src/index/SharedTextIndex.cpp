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

#include "index/SharedTextIndex.h"

#include <functional>

#include "common/EasyAssert.h"
#include "common/Consts.h"
#include "log/Log.h"
#include "tantivy-binding.h"

namespace milvus::index {

using stdclock = std::chrono::high_resolution_clock;

namespace {

void
CheckRustResult(const RustResult& result, const std::string& operation) {
    if (!result.success) {
        std::string error_msg = result.error ? result.error : "unknown error";
        free_rust_result(const_cast<RustResult&>(result));
        ThrowInfo(ErrorCode::UnexpectedError,
                  "SharedTextIndex {} failed: {}",
                  operation,
                  error_msg);
    }
}

}  // namespace

// ==================== SharedTextIndexWriter ====================

SharedTextIndexWriter::SharedTextIndexWriter(const std::string& field_name,
                                             const std::string& tokenizer_name,
                                             const std::string& analyzer_params,
                                             size_t num_threads,
                                             size_t memory_budget)
    : writer_(nullptr), last_commit_time_(stdclock::now()) {
    auto result = tantivy_create_shared_text_writer(field_name.c_str(),
                                                    tokenizer_name.c_str(),
                                                    analyzer_params.c_str(),
                                                    num_threads,
                                                    memory_budget);
    CheckRustResult(result, "create writer");
    writer_ = result.value.ptr._0;
}

SharedTextIndexWriter::~SharedTextIndexWriter() {
    if (writer_) {
        tantivy_free_shared_text_writer(writer_);
        writer_ = nullptr;
    }
}

void
SharedTextIndexWriter::AddText(int64_t segment_id,
                               int64_t local_doc_id,
                               const std::string& text) {
    auto result =
        tantivy_shared_text_writer_add_text(writer_,
                                            static_cast<uint64_t>(segment_id),
                                            static_cast<uint64_t>(local_doc_id),
                                            text.c_str());
    CheckRustResult(result, "add text");
    free_rust_result(result);
}

void
SharedTextIndexWriter::AddTexts(int64_t segment_id,
                                const std::string* texts,
                                size_t num_texts,
                                int64_t offset_begin) {
    std::vector<const char*> text_ptrs;
    text_ptrs.reserve(num_texts);
    for (size_t i = 0; i < num_texts; ++i) {
        text_ptrs.push_back(texts[i].c_str());
    }

    auto result = tantivy_shared_text_writer_add_texts(
        writer_,
        static_cast<uint64_t>(segment_id),
        text_ptrs.data(),
        num_texts,
        static_cast<uint64_t>(offset_begin));
    CheckRustResult(result, "add texts");
    free_rust_result(result);
}

void
SharedTextIndexWriter::DeleteSegment(int64_t segment_id) {
    auto result = tantivy_shared_text_writer_delete_segment(
        writer_, static_cast<uint64_t>(segment_id));
    CheckRustResult(result, "delete segment");
    free_rust_result(result);
}

void
SharedTextIndexWriter::Commit() {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    auto result = tantivy_shared_text_writer_commit(writer_);
    CheckRustResult(result, "commit");
    free_rust_result(result);
    last_commit_time_.store(stdclock::now());
}

std::shared_ptr<SharedTextIndexReader>
SharedTextIndexWriter::CreateReader() {
    auto result = tantivy_shared_text_writer_create_reader(writer_);
    CheckRustResult(result, "create reader");
    return std::make_shared<SharedTextIndexReader>(result.value.ptr._0);
}

void
SharedTextIndexWriter::RegisterTokenizer(const std::string& tokenizer_name,
                                         const std::string& analyzer_params) {
    auto result = tantivy_shared_text_writer_register_tokenizer(
        writer_, tokenizer_name.c_str(), analyzer_params.c_str());
    CheckRustResult(result, "register tokenizer on writer");
    free_rust_result(result);
}

// ==================== SharedTextIndexReader ====================

SharedTextIndexReader::SharedTextIndexReader(void* reader) : reader_(reader) {
    AssertInfo(reader_ != nullptr, "reader pointer is null");
}

SharedTextIndexReader::~SharedTextIndexReader() {
    if (reader_) {
        tantivy_free_shared_text_reader(reader_);
        reader_ = nullptr;
    }
}

void
SharedTextIndexReader::Reload() {
    auto result = tantivy_shared_text_reader_reload(reader_);
    CheckRustResult(result, "reload reader");
    free_rust_result(result);
}

std::vector<int64_t>
SharedTextIndexReader::MatchQuery(int64_t segment_id,
                                  const std::string& query) {
    size_t result_len = 0;
    auto result = tantivy_shared_text_reader_match_query(
        reader_, static_cast<uint64_t>(segment_id), query.c_str(), &result_len);
    CheckRustResult(result, "match query");

    std::vector<int64_t> results;
    if (result_len > 0 && result.value.ptr._0 != nullptr) {
        uint64_t* ptr = static_cast<uint64_t*>(result.value.ptr._0);
        results.reserve(result_len);
        for (size_t i = 0; i < result_len; ++i) {
            results.push_back(static_cast<int64_t>(ptr[i]));
        }
        tantivy_free_u64_array(ptr, result_len);
    }

    return results;
}

std::vector<int64_t>
SharedTextIndexReader::MatchQueryWithMinimum(int64_t segment_id,
                                             const std::string& query,
                                             uint32_t min_should_match) {
    size_t result_len = 0;
    auto result = tantivy_shared_text_reader_match_query_with_minimum(
        reader_,
        static_cast<uint64_t>(segment_id),
        query.c_str(),
        static_cast<size_t>(min_should_match),
        &result_len);
    CheckRustResult(result, "match query with minimum");

    std::vector<int64_t> results;
    if (result_len > 0 && result.value.ptr._0 != nullptr) {
        uint64_t* ptr = static_cast<uint64_t*>(result.value.ptr._0);
        results.reserve(result_len);
        for (size_t i = 0; i < result_len; ++i) {
            results.push_back(static_cast<int64_t>(ptr[i]));
        }
        tantivy_free_u64_array(ptr, result_len);
    }

    return results;
}

std::vector<int64_t>
SharedTextIndexReader::PhraseMatchQuery(int64_t segment_id,
                                        const std::string& query,
                                        uint32_t slop) {
    size_t result_len = 0;
    auto result = tantivy_shared_text_reader_phrase_match_query(
        reader_,
        static_cast<uint64_t>(segment_id),
        query.c_str(),
        slop,
        &result_len);
    CheckRustResult(result, "phrase match query");

    std::vector<int64_t> results;
    if (result_len > 0 && result.value.ptr._0 != nullptr) {
        uint64_t* ptr = static_cast<uint64_t*>(result.value.ptr._0);
        results.reserve(result_len);
        for (size_t i = 0; i < result_len; ++i) {
            results.push_back(static_cast<int64_t>(ptr[i]));
        }
        tantivy_free_u64_array(ptr, result_len);
    }

    return results;
}

void
SharedTextIndexReader::RegisterTokenizer(const std::string& tokenizer_name,
                                         const std::string& analyzer_params) {
    auto result = tantivy_shared_text_reader_register_tokenizer(
        reader_, tokenizer_name.c_str(), analyzer_params.c_str());
    CheckRustResult(result, "register tokenizer on reader");
    free_rust_result(result);
}

// ==================== SharedTextIndex ====================

SharedTextIndex::SharedTextIndex(const std::string& field_name,
                                 const std::string& tokenizer_name,
                                 const std::string& analyzer_params,
                                 int64_t commit_interval_ms)
    : tokenizer_name_(tokenizer_name),
      analyzer_params_(analyzer_params),
      last_commit_time_(stdclock::now()),
      commit_interval_ms_(commit_interval_ms) {
    writer_ = std::make_unique<SharedTextIndexWriter>(
        field_name, tokenizer_name, analyzer_params);
}

void
SharedTextIndex::RegisterSegment(int64_t segment_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    registered_segments_.insert(segment_id);
    LOG_DEBUG("SharedTextIndex registered segment {}", segment_id);
}

void
SharedTextIndex::UnregisterSegment(int64_t segment_id) {
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        registered_segments_.erase(segment_id);
    }

    // Delete documents for this segment
    writer_->DeleteSegment(segment_id);
    LOG_DEBUG("SharedTextIndex unregistered segment {}", segment_id);
}

void
SharedTextIndex::AddTexts(int64_t segment_id,
                          const std::string* texts,
                          const bool* valids,
                          size_t n,
                          int64_t offset_begin) {
    // Add texts one by one, skipping invalid ones
    for (size_t i = 0; i < n; ++i) {
        if (valids == nullptr || valids[i]) {
            writer_->AddText(segment_id, offset_begin + i, texts[i]);
        }
    }

    TryCommit();
}

TargetBitmap
SharedTextIndex::MatchQuery(int64_t segment_id,
                            const std::string& query,
                            uint32_t min_should_match,
                            int64_t row_count) {
    TryCommit();

    auto reader = GetOrCreateReader();
    reader->RegisterTokenizer(tokenizer_name_, analyzer_params_);

    std::vector<int64_t> results;
    if (min_should_match <= 1) {
        results = reader->MatchQuery(segment_id, query);
    } else {
        results =
            reader->MatchQueryWithMinimum(segment_id, query, min_should_match);
    }

    TargetBitmap bitset(row_count);
    for (int64_t doc_id : results) {
        if (doc_id >= 0 && doc_id < row_count) {
            bitset.set(doc_id);
        }
    }

    return bitset;
}

TargetBitmap
SharedTextIndex::PhraseMatchQuery(int64_t segment_id,
                                  const std::string& query,
                                  uint32_t slop,
                                  int64_t row_count) {
    TryCommit();

    auto reader = GetOrCreateReader();
    reader->RegisterTokenizer(tokenizer_name_, analyzer_params_);

    auto results = reader->PhraseMatchQuery(segment_id, query, slop);

    TargetBitmap bitset(row_count);
    for (int64_t doc_id : results) {
        if (doc_id >= 0 && doc_id < row_count) {
            bitset.set(doc_id);
        }
    }

    return bitset;
}

bool
SharedTextIndex::ShouldCommit() const {
    auto now = stdclock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - last_commit_time_.load())
                        .count();
    return duration > commit_interval_ms_;
}

void
SharedTextIndex::TryCommit() {
    if (ShouldCommit()) {
        Commit();
        Reload();
    }
}

void
SharedTextIndex::Commit() {
    writer_->Commit();
    last_commit_time_.store(stdclock::now());
}

void
SharedTextIndex::Reload() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (reader_) {
        reader_->Reload();
    }
}

std::shared_ptr<SharedTextIndexReader>
SharedTextIndex::GetOrCreateReader() {
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (reader_) {
            return reader_;
        }
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!reader_) {
        reader_ = writer_->CreateReader();
    }
    return reader_;
}

size_t
SharedTextIndex::GetSegmentCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return registered_segments_.size();
}

// ==================== SharedTextIndexManager ====================

SharedTextIndexManager&
SharedTextIndexManager::Instance() {
    static SharedTextIndexManager instance;
    return instance;
}

std::shared_ptr<SharedTextIndex>
SharedTextIndexManager::GetOrCreate(const SharedIndexKey& key,
                                    const std::string& field_name,
                                    const std::string& tokenizer_name,
                                    const std::string& analyzer_params) {
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = indexes_.find(key);
        if (it != indexes_.end()) {
            return it->second;
        }
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    // Double check after acquiring write lock
    auto it = indexes_.find(key);
    if (it != indexes_.end()) {
        return it->second;
    }

    // Create new shared index
    auto index = std::make_shared<SharedTextIndex>(
        field_name, tokenizer_name, analyzer_params);
    indexes_[key] = index;

    LOG_INFO("SharedTextIndexManager created new index for analyzer_hash={}",
             key.analyzer_params_hash);

    return index;
}

void
SharedTextIndexManager::TryRelease(const SharedIndexKey& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = indexes_.find(key);
    if (it != indexes_.end() && it->second->GetSegmentCount() == 0) {
        indexes_.erase(it);
        LOG_INFO("SharedTextIndexManager released index for analyzer_hash={}",
                 key.analyzer_params_hash);
    }
}

size_t
SharedTextIndexManager::GetIndexCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return indexes_.size();
}

}  // namespace milvus::index
