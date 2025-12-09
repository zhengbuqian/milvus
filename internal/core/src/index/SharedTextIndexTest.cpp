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

#include <gtest/gtest.h>
#include <set>
#include <string>
#include <vector>

#include "index/SharedTextIndex.h"

using namespace milvus::index;

class SharedTextIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }

    void
    TearDown() override {
    }
};

TEST_F(SharedTextIndexTest, BasicWriteAndQuery) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    // Add documents for segment 1
    writer.AddText(1, 0, "hello world");
    writer.AddText(1, 1, "hello rust");

    // Add documents for segment 2
    writer.AddText(2, 0, "hello python");
    writer.AddText(2, 1, "goodbye world");

    writer.Commit();

    auto reader = writer.CreateReader();

    // Query segment 1 for "hello"
    auto results = reader->MatchQuery(1, "hello");
    std::set<int64_t> result_set(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0, 1}));

    // Query segment 2 for "hello"
    results = reader->MatchQuery(2, "hello");
    result_set = std::set<int64_t>(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0}));

    // Query segment 1 for "world"
    results = reader->MatchQuery(1, "world");
    result_set = std::set<int64_t>(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0}));

    // Query segment 2 for "world"
    results = reader->MatchQuery(2, "world");
    result_set = std::set<int64_t>(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{1}));
}

TEST_F(SharedTextIndexTest, SegmentIsolation) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    // Add same text to different segments with different local_doc_ids
    writer.AddText(100, 0, "unique content");
    writer.AddText(200, 5, "unique content");
    writer.AddText(300, 10, "different content");

    writer.Commit();

    auto reader = writer.CreateReader();

    // Each segment should only see its own docs
    auto results = reader->MatchQuery(100, "unique");
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 0);

    results = reader->MatchQuery(200, "unique");
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 5);

    results = reader->MatchQuery(300, "unique");
    EXPECT_TRUE(results.empty());

    results = reader->MatchQuery(300, "different");
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 10);
}

TEST_F(SharedTextIndexTest, DeleteSegment) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    writer.AddText(1, 0, "hello world");
    writer.AddText(2, 0, "hello world");
    writer.Commit();

    auto reader = writer.CreateReader();

    // Both segments have data
    EXPECT_FALSE(reader->MatchQuery(1, "hello").empty());
    EXPECT_FALSE(reader->MatchQuery(2, "hello").empty());

    // Delete segment 1
    writer.DeleteSegment(1);
    writer.Commit();
    reader->Reload();

    // Segment 1 should be empty, segment 2 should still have data
    EXPECT_TRUE(reader->MatchQuery(1, "hello").empty());
    EXPECT_FALSE(reader->MatchQuery(2, "hello").empty());
}

TEST_F(SharedTextIndexTest, PhraseMatch) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    writer.AddText(1, 0, "hello world today");
    writer.AddText(1, 1, "hello beautiful world");
    writer.AddText(2, 0, "hello world");

    writer.Commit();

    auto reader = writer.CreateReader();

    // Exact phrase match with slop=0
    auto results = reader->PhraseMatchQuery(1, "hello world", 0);
    std::set<int64_t> result_set(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0}));

    // Phrase match with slop=1 should also match "hello beautiful world"
    results = reader->PhraseMatchQuery(1, "hello world", 1);
    result_set = std::set<int64_t>(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0, 1}));

    // Segment 2 should have its own results
    results = reader->PhraseMatchQuery(2, "hello world", 0);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 0);
}

TEST_F(SharedTextIndexTest, MinShouldMatch) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    writer.AddText(1, 0, "a b");
    writer.AddText(1, 1, "a c");
    writer.AddText(1, 2, "b c");
    writer.AddText(1, 3, "a b c");

    writer.Commit();

    auto reader = writer.CreateReader();

    // min=1: any token matches
    auto results = reader->MatchQueryWithMinimum(1, "a b", 1);
    std::set<int64_t> result_set(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0, 1, 2, 3}));

    // min=2: at least 2 tokens must match
    results = reader->MatchQueryWithMinimum(1, "a b c", 2);
    result_set = std::set<int64_t>(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0, 1, 2, 3}));

    // min=3: all 3 tokens must match
    results = reader->MatchQueryWithMinimum(1, "a b c", 3);
    result_set = std::set<int64_t>(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{3}));
}

TEST_F(SharedTextIndexTest, SharedTextIndexClass) {
    SharedTextIndex index("text", "default", "{}", 200);

    // Register segments
    index.RegisterSegment(1);
    index.RegisterSegment(2);

    EXPECT_EQ(index.GetSegmentCount(), 2);

    // Add texts for segment 1
    std::vector<std::string> texts1 = {"hello world", "hello rust"};
    index.AddTexts(1, texts1.data(), nullptr, texts1.size(), 0);

    // Add texts for segment 2
    std::vector<std::string> texts2 = {"hello python", "goodbye world"};
    index.AddTexts(2, texts2.data(), nullptr, texts2.size(), 0);

    index.Commit();

    // Query segment 1
    auto bitset = index.MatchQuery(1, "hello", 1, 2);
    EXPECT_TRUE(bitset[0]);
    EXPECT_TRUE(bitset[1]);

    // Query segment 2
    bitset = index.MatchQuery(2, "hello", 1, 2);
    EXPECT_TRUE(bitset[0]);
    EXPECT_FALSE(bitset[1]);

    // Query segment 2 for "world"
    bitset = index.MatchQuery(2, "world", 1, 2);
    EXPECT_FALSE(bitset[0]);
    EXPECT_TRUE(bitset[1]);

    // Unregister segment 1
    index.UnregisterSegment(1);
    EXPECT_EQ(index.GetSegmentCount(), 1);

    // After commit/reload, segment 1 data should be gone
    index.Commit();
    index.Reload();

    bitset = index.MatchQuery(1, "hello", 1, 2);
    EXPECT_FALSE(bitset[0]);
    EXPECT_FALSE(bitset[1]);

    // Segment 2 should still work
    bitset = index.MatchQuery(2, "hello", 1, 2);
    EXPECT_TRUE(bitset[0]);
}

TEST_F(SharedTextIndexTest, SharedTextIndexWithNulls) {
    SharedTextIndex index("text", "default", "{}", 200);

    index.RegisterSegment(1);

    std::vector<std::string> texts = {"hello world", "", "hello rust"};
    bool valids[] = {true, false, true};

    index.AddTexts(1, texts.data(), valids, texts.size(), 0);
    index.Commit();

    auto bitset = index.MatchQuery(1, "hello", 1, 3);
    EXPECT_TRUE(bitset[0]);
    EXPECT_FALSE(bitset[1]);  // null/invalid
    EXPECT_TRUE(bitset[2]);
}

TEST_F(SharedTextIndexTest, SharedTextIndexPhraseMatch) {
    SharedTextIndex index("text", "default", "{}", 200);

    index.RegisterSegment(1);

    std::vector<std::string> texts = {
        "hello world today", "hello beautiful world", "world hello"};
    index.AddTexts(1, texts.data(), nullptr, texts.size(), 0);
    index.Commit();

    // Exact phrase
    auto bitset = index.PhraseMatchQuery(1, "hello world", 0, 3);
    EXPECT_TRUE(bitset[0]);
    EXPECT_FALSE(bitset[1]);
    EXPECT_FALSE(bitset[2]);

    // With slop=1
    bitset = index.PhraseMatchQuery(1, "hello world", 1, 3);
    EXPECT_TRUE(bitset[0]);
    EXPECT_TRUE(bitset[1]);
    EXPECT_FALSE(bitset[2]);
}

TEST_F(SharedTextIndexTest, ManagerBasic) {
    auto& manager = SharedTextIndexManager::Instance();

    // Initially no indexes
    size_t initial_count = manager.GetIndexCount();

    // Create key from analyzer params hash
    std::string analyzer_params1 = "{}";
    SharedIndexKey key1{std::hash<std::string>{}(analyzer_params1)};

    // Get or create index
    auto index1 =
        manager.GetOrCreate(key1, "text", "milvus_tokenizer", analyzer_params1);
    EXPECT_NE(index1, nullptr);
    EXPECT_EQ(manager.GetIndexCount(), initial_count + 1);

    // Get same index again (same analyzer params)
    auto index2 =
        manager.GetOrCreate(key1, "text", "milvus_tokenizer", analyzer_params1);
    EXPECT_EQ(index1.get(), index2.get());
    EXPECT_EQ(manager.GetIndexCount(), initial_count + 1);

    // Different analyzer params should create new index
    std::string analyzer_params2 = R"({"tokenizer": "jieba"})";
    SharedIndexKey key2{std::hash<std::string>{}(analyzer_params2)};
    auto index3 =
        manager.GetOrCreate(key2, "text", "milvus_tokenizer", analyzer_params2);
    EXPECT_NE(index1.get(), index3.get());
    EXPECT_EQ(manager.GetIndexCount(), initial_count + 2);
}

TEST_F(SharedTextIndexTest, BatchAddTexts) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    std::vector<std::string> texts = {
        "document one", "document two", "document three"};

    std::vector<const char*> text_ptrs;
    for (const auto& t : texts) {
        text_ptrs.push_back(t.c_str());
    }

    writer.AddTexts(1, texts.data(), texts.size(), 0);
    writer.Commit();

    auto reader = writer.CreateReader();

    auto results = reader->MatchQuery(1, "document");
    EXPECT_EQ(results.size(), 3);

    std::set<int64_t> result_set(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{0, 1, 2}));

    results = reader->MatchQuery(1, "two");
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], 1);
}

TEST_F(SharedTextIndexTest, LargeDocIds) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    // Test with large doc IDs
    writer.AddText(1, 1000000, "large doc id");
    writer.AddText(1, 2000000, "another large doc id");
    writer.Commit();

    auto reader = writer.CreateReader();

    auto results = reader->MatchQuery(1, "large");
    EXPECT_EQ(results.size(), 2);

    std::set<int64_t> result_set(results.begin(), results.end());
    EXPECT_EQ(result_set, (std::set<int64_t>{1000000, 2000000}));
}

TEST_F(SharedTextIndexTest, EmptyQuery) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    writer.AddText(1, 0, "hello world");
    writer.Commit();

    auto reader = writer.CreateReader();

    // Empty query should return empty results
    auto results = reader->MatchQuery(1, "");
    EXPECT_TRUE(results.empty());

    results = reader->PhraseMatchQuery(1, "", 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(SharedTextIndexTest, NonExistentSegment) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    writer.AddText(1, 0, "hello world");
    writer.Commit();

    auto reader = writer.CreateReader();

    // Query non-existent segment should return empty
    auto results = reader->MatchQuery(999, "hello");
    EXPECT_TRUE(results.empty());
}

TEST_F(SharedTextIndexTest, MultipleCommitsAndReloads) {
    SharedTextIndexWriter writer("text", "default", "{}", 1, 50 * 1024 * 1024);

    writer.AddText(1, 0, "first");
    writer.Commit();

    auto reader = writer.CreateReader();
    auto results = reader->MatchQuery(1, "first");
    EXPECT_EQ(results.size(), 1);

    // Add more data
    writer.AddText(1, 1, "second");
    writer.Commit();
    reader->Reload();

    results = reader->MatchQuery(1, "first");
    EXPECT_EQ(results.size(), 1);
    results = reader->MatchQuery(1, "second");
    EXPECT_EQ(results.size(), 1);

    // Add even more data
    writer.AddText(1, 2, "third");
    writer.Commit();
    reader->Reload();

    results = reader->MatchQuery(1, "first");
    EXPECT_EQ(results.size(), 1);
    results = reader->MatchQuery(1, "second");
    EXPECT_EQ(results.size(), 1);
    results = reader->MatchQuery(1, "third");
    EXPECT_EQ(results.size(), 1);
}
