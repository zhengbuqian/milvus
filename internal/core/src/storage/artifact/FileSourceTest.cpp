// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Slice.h"
#include "index/test_utils/AssertHelpers.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage::test {
namespace {

using milvus::index::test::ExpectSegcoreError;

NamedBuffer
Buffer(std::string_view value) {
    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[value.size()]);
    if (!value.empty()) {
        std::memcpy(data.get(), value.data(), value.size());
    }
    return {std::move(data), value.size()};
}

NamedBufferSet
Buffers(
    std::initializer_list<std::pair<std::string, std::string_view>> values) {
    NamedBufferSet result;
    for (const auto& [name, value] : values) {
        result.emplace(name, Buffer(value));
    }
    return result;
}

std::shared_ptr<LocalDirectory>
MakeTestDirectory() {
    return LocalDirectory::CreateOwned(std::filesystem::temp_directory_path(),
                                       "milvus-filesource-test-XXXXXX",
                                       "NamedBufferSource test");
}

void
WriteFile(const std::filesystem::path& path, std::string_view value) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(value.data(), static_cast<std::streamsize>(value.size()));
    output.close();
    ASSERT_FALSE(output.fail());
}

std::string
ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.good()) {
        throw std::logic_error("cannot open test file " + path.string());
    }
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

size_t
StagingFileCount(const std::filesystem::path& directory) {
    if (!std::filesystem::exists(directory)) {
        return 0;
    }
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const auto name = entry.path().filename().string();
        if (name.rfind(".milvus-artifact", 0) == 0) {
            ++count;
        }
    }
    return count;
}

class FileSliceSizeGuard {
 public:
    explicit FileSliceSizeGuard(int64_t value)
        : original_(FILE_SLICE_SIZE.load()) {
        FILE_SLICE_SIZE.store(value);
    }

    ~FileSliceSizeGuard() {
        FILE_SLICE_SIZE.store(original_);
    }

 private:
    int64_t original_;
};

TEST(NamedBufferSourceTest, ReadsOwnedBinaryAndEmptyEntries) {
    std::unique_ptr<NamedBufferSource> source;
    {
        auto buffers =
            Buffers({{"binary", std::string_view("a\0b", 3)}, {"empty", ""}});
        source = std::make_unique<NamedBufferSource>(buffers);
        buffers.clear();
    }

    EXPECT_EQ(source->Gen(), Generation::V1V2);
    EXPECT_EQ(source->EntryNames(),
              (std::vector<std::string>{"binary", "empty"}));
    EXPECT_TRUE(source->HasEntry("binary"));
    EXPECT_FALSE(source->HasEntry("missing"));
    EXPECT_EQ(source->EntrySize("binary"), 3);
    EXPECT_EQ(source->EntrySize("empty"), 0);
    EXPECT_EQ(source->ReadEntry("binary"), (std::vector<uint8_t>{'a', 0, 'b'}));
    EXPECT_TRUE(source->ReadEntry("empty").empty());
    EXPECT_FALSE(source->GetMeta("anything").has_value());
}

TEST(NamedBufferSourceTest, RejectsMissingLogicalEntry) {
    NamedBufferSource source(Buffers({{"entry", "value"}}));

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(source.EntrySize("missing")); });
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(source.ReadEntry("missing")); });
}

TEST(NamedBufferSourceTest, RejectsNullNonEmptyPhysicalEntry) {
    NamedBufferSet buffers;
    buffers.emplace("entry", NamedBuffer{nullptr, 1});

    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { NamedBufferSource source(buffers); });
}

TEST(NamedBufferSourceTest, DiskEngineFilesAreUnsupported) {
    NamedBufferSource source(Buffers({{"entry", "value"}}));

    ExpectSegcoreError(ErrorCode::Unsupported, [&] {
        static_cast<void>(source.OpenDiskEngineFiles(
            static_cast<DiskEngineFileMode>(0), {"entry"}));
    });
}

TEST(NamedBufferSourceTest, ReassemblesPublicSmallSlices) {
    FileSliceSizeGuard slice_size(3);
    NamedBufferSink sink;
    sink.WriteEntry("payload", "abcdefgh", 8);
    static_cast<void>(sink.Finish());
    const auto buffers = sink.Take();
    EXPECT_TRUE(buffers.contains(INDEX_FILE_SLICE_META));
    EXPECT_TRUE(buffers.contains("payload_0"));
    EXPECT_TRUE(buffers.contains("payload_1"));
    EXPECT_TRUE(buffers.contains("payload_2"));

    NamedBufferSource source(buffers);

    EXPECT_EQ(source.EntryNames(), (std::vector<std::string>{"payload"}));
    EXPECT_EQ(source.EntrySize("payload"), 8);
    EXPECT_EQ(source.ReadEntry("payload"),
              (std::vector<uint8_t>{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}));
}

TEST(NamedBufferSourceTest, ReassemblesIndependentManualSlices) {
    auto buffers = Buffers(
        {{"payload_0", "abc"},
         {"payload_1", "def"},
         {"payload_2", "gh"},
         {INDEX_FILE_SLICE_META,
          R"({"meta":[{"name":"payload","slice_num":3,"total_len":8}]})"}});

    NamedBufferSource source(buffers);

    EXPECT_EQ(source.EntryNames(), (std::vector<std::string>{"payload"}));
    EXPECT_EQ(source.EntrySize("payload"), 8);
    EXPECT_EQ(source.ReadEntry("payload"),
              (std::vector<uint8_t>{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}));
}

TEST(NamedBufferSourceTest, MalformedSliceMetadataFailsInControlFlow) {
    auto buffers = Buffers({{INDEX_FILE_SLICE_META, "{"}});

    EXPECT_THROW({ NamedBufferSource source(buffers); }, std::exception);
}

TEST(NamedBufferSourceTest, MaterializesOneEntryAndCreatesParents) {
    NamedBufferSource source(Buffers({{"entry", "new-value"}}));
    auto directory = MakeTestDirectory();
    const auto target =
        std::filesystem::path(directory->Path()) / "nested" / "target";

    source.ReadEntryToLocalFile("entry", target.string());

    EXPECT_EQ(ReadFile(target), "new-value");
    EXPECT_EQ(StagingFileCount(target.parent_path()), 0);
}

TEST(NamedBufferSourceTest, MissingSingleEntryPreservesExistingTarget) {
    NamedBufferSource source(Buffers({{"entry", "value"}}));
    auto directory = MakeTestDirectory();
    const auto target = std::filesystem::path(directory->Path()) / "target";
    WriteFile(target, "old-value");

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        source.ReadEntryToLocalFile("missing", target.string());
    });

    EXPECT_EQ(ReadFile(target), "old-value");
    EXPECT_EQ(StagingFileCount(directory->Path()), 0);
}

TEST(NamedBufferSourceTest, ConcatenatesEntriesInRequestedOrder) {
    NamedBufferSource source(
        Buffers({{"a", "alpha"}, {"b", "B"}, {"empty", ""}}));
    auto directory = MakeTestDirectory();
    const auto target = std::filesystem::path(directory->Path()) / "joined";

    source.ReadEntriesToLocalFile({"b", "a", "empty", "b"}, target.string());

    EXPECT_EQ(ReadFile(target), "BalphaB");
    EXPECT_EQ(StagingFileCount(directory->Path()), 0);
}

TEST(NamedBufferSourceTest, MissingConcatenatedEntryPreservesExistingTarget) {
    NamedBufferSource source(Buffers({{"a", "alpha"}}));
    auto directory = MakeTestDirectory();
    const auto target = std::filesystem::path(directory->Path()) / "joined";
    WriteFile(target, "old-value");

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        source.ReadEntriesToLocalFile({"a", "missing"}, target.string());
    });

    EXPECT_EQ(ReadFile(target), "old-value");
    EXPECT_EQ(StagingFileCount(directory->Path()), 0);
}

TEST(NamedBufferSourceTest, EmptyConcatenationPublishesAnEmptyFile) {
    NamedBufferSource source(Buffers({{"entry", "value"}}));
    auto directory = MakeTestDirectory();
    const auto target = std::filesystem::path(directory->Path()) / "joined";
    WriteFile(target, "old-value");

    source.ReadEntriesToLocalFile({}, target.string());

    EXPECT_EQ(ReadFile(target), "");
    EXPECT_EQ(StagingFileCount(directory->Path()), 0);
}

TEST(NamedBufferSourceTest, MaterializesDirectoryInRequestedOrder) {
    NamedBufferSource source(
        Buffers({{"left/first", "one"}, {"right/second", "two"}}));
    auto directory = MakeTestDirectory();
    const auto target_dir =
        std::filesystem::path(directory->Path()) / "materialized";
    WriteFile(target_dir / "second", "old-two");

    const auto paths = source.ReadEntriesToLocalDir(
        {"right/second", "left/first"}, target_dir.string());

    EXPECT_EQ(paths,
              (std::vector<std::string>{(target_dir / "second").string(),
                                        (target_dir / "first").string()}));
    EXPECT_EQ(ReadFile(target_dir / "second"), "two");
    EXPECT_EQ(ReadFile(target_dir / "first"), "one");
    EXPECT_EQ(StagingFileCount(target_dir), 0);
}

TEST(NamedBufferSourceTest, MissingDirectoryEntryPreservesEveryTarget) {
    NamedBufferSource source(Buffers({{"first", "new-one"}}));
    auto directory = MakeTestDirectory();
    const auto target_dir =
        std::filesystem::path(directory->Path()) / "materialized";
    WriteFile(target_dir / "first", "old-one");
    WriteFile(target_dir / "missing", "old-missing");

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(source.ReadEntriesToLocalDir({"first", "missing"},
                                                       target_dir.string()));
    });

    EXPECT_EQ(ReadFile(target_dir / "first"), "old-one");
    EXPECT_EQ(ReadFile(target_dir / "missing"), "old-missing");
    EXPECT_EQ(StagingFileCount(target_dir), 0);
}

TEST(NamedBufferSourceTest, CollidingBaseNamesPreserveExistingTarget) {
    NamedBufferSource source(
        Buffers({{"left/shared", "left"}, {"right/shared", "right"}}));
    auto directory = MakeTestDirectory();
    const auto target_dir =
        std::filesystem::path(directory->Path()) / "materialized";
    WriteFile(target_dir / "shared", "old-value");

    ExpectSegcoreError(ErrorCode::UnexpectedError, [&] {
        static_cast<void>(source.ReadEntriesToLocalDir(
            {"left/shared", "right/shared"}, target_dir.string()));
    });

    EXPECT_EQ(ReadFile(target_dir / "shared"), "old-value");
    EXPECT_EQ(StagingFileCount(target_dir), 0);
}

TEST(NamedBufferSourceTest, EmptyDirectoryRequestCreatesNoFiles) {
    NamedBufferSource source(Buffers({{"entry", "value"}}));
    auto directory = MakeTestDirectory();
    const auto target_dir =
        std::filesystem::path(directory->Path()) / "materialized";

    const auto paths = source.ReadEntriesToLocalDir({}, target_dir.string());

    EXPECT_TRUE(paths.empty());
    EXPECT_TRUE(std::filesystem::is_directory(target_dir));
    EXPECT_TRUE(std::filesystem::is_empty(target_dir));
}

}  // namespace
}  // namespace milvus::storage::test
