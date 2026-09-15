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

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "index/test_utils/AssertHelpers.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage::test {
namespace {

using milvus::index::test::ExpectSegcoreError;

std::shared_ptr<LocalDirectory>
MakeTestDirectory() {
    return LocalDirectory::CreateOwned(std::filesystem::temp_directory_path(),
                                       "milvus-filesink-test-XXXXXX",
                                       "NamedBufferSink test");
}

void
WriteFile(const std::filesystem::path& path, std::string_view value) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(value.data(), static_cast<std::streamsize>(value.size()));
    output.close();
    ASSERT_FALSE(output.fail());
}

std::vector<uint8_t>
Bytes(const NamedBuffer& buffer) {
    if (buffer.size == 0) {
        return {};
    }
    return {buffer.data.get(), buffer.data.get() + buffer.size};
}

TEST(NamedBufferSinkTest, RequiresSuccessfulFinishBeforeReadingData) {
    NamedBufferSink sink;

    EXPECT_EQ(sink.Gen(), Generation::V1V2);
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Data()); });
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Take()); });
}

TEST(NamedBufferSinkTest, EmptyFinishPublishesAnEmptyArtifact) {
    NamedBufferSink sink;

    const auto stats = sink.Finish();

    EXPECT_EQ(stats.MemSize(), 0);
    EXPECT_TRUE(stats.Files().empty());
    EXPECT_TRUE(sink.Data().empty());
    EXPECT_TRUE(sink.Take().empty());
    sink.ReleaseLocalStaging();
    sink.ReleaseLocalStaging();
}

TEST(NamedBufferSinkTest, PublishesBinaryEmptyAndBorrowedFileEntries) {
    auto directory = MakeTestDirectory();
    const auto borrowed = std::filesystem::path(directory->Path()) / "input";
    WriteFile(borrowed, "local-value");

    NamedBufferSink sink;
    std::array<uint8_t, 4> binary = {0x00, 0x7f, 0x80, 0xff};
    sink.WriteEntry("binary", binary.data(), binary.size());
    sink.WriteEntry("empty", nullptr, 0);
    sink.WriteEntryFromLocalFile("local", borrowed.string());
    binary.fill(0);

    const auto stats = sink.Finish();

    EXPECT_EQ(stats.MemSize(), 15);
    EXPECT_TRUE(stats.Files().empty());
    ASSERT_EQ(sink.Data().size(), 3);
    EXPECT_EQ(Bytes(sink.Data().at("binary")),
              (std::vector<uint8_t>{0x00, 0x7f, 0x80, 0xff}));
    EXPECT_TRUE(Bytes(sink.Data().at("empty")).empty());
    EXPECT_EQ(Bytes(sink.Data().at("local")),
              (std::vector<uint8_t>{
                  'l', 'o', 'c', 'a', 'l', '-', 'v', 'a', 'l', 'u', 'e'}));
    EXPECT_TRUE(std::filesystem::exists(borrowed));

    sink.ReleaseLocalStaging();
    EXPECT_TRUE(std::filesystem::exists(borrowed));
    const auto taken = sink.Take();
    EXPECT_EQ(taken.size(), 3);
    EXPECT_TRUE(sink.Data().empty());
}

TEST(NamedBufferSinkTest, FinishedSinkRejectsFurtherMutation) {
    NamedBufferSink sink;
    static constexpr std::array<uint8_t, 1> value = {1};
    sink.WriteEntry("entry", value.data(), value.size());
    static_cast<void>(sink.Finish());

    ExpectSegcoreError(ErrorCode::UnexpectedError, [&] {
        sink.WriteEntry("another", value.data(), value.size());
    });
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Finish()); });
}

TEST(NamedBufferSinkTest, RawFileOperationIsUnsupportedAndFailsSink) {
    NamedBufferSink sink;
    ExpectSegcoreError(ErrorCode::Unsupported, [&] {
        sink.WriteRawEntryFromLocalFile("raw", "/unused");
    });

    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Finish()); });
}

TEST(NamedBufferSinkTest, MetadataOperationIsUnsupportedAndFailsSink) {
    NamedBufferSink sink;
    ExpectSegcoreError(ErrorCode::Unsupported,
                       [&] { sink.PutMeta("key", nlohmann::json(1)); });

    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { sink.WriteEntry("entry", nullptr, 0); });
}

TEST(NamedBufferSinkTest, DuplicateEntryFailsSink) {
    NamedBufferSink sink;
    static constexpr std::array<uint8_t, 1> value = {1};
    sink.WriteEntry("entry", value.data(), value.size());

    ExpectSegcoreError(ErrorCode::UnexpectedError, [&] {
        sink.WriteEntry("entry", value.data(), value.size());
    });
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Finish()); });
}

TEST(NamedBufferSinkTest, NullNonEmptyEntryFailsSink) {
    NamedBufferSink sink;

    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { sink.WriteEntry("entry", nullptr, 1); });
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Finish()); });
}

TEST(NamedBufferSinkTest, MissingBorrowedFileFailsSink) {
    auto directory = MakeTestDirectory();
    const auto missing =
        (std::filesystem::path(directory->Path()) / "missing").string();
    NamedBufferSink sink;

    ExpectSegcoreError(ErrorCode::FileOpenFailed,
                       [&] { sink.WriteEntryFromLocalFile("entry", missing); });
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(sink.Finish()); });
}

}  // namespace
}  // namespace milvus::storage::test
