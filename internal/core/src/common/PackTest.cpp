// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>

#include "common/Pack.h"

namespace milvus {

namespace {

// Helper to create a shared_ptr<uint8_t[]> from raw data
std::shared_ptr<uint8_t[]>
MakeBinary(const void* data, size_t size) {
    auto binary = std::shared_ptr<uint8_t[]>(new uint8_t[size]);
    std::memcpy(binary.get(), data, size);
    return binary;
}

}  // namespace

class PackTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create temp directory
        temp_dir_ = std::filesystem::temp_directory_path() / "pack_test";
        std::filesystem::create_directories(temp_dir_);
    }

    void
    TearDown() override {
        // Clean up temp directory
        std::filesystem::remove_all(temp_dir_);
    }

    std::filesystem::path temp_dir_;
};

TEST_F(PackTest, PackUnpackBinarySet) {
    // Create a BinarySet with multiple entries
    BinarySet original;

    // Add first entry
    std::string data1 = "Hello, World!";
    original.Append(
        "file1.txt", MakeBinary(data1.data(), data1.size()), data1.size());

    // Add second entry with binary data
    std::vector<uint8_t> data2 = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
    original.Append(
        "binary.bin", MakeBinary(data2.data(), data2.size()), data2.size());

    // Add third entry with empty data
    original.Append("empty.dat", MakeBinary("", 1), 0);

    // Pack to blob
    auto blob = PackBinarySetToBlob(original);
    EXPECT_GT(blob.size(), 0);

    // Unpack from blob
    auto restored = UnpackBlobToBinarySet(blob);

    // Verify all entries are restored
    EXPECT_EQ(restored.binary_map_.size(), original.binary_map_.size());

    // Verify file1.txt
    auto it1 = restored.binary_map_.find("file1.txt");
    ASSERT_NE(it1, restored.binary_map_.end());
    EXPECT_EQ(it1->second->size, data1.size());
    EXPECT_EQ(std::memcmp(it1->second->data.get(), data1.data(), data1.size()),
              0);

    // Verify binary.bin
    auto it2 = restored.binary_map_.find("binary.bin");
    ASSERT_NE(it2, restored.binary_map_.end());
    EXPECT_EQ(it2->second->size, data2.size());
    EXPECT_EQ(std::memcmp(it2->second->data.get(), data2.data(), data2.size()),
              0);

    // Verify empty.dat
    auto it3 = restored.binary_map_.find("empty.dat");
    ASSERT_NE(it3, restored.binary_map_.end());
    EXPECT_EQ(it3->second->size, 0);
}

TEST_F(PackTest, PackUnpackDirectory) {
    // Create source directory with files
    auto src_dir = temp_dir_ / "source";
    std::filesystem::create_directories(src_dir);

    // Create file1
    {
        std::ofstream f((src_dir / "file1.txt").string());
        f << "Hello, World!";
    }

    // Create file2 with binary data
    {
        std::ofstream f((src_dir / "binary.bin").string(), std::ios::binary);
        std::vector<uint8_t> data = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
        f.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    // Pack directory to blob
    auto blob = PackDirectoryToBlob(src_dir.string());
    EXPECT_GT(blob.size(), 0);

    // Unpack to destination directory
    auto dst_dir = temp_dir_ / "destination";
    UnpackBlobToDirectory(blob, dst_dir.string());

    // Verify files exist
    EXPECT_TRUE(std::filesystem::exists(dst_dir / "file1.txt"));
    EXPECT_TRUE(std::filesystem::exists(dst_dir / "binary.bin"));

    // Verify file1 content
    {
        std::ifstream f((dst_dir / "file1.txt").string());
        std::string content((std::istreambuf_iterator<char>(f)),
                            std::istreambuf_iterator<char>());
        EXPECT_EQ(content, "Hello, World!");
    }

    // Verify binary.bin content
    {
        std::ifstream f((dst_dir / "binary.bin").string(), std::ios::binary);
        std::vector<uint8_t> content((std::istreambuf_iterator<char>(f)),
                                     std::istreambuf_iterator<char>());
        std::vector<uint8_t> expected = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
        EXPECT_EQ(content, expected);
    }
}

TEST_F(PackTest, PackUnpackEmptyBinarySet) {
    BinarySet original;

    // Pack empty BinarySet
    auto blob = PackBinarySetToBlob(original);
    EXPECT_EQ(blob.size(), 4);  // Just the count (0)

    // Unpack and verify
    auto restored = UnpackBlobToBinarySet(blob);
    EXPECT_EQ(restored.binary_map_.size(), 0);
}

TEST_F(PackTest, PackUnpackLargeData) {
    // Create large data (1MB)
    const size_t data_size = 1024 * 1024;
    std::vector<uint8_t> large_data(data_size);
    for (size_t i = 0; i < data_size; ++i) {
        large_data[i] = static_cast<uint8_t>(i & 0xFF);
    }

    BinarySet original;
    original.Append(
        "large.bin", MakeBinary(large_data.data(), data_size), data_size);

    // Pack and unpack
    auto blob = PackBinarySetToBlob(original);
    auto restored = UnpackBlobToBinarySet(blob);

    // Verify
    EXPECT_EQ(restored.binary_map_.size(), 1);
    auto it = restored.binary_map_.find("large.bin");
    ASSERT_NE(it, restored.binary_map_.end());
    EXPECT_EQ(it->second->size, data_size);
    EXPECT_EQ(std::memcmp(it->second->data.get(), large_data.data(), data_size),
              0);
}

TEST_F(PackTest, IsUnifiedScalarIndexVersion) {
    // Version 0, 1, 2 should not be unified
    EXPECT_FALSE(IsUnifiedScalarIndexVersion(0));
    EXPECT_FALSE(IsUnifiedScalarIndexVersion(1));
    EXPECT_FALSE(IsUnifiedScalarIndexVersion(2));

    // Version 3+ should be unified
    EXPECT_TRUE(IsUnifiedScalarIndexVersion(3));
    EXPECT_TRUE(IsUnifiedScalarIndexVersion(4));
    EXPECT_TRUE(IsUnifiedScalarIndexVersion(100));
}

}  // namespace milvus
