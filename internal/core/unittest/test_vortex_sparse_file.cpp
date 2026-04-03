// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// POC test: Verify that a vortex file with only footer + specific field segments
// filled in (sparse file) can be read correctly via the scan API.

#include <gtest/gtest.h>
#include <memory>
#include <cstring>
#include <vector>
#include <iostream>
#include <string>
#include <random>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/api.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/io/memory.h>

#include "milvus-storage/common/constants.h"
#include "milvus-storage/format/vortex/vortex_writer.h"
#include "milvus-storage/format/vortex/vortex_format_reader.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/filesystem/ffi/filesystem_internal.h"
#include "milvus-storage/properties.h"

namespace milvus_storage {

using namespace vortex;

// A minimal in-memory filesystem that serves a single file from a buffer.
class SingleBufferFileSystem : public arrow::fs::FileSystem {
 public:
  SingleBufferFileSystem(std::string path, std::shared_ptr<arrow::Buffer> buffer)
      : arrow::fs::FileSystem(arrow::io::default_io_context()),
        path_(std::move(path)),
        buffer_(std::move(buffer)) {}

  std::string type_name() const override { return "mem"; }

  bool Equals(const FileSystem& other) const override { return false; }

  arrow::Result<arrow::fs::FileInfo> GetFileInfo(const std::string& path) override {
    if (path == path_) {
      arrow::fs::FileInfo info;
      info.set_path(path);
      info.set_type(arrow::fs::FileType::File);
      info.set_size(buffer_->size());
      return info;
    }
    return arrow::Status::IOError("File not found: ", path);
  }

  arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
      const arrow::fs::FileSelector& select) override {
    return arrow::Status::NotImplemented("GetFileInfo with selector");
  }

  arrow::Status CreateDir(const std::string&, bool) override {
    return arrow::Status::NotImplemented("CreateDir");
  }
  arrow::Status DeleteDir(const std::string&) override {
    return arrow::Status::NotImplemented("DeleteDir");
  }
  arrow::Status DeleteDirContents(const std::string&, bool) override {
    return arrow::Status::NotImplemented("DeleteDirContents");
  }
  arrow::Status DeleteRootDirContents() override {
    return arrow::Status::NotImplemented("DeleteRootDirContents");
  }
  arrow::Status DeleteFile(const std::string&) override {
    return arrow::Status::NotImplemented("DeleteFile");
  }
  arrow::Status Move(const std::string&, const std::string&) override {
    return arrow::Status::NotImplemented("Move");
  }
  arrow::Status CopyFile(const std::string&, const std::string&) override {
    return arrow::Status::NotImplemented("CopyFile");
  }

  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const std::string& path) override {
    if (path == path_) {
      return std::make_shared<arrow::io::BufferReader>(buffer_);
    }
    return arrow::Status::IOError("File not found: ", path);
  }

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override {
    if (path == path_) {
      return std::make_shared<arrow::io::BufferReader>(buffer_);
    }
    return arrow::Status::IOError("File not found: ", path);
  }

  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
      const std::string&, const std::shared_ptr<const arrow::KeyValueMetadata>&) override {
    return arrow::Status::NotImplemented("OpenOutputStream");
  }

  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
      const std::string&, const std::shared_ptr<const arrow::KeyValueMetadata>&) override {
    return arrow::Status::NotImplemented("OpenAppendStream");
  }

 private:
  std::string path_;
  std::shared_ptr<arrow::Buffer> buffer_;
};

// Helper: print byte ranges and return total bytes needed
static uint64_t PrintByteRanges(const std::string& field_name,
                                const std::vector<uint64_t>& ranges,
                                int64_t file_size) {
  std::cout << "Byte ranges for field '" << field_name << "':" << std::endl;
  std::cout << "  Footer: [" << ranges[0] << ", " << ranges[1] << ")" << std::endl;
  uint64_t total = ranges[1] - ranges[0];
  for (size_t i = 2; i < ranges.size(); i += 2) {
    std::cout << "  Segment: [" << ranges[i] << ", " << ranges[i + 1] << ")" << std::endl;
    total += ranges[i + 1] - ranges[i];
  }
  std::cout << "  Total: " << total << " / " << file_size
            << " (" << (100.0 * total / file_size) << "%)" << std::endl;
  return total;
}

// Helper: create sparse buffer from full buffer using byte ranges
static std::shared_ptr<arrow::Buffer> CreateSparseBuffer(
    const std::shared_ptr<arrow::Buffer>& full_buffer,
    const std::vector<uint64_t>& ranges,
    int64_t file_size,
    std::unique_ptr<uint8_t[]>& backing_store) {
  backing_store = std::make_unique<uint8_t[]>(file_size);
  std::memset(backing_store.get(), 0, file_size);
  const uint8_t* src = full_buffer->data();
  for (size_t i = 0; i < ranges.size(); i += 2) {
    uint64_t start = ranges[i];
    uint64_t end = ranges[i + 1];
    std::memcpy(backing_store.get() + start, src + start, end - start);
  }
  return arrow::Buffer::Wrap(backing_store.get(), file_size);
}

// Helper: verify sparse scan results for a single field
static void VerifySparseRead(
    const std::shared_ptr<SingleBufferFileSystem>& sparse_fs,
    const std::shared_ptr<arrow::Schema>& full_schema,
    int field_index,
    const std::string& test_file,
    const api::Properties& properties,
    int64_t total_expected_rows,
    std::function<void(const std::shared_ptr<arrow::StructArray>&, int64_t row_offset)> verify_fn) {
  auto projected_schema = arrow::schema({full_schema->field(field_index)});
  auto field_name = full_schema->field(field_index)->name();

  VortexFormatReader reader(sparse_fs, projected_schema, test_file, properties,
                            std::vector<std::string>{field_name});
  auto open_status = reader.open();
  ASSERT_TRUE(open_status.ok()) << open_status.ToString();

  auto read_result = reader.blocking_read(0, total_expected_rows);
  ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
  auto chunked_array = read_result.ValueOrDie();

  ASSERT_GT(chunked_array->num_chunks(), 0);
  int64_t total_rows = 0;
  for (int i = 0; i < chunked_array->num_chunks(); i++) {
    auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(chunked_array->chunk(i));
    ASSERT_NE(struct_arr, nullptr);
    verify_fn(struct_arr, total_rows);
    total_rows += struct_arr->length();
  }
  ASSERT_EQ(total_rows, total_expected_rows);
  std::cout << "  Verified " << field_name << ": " << total_rows << " rows OK" << std::endl;
}

class VortexSparseFileTest : public ::testing::Test {
 protected:
  static constexpr int64_t kTotalRows = 5000000;
  static constexpr int32_t kBatchSize = 50000;
  static constexpr int kNumBatches = kTotalRows / kBatchSize;

  void SetUp() override {
    schema_ = arrow::schema({
        arrow::field("int32", arrow::int32(), false,
                     arrow::key_value_metadata({ARROW_FIELD_ID_KEY}, {"100"})),
        arrow::field("int64", arrow::int64(), false,
                     arrow::key_value_metadata({ARROW_FIELD_ID_KEY}, {"200"})),
        arrow::field("float", arrow::float32(), false,
                     arrow::key_value_metadata({ARROW_FIELD_ID_KEY}, {"300"})),
        arrow::field("str", arrow::utf8(), false,
                     arrow::key_value_metadata({ARROW_FIELD_ID_KEY}, {"400"})),
    });

    api::SetValue(properties_, PROPERTY_FS_ROOT_PATH, "/tmp/milvus-vortex-sparse-test");
    local_fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

    auto status = local_fs_->CreateDir("/tmp/milvus-vortex-sparse-test");
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Clean up before test (not after, so file remains for vx browse)
    local_fs_->DeleteFile(test_file_).ok();

    // Pre-generate random data for reproducibility (fixed seed)
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> i32_dist(-1000000, 1000000);
    std::uniform_int_distribution<int64_t> i64_dist(-1000000000LL, 1000000000LL);
    std::uniform_real_distribution<float> f32_dist(-1e6f, 1e6f);

    all_i32_.resize(kTotalRows);
    all_i64_.resize(kTotalRows);
    all_f32_.resize(kTotalRows);
    all_str_.resize(kTotalRows);

    // Random string charset
    const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<int> len_dist(5, 30);
    std::uniform_int_distribution<int> char_dist(0, charset.size() - 1);

    for (int64_t i = 0; i < kTotalRows; i++) {
      all_i32_[i] = i32_dist(rng);
      all_i64_[i] = i64_dist(rng);
      all_f32_[i] = f32_dist(rng);

      int len = len_dist(rng);
      std::string s(len, ' ');
      for (int j = 0; j < len; j++) {
        s[j] = charset[char_dist(rng)];
      }
      all_str_[i] = std::move(s);
    }
  }

  std::shared_ptr<arrow::RecordBatch> MakeBatch(int32_t offset, int32_t count) {
    arrow::Int32Builder i32_builder;
    arrow::Int64Builder i64_builder;
    arrow::FloatBuilder f32_builder;
    arrow::StringBuilder str_builder;

    for (int32_t i = offset; i < offset + count; i++) {
      EXPECT_TRUE(i32_builder.Append(all_i32_[i]).ok());
      EXPECT_TRUE(i64_builder.Append(all_i64_[i]).ok());
      EXPECT_TRUE(f32_builder.Append(all_f32_[i]).ok());
      EXPECT_TRUE(str_builder.Append(all_str_[i]).ok());
    }

    std::shared_ptr<arrow::Array> i32_arr, i64_arr, f32_arr, str_arr;
    EXPECT_TRUE(i32_builder.Finish(&i32_arr).ok());
    EXPECT_TRUE(i64_builder.Finish(&i64_arr).ok());
    EXPECT_TRUE(f32_builder.Finish(&f32_arr).ok());
    EXPECT_TRUE(str_builder.Finish(&str_arr).ok());

    return arrow::RecordBatch::Make(schema_, count, {i32_arr, i64_arr, f32_arr, str_arr});
  }

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::fs::LocalFileSystem> local_fs_;
  api::Properties properties_;
  std::string test_file_ = "/tmp/milvus-vortex-sparse-test/sparse_test.vortex";

  // Pre-generated random data for verification
  std::vector<int32_t> all_i32_;
  std::vector<int64_t> all_i64_;
  std::vector<float> all_f32_;
  std::vector<std::string> all_str_;
};

TEST_F(VortexSparseFileTest, SparseFileFieldScan) {
  // Step 1: Write a vortex file with multiple batches
  {
    VortexFileWriter writer(local_fs_, schema_, test_file_, properties_);
    for (int i = 0; i < kNumBatches; i++) {
      auto batch = MakeBatch(i * kBatchSize, kBatchSize);
      auto status = writer.Write(batch);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    auto flush_status = writer.Flush();
    ASSERT_TRUE(flush_status.ok()) << flush_status.ToString();
    auto close_result = writer.Close();
    ASSERT_TRUE(close_result.ok()) << close_result.status().ToString();
    ASSERT_EQ(kTotalRows, close_result.ValueOrDie().end_index);
  }
  std::cout << "Written " << kTotalRows << " rows to " << test_file_ << std::endl;

  // Step 2: Read the full file into memory
  auto file_info_result = local_fs_->GetFileInfo(test_file_);
  ASSERT_TRUE(file_info_result.ok()) << file_info_result.status().ToString();
  int64_t file_size = file_info_result.ValueOrDie().size();
  ASSERT_GT(file_size, 0);

  auto input_result = local_fs_->OpenInputFile(test_file_);
  ASSERT_TRUE(input_result.ok()) << input_result.status().ToString();
  auto full_buffer_result = input_result.ValueOrDie()->Read(file_size);
  ASSERT_TRUE(full_buffer_result.ok()) << full_buffer_result.status().ToString();
  auto full_buffer = full_buffer_result.ValueOrDie();
  std::cout << "Full file size: " << file_size << " bytes" << std::endl;

  // Step 3: Open VortexFile to get byte ranges for each field
  auto fs_holder = std::make_shared<FileSystemWrapper>(local_fs_);
  auto vx_file = VortexFile::Open((uint8_t*)fs_holder.get(), test_file_);
  std::cout << "Row count: " << vx_file.RowCount() << std::endl;

  // Print full layout tree
  std::cout << "\n=== Layout Tree ===" << std::endl;
  std::cout << vx_file.LayoutTreeString() << std::endl;

  // Print per-field chunk info
  std::vector<std::string> field_names = {"int32", "int64", "float", "str"};
  for (const auto& name : field_names) {
    auto offsets = vx_file.FieldChunkOffsets(name);
    ASSERT_GE(offsets.size(), 2u);
    uint64_t num_chunked = offsets[0];
    uint64_t total_chunks = offsets[1];
    std::cout << "\n--- Field '" << name << "' ---" << std::endl;
    std::cout << "  ChunkedLayout nodes: " << num_chunked << std::endl;
    std::cout << "  Total Chunk (FlatLayout) children: " << total_chunks << std::endl;
    size_t pos = 2;
    while (pos + 3 < offsets.size()) {
      uint64_t chunk_idx = offsets[pos];
      uint64_t row_offset = offsets[pos + 1];
      uint64_t row_count = offsets[pos + 2];
      uint64_t num_segs = offsets[pos + 3];
      std::cout << "  Chunk[" << chunk_idx << "]: row_offset=" << row_offset
                << " row_count=" << row_count
                << " segments=[";
      for (uint64_t s = 0; s < num_segs; s++) {
        if (s > 0) std::cout << ", ";
        std::cout << offsets[pos + 4 + s];
      }
      std::cout << "]" << std::endl;
      pos += 4 + num_segs;
    }
  }

  // Print zones info for all fields
  std::cout << "\n=== Zones Segments ===" << std::endl;
  for (const auto& name : field_names) {
    auto zones = vx_file.FieldZonesInfo(name);
    uint64_t count = zones[0];
    std::cout << "Field '" << name << "': " << count << " zones segment(s)" << std::endl;
    for (size_t i = 1; i + 2 < zones.size(); i += 3) {
      std::cout << "  SegmentId(" << zones[i] << "): byte_range=["
                << zones[i + 1] << ", " << (zones[i + 1] + zones[i + 2])
                << ") (" << zones[i + 2] << " bytes)" << std::endl;
    }
  }

  // Get and print byte ranges for all fields
  std::cout << "\n=== Byte Ranges ===" << std::endl;
  for (const auto& name : field_names) {
    auto ranges = vx_file.FieldByteRanges(name, static_cast<uint64_t>(file_size));
    ASSERT_GE(ranges.size(), 2u);
    ASSERT_EQ(ranges.size() % 2, 0u);
    PrintByteRanges(name, ranges, file_size);
  }

  // Step 4-7: Test sparse read for each field independently
  std::cout << "\n--- Sparse read verification ---" << std::endl;

  // Test int32 field
  {
    auto ranges = vx_file.FieldByteRanges("int32", static_cast<uint64_t>(file_size));
    std::unique_ptr<uint8_t[]> backing;
    auto sparse_buf = CreateSparseBuffer(full_buffer, ranges, file_size, backing);
    auto sparse_fs = std::make_shared<SingleBufferFileSystem>(test_file_, sparse_buf);

    VerifySparseRead(sparse_fs, schema_, 0, test_file_, properties_, kTotalRows,
        [this](const std::shared_ptr<arrow::StructArray>& s, int64_t offset) {
          auto col = std::dynamic_pointer_cast<arrow::Int32Array>(s->GetFieldByName("int32"));
          ASSERT_NE(col, nullptr);
          for (int64_t j = 0; j < col->length(); j++) {
            ASSERT_EQ(col->Value(j), all_i32_[offset + j]);
          }
        });
  }

  // Test int64 field
  {
    auto ranges = vx_file.FieldByteRanges("int64", static_cast<uint64_t>(file_size));
    std::unique_ptr<uint8_t[]> backing;
    auto sparse_buf = CreateSparseBuffer(full_buffer, ranges, file_size, backing);
    auto sparse_fs = std::make_shared<SingleBufferFileSystem>(test_file_, sparse_buf);

    VerifySparseRead(sparse_fs, schema_, 1, test_file_, properties_, kTotalRows,
        [this](const std::shared_ptr<arrow::StructArray>& s, int64_t offset) {
          auto col = std::dynamic_pointer_cast<arrow::Int64Array>(s->GetFieldByName("int64"));
          ASSERT_NE(col, nullptr);
          for (int64_t j = 0; j < col->length(); j++) {
            ASSERT_EQ(col->Value(j), all_i64_[offset + j]);
          }
        });
  }

  // Test float field
  {
    auto ranges = vx_file.FieldByteRanges("float", static_cast<uint64_t>(file_size));
    std::unique_ptr<uint8_t[]> backing;
    auto sparse_buf = CreateSparseBuffer(full_buffer, ranges, file_size, backing);
    auto sparse_fs = std::make_shared<SingleBufferFileSystem>(test_file_, sparse_buf);

    VerifySparseRead(sparse_fs, schema_, 2, test_file_, properties_, kTotalRows,
        [this](const std::shared_ptr<arrow::StructArray>& s, int64_t offset) {
          auto col = std::dynamic_pointer_cast<arrow::FloatArray>(s->GetFieldByName("float"));
          ASSERT_NE(col, nullptr);
          for (int64_t j = 0; j < col->length(); j++) {
            ASSERT_FLOAT_EQ(col->Value(j), all_f32_[offset + j]);
          }
        });
  }

  // Test str field
  {
    auto ranges = vx_file.FieldByteRanges("str", static_cast<uint64_t>(file_size));
    std::unique_ptr<uint8_t[]> backing;
    auto sparse_buf = CreateSparseBuffer(full_buffer, ranges, file_size, backing);
    auto sparse_fs = std::make_shared<SingleBufferFileSystem>(test_file_, sparse_buf);

    VerifySparseRead(sparse_fs, schema_, 3, test_file_, properties_, kTotalRows,
        [this](const std::shared_ptr<arrow::StructArray>& s, int64_t offset) {
          auto col = std::dynamic_pointer_cast<arrow::StringArray>(s->GetFieldByName("str"));
          ASSERT_NE(col, nullptr);
          for (int64_t j = 0; j < col->length(); j++) {
            ASSERT_EQ(col->GetString(j), all_str_[offset + j]);
          }
        });
  }

  std::cout << "\nSUCCESS: All 4 fields verified via sparse file scan ("
            << kTotalRows << " rows each)!" << std::endl;
  std::cout << "File preserved at: " << test_file_ << std::endl;
  std::cout << "Inspect with: vx browse " << test_file_ << std::endl;
}

}  // namespace milvus_storage
