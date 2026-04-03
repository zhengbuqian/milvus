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
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace milvus {

/// Read-only Arrow FileSystem backed by in-memory buffers.
///
/// Files are registered via Register(path, buffer) and accessed through the
/// standard Arrow FileSystem interface.  The `type_name()` is "mem" so that
/// FilesystemCache can route mem:// URIs to this implementation.
class BufferFileSystem : public arrow::fs::FileSystem {
 public:
    /// Global singleton — all vortex translators share the same FS,
    /// distinguished by unique mem:// paths.
    static std::shared_ptr<BufferFileSystem>
    getInstance() {
        static auto instance = std::make_shared<BufferFileSystem>();
        return instance;
    }

    BufferFileSystem() = default;
    ~BufferFileSystem() override = default;

    // ---- buffer management ----

    void
    Register(const std::string& path, std::shared_ptr<arrow::Buffer> buffer) {
        std::lock_guard<std::mutex> lk(mu_);
        auto [_, inserted] = buffers_.emplace(path, std::move(buffer));
        if (!inserted) {
            throw std::runtime_error(
                "BufferFileSystem: path already registered: " + path);
        }
    }

    void
    Unregister(const std::string& path) {
        std::lock_guard<std::mutex> lk(mu_);
        buffers_.erase(path);
    }

    // ---- FileSystem interface ----

    std::string
    type_name() const override {
        return "mem";
    }

    bool
    Equals(const arrow::fs::FileSystem& other) const override {
        return this == &other;
    }

    arrow::Result<arrow::fs::FileInfo>
    GetFileInfo(const std::string& path) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = buffers_.find(path);
        if (it == buffers_.end()) {
            arrow::fs::FileInfo info;
            info.set_path(path);
            info.set_type(arrow::fs::FileType::NotFound);
            return info;
        }
        arrow::fs::FileInfo info;
        info.set_path(path);
        info.set_type(arrow::fs::FileType::File);
        info.set_size(it->second->size());
        return info;
    }

    arrow::Result<arrow::fs::FileInfoVector>
    GetFileInfo(const arrow::fs::FileSelector& /*select*/) override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::GetFileInfo(selector)");
    }

    arrow::Status
    CreateDir(const std::string& /*path*/, bool /*recursive*/) override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::CreateDir");
    }

    arrow::Status
    DeleteDir(const std::string& /*path*/) override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::DeleteDir");
    }

    arrow::Status
    DeleteDirContents(const std::string& /*path*/,
                      bool /*missing_dir_ok*/) override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::DeleteDirContents");
    }

    arrow::Status
    DeleteRootDirContents() override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::DeleteRootDirContents");
    }

    arrow::Status
    DeleteFile(const std::string& path) override {
        std::lock_guard<std::mutex> lk(mu_);
        buffers_.erase(path);
        return arrow::Status::OK();
    }

    arrow::Status
    Move(const std::string& /*src*/,
         const std::string& /*dest*/) override {
        return arrow::Status::NotImplemented("BufferFileSystem::Move");
    }

    arrow::Status
    CopyFile(const std::string& /*src*/,
             const std::string& /*dest*/) override {
        return arrow::Status::NotImplemented("BufferFileSystem::CopyFile");
    }

    arrow::Result<std::shared_ptr<arrow::io::InputStream>>
    OpenInputStream(const std::string& path) override {
        ARROW_ASSIGN_OR_RAISE(auto buf, LookupBuffer(path));
        return std::make_shared<arrow::io::BufferReader>(buf);
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ARROW_ASSIGN_OR_RAISE(auto buf, LookupBuffer(path));
        return std::make_shared<arrow::io::BufferReader>(buf);
    }

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    OpenOutputStream(
        const std::string& /*path*/,
        const std::shared_ptr<const arrow::KeyValueMetadata>& /*metadata*/)
        override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::OpenOutputStream");
    }

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    OpenAppendStream(
        const std::string& /*path*/,
        const std::shared_ptr<const arrow::KeyValueMetadata>& /*metadata*/)
        override {
        return arrow::Status::NotImplemented(
            "BufferFileSystem::OpenAppendStream");
    }

 private:
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    LookupBuffer(const std::string& path) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = buffers_.find(path);
        if (it == buffers_.end()) {
            return arrow::Status::IOError(
                "BufferFileSystem: file not found: ", path);
        }
        return it->second;
    }

    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

}  // namespace milvus
