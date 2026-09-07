// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "filemanager/FileManager.h"

namespace milvus::storage {
class DiskFileManagerImpl;
struct FileManagerContext;
}  // namespace milvus::storage

namespace milvus::index {

// Read-only FileManager for stream-loading a disk-vector artifact. It retains
// the real storage manager but opens only exact remote objects advertised by
// the current engine-entry inventory.
class VectorDiskLoadFileManager final
    : public milvus::FileManager,
      public std::enable_shared_from_this<VectorDiskLoadFileManager> {
 public:
    VectorDiskLoadFileManager(
        const storage::FileManagerContext& context,
        const std::vector<std::string>& remote_paths,
        const std::vector<std::string>& engine_entry_names);
    ~VectorDiskLoadFileManager() override;

    bool
    LoadFile(const std::string& filename) override;
    bool
    AddFile(const std::string& filename) override;
    bool
    AddFileMeta(const milvus::FileMeta& file_meta) override;
    std::optional<bool>
    IsExisted(const std::string& filename) override;
    bool
    RemoveFile(const std::string& filename) override;
    std::shared_ptr<milvus::InputStream>
    OpenInputStream(const std::string& filename) override;
    std::shared_ptr<milvus::OutputStream>
    OpenOutputStream(const std::string& filename) override;

    std::string
    LocalIndexPrefix() const;
    void
    RethrowFirstFailure() const;
    void
    RecordFailure(std::exception_ptr failure) noexcept;

 private:
    std::string
    ResolveRemotePath(const std::string& filename) const;
    void
    ValidateRawPath(const std::string& filename) const;

    std::shared_ptr<storage::DiskFileManagerImpl> delegate_;
    std::unordered_set<std::string> allowed_remote_paths_;
    mutable std::mutex mutex_;
    std::exception_ptr first_failure_;
};

}  // namespace milvus::index
