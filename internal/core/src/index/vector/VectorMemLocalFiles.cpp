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

#include "index/vector/VectorMemLocalFiles.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <utility>

#include <unistd.h>

#include "common/EasyAssert.h"

namespace milvus::index {

VectorMemLocalFiles::VectorMemLocalFiles(std::string directory)
    : directory_(std::move(directory)) {
}

std::shared_ptr<VectorMemLocalFiles>
VectorMemLocalFiles::Create(const std::string& parent) {
    if (parent.empty()) {
        ThrowInfo(UnexpectedError, "vector mmap staging parent is empty");
    }
    std::error_code error;
    std::filesystem::create_directories(parent, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create vector mmap staging root {}: {}",
                  parent,
                  error.message());
    }

    auto pattern =
        (std::filesystem::path(parent) / "vector_mem_XXXXXX").string();
    // Own the mutable template before mkdtemp. Once the directory exists,
    // cleanup is armed without another allocation.
    auto owner = std::shared_ptr<VectorMemLocalFiles>(
        new VectorMemLocalFiles(std::move(pattern)));
    if (::mkdtemp(owner->directory_.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create vector mmap staging directory in {}: {}",
                  parent,
                  std::strerror(errno));
    }
    owner->created_ = true;
    return owner;
}

VectorMemLocalFiles::~VectorMemLocalFiles() {
    if (!created_) {
        return;
    }
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
}

const std::string&
VectorMemLocalFiles::Directory() const {
    return directory_;
}

}  // namespace milvus::index
