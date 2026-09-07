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

#include "index/vector/VectorDiskLocalFiles.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <utility>

#include <unistd.h>

#include "common/EasyAssert.h"

namespace milvus::index {

VectorDiskLocalFiles::VectorDiskLocalFiles(std::string directory)
    : directory_(std::move(directory)) {
}

std::shared_ptr<VectorDiskLocalFiles>
VectorDiskLocalFiles::Create(const std::string& parent) {
    if (parent.empty()) {
        ThrowInfo(UnexpectedError, "vector disk staging parent is empty");
    }
    std::error_code error;
    std::filesystem::create_directories(parent, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create vector disk staging root {}: {}",
                  parent,
                  error.message());
    }

    auto pattern =
        (std::filesystem::path(parent) / "vector_disk_XXXXXX").string();
    auto owner = std::shared_ptr<VectorDiskLocalFiles>(
        new VectorDiskLocalFiles(std::move(pattern)));
    if (::mkdtemp(owner->directory_.data()) == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to create vector disk staging directory in {}: {}",
                  parent,
                  std::strerror(errno));
    }
    owner->created_ = true;
    return owner;
}

VectorDiskLocalFiles::~VectorDiskLocalFiles() {
    if (!created_) {
        return;
    }
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
}

const std::string&
VectorDiskLocalFiles::Directory() const {
    return directory_;
}

bool
VectorDiskLocalFiles::Owns(const std::string& path) const {
    if (!created_ || path.empty()) {
        return false;
    }
    std::error_code error;
    const auto base = std::filesystem::weakly_canonical(directory_, error);
    if (error) {
        return false;
    }
    const auto candidate = std::filesystem::weakly_canonical(path, error);
    if (error || candidate == base) {
        return false;
    }
    auto base_it = base.begin();
    auto candidate_it = candidate.begin();
    for (; base_it != base.end(); ++base_it, ++candidate_it) {
        if (candidate_it == candidate.end() || *base_it != *candidate_it) {
            return false;
        }
    }
    return true;
}

}  // namespace milvus::index
