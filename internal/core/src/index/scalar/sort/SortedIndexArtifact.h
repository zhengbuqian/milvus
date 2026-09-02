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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/scalar/sort/IndexStructure.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACTS of the sorted family (§6). Memory-shaped.
//
// Two artifacts for the same reason there are two readers: the numeric and
// string on-disk formats genuinely differ — the string one carries a magic
// number and a format version (`StringIndexSort.h:45-47`, validated in
// `ParseBinaryData`, `.cpp:86-139`) and the numeric one carries neither.
// Unifying the FORMATS is a follow-up; unifying the CLASSES without unifying
// the formats would only hide the difference behind a branch.

namespace milvus::index {

template <typename T>
class SortedIndexArtifact final : public storage::Artifact {
 public:
    SortedIndexArtifact(std::vector<IndexStructure<T>> data,
                        TargetBitmap valid_bitset,
                        size_t total_num_rows,
                        DataType value_type,
                        bool nested);

    ~SortedIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::vector<IndexStructure<T>> data_;
    TargetBitmap valid_bitset_;
    size_t total_num_rows_{0};
    DataType value_type_{DataType::NONE};
    bool nested_{false};
};

class SortedStringIndexArtifact final : public storage::Artifact {
 public:
    SortedStringIndexArtifact(std::vector<std::string> unique_values,
                              std::vector<std::vector<uint32_t>> posting_lists,
                              TargetBitmap valid_bitset,
                              size_t total_num_rows,
                              bool nested);

    ~SortedStringIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::vector<std::string> unique_values_;
    std::vector<std::vector<uint32_t>> posting_lists_;
    TargetBitmap valid_bitset_;
    size_t total_num_rows_{0};
    bool nested_{false};
};

}  // namespace milvus::index
