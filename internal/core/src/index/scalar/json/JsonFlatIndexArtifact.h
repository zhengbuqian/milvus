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

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"
#include "tantivy-wrapper.h"

// The ARTIFACT of the JSON flat family (§6). File-shaped (tantivy).

namespace milvus::index {

class JsonFlatIndexArtifact final : public storage::Artifact {
 public:
    JsonFlatIndexArtifact(
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::string local_dir,
        std::vector<size_t> null_offsets,
        std::string nested_path);

    ~JsonFlatIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::string local_dir_;
    std::vector<size_t> null_offsets_;
    std::string nested_path_;
};

}  // namespace milvus::index
