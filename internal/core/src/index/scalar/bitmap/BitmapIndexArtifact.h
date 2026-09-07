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
#include <map>
#include <memory>
#include <string>
#include <variant>

#include <roaring/roaring.hh>

#include "common/Types.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the bitmap family (§6). Memory-shaped: `Serialize` really
// does encode.

namespace milvus::index {

template <typename T>
struct BitmapRewriteStateType {
    using type = typename BitmapIndexReader<T>::OpenArgs;
};

template <>
struct BitmapRewriteStateType<std::string> {
    using type = BitmapStringIndexReader::OpenArgs;
};

template <typename T>
class BitmapIndexArtifact final : public storage::Artifact {
 public:
    using RewriteState = typename BitmapRewriteStateType<T>::type;

    BitmapIndexArtifact(std::map<T, roaring::Roaring> postings,
                        TargetBitmap valid_bitset,
                        size_t total_num_rows,
                        DataType value_type,
                        bool nested,
                        bool nullable,
                        bool value_lookup,
                        bool offset_cache);

    explicit BitmapIndexArtifact(RewriteState state);

    ~BitmapIndexArtifact() override;

    std::shared_ptr<storage::LoadedArtifact>
    OpenReader() const override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    struct BuilderState {
        std::map<T, roaring::Roaring> postings;
        TargetBitmap valid_bitset;
        size_t total_num_rows{0};
        DataType value_type{DataType::NONE};
        bool nested{false};
        bool nullable{false};
        bool value_lookup{true};
        bool offset_cache{false};
    };

    std::variant<BuilderState, RewriteState> state_;
};

}  // namespace milvus::index
