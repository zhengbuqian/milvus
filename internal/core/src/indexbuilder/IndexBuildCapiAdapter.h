// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common/FieldData.h"
#include "index/IndexTypeAdapter.h"
#include "indexbuilder/IndexBuildService.h"
#include "pb/cgo_msg.pb.h"
#include "pb/index_cgo_msg.pb.h"

namespace milvus::indexbuilder {

// Selects the production request shape. Scalar, text, and vector requests are
// normalized once here before the unified BuildSession owns their lifecycle.
enum class BuildPurpose {
    ScalarIndex,
    TextIndex,
    VectorIndex,
};

// The protobuf adapter produces only the two native values whose ownership is
// needed by IndexBuildService. Family/value/type normalization is stored once
// in request; there is no parallel AdaptedIndexType copy to drift.
struct PreparedBuild {
    BuildRequest request;
    storage::FileManagerContext file_manager_context;
};

struct PreparedDirectBuild {
    DataType source_type{DataType::NONE};
    index::AdaptedIndexType adapted;
};

// Borrowed payload shape accepted by the existing six direct vector C
// entrances. payload_count is an element count for VECTOR_FLOAT/VECTOR_INT8,
// a byte count for VECTOR_FLOAT16/VECTOR_BFLOAT16/VECTOR_BINARY, and a native
// SparseRow object count for VECTOR_SPARSE_U32_F32. Binary dimensions are
// measured in bits. has_validity distinguishes an absent validity array from
// an explicitly empty one.
struct DirectVectorInput {
    DataType source_type{DataType::NONE};
    int64_t configured_dim{0};
    int64_t payload_count{0};
    const void* payload{nullptr};
    bool has_validity{false};
    const bool* valid_data{nullptr};
    int64_t logical_rows{0};
    int64_t sparse_dim{0};
};

PreparedBuild
AdaptBuildIndexInfo(const proto::indexcgo::BuildIndexInfo& info,
                    BuildPurpose purpose);

PreparedDirectBuild
AdaptDirectBuild(DataType source_type,
                 const proto::indexcgo::TypeParams& type_params,
                 const proto::indexcgo::IndexParams& index_params);

// Converts the legacy direct-build C payload into one owning native batch.
// Bool and string payload sizes are protobuf byte lengths; fixed-width sizes
// are logical row counts, matching the existing C ABI.
FieldDataPtr
AdaptDirectScalarFieldData(DataType source_type,
                           int64_t size,
                           const void* field_data);

// Converts compact direct vector input into one owning FieldData batch. With
// validity, logical_rows is the validity domain and its true count must equal
// the compact physical payload rows. Without validity, the logical domain is
// exactly the physical payload row count.
FieldDataPtr
AdaptDirectVectorFieldData(const DirectVectorInput& input);

// Existing C-ABI projection of published artifact statistics. Text and scalar
// share the same native ArtifactStats; storage-path selection already made the
// returned file names relative to their correct remote namespace.
proto::cgo::IndexStats
AdaptArtifactStats(const storage::ArtifactStats& stats);

}  // namespace milvus::indexbuilder
