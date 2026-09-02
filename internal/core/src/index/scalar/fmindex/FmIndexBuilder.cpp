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

#include "index/scalar/fmindex/FmIndexBuilder.h"

#include <utility>

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/fmindex/FmIndexArtifact.h"

namespace milvus::index {

FmIndexBuilder::FmIndexBuilder(FmIndexBuildParams params)
    : params_(std::move(params)) {
    // TODO: move existing logic here (see FMIndex.cpp:148-162 — the parameter
    // clamping `sa_sample_rate == 0 -> 8`, `block_bytes == 0 -> 64`).
    // NOT moved: the `MemFileManagerImpl` / `DiskFileManagerImpl` construction
    // at FMIndex.cpp:157-161 (§10 rule 2).
}

FmIndexBuilder::~FmIndexBuilder() = default;

BuilderInputSpec
FmIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
FmIndexBuilder::Add(size_t n, const std::string_view* values,
                    const bool* valid) {
    // TODO: move existing logic here (see FMIndex.cpp:180-240 — the per-row
    // document/validity collection and null-bitmap accumulation half of
    // BuildWithFieldData).
    //
    // Gone: the `storage::CacheRawDataAndFillMissing` entry point
    // (FMIndex.cpp:164-169). §6.1.2 replaces the three near-identical raw-data
    // pullers (`CacheRawDataAndFillMissing` / `CacheRawDataToMemory` /
    // `CacheRawDataToDisk`) with ONE shared materializer driven by
    // `InputSpec()`; the family no longer owns an ingest path.
}

storage::ArtifactPtr
FmIndexBuilder::Seal() && {
    // TODO: move existing logic here (see FMIndex.cpp:244-251 — the libsais
    // build via the file-local BuildFMIndexLibrary helper at FMIndex.cpp:80-105,
    // plus ComputeTotalTokens at FMIndex.h:318-327).
    return nullptr;
}

namespace {

const bool kFmBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kFmIndex, [](const BuildParams& params) {
            // TODO: read FM_SA_SAMPLE_RATE / FM_BLOCK_BYTES (index/Meta.h:65,68).
            return std::make_unique<FmIndexBuilder>(FmIndexBuildParams{});
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
