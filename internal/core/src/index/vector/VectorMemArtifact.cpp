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

#include "index/vector/VectorMemArtifact.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/EasyAssert.h"
#include "index/vector/VectorMemReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "knowhere/binaryset.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

constexpr const char* kEmptyEmbListOffsets = "empty_emb_list_offsets";

void
AppendEmptyEmbListOffsets(const KnowhereEngine& engine,
                          knowhere::BinarySet& entries) {
    const auto& offsets = engine.EmptyEmbListOffsets();
    if (offsets.empty()) {
        return;
    }

    const auto wire_count = ToValidDataCount(offsets.size());
    constexpr size_t header_size = sizeof(int64_t) + sizeof(uint64_t);
    if (offsets.size() >
        (std::numeric_limits<size_t>::max() - header_size) / sizeof(size_t)) {
        ThrowInfo(UnexpectedError,
                  "empty embedding-list offset payload size overflows");
    }
    const auto payload_size = header_size + offsets.size() * sizeof(size_t);
    if (payload_size >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(UnexpectedError,
                  "empty embedding-list offset payload exceeds wire size");
    }

    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[payload_size]);
    auto* cursor = data.get();
    const auto dim = engine.Dim();
    std::memcpy(cursor, &dim, sizeof(dim));
    cursor += sizeof(dim);
    std::memcpy(cursor, &wire_count, sizeof(wire_count));
    cursor += sizeof(wire_count);
    std::memcpy(cursor, offsets.data(), offsets.size() * sizeof(size_t));
    entries.Append(kEmptyEmbListOffsets,
                   std::move(data),
                   static_cast<int64_t>(payload_size));
}

void
ValidateEntries(const knowhere::BinarySet& entries) {
    for (const auto& [name, entry] : entries.binary_map_) {
        AssertInfo(entry != nullptr,
                   "serialized vector entry {} has no descriptor",
                   name);
        AssertInfo(entry->size >= 0,
                   "serialized vector entry {} has negative size {}",
                   name,
                   entry->size);
        AssertInfo(static_cast<uint64_t>(entry->size) <=
                       std::numeric_limits<size_t>::max(),
                   "serialized vector entry {} exceeds platform size",
                   name);
        AssertInfo(entry->size == 0 || entry->data != nullptr,
                   "serialized vector entry {} has null data",
                   name);
    }
}

}  // namespace

template <typename T>
VectorMemArtifact<T>::VectorMemArtifact(
    KnowhereEngine engine,
    VectorValidData valid,
    std::vector<size_t> empty_emb_list_offsets)
    : engine_(std::move(engine)), valid_(std::move(valid)) {
    const auto& existing = engine_.EmptyEmbListOffsets();
    if (!empty_emb_list_offsets.empty()) {
        AssertInfo(existing.empty() || existing == empty_emb_list_offsets,
                   "artifact offsets conflict with the engine's existing "
                   "empty embedding-list offsets");
        if (existing.empty()) {
            engine_.SetEmptyEmbListOffsets(std::move(empty_emb_list_offsets));
        }
    }

    const auto& offsets = engine_.EmptyEmbListOffsets();
    if (offsets.empty()) {
        return;
    }
    AssertInfo(engine_.ElemType() != DataType::NONE,
               "empty embedding-list offsets require embedding-list mode");
    AssertInfo(offsets.front() == 0,
               "empty embedding-list offsets must start at zero");
    AssertInfo(offsets.back() == 0,
               "empty embedding-list offsets must contain no vectors");
    AssertInfo(std::is_sorted(offsets.begin(), offsets.end()),
               "empty embedding-list offsets must be monotonic");
}

template <typename T>
VectorMemArtifact<T>::VectorMemArtifact(
    KnowhereEngine engine,
    VectorValidData valid,
    std::shared_ptr<VectorMemLocalFiles> local_files)
    : local_files_(std::move(local_files)),
      engine_(std::move(engine)),
      valid_(std::move(valid)) {
}

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
VectorMemArtifact<T>::OpenReader() const {
    // A handle copy of the knowhere node plus a shared validity mapping — see
    // the note on `KnowhereEngine`'s copy ctor.
    return std::make_shared<VectorMemReader<T>>(engine_, valid_, local_files_);
}

template <typename T>
void
VectorMemArtifact<T>::Serialize(storage::FileSink& sink) const {
    if (sink.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "in-memory vector artifacts have no V3 persisted format");
    }

    knowhere::BinarySet entries;
    if (!engine_.EmptyEmbListOffsets().empty()) {
        AppendEmptyEmbListOffsets(engine_, entries);
    } else if (!IsAllNullNullable(valid_.Mapping())) {
        const auto status = engine_.Raw().Serialize(entries);
        if (status != knowhere::Status::success) {
            ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                      "failed to serialize vector index: status {} ({})",
                      static_cast<int>(status),
                      knowhere::Status2String(status));
        }
    }
    AppendValidDataToBinarySet(valid_.Mapping(), entries);
    ValidateEntries(entries);

    // FileSink owns physical slicing and publication. All entries are prepared
    // and validated before the first write; a later sink failure leaves that
    // sink failed and must not be followed by Finish().
    for (const auto& [name, entry] : entries.binary_map_) {
        sink.WriteEntry(
            name, entry->data.get(), static_cast<size_t>(entry->size));
    }
}

template class VectorMemArtifact<float>;
template class VectorMemArtifact<bin1>;
template class VectorMemArtifact<float16>;
template class VectorMemArtifact<bfloat16>;
template class VectorMemArtifact<int8>;
template class VectorMemArtifact<sparse_u32_f32>;

}  // namespace milvus::index
