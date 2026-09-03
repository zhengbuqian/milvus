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
#include <utility>

#include "common/GrowingOffsetMapping.h"
#include "common/OffsetMapping.h"
#include "common/SealedOffsetMapping.h"

// The nullable-vector offset mapping, HELD BY COMPOSITION.
//
// See core_refactor/01-scalar-index.md §11.3 (re-homing) and §3 principle 2.
//
// A nullable vector field does not hand its null rows to knowhere, so the index
// numbers rows in a denser PHYSICAL space and every consumer of a hit has to map
// back. Today this state (`offset_mapping_` plus six accessors and two mutators)
// sits on the shared base class `VectorIndex` (`index/VectorIndex.h:192-241,278`)
// and is therefore inherited by both index families. `IndexBase` retires and
// implementation classes may not inherit each other (§10 rule 3), so it becomes
// a member.
//
// THE MUTATORS AND THE ACCESSORS BELONG TO DIFFERENT INTERFACES, and separating
// them is most of the value of moving this out of a base class:
//   - `Append` (today `VectorIndex::UpdateValidData`) is the APPENDER's
//     (`segcore/FieldIndexing.cpp:333,409,486,562` call it per batch);
//   - `Build` (today `VectorIndex::BuildValidData`) is the BUILDER's
//     (`indexbuilder/VecIndexCreator.cpp:78,82`,
//     `segcore/storagev1translator/InterimSealedIndexTranslator.cpp:202,237`);
//   - everything else is the READER's, exposed through
//     `VectorNullableReader` (VectorFamilyReaders.h).
// On today's class all eight are public on every vector index at every stage.

namespace milvus::index {

class VectorValidData {
 public:
    // Starts in growing shape, matching today's `VectorIndex` ctor
    // (`index/VectorIndex.h:50-55`), so an appender can push validity in before
    // anything is sealed.
    VectorValidData()
        : mapping_(std::make_shared<milvus::GrowingOffsetMapping>()) {
    }

    // --- appender side ------------------------------------------------------

    // Today `VectorIndex::UpdateValidData` (`index/VectorIndex.h:192-199`):
    // asserts the mapping is still the growing one, then appends.
    void
    Append(const bool* valid_data, int64_t count);

    // --- builder side -------------------------------------------------------

    // Today `VectorIndex::BuildValidData` (`index/VectorIndex.h:201-208`):
    // replaces the mapping with a sealed one built in one shot. `options` decides
    // whether the sealed mapping is mmap-backed
    // (`VectorIndexValidDataUtils.h:139-156`).
    void
    Build(const bool* valid_data,
          int64_t total_count,
          const milvus::OffsetMappingBuildOptions& options = {});

    void
    Reset(std::shared_ptr<milvus::OffsetMapping> mapping) {
        mapping_ = std::move(mapping);
    }

    // --- reader side (see VectorNullableReader) -----------------------------

    bool
    Enabled() const {
        return mapping_->IsEnabled();
    }

    int64_t
    ValidCount() const {
        return mapping_->GetValidCount();
    }

    bool
    IsRowValid(int64_t logical_offset) const {
        return !mapping_->IsEnabled() || mapping_->IsValid(logical_offset);
    }

    int64_t
    PhysicalOffset(int64_t logical_offset) const {
        return mapping_->GetPhysicalOffset(logical_offset);
    }

    int64_t
    LogicalOffset(int64_t physical_offset) const {
        return mapping_->GetLogicalOffset(physical_offset);
    }

    // Borrowed. `query/` and `exec/` take this by reference and, in the
    // iterator case, keep a RAW POINTER to it with no pin — see §12.1(b) and
    // the note in contracts/VectorReaders.h. Refactor phase 1 deliberately does
    // not change that.
    const milvus::OffsetMapping&
    Mapping() const {
        return *mapping_;
    }

 private:
    // SHARED, not unique: an artifact and the reader it opens in place hold the
    // same mapping (`storage::Artifact::OpenReader()` is const, §6.2), and on the
    // growing side one appender and every snapshot it has handed out share one
    // mapping — which is already today's behaviour, since
    // `VectorIndex::UpdateValidData` appends to the same `GrowingOffsetMapping`
    // that concurrent queries are reading through `GetOffsetMapping()`.
    std::shared_ptr<milvus::OffsetMapping> mapping_;
};

}  // namespace milvus::index
