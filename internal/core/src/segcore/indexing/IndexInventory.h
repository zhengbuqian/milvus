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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "index/contracts/IndexReader.h"
#include "index/contracts/JsonIndexReader.h"
#include "index/contracts/NgramReader.h"
#include "index/contracts/NullReader.h"
#include "index/contracts/PatternMatchReader.h"
#include "index/contracts/ScalarPredicateReader.h"
#include "index/contracts/ScalarValueReader.h"
#include "index/contracts/SpatialReader.h"
#include "index/contracts/TextMatchReader.h"
#include "segcore/indexing/FieldIndexCapability.h"
#include "segcore/indexing/Pinned.h"

// The segment's index inventory: the one place that knows WHICH indexes this
// segment has, holds their cache slots, and hands out typed pinned reader
// interfaces.
//
// See core_refactor/01-scalar-index.md §4.3 (the object model) and
// core_refactor/README.md §8.1 ("load stays L3 — load builds the inventory")
// and §8 item 4 ("the narrow contracts are defined in index itself; segcore's
// adapter layer is deleted and the indexing module degenerates to a
// segment-level inventory plus pin management"). This header is that
// degenerated module.
//
// -------------------------------------------------------------------------
// OBJECT MODEL (§4.3's table). THREE THINGS WITH THREE DIFFERENT LIFETIMES:
//
//   implementation instance (`InvertedIndex<int64_t>` ...)
//       created by  : `index::IndexLoader::Open()`, i.e. AT INDEX LOAD TIME
//       lives       : long; rebuilt only when cachinglayer evicts it and it is
//                     loaded again
//       cost        : one load
//
//   `index::ReaderCaps`
//       created by  : `index::IndexLoader::DeriveCaps(load_meta)` when the
//                     inventory entry is built
//       lives       : as long as the inventory entry
//       cost        : a pure data copy
//
//   `Pinned<ReaderT>`
//       created by  : the Pin* exits below, ONCE PER EXPRESSION NODE
//       lives       : the stack frame that holds it
//       cost        : two pointers
//
// So the query interface is NOT a per-query proxy. It is an interface view of
// the long-lived index object. §4.3 spells the query-time sequence out:
//
//   1. caps    = inventory.Capability(field_id)   // pure data: no pin, no cast
//   2. exec decides the path from caps
//   3. pinned  = inventory.PinScalarPredicate<T>(op_ctx, key)   // once per node
//         inside: slot->PinCells() -> IndexReaderBase*
//                 sibling cast     -> ScalarPredicateReader<T>*
//                 return Pinned{keep_alive, ptr}
//   4. per batch: pinned->In(...)                 // direct virtual call
//
// Step 1 MUST NOT PIN — hard constraint, see FieldIndexCapability.h.
//
// -------------------------------------------------------------------------
// THE INVENTORY HOLDS `CacheSlot`, NOT THE INDEX OBJECT (§4.3).
// Today that is `index::CacheIndexBasePtr = shared_ptr<CacheSlot<IndexBase>>`
// (`index/Index.h:167`); after refactor phase 1 it is
// `CacheSlot<index::IndexReaderBase>` — `IndexReaderBase` takes over
// `IndexBase`'s type-erased handle role (§11.2 item 3). The index object lives
// INSIDE the slot and can be evicted, which is exactly why taking a reader
// interface is pin-then-cast and never cast-then-pin: before the pin the object
// may not exist at all.
//
// ON `dynamic_cast`: the sibling cast below IS a `dynamic_cast`, and that is
// the design (§4 note: "IndexReaderBase -> reader interface is a single
// sibling cast"). §10 rule 3 / the refactor phase 1 exit criterion "exec's
// `dynamic_cast` to concrete indexes drops to zero" is about EXEC casting to
// CONCRETE INDEX CLASSES; it is not a ban on the one base-class-to-reader
// cast that this pin exit performs.

namespace milvus::segcore {

class IndexInventory {
 public:
    using RootSlot =
        std::shared_ptr<milvus::cachinglayer::CacheSlot<index::IndexReaderBase>>;

    struct Entry {
        // Pure data, readable without a pin.
        IndexCapabilityEntry meta;

        // The type-erased base class. Holding `CacheSlot<IndexReaderBase>` (not
        // `CacheSlot<InvertedIndex<int64_t>>`) is what lets one inventory hold
        // every family; §3 principle 4: "templates stay on the hot path, type
        // erasure only on the management plane".
        RootSlot slot;
    };

    // ---- Build side (load) --------------------------------------------------
    // Called by segcore's load path once per index, after
    // `index::IndexLoader::Open()` produced the reader and
    // `index::IndexLoader::DeriveCaps()` produced the caps.
    //
    // TODO: move existing logic here — the registration sites today are
    // `ChunkedSegmentSealedImpl::LoadIndex` / `LoadScalarIndex` filling
    // `RuntimeResourceState::scalar_indexings` /`ngram_indexings` /
    // `json_indices` / `text_indexes` (segcore/ChunkedSegmentSealedImpl.h:347-360)
    // and `SealedIndexingRecord::append_field_indexing`
    // (segcore/SealedIndexingRecord.h:36). Those five parallel maps collapse
    // into this one table plus the caps record.
    void
    Register(Entry entry);

    void
    Drop(const IndexKey& key);

    void
    Clear();

    // ---- Capability plane: NO PIN (§4.1 hard constraint) --------------------
    // Aggregates the caps of every entry on `field_id`. This is the ONLY thing
    // `exec::DetermineExecPath` is allowed to read.
    //
    // TODO: move existing logic here — the pre-pin existence probes today are
    // `SegmentInternalInterface::HasIndex` / `HasJsonIndex` /
    // `GetJsonFlatIndexNestedPath` (segcore/SegmentInterface.h:236-243) and
    // `SegmentExpr::HasCompatibleScalarIndex` (exec/expression/Expr.h:2573).
    FieldIndexCapability
    Capability(FieldId field_id) const;

    // ---- Pin exits: one per query interface (§4.3 step 3) -------------------
    // Every one of these is "pin the slot, sibling-cast the base class to the
    // reader interface, wrap in `Pinned<>`", plus the §4.1 consistency
    // assertion
    //     Assert(SameCaps(reader->Caps(), entry.meta.caps))
    // An empty `Pinned<>` means the reader interface is absent — never an
    // exception (§3 principle 3, §10 rule 4).
    //
    // FREQUENCY IS ONCE PER EXPRESSION NODE. §4.3: this is the shape today's
    // `SegmentExpr::EnsurePinnedIndex()` (exec/expression/Expr.h:395,
    // idempotent, called only after the path is committed) already has;
    // refactor phase 1 moves it out of exec and into here. The per-batch
    // `dynamic_cast`s that DO exist today are on the by-offsets path
    // (`ProcessIndexChunksByOffsets`, Expr.h:698; and
    // `ProcessIndexLookupByOffsetsImpl`, Expr.h:765) and disappear once the
    // handle is typed.

    template <typename T>
    Pinned<index::ScalarPredicateReader<T>>
    PinScalarPredicate(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — see
        // exec/expression/Expr.h:678,745,2564 (`dynamic_cast<const Index*>`
        // on `pinned_index_[0]`).
        return PinReader<index::ScalarPredicateReader<T>>(op_ctx, key);
    }

    Pinned<index::PatternMatchReader>
    PinPatternMatch(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — exec/expression/UnaryExpr.h:646,718
        // (`index->SupportPatternMatch()` probe + call). The probe is gone:
        // `caps.pattern_match` answers it before the pin.
        return PinReader<index::PatternMatchReader>(op_ctx, key);
    }

    Pinned<index::TextMatchReader>
    PinTextMatch(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — segcore/SegmentInterface.h:221
        // (`GetTextIndex`, which returns `PinWrapper<index::TextMatchIndex*>`,
        // i.e. a CONCRETE class) and its `TextIndexVariant` storage in
        // segcore/ChunkedSegmentSealedImpl.h:320-323.
        return PinReader<index::TextMatchReader>(op_ctx, key);
    }

    // Candidate family. `caps.exact == false`: exec verifies (§5.4).
    Pinned<index::NgramReader>
    PinNgram(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — segcore/SegmentInterface.h:265,268
        // (`GetNgramIndex` / `GetNgramIndexForJson`, both returning the
        // concrete `index::NgramInvertedIndex*`).
        return PinReader<index::NgramReader>(op_ctx, key);
    }

    // Candidate family. `caps.exact == false`: exec refines (§5.6).
    Pinned<index::SpatialReader>
    PinSpatial(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — the RTree pin currently rides the
        // generic `PinIndex` path (segcore/SegmentInterface.h:245) and exec
        // casts to `index::RTreeIndex` itself.
        return PinReader<index::SpatialReader>(op_ctx, key);
    }

    // Unconditional cross-family interface: every scalar family really
    // implements it, so it carries no `ReaderCaps` bit (§5 "cross-family
    // interface"). Present whenever the field has any scalar index at all —
    // including RTree, whose `IsNull`/`IsNotNull` are real implementations on a
    // live path (`PhyNullExpr` -> `ProcessIndexChunksForValid`, Expr.h:2457).
    Pinned<index::NullReader>
    PinNull(milvus::OpContext* op_ctx, const IndexKey& key) const {
        return PinReader<index::NullReader>(op_ctx, key);
    }

    template <typename T>
    Pinned<index::ScalarValueReader<T>>
    PinValue(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — `ReverseDataFromIndex`
        // (segcore/Utils.h:151) for reduce materialization, and the
        // `SupportFastReverseLookup()` probes at
        // exec/expression/BloomFilterExpr.h:398 and
        // exec/expression/RoaringFilterExpr.h:179. The probe becomes
        // `caps.cheap_value_lookup`, read before the pin; the DECISION
        // ("reverse lookup is too expensive, go back to the raw column")
        // stays with the consumer (§5.5).
        return PinReader<index::ScalarValueReader<T>>(op_ctx, key);
    }

    Pinned<index::JsonIndexReader>
    PinJson(milvus::OpContext* op_ctx, const IndexKey& key) const {
        // TODO: move existing logic here — segcore/SegmentInterface.h:222
        // (`PinJsonIndex`, which returns
        // `std::vector<PinWrapper<const index::IndexBase*>>` and leaves exec
        // to `dynamic_cast` to `index::JsonFlatIndex`, e.g.
        // exec/expression/Expr.h:2654 `PinnedJsonIndexIsFlat`).
        return PinReader<index::JsonIndexReader>(op_ctx, key);
    }

 private:
    // The single implementation of "pin the base class, sibling-cast to the
    // reader interface".
    //
    // Body (to write):
    //   auto it = entries_.find(key);
    //   if (it == entries_.end() || it->second.slot == nullptr) return {};
    //   auto accessor = SemiInlineGet(it->second.slot->PinCells(op_ctx, {0}));
    //   const index::IndexReaderBase* base = accessor->get_cell_of(0);
    //   // §4.1 consistency assertion — the caps the path decision was made on
    //   // must be the caps the object actually reports.
    //   AssertInfo(SameCaps(base->Caps(), it->second.meta.caps),
    //              "inventory caps diverged from reader caps for field {}",
    //              key.field_id.get());
    //   const auto* reader = dynamic_cast<const ReaderT*>(base);  // cross cast
    //   if (reader == nullptr) return {};
    //   return Pinned<ReaderT>(std::move(accessor), reader);
    //
    // TODO: move existing logic here — the pin half is
    // `ChunkedSegmentSealedImpl.h:159` (`SemiInlineGet(slot->PinCells(op_ctx,
    // {0}))`) and `JsonKeyStats::GetBsonIndex`
    // (segcore/json_stats/JsonKeyStats.h, same three lines); the cast half is
    // every `dynamic_cast` listed on the Pin* methods above.
    template <typename ReaderT>
    Pinned<ReaderT>
    PinReader(milvus::OpContext* op_ctx, const IndexKey& key) const;

    std::unordered_map<IndexKey, Entry, IndexKeyHash> entries_;
};

// -------------------------------------------------------------------------
// NOT IN THIS HEADER, ON PURPOSE
//
// 1. THE ELEMENT -> ROW MAPPING (`ArrayOffsetsSealed`). §5.8 settles where it
//    belongs: the mapping is a derivative of the COLUMN, and the index does not
//    own it. It is produced by COLUMN load
//    (`ArrayOffsetsSealed::BuildFromColumn`,
//    segcore/ChunkedSegmentSealedImpl.cpp:6963), SHARED PER STRUCT
//    (`struct_to_array_offsets[struct_name]`, `:6957-6966`, with
//    `array_offsets_map[field_id]` only a second lookup key) and REPLACED WITH
//    THE COLUMN (erased on release/reopen at `:4003,4018`, copied into the new
//    runtime snapshot on COW at `:1029,1425`). It therefore stays in the
//    segment's runtime state — `RuntimeResourceState::struct_to_array_offsets`
//    / `array_offsets_map`, reached through
//    `SegmentInternalInterface::GetArrayOffsets` — and is NEITHER held by nor
//    injected into any index. Doing otherwise would break the reader's
//    "immutable after `Seal()`/`Open()`" guarantee and, worse, let a reader
//    capture a `shared_ptr` that goes STALE (not dangling) across a column
//    reopen: silently wrong results.
//
// 2. CACHE ACCOUNTING. `CellByteSize` / `SetCellSize` / `ComputeByteSize` come
//    off the index base class (§4.2); accounting belongs to the load-side
//    translator.
//    Note the open question §12.3: `cell_size_` has TWO INCOMPATIBLE fillings
//    today (pre-compression FILE size for most families via
//    `V1SealedIndexTranslator.cpp:157`, measured RESIDENT memory for text match
//    and FMIndex via `TextMatchIndexTranslator.cpp:125`), and the cache uses it
//    for admission and eviction. Do not "unify the accessors" without first
//    defining the number.
//
// 3. THE GROWING SIDE. Appenders are held by `GrowingIndexSet`
//    (segcore/indexing/GrowingIndexSet.h) and its read path is a SHARED
//    SNAPSHOT, not a pin — §4.3 "the asymmetry on the growing side".

}  // namespace milvus::segcore
