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
#include <string_view>
#include <utility>
#include <vector>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/contracts/IndexReader.h"

// JSON path-addressed predicate indexes. Pure mixin, does NOT derive from
// `IndexReaderBase` (§4) — it RETURNS one, which is a different thing.
//
// See core_refactor/01-scalar-index.md §5.7, and §12.4 for the open question
// about the cast type vocabulary.
//
// SCOPE IS NARROW. This family covers only JSON indexes built ON VALUES: the
// per-path cast index, and `JsonFlatIndex` (one tantivy index covering all
// paths of the field). JSON SHREDDING IS NOT HERE — `JsonKeyStats` (typed
// sub-columns, the shared BSON sub-column, and the BSON locator inverted index)
// is a PHYSICAL COLUMN LAYOUT, not an index, and moves out of the index
// component entirely (§1). With it goes the `NotImplemented` flood it caused.
// Within refactor phase 1 it lands in `segcore/json_stats/` (drop the
// inheritance clause + `git mv`); the end state puts the sub-columns in
// columnar-format and the layout directory in segcore.
//
// The only structural difference from a regular scalar index is ONE EXTRA LEVEL
// OF PATH ADDRESSING: on the same field, `a.b` is an int64 predicate interface
// and `a.c` a string one. So this family defines no new query semantics — only
// ROUTING.
//
// Cast-type and existence routing belong here instead of on the shared reader
// base.

namespace milvus::index {

// A path-resolution result never extends the lifetime of the field-level
// reader. Owned results contain only a lightweight path view; borrowed results
// point at an existing child reader. In both cases the caller must keep the
// parent cache pin alive while using the result.
class JsonResolvedReader {
 public:
    JsonResolvedReader() = default;

    JsonResolvedReader(const JsonResolvedReader&) = delete;
    JsonResolvedReader&
    operator=(const JsonResolvedReader&) = delete;

    JsonResolvedReader(JsonResolvedReader&& other) noexcept
        : owned_(std::move(other.owned_)), reader_(other.reader_) {
        if (owned_ != nullptr) {
            reader_ = owned_.get();
        }
        other.reader_ = nullptr;
    }

    JsonResolvedReader&
    operator=(JsonResolvedReader&& other) noexcept {
        if (this != &other) {
            owned_ = std::move(other.owned_);
            reader_ = owned_ != nullptr ? owned_.get() : other.reader_;
            other.reader_ = nullptr;
        }
        return *this;
    }

    static JsonResolvedReader
    Owned(std::unique_ptr<const IndexReaderBase> reader) noexcept {
        JsonResolvedReader result;
        result.owned_ = std::move(reader);
        result.reader_ = result.owned_.get();
        return result;
    }

    static JsonResolvedReader
    Borrowed(const IndexReaderBase* reader) noexcept {
        JsonResolvedReader result;
        result.reader_ = reader;
        return result;
    }

    const IndexReaderBase*
    get() const noexcept {
        return reader_;
    }

    const IndexReaderBase*
    operator->() const noexcept {
        return reader_;
    }

    explicit operator bool() const noexcept {
        return reader_ != nullptr;
    }

 private:
    std::unique_ptr<const IndexReaderBase> owned_;
    const IndexReaderBase* reader_{nullptr};
};

// NATIVE ENUM. Today `index::JsonValueType` is an ALIAS OF THE TANTIVY ENUM
// `::JsonExistValueType` (`index/JsonFlatIndex.h:31`,
// `thirdparty/tantivy/tantivy-binding/include/tantivy-binding.h:9`). An engine
// type has no business on a contract signature — engines are COMPOSED inside
// implementations (§3 principle 2), and the tantivy <-> native mapping belongs
// inside the JsonFlat implementation, exactly as the proto <-> native mapping
// belongs in plan/exec.
//
// !! NAME COLLISION, ON PURPOSE: this keeps §5.7's spelling. Until
// `JsonFlatIndex.h`'s alias is deleted as part of that class's migration to
// composition, both declare `milvus::index::JsonValueType` and the two headers
// cannot be included in one TU. That is a migration ordering constraint, not a
// design choice — the alias is scheduled to disappear (§8 mapping table:
// `JsonFlatIndex` -> `JsonIndexReader`).
enum class JsonValueType {
    Any,
    Numeric,
    String,
    Bool,
};

class JsonIndexReader {
 public:
    virtual ~JsonIndexReader() = default;

    // Is there a usable predicate interface at this path? If so, return the
    // type-erased base class; the consumer does ONE sibling cast, guided by
    // `cast_type`, to a `ScalarPredicateReader<T>` or a `PatternMatchReader`.
    //
    // Returning null means "no index at this path" and exec falls back to a
    // column scan. WHERE that scan lands — a shredded typed sub-column or the
    // raw JSON column — is columnar-format's business; this family neither knows
    // nor needs to know (§5.7).
    //
    // CAST TYPE VOCABULARY: `JsonCastType`, NOT `milvus::DataType`.
    // §5.7's snippet writes `DataType`, but §12.4 flags that as a decision, not
    // a transcription detail: today's vocabulary is `JsonCastType`
    // (`common/JsonCastType.h:25`), a closed six-value enum cut to TANTIVY's type
    // system (it has `ToTantivyType()`), while `milvus::DataType` can express
    // GEOMETRY (`common/Types.h:85`). Writing `DataType` would already be
    // choosing §12.4's option 1 — upgrading the vocabulary to the column type
    // system — and that question belongs to the wide-table modelling design, not
    // to this document. §12.4 says in as many words: "`JsonCastType` is the
    // conservative choice, `DataType` is betting early." This skeleton takes the
    // conservative one. Revisit when geo/timestamptz casts become real.
    virtual JsonResolvedReader
    Resolve(std::string_view path, JsonCastType cast_type) const = 0;

    // Path existence. An inverted index built on VALUES can answer this on its
    // own (`json_exist_query`) with no read-back of the column — that is exactly
    // what distinguishes it from the BSON locator inverted index, which must
    // re-read the blob (`ExistsExpr.cpp:264-270`) and is therefore column
    // layout, not an index (§1). The path must first have produced a non-empty
    // `CastTypesOf` result; see that method's routing contract below.
    virtual TargetBitmap
    Exists(std::string_view path,
           JsonValueType type = JsonValueType::Any) const = 0;

    // See the `Resolve` comment for why the vocabulary is `JsonCastType`; §5.7
    // writes `std::vector<DataType>` here for the same reason it writes
    // `DataType` there, and it is changed for the same reason.
    //
    // An empty result means this reader cannot answer the SHAPE of the path
    // (for example, JsonFlat cannot distinguish a positional array segment).
    // It does not mean that no row contains the path.  Callers must use this
    // answer before `Exists`: `Exists` is defined only for a path whose
    // `CastTypesOf` result is non-empty.  A supported path that happens to be
    // absent from every row still reports its available cast type here and
    // returns an all-zero bitmap from `Exists`.
    virtual std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const = 0;
};

// NOTE on the per-path cast index: it needs NO contract of its own. It is a
// plain `ScalarPredicateReader<T>`, registered in the inventory under the key
// `(field, path)`; for it `JsonIndexReader` degenerates to a lookup table
// (§5.7, §12.4).

}  // namespace milvus::index
