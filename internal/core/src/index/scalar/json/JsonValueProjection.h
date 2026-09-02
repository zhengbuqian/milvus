// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

// JSON value projection: raw JSON documents -> typed values for a given path.
//
// Was `index/JsonIndexBuilder.{h,cpp}`. Renamed because it is NOT a Builder in
// the four-face sense (01-scalar-index.md §3, §6.1): it is not a class, has no
// `Seal()`, produces no `storage::Artifact`, and holds no state. It is the
// SHAPE CONVERSION that sits in front of a builder's push path — the step that
// turns "row of JSON" into "value of type T at this path", after which an
// ordinary `IndexBuilder<T>` of the inverted / bitmap / sorted family does the
// rest.
//
// That is exactly §5.7's position on per-path cast indexes: "a per-path cast
// index needs no contract of its own — it IS an ordinary
// `ScalarPredicateReader<T>`, registered in the inventory under a
// `(field, path)` key". The JSON-ness is entirely in how the values are
// produced, so it belongs here, in front of the builder, and nowhere else.
//
// It replaces the four near-identical build methods on the two decorators that
// are deleted with this wave:
//   `JsonScalarIndexWrapper::BuildWithFieldData`  JsonScalarIndexWrapper.h:83-98
//   `JsonScalarIndexWrapper::Build`               JsonScalarIndexWrapper.h:100-119
//   `JsonHybridScalarIndex::BuildWithFieldData`   JsonHybridScalarIndex.h:60-89
//   `JsonHybridScalarIndex::Build`                JsonHybridScalarIndex.h:91-147
//
// The old name also collided with the contract's `IndexBuilder<T>`.
//
// ONE EXTERNAL CALLER to keep in mind when this moves:
// `segcore/ChunkedSegmentSealedImpl.cpp:384-385` calls
// `milvus::index::json::IsDataTypeSupported(...)` when picking a JSON index.
// The other two functions have no callers outside index/.

#pragma once
#include <stdint.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "pb/schema.pb.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace json {
// Returns true if the JSON cast_type is compatible with the raw JSON value's
// data_type (e.g., DOUBLE cast can accept INT64 JSON values). Array cast
// types require is_array=true.
bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array);
}  // namespace json

template <typename T>
using JsonDataAdder =
    std::function<void(const T* data, int64_t size, int64_t offset)>;

using JsonErrorRecorder = std::function<void(const Json& json,
                                             const std::string& nested_path,
                                             simdjson::error_code error)>;

using JsonNullAdder = std::function<void(int64_t offset)>;
using JsonNonExistAdder = std::function<void(int64_t offset)>;

// Result of converting JSON field data to typed FieldData.
struct JsonToTypedResult {
    // Typed field data with nullable semantics. Rows where the path doesn't
    // exist or the cast fails are marked as invalid.
    FieldDataPtr field_data;

    // Offsets of rows where the JSON path does not exist (row is null, path
    // missing, or path value is null). This is a SUBSET of the invalid rows
    // in field_data — rows that exist but fail to cast are NOT included.
    // Used for EXISTS queries: Exists() should return true for rows where
    // the path exists, even if the value can't be cast to the index type.
    std::vector<size_t> non_exist_offsets;
};

// Convert JSON field data into typed FieldData by extracting values at
// the given nested_path.
template <typename T>
JsonToTypedResult
ConvertJsonToTypedFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

// A helper function for processing json data for building inverted index
template <typename T>
void
ProcessJsonFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<T> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

extern template void
ProcessJsonFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<bool> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

extern template void
ProcessJsonFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<int64_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

extern template void
ProcessJsonFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<double> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

extern template void
ProcessJsonFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<std::string> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

}  // namespace milvus::index