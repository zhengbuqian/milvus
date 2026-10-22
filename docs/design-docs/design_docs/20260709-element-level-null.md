# Array Element-Level Null

## Scope

This design extends row-level null support to elements of scalar `Array` and
`ArrayOfVector`, and to the inner Array and leaf of `Array<Array<T>>`.
`FieldSchema.nullable` controls the whole row, `element_nullable` controls the
first Array element, and the nested scalar leaf uses its `TypeSchema.nullable`.

The design targets Storage V2. Storage V1 rejects all new-format fields.
Nested Arrays, vector Arrays, and element-nullable Arrays are allowed only as
`StructArrayField` sub-fields. Top-level `Array<nullable T>`,
`Array<Array<T>>`, and `Array<Vector>` are rejected. Nesting is limited to
`Array<Array<scalar leaf>>`; the leaf cannot be a vector, and a Struct object
cannot itself be null.

## Semantics

These semantics apply to row-level filters, element-level filters
(`MATCH_*`, `element_filter`) and search on new-format struct sub-fields.
[Query and Index](#query-and-index) lists the supported forms.

Row validity is shared by all sub-fields of a struct, and `s IS NULL` on the
struct field tests the whole row. An empty array and an array containing only
null elements are both non-null rows. `IS NULL` / `IS NOT NULL` on a
sub-field (`s[b]`, `s[i][b]`) is rejected; whether one element is null is
tested with `$[b] IS NULL` inside `MATCH_*` or `element_filter`. A subscript
`s[i][b]` evaluates to `NULL` when the row is null, the index is out of range,
or the selected element is null:

| State | `s[i][b] > 1` | `not (s[i][b] > 1)` |
| --- | --- | --- |
| Null row | `NULL` | `NULL` |
| Index out of range | `NULL` | `NULL` |
| Null element | `NULL` | `NULL` |
| Non-null element `x` | `x > 1` | `not (x > 1)` |

Here `NULL` means an unknown (UNKNOWN) predicate result, not a valid `false`;
`&&`, `||` and `not` follow three-valued logic, and a filter keeps only rows
whose result is `true`. Comparisons, term tests, arithmetic and string
predicates on an accessed element propagate that result. Array membership
operations (`array_contains*`) skip null slots. Null vectors do not
participate in similarity search. `array_length` and capacity limits count
logical slots, including null slots.

## Representation and Data Flow

The existing `ScalarField` and `VectorField` messages carry `valid_data` for
their immediate logical values; no nullable wrapper messages are needed.
Row validity is written to both `FieldData.valid_data` and the outer
`ScalarField.valid_data` or `VectorField.valid_data`, with identical bits.
Only the corresponding inner `ScalarField.valid_data` or
`VectorField.valid_data` carries element or leaf validity. Go storage
containers keep row validity separate from child validity.

For each nullable inner level, the bitmap has one entry per logical value.
At the Proxy input boundary, payloads at that level are compact:

```text
len(child.valid_data)             = logical element count
count(child.valid_data == true)   = physical payload element count
```

Proxy validates this relationship before normalization, including at both
inner levels of a nested scalar Array. Row payloads are also compact; Proxy
restores null row positions without changing child validity. Scalar levels
become dense (one payload or placeholder per logical position), while vector
payloads remain compact:

| Type | Logical value | Input payload | Payload after Proxy |
| --- | --- | --- | --- |
| Scalar Array | `[10, null, 30]` | `[10, 30]` | `[10, 0, 30]` |
| Nested scalar Array | `[[10, null], null, []]` | `[[10], []]` | `[[10, 0], [], []]` |
| ArrayOfVector | `[vec0, null, vec2]` | `[vec0, vec2]` | `[vec0, vec2]` |

The single-level scalar and vector examples retain child validity
`[true, false, true]`. In the nested example, the outer child validity is
`[true, false, true]` and the first inner Array's leaf validity is
`[true, false]`. Each null scalar leaf gets a zero value (`0`, `""`, or
`false`); a null inner Array gets an empty `ScalarField` of the leaf type.
The null and empty inner Arrays have the same empty payload and differ in the
parent validity. Non-nullable levels omit `valid_data`.

For example, after Proxy normalization the nested row above has this proto
shape (the `FieldData`/outer-message row validity is shown separately):

```text
FieldData.valid_data = [true, ...]
FieldData.scalars.valid_data = [true, ...]  // identical row bits
FieldData.scalars.array_data.data[row] = ScalarField{
  ArrayData: ArrayArray{ElementType: Int64, Data: [
    ScalarField{LongData: [10, 0], valid_data: [true, false]},
    ScalarField{LongData: []},  // null inner Array: empty Int64 payload
    ScalarField{LongData: []}   // non-null empty inner Array
  ]},
  valid_data: [true, false, true]  // inner Array validity for this row
}
```

The row-level `valid_data` above belongs to the outer field message; the
per-row `ScalarField` inside `ArrayArray.data` carries the inner Array
validity. For `ArrayOfVector`, each row's `VectorField.valid_data` counts
logical vector elements, but the vector payload contains only valid vectors.
A null row uses the existing `NewEmptyArrayOfVectorRow` placeholder.

The insert-to-query flow is:

```text
Insert request -> Proxy validation and normalization -> WAL
  -> flusher -> Storage V2 (Arrow -> Parquet / Vortex)
  -> QueryNode load -> runtime data -> queries and indexes
```

WAL preserves the normalized payload without interpreting element nulls.
Sorting, merging, flattening, and retrieval must preserve each element's
logical position and validity together with its value.

Storage V2 selects the physical format by the full field schema:

| Field | Arrow representation | Status |
| --- | --- | --- |
| Single-level scalar Array, non-nullable elements | One `Binary` per row containing a serialized `ScalarField` | Existing format |
| Vector Array, non-nullable elements | `list<item: fixed_size_binary[dim*width] not null>` | Existing format |
| Single-level scalar Array, nullable elements | `list<item: T nullable>` | New format |
| Vector Array, nullable elements | `list<item: binary nullable>` | New format |
| Nested scalar `Array<Array<T>>`, whether or not its inner levels are nullable | `list<item: list<item: T> >` | New format |

For example, nested Int64 with non-nullable inner levels is
`list<item: list<item: int64 not null> not null>`; the top-level field has
its own row nullable flag.

The top-level Arrow field's nullable flag equals `FieldSchema.nullable`.
The first list's `item` nullable flag equals `FieldSchema.element_nullable`;
the nested leaf's flag equals the leaf `TypeSchema.nullable`. The vector
`list<Binary>` item is nullable, while the existing
`list<FixedSizeBinary>` item is not. Every list child field is named `item`.

Scalar leaf `T` uses native widths in Go and C++: Bool→`bool`, Int8→`int8`,
Int16→`int16`, Int32→`int32`, Int64→`int64`, Float→`float32`,
Double→`float64`, VarChar→`utf8` (Go `arrow.BinaryTypes.String`, C++
`arrow::utf8()`). In particular, Int8 and Int16 are not widened to int32 as
they are in the proto representation.

`list<Binary>` preserves null vector positions without allocating a full
vector placeholder. Each non-null child must match the schema's vector width;
serialization maps compact vectors to logical child positions and reading
restores compact vectors and element validity.

The Parquet writer applies column properties to the converted leaf path
(for example, `vectors.list.element`; parquet-arrow renames the Arrow `item`
child to `element` in the Parquet schema): `FixedSizeBinary` leaves are
`UNCOMPRESSED` with statistics disabled, `Binary` leaves have statistics
disabled while retaining the caller's compression, and other leaves retain
their normal properties.

A null list row or null inner list must have a zero-length child segment
(repeated offsets). Parquet rejects a null list with a non-empty child
segment; Vortex does not check it. Arrow list copies, including compaction
builder copies and slices, must preserve this invariant.

## Schema and Runtime Routing

Only nested scalar Arrays carry a wire `TypeSchema`:
`data_type == Array` and `element_type == Array`. The root node's `nullable`
must equal `FieldSchema.nullable`; the second node's `nullable` must equal
`FieldSchema.element_nullable`; the leaf node's `nullable` is independent.
Single-level fields use `nullable`, `element_nullable`, `data_type`, and
`element_type` without a wire `TypeSchema`. Schema alter APIs cannot change
`nullable` or `element_nullable`.

In C++, new-format scalar fields (`list<T>` and `list<list<T>>`) load into
`ColumnarArrayChunk` directly from Arrow `ListArray`, with leaf validity and
native-width values; they do not pass through proto Binary. Existing scalar
proto Binary fields continue to use `ArrayChunk`. Both vector Array formats
load into `VectorArrayChunk`, which retains compact vectors and element
validity. For a single-level element-nullable scalar Array, `FieldMeta`
synthesizes an internal TypeSchema so `ColumnarArrayChunk` and `ArrayValue`
use the same representation for single-level and nested fields. Growing
segments store these scalar fields as one `ColumnarArrayChunk` per insert or
load batch written into a growing chunk and read rows as `ArrayValueView`;
vector Arrays are stored per
batch as well and read as `VectorArrayView` (see
[Recursive Array Columnar Layout](./20260724-recursive-array-struct-column-layout.md#growing-segments)).
The growing-flush
path (`FlushGrowingSegmentData`/`AllowGrowingSourceFlush`) is being removed;
it receives no support or checks for these formats.

## Query and Index

Ordinary filters produce row bits; `element_filter` and `MATCH_*` child
expressions produce element bits. Both apply row, bounds and element validity
as described in [Semantics](#semantics).

### Element ids

Outside the index and brute-force internals, element ids are logical: every
element of a non-null row takes an id, null elements included, and a null row
takes none. This is the numbering of `StructElementOffsets`, so
`element_filter` bits, element-level search results and the element indices
returned to the user refer to the same logical positions.

### Vector sub-fields

An element-nullable `ArrayOfVector` sub-field is indexed and searched at
element level with element-level metrics (L2, IP, COSINE, and others). Search
and hybrid search accept a plain vector placeholder on it. MAX_SIM-family
metrics, emb-list indexes and EmbeddingList placeholders are rejected;
autoindex does not map those fields to a MAX_SIM default. Like any vector
field, the sub-field needs an index before a load that includes it.

- Index build: embedding-list mode is used only under an embedding-list
  metric. Under an element-level metric the memory and DiskANN materializers
  pass the flattened stored vectors without embedding offsets and, for an
  element-nullable field, an element-domain validity bitmap with one bit per
  logical element, so the knowhere IdMap returns logical element ids. A
  segment whose elements are all null builds an IdMap-only index; a segment
  without elements is skipped as empty. At load, the row-count and
  validity-length checks do not apply to element-level VECTOR_ARRAY indexes.
- Brute force: an element-nullable chunk stores only valid vectors. Sealed
  columns build and cache, per chunk, the logical element id of each stored
  vector; growing search computes the ids per searched range. The ids are
  attached to the bitset, so filtering and results use logical element ids.
  The cached search iterator does the same.
- Index-build threshold: DataCoord finishes a VectorArray index task without a
  build when the segment holds too few vectors. Binlog writers (flush and the
  packed StorageV2/V3 writers used by compaction, import and sort stats)
  record `Binlog.field_valid_element_counts`, the valid elements of non-null
  rows without null elements, zero included. When `Statistics` is aggregated
  from binlogs, `Statistics.valid_element_counts` sums them per field and has
  an entry only when every binlog of the field recorded a count. Bump-schema
  additive reconciliation writes no per-binlog count; it sends the counts of
  the fields it materializes directly as a `Statistics` delta that DataCoord
  adds onto the segment. An element-nullable VectorArray with an entry uses it
  for the threshold. Without an entry (older data), DataCoord falls back to
  the estimate from binlog bytes, which counts the offset and validity bytes
  of null elements as vectors; a sub-field absent from an older-schema
  segment counts as zero vectors; a manifest-backed segment whose in-memory
  binlogs do not list the field skips the vector-count threshold and leaves
  the decision to the worker build.

### Row-level filters

- Subscript predicates on `s[i][b]` (comparison, `in`, `like`, range,
  arithmetic) are UNKNOWN for a null row, an out-of-range index or a null
  element, and stay UNKNOWN under `not`. Subscripts are parsed with a range
  check, so an index that overflows is out of range rather than an error.
- `array_contains`, `array_contains_any` and `array_contains_all` on `s[b]`:
  null elements match nothing, and the result of a non-null row is only
  `true` or `false`. For a null struct row the result is UNKNOWN, also when
  the target array is empty.
- `s[b] == [...]` / `!=`: an array holding a null element never equals a
  literal, since a literal cannot spell null.
- `array_length(s[b])` counts null elements, also on element-nullable vector
  sub-fields.
- The parser rejects `array_contains*` and `array_length` on a single array
  element (`arr[i]`, `s[i][scalar_sub]`), and `IS NULL` / `IS NOT NULL` on a
  sub-field (`s[b]`, `s[i][b]`).

Rules for nested `Array<Array<T>>` sub-fields (`s[i][tags]`, and the rejection
of `array_contains*` on the whole sub-field) are in
[Struct Data Type](./20260306-struct.md#nested-array-sub-fields-in-filters).

### Element-level filters

`MATCH_*` and `element_filter` predicates run on element-nullable sub-fields,
including sub-fields added to an existing struct.

- A null sub-field value makes the element comparison UNKNOWN; `&&`, `||` and
  `!` follow three-valued logic. Sub-fields of one element stay aligned by
  element id even when their null layouts differ.
- `element_filter` selects only `true` elements. `MATCH_*` counts an UNKNOWN
  element as a miss, so `MATCH_ALL` is `false` when any element is UNKNOWN; an
  empty array still satisfies `MATCH_ALL`.
- `$[b] IS NULL` / `IS NOT NULL` is accepted inside `MATCH_*` and
  `element_filter` for scalar, vector and `Array<T>` sub-fields. It reads the
  element's own validity and is only `true` or `false`; on a sub-field that
  cannot hold null elements `IS NULL` is always `false`. `IS NULL` outside an
  element context (`clips[b] IS NULL`) stays rejected.
- On an `Array<T>` sub-field, `$[tags]` is an `Array<T>` value: a null leaf
  matches nothing in `array_contains*`, `array_length` counts null leaves, a
  null inner array is UNKNOWN, and `$[tags] IS NULL` is `true` only for a null
  inner array.

### Not supported

- Scalar indexes on element-nullable or nested scalar Array fields;
  CreateIndex rejects them.
- MAX_SIM-family metrics, emb-list indexes and EmbeddingList search on
  element-nullable vector Arrays; these are out of scope for this project.
- Binlog import (backup restore) and snapshot-source import into a collection
  with any element-nullable (scalar or vector) or nested Array field, and
  NumPy import into a collection with any struct field; see
  [Bulk Import](#bulk-import).
- Element-nullable (scalar or vector) or nested Array fields in external
  collections.

## Bulk Import

JSON, JSONL, CSV and Parquet imports read element-nullable and nested Array
struct sub-fields. The readers produce the same storage layout as the insert
path (dense scalar payload with element validity, one inner array per element
for `Array<Array<T>>`, only the valid vectors for a vector sub-field), so an
import and an insert of the same rows write the same records. Import
V2 and Import V3 use the same readers and the same DataNode post-processing.

### Input shapes

- JSON / JSONL: a struct value is a list of element objects. For a struct
  with sub-fields `id` (Int64), `label` (element-nullable VarChar), `tags`
  (element-nullable `Array<Array<Int64>>` with a nullable leaf) and `emb`
  (element-nullable 2-dim float vector), a row of three elements is
  `[{"id": 1, "label": "a", "tags": [1, null], "emb": [0.1, 0.2]}, {"id": 2, "label": null, "tags": null, "emb": null}, {"id": 3, "tags": [], "emb": [0.3, 0.4]}]`:
  the second element has a null label, a null inner array and a null
  vector, the third a missing (null) label and an empty inner array.
  A scalar sub-field takes one value per element, `null` for a null element.
  A nested `Array<Array<T>>` sub-field takes one inner array per element:
  `null` is a null inner array, `null` inside it a null leaf. A vector
  sub-field takes one vector per element, `null` for a null vector. A whole
  struct row is `null` (nullable struct only).
- CSV: the struct cell holds the same JSON text as the JSON format; the
  `nullkey` (or JSON `null`) marks a null struct row.
- Parquet: one column per struct, `list<struct<sub: C, ...>>`. C is the scalar
  type for a scalar sub-field, `list<T>` for a nested sub-field (so the column
  is `list<struct<..., tags: list<T>>>`), and the list type of one vector
  (`list<float32>` for a float vector) for a vector sub-field. A null element is a null child value, a
  null inner array or vector a null list, a null leaf a null list item; an
  Arrow `null`-typed child means every element is null. Flat `struct[sub]`
  columns are rejected, as before.

### Rules

- A `null` is accepted only where the schema allows it: the element of an
  element-nullable sub-field, the leaf of a nested sub-field whose leaf
  `TypeSchema` is nullable, and the row of a nullable struct. A `null`
  anywhere else fails the import; it is never converted to a zero value.
- A struct element cannot be null (`[null, {...}]`, or a null struct slot in
  Parquet); set its sub-field values to null instead.
- A missing key of an element-nullable sub-field in a JSON/CSV element, or a
  missing child in the Parquet struct, is a null element. For a whole missing
  Parquet child the DataNode fills each row with as many null elements as its
  sibling sub-fields have; a null struct row stays null. When the Parquet
  struct carries none of the current sub-fields (all its children were
  dropped), the reader takes the element count and row validity of each row
  from the outer list itself and fills the same null elements. Files written
  before a sub-field was added therefore import without it.
- A missing non-element-nullable sub-field fails the import.
- Keys and Parquet children that are not sub-fields of the collection,
  including dropped sub-fields, are ignored. This differs from insert and
  REST, which reject a dropped sub-field in the request.
- Int8 and Int16 elements are range checked, for every struct sub-field.
- Parquet struct children accept the integer and floating widening that
  top-level Parquet columns accept (for example `int8` into an Int32
  sub-field), also for sub-fields that are not element-nullable.
- Parquet nullable flags: the reader checks the actual null values of each
  level against the schema and never rejects a file only because of an
  Arrow field's `nullable` flag. A file with every level marked nullable and
  a file whose flags are stricter than the schema (for example a struct
  object item, vector item or child marked non-nullable) are both accepted
  when their values are valid.
- Capacity limits count logical elements, null elements included.

### Unsupported import paths

- Binlog import (backup restore) and snapshot-source import into a collection
  with an element-nullable or nested Array field are rejected with
  ParameterInvalid before the job is accepted: they copy source binlogs and
  cannot produce the new layout.
- NumPy has one file per top-level field and no form for a struct field. A
  NumPy import into a collection with any struct field is rejected, including
  collections whose struct sub-fields are neither element-nullable nor
  nested.

## Client APIs

### REST v2

- Schema: a struct sub-field takes `elementNullable` and, for an
  `Array<Array<T>>` sub-field (`dataType: "Array"`,
  `elementDataType: "Array"`), a `typeSchema` that mirrors the proto
  `TypeSchema` (`leafType` / `arrayElement` / `typeParams` / `nullable`).
  REST fills the root `max_capacity` from the struct and the inner array's
  `nullable` from `elementNullable`, and rejects an explicit value that
  differs. A sub-field `nullable` may be given only when it equals the
  struct's; omitted, it inherits. Describe prints `elementNullable` and
  `typeSchema`, so a describe result can be sent back to create.
- Writes: in a struct element object, a JSON `null` or a missing key of an
  element-nullable sub-field is a null element; `null` on any other sub-field,
  unknown keys and duplicate keys are rejected.
- Output: null elements and null leaves are returned as `null`; vectors of an
  element-nullable vector sub-field are returned at their logical positions,
  with `null` for null elements; `Array<T>` values are converted recursively.
  An element-level search hit carries `"offset"`, the matched element
  position. An element-level query (`element_filter`) row carries
  `"offset": [..]`, the matched element positions; rows are not expanded, so
  the row count and shape of other responses do not change.

### Go SDK

- Schema: in an `entity.StructSchema`, a sub-field's `Nullable` maps to
  `element_nullable` (row-level `nullable` comes from the struct), and
  `Field.ElementNullable` marks the leaves of an `Array<T>` sub-field
  (`DataType` Array with a scalar `ElementType`, stored as
  `Array<Array<T>>`) as nullable. `Array<T>` sub-fields are sent with a
  three-level `type_schema`; describe converts it back.
- Columns: element-nullable Array and VectorArray columns
  (`NewElementNullableColumnXxxArray`, `NewElementNullableColumnXxxVectorArray`)
  store compact values plus per-row element validity, like row-level nullable
  columns, with a sparse mode for results. `ColumnXxxArrayArray` holds an
  `Array<T>` sub-field with inner-array and leaf validity. Row values for
  `AppendValue` and from `Get` are `[]*T` with nil for a null scalar element,
  a vector slice with nil for a null vector element, and `[][]*T` for an
  `Array<T>` sub-field with a nil inner slice for a null inner array and nil
  for a null leaf. A struct column
  fills an omitted element-nullable sub-field with null elements and checks
  that all sub-fields of a row have the same element count.
- Results: `ResultSet.ElementOffsets` holds the matched element position of
  each hit of an element-level search (also hybrid search and the search
  iterator); `ResultSet.QueryElementOffsets` holds the matched element
  positions of each row of an element-level query (also the query iterator).
