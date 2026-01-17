# Unified Scalar Index Format Bugfix TODO

This is a working checklist for the PR #44 follow-up.

## TL;DR conclusions (do not deviate)

### Versioning rules
- **Go and C++ must both define a legacy fallback constant** used when scalar index meta/config does not contain an engine version:
  - Go: `LastScalarIndexEngineVersionWithoutMeta = int32(2)`
  - C++: `kLastScalarIndexEngineVersionWithoutMeta = 2`
- **Remove `kDefaultScalarIndexEngineVersion` entirely** (do not use it as fallback).
- **Control plane (Go) must always pass `SCALAR_INDEX_ENGINE_VERSION` in config.**
  - If meta missing version (legacy), Go must pass **2**.

### Packed unified file naming
- Unified packed entry key / file basename must be:
  - `packed_<index_type>_v<ver>`
- `<index_type>` must be a **short, stable lowercase token**, e.g. `textmatch`, `ngram`, `inverted`, etc.
- Parsing/version inference is allowed **only when config version is missing**.

### Version resolution at load time (C++ side)
1) If config has `SCALAR_INDEX_ENGINE_VERSION`: **use it**.
2) Else try infer `ver` by parsing packed entry name suffix `_v<ver>`.
3) Else fallback to `kLastScalarIndexEngineVersionWithoutMeta` (=2). **Do not error.**

### Review comment sanity check
- The Gemini review claim about “`NgramInvertedIndex` missing Serialize for `avg_row_size_`” is **incorrect** on this branch:
  - `NgramInvertedIndex::Serialize` exists and unified load reads `ngram_avg_row_size`.

## Checklist

### Version constants
- [x] (Go) Add constant `LastScalarIndexEngineVersionWithoutMeta = 2`
- [x] (C++) Add constant `kLastScalarIndexEngineVersionWithoutMeta = 2`
- [x] Delete/stop using `kDefaultScalarIndexEngineVersion` everywhere

### Packed filename helpers
- [x] Implement `FormatPackedIndexFileName(token, ver)`
- [x] Implement `TryParsePackedIndexFileName(filename, &token, &ver)`

### Version resolution helper
- [x] Prefer config version
- [x] If config missing: infer from filename
- [x] If still missing: fallback to v2 constant
- [x] Filename inference only when config missing

### Update packed entry naming usage
- [x] Upload path: write packed payload using new name `packed_<type>_v<ver>` (TextMatchIndex uses FormatPackedIndexFileName)
- [x] Load path: locate packed entry by scanning keys and parsing via `TryParsePackedIndexFileName`

### RTree index unified format support
- [x] Add unified format support to RTreeIndex Upload
- [x] Add unified format support to RTreeIndex Load

### TextMatchIndex propagation
- [x] Ensure Go passes `SCALAR_INDEX_ENGINE_VERSION` into config for load/build
- [x] Fix `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp` to include version in config

### Optional perf follow-ups
- [x] Reduce extra memcpys (avoid vector copies in unpack)
- [x] Provide pointer overload: `UnpackBlobToDirectory(const uint8_t*, size_t, ...)`
- [x] Add `PackBinarySetToBinary` and `PackDirectoryToBinary` to avoid intermediate vector copies
