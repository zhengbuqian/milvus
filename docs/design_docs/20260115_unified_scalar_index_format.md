# Unified Scalar Index Format Design

## 1. Background & Motivation

### 1.1 Current Problems

| Problem | Description | Impact |
|---------|-------------|--------|
| S3 File Fragmentation | Each index produces multiple files (Tantivy: 10-50 files, others sliced at 16MB) | High S3 request cost, increased load latency |
| etcd Pressure | Index metadata stores full file path list | etcd value too large, performance degradation |
| Unnecessary Slicing | 16MB forced slicing via `Disassemble()` | Increased file count and management complexity |

### 1.2 Design Goals

1. **Single File Storage**: All index data in one file, no slicing
2. **Minimal Changes**: Leverage existing version mechanism

## 2. Design Overview

### 2.1 Version Control

Use existing `CurrentScalarIndexEngineVersion` to route between legacy and unified format:

| Version | Format | Description |
|---------|--------|-------------|
| ≤ 2 | Legacy | Multiple files, 16MB slicing |
| **≥ 3** | **Unified** | **Single file, no slicing** |

### 2.2 File Format

Index file is a pure binary blob. All metadata (IndexType, NumRows, IndexSerializedSize, CurrentScalarIndexVersion) already exists in etcd's `SegmentIndex`.

```
┌──────────────────────────────────────────────────────────────────┐
│                     Index Binary Blob                             │
│  - For single-output indexes: direct serialization               │
│  - For multi-file indexes: packed format (see 4.2)               │
└──────────────────────────────────────────────────────────────────┘
```

## 3. Index Type Analysis

### 3.1 Scalar Index Types

| Index Type | Current Output | Metadata Location | Change Needed |
|------------|---------------|-------------------|---------------|
| **BitmapIndex** | BinarySet + slicing | `SegmentIndex` | Skip slicing |
| **ScalarIndexSort** | BinarySet + slicing | `SegmentIndex` | Skip slicing |
| **HybridScalarIndex** | Delegates to internal index | `SegmentIndex` | Skip slicing |
| **StringIndexMarisa** | BinarySet + slicing | `SegmentIndex` | Skip slicing |
| **InvertedIndexTantivy** | Directory files | `SegmentIndex` | Pack files |
| **TextMatchIndex** | Directory files | `SegmentInfo.textStatsLogs` | Pack files + Proto change |
| **NgramInvertedIndex** | Directory files | `SegmentIndex` | Pack files |
| **JsonInvertedIndex** | Directory files | `SegmentIndex` | Pack files |

### 3.2 Change Categories

**Category A - Skip Slicing:**
- BitmapIndex, ScalarIndexSort, HybridScalarIndex, StringIndexMarisa
- These indexes call `Disassemble()` which slices large files. For v3, skip this step.

**Category B - Pack Multi-Files (Standard Index):**
- InvertedIndexTantivy, NgramInvertedIndex, JsonInvertedIndex
- These indexes produce multiple files in a directory. For v3, pack all files into a single blob.
- Version info stored in `SegmentIndex.CurrentScalarIndexVersion`.

**Category C - Pack Multi-Files (Stats-based):**
- TextMatchIndex
- Same pack/unpack logic as Category B, but metadata stored in `SegmentInfo.textStatsLogs`.
- Requires proto change to add version field (see Section 5.5).

## 4. Implementation

### 4.1 Category A: Skip Slicing

For v3, skip the `Disassemble()` call in `Serialize()`. On load, skip `Assemble()` since data is already complete.

### 4.2 Category B: Packing Format

Pack all files in a directory into a single blob:

```
┌─────────────────────────────────────────────────────────────────┐
│ File Count (4 bytes, uint32, little-endian)                     │
├─────────────────────────────────────────────────────────────────┤
│ File Entry 1:                                                   │
│   - Name Length (4 bytes, uint32)                               │
│   - Name (variable bytes, UTF-8, no null terminator)            │
│   - Data Size (8 bytes, uint64)                                 │
│   - Data (variable bytes)                                       │
├─────────────────────────────────────────────────────────────────┤
│ File Entry 2: ...                                               │
└─────────────────────────────────────────────────────────────────┘
```

On upload: traverse directory, pack all files into blob, upload as single file.
On load: download single file, unpack to directory, then load as before.

## 5. Code Changes Summary

### 5.1 Version Bump

`pkg/common/common.go`: `CurrentScalarIndexEngineVersion` from 2 → 3

### 5.2 New Utility File

`internal/core/src/common/Pack.h/.cpp`:
- `PackDirectoryToBlob()` / `UnpackBlobToDirectory()`

### 5.3 Index Changes

| File | Change |
|------|--------|
| `BitmapIndex.cpp` | Skip `Disassemble()` for v3 |
| `ScalarIndexSort.cpp` | Skip `Disassemble()` for v3 |
| `HybridScalarIndex.cpp` | Skip `Disassemble()` for v3 |
| `StringIndexMarisa.cpp` | Skip `Disassemble()` for v3 |
| `InvertedIndexTantivy.cpp` | Add pack/unpack for v3 |
| `TextMatchIndex.cpp` | Add pack/unpack for v3 |
| `NgramInvertedIndex.cpp` | Add pack/unpack for v3 |
| `JsonInvertedIndex.cpp` | Add pack/unpack for v3 |

### 5.4 FileManager Changes

| File | Change |
|------|--------|
| `MemFileManagerImpl.cpp` | Add `AddFileWithoutSlice()` |
| `DiskFileManagerImpl.cpp` | Add `AddSingleBlob()`, `DownloadFile()` |

### 5.5 Proto Change for TextMatchIndex

TextMatchIndex metadata is stored in `TextIndexStats`, which currently lacks a format version field. This message is defined in two files that need to be kept in sync:
- `pkg/proto/data_coord.proto`
- `pkg/proto/segcore.proto`

**New field to add**:
```protobuf
message TextIndexStats {
  ...
  int64 buildID = 6;
  int32 current_scalar_index_version = 7;  // NEW: format version
}
```

**Build-time**: Set `current_scalar_index_version` to `CurrentScalarIndexEngineVersion` when creating TextIndexStats.

**Load-time**: Check `current_scalar_index_version`:
- `≥ 3`: Single packed file, unpack before loading
- `< 3` or `0` (default): Legacy multi-file format

## 6. What Doesn't Change

| Component | Reason |
|-----------|--------|
| etcd schema | `IndexFileKeys[]` / `files[]` just has 1 entry instead of N |
| Coordinator logic | Just passes through file paths |
| Index build/query algorithms | Only serialization path changes |

## 7. Version Coordination

### 7.1 Upgrade Flow

1. Deploy new IndexNode and QueryNode code
2. Each QN registers with CurrentVersion=3
3. Coordinator computes: min(all QN versions)
4. When all QNs upgraded: version=3, new indexes use unified format
5. Old indexes still loadable (version check at load time)

### 7.2 Rollback Safety

1. Roll back some QNs to v2
2. Coordinator computes min=2
3. New indexes built in legacy format
4. Existing v3 indexes need v3 QNs to load

## 8. Performance Impact

### 8.1 S3 Requests

| Index Type | Legacy (v2) | Unified (v3) |
|------------|-------------|--------------|
| Tantivy/TextMatch | 10-50 requests | 1 request |
| Bitmap (large) | 2-10 requests | 1 request |
| Others | 1-2 requests | 1 request |

### 8.2 etcd Storage

| Index Type | Legacy (v2) | Unified (v3) |
|------------|-------------|--------------|
| Tantivy | 1-5 KB | ~200 bytes |
| Others | 200-500 bytes | ~200 bytes |
