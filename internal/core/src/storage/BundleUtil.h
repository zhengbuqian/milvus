// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace milvus::storage {

inline constexpr const char* TANTIVY_BUNDLE_FILE_NAME = "tantivy_index.bundle";
inline constexpr uint32_t TANTIVY_BUNDLE_FORMAT_VERSION = 1;
inline constexpr char TANTIVY_BUNDLE_MAGIC[8] = {'T', 'A', 'N', 'T', 'I', 'V', 'Y', 'B'};

struct BundleEntry {
    std::string name;
    uint64_t offset;
    uint64_t size;
};

// Packs all regular files in dir_path into a single bundle at bundle_path.
// include_pred returns true to include the file (by base filename).
void
PackDirToBundle(const std::string& dir_path,
                const std::string& bundle_path,
                const std::function<bool(const std::string&)>& include_pred);

// Reads bundle header entries without extracting payloads.
std::vector<BundleEntry>
ReadBundleEntries(const std::string& bundle_path);

// Extracts bundle payloads into output_dir, creating/overwriting files.
void
UnpackBundleToDir(const std::string& bundle_path,
                  const std::string& output_dir);

}  // namespace milvus::storage
