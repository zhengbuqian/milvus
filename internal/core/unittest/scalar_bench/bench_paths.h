// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>

namespace milvus {
namespace scalar_bench {

// Return the base directory for all scalar bench disk usage.
// Example: "/tmp/milvus/scalar_bench/"
const std::string& GetBasePath();

// Return subdirectories under the base path for different usage types.
// These functions ensure the directories exist.
std::string GetStorageRoot();   // base path
std::string GetStorageDir();    // for LocalChunkManagerSingleton and general storage
std::string GetSegmentsDir();   // for per-segment local chunk manager data
std::string GetResultsDir();    // for benchmark outputs like CSV, logs, reports
std::string GetTempDir();        // for temporary files like perf data

// Utilities
std::string PathJoin(const std::string& base, const std::string& name);
void EnsureDirExists(const std::string& path);

} // namespace scalar_bench
} // namespace milvus


