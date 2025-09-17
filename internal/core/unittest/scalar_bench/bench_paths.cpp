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

#include "bench_paths.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>

namespace milvus {
namespace scalar_bench {

namespace {
static const char* kDefaultBase = "/tmp/milvus/scalar_bench/";
}

const std::string& GetBasePath() {
    static const std::string base_path = kDefaultBase;
    return base_path;
}

std::string PathJoin(const std::string& base, const std::string& name) {
    if (base.empty()) {
        return name;
    }
    if (!base.empty() && base.back() == '/') {
        return base + name;
    }
    return base + "/" + name;
}

void EnsureDirExists(const std::string& path) {
    // Create nested directories. Simple implementation: iterate segments.
    if (path.empty()) {
        return;
    }

    std::string current;
    if (path[0] == '/') {
        current = "/";
    }

    size_t start = (path[0] == '/') ? 1 : 0;
    while (start <= path.size()) {
        size_t pos = path.find('/', start);
        std::string part = path.substr(start, pos == std::string::npos ? std::string::npos : pos - start);
        if (!part.empty()) {
            if (current.size() > 1 && current.back() != '/') {
                current.push_back('/');
            }
            current += part;
            struct stat st;
            if (stat(current.c_str(), &st) != 0) {
                ::mkdir(current.c_str(), 0755);
            }
        }
        if (pos == std::string::npos) {
            break;
        }
        start = pos + 1;
    }
}

std::string GetStorageRoot() {
    auto path = GetBasePath();
    EnsureDirExists(path);
    return path;
}

std::string GetStorageDir() {
    auto path = PathJoin(GetBasePath(), "storage");
    EnsureDirExists(path);
    return path + "/";
}

std::string GetSegmentsDir() {
    auto path = PathJoin(GetBasePath(), "segments");
    EnsureDirExists(path);
    return path + "/"; // keep trailing slash for chunk manager root if needed
}

std::string GetResultsDir() {
    auto path = PathJoin(GetBasePath(), "results");
    EnsureDirExists(path);
    return path + "/";
}

} // namespace scalar_bench
} // namespace milvus


