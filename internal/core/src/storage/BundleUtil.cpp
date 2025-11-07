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

#include "storage/BundleUtil.h"

#include <boost/filesystem.hpp>
#include <cstring>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/FileWriter.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::storage {

void
PackDirToBundle(const std::string& dir_path,
                const std::string& bundle_path,
                const std::function<bool(const std::string&)>& include_pred) {
    struct EntryMeta {
        std::string name;
        uint64_t size;
    };
    std::vector<EntryMeta> entries;
    boost::filesystem::path p(dir_path);
    boost::filesystem::directory_iterator end_iter;
    for (boost::filesystem::directory_iterator iter(p); iter != end_iter;
         ++iter) {
        if (boost::filesystem::is_directory(*iter)) {
            continue;
        }
        auto filename = iter->path().filename().string();
        if (!include_pred(filename)) {
            continue;
        }
        auto sz = boost::filesystem::file_size(*iter);
        entries.push_back({filename, static_cast<uint64_t>(sz)});
    }

    FileWriter writer(bundle_path, io::Priority::MIDDLE);
    writer.Write(TANTIVY_BUNDLE_MAGIC, sizeof(TANTIVY_BUNDLE_MAGIC));
    uint32_t ver = TANTIVY_BUNDLE_FORMAT_VERSION;
    writer.Write(&ver, sizeof(ver));
    uint32_t file_count = static_cast<uint32_t>(entries.size());
    writer.Write(&file_count, sizeof(file_count));

    uint64_t header_bytes = 0;
    for (auto& e : entries) {
        header_bytes += sizeof(uint32_t);
        header_bytes += static_cast<uint32_t>(e.name.size());
        header_bytes += sizeof(uint64_t);
        header_bytes += sizeof(uint64_t);
    }
    uint64_t data_off = sizeof(TANTIVY_BUNDLE_MAGIC) + sizeof(ver) +
                        sizeof(file_count) + header_bytes;
    uint64_t cur = 0;
    for (auto& e : entries) {
        uint32_t name_len = static_cast<uint32_t>(e.name.size());
        writer.Write(&name_len, sizeof(name_len));
        writer.Write(e.name.data(), name_len);
        uint64_t off = data_off + cur;
        writer.Write(&off, sizeof(off));
        writer.Write(&e.size, sizeof(e.size));
        cur += e.size;
    }

    auto local_cm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    const size_t buf_size = 1 << 20;
    std::vector<uint8_t> buf(buf_size);
    for (auto& e : entries) {
        auto file_path = (boost::filesystem::path(dir_path) / e.name).string();
        uint64_t remaining = e.size, o = 0;
        while (remaining > 0) {
            auto to_read = static_cast<uint64_t>(
                std::min<uint64_t>(buf_size, remaining));
            local_cm->Read(file_path, o, buf.data(), to_read);
            writer.Write(buf.data(), to_read);
            remaining -= to_read;
            o += to_read;
        }
    }
    writer.Finish();
}

std::vector<BundleEntry>
ReadBundleEntries(const std::string& bundle_path) {
    auto local_cm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto read_exact = [&](uint64_t off, void* dst, size_t n) {
        local_cm->Read(bundle_path, off, dst, n);
    };
    uint64_t off = 0;
    char magic[8];
    read_exact(off, magic, sizeof(magic));
    off += sizeof(magic);
    if (std::memcmp(magic, TANTIVY_BUNDLE_MAGIC, 8) != 0) {
        ThrowInfo(ErrorCode::FileReadFailed,
                  "invalid tantivy bundle magic for %s",
                  bundle_path);
    }
    uint32_t ver = 0;
    read_exact(off, &ver, sizeof(ver));
    off += sizeof(ver);
    if (ver != TANTIVY_BUNDLE_FORMAT_VERSION) {
        ThrowInfo(ErrorCode::NotImplemented,
                  "unsupported tantivy bundle version: %d",
                  ver);
    }
    uint32_t cnt = 0;
    read_exact(off, &cnt, sizeof(cnt));
    off += sizeof(cnt);
    std::vector<BundleEntry> entries;
    entries.reserve(cnt);
    for (uint32_t i = 0; i < cnt; ++i) {
        uint32_t name_len = 0;
        read_exact(off, &name_len, sizeof(name_len));
        off += sizeof(name_len);
        std::string name;
        name.resize(name_len);
        if (name_len > 0) {
            read_exact(off, name.data(), name_len);
        }
        off += name_len;
        uint64_t o = 0, s = 0;
        read_exact(off, &o, sizeof(o));
        off += sizeof(o);
        read_exact(off, &s, sizeof(s));
        off += sizeof(s);
        entries.push_back({std::move(name), o, s});
    }
    return entries;
}

void
UnpackBundleToDir(const std::string& bundle_path,
                  const std::string& output_dir) {
    auto local_cm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto entries = ReadBundleEntries(bundle_path);
    const size_t buf_size = 1 << 20;
    for (auto& e : entries) {
        auto out_path = (boost::filesystem::path(output_dir) / e.name).string();
        FileWriter fw(out_path, io::Priority::HIGH);
        uint64_t remaining = e.size, cur = 0;
        std::vector<uint8_t> buf(buf_size);
        while (remaining > 0) {
            auto to_read = static_cast<uint64_t>(
                std::min<uint64_t>(buf_size, remaining));
            local_cm->Read(bundle_path, e.offset + cur, buf.data(), to_read);
            fw.Write(buf.data(), to_read);
            remaining -= to_read;
            cur += to_read;
        }
        fw.Finish();
    }
}

}  // namespace milvus::storage
