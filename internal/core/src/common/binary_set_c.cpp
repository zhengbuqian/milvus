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

#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/NamedBuffer.h"
#include "common/binary_set_c.h"
#include "monitor/scope_metric.h"

CStatus
NewBinarySet(CBinarySet* c_binary_set) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(c_binary_set != nullptr,
                   "BinarySet output pointer was null");
        auto binary_set = std::make_unique<milvus::NamedBufferSet>();
        *c_binary_set = binary_set.release();
        auto status = CStatus();
        status.error_code = milvus::ErrorCode::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::ErrorCode::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

void
DeleteBinarySet(CBinarySet c_binary_set) {
    SCOPE_CGO_CALL_METRIC();

    auto binary_set = static_cast<milvus::NamedBufferSet*>(c_binary_set);
    delete binary_set;
}

CStatus
AppendIndexBinary(CBinarySet c_binary_set,
                  void* index_binary,
                  int64_t index_size,
                  const char* c_index_key) {
    SCOPE_CGO_CALL_METRIC();

    auto status = CStatus();
    try {
        AssertInfo(c_binary_set != nullptr, "BinarySet pointer was null");
        AssertInfo(index_size >= 0,
                   "BinarySet entry size cannot be negative");
        AssertInfo(c_index_key != nullptr, "BinarySet entry key was null");
        AssertInfo(index_binary != nullptr || index_size == 0,
                   "BinarySet entry data was null for {} bytes",
                   index_size);
        const auto size = static_cast<uint64_t>(index_size);
        AssertInfo(
            size <= static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
            "BinarySet entry size exceeds size_t");
        auto binary_set = static_cast<milvus::NamedBufferSet*>(c_binary_set);
        std::string index_key(c_index_key);
        const auto native_size = static_cast<size_t>(size);
        auto data = std::shared_ptr<uint8_t[]>(new uint8_t[native_size]);
        if (native_size != 0) {
            milvus::fastmem::FastMemcpy(
                data.get(), index_binary, native_size);
        }
        (*binary_set)[std::move(index_key)] =
            milvus::NamedBuffer{std::move(data), native_size};

        status.error_code = milvus::ErrorCode::Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = milvus::ErrorCode::UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

int
GetBinarySetSize(CBinarySet c_binary_set) {
    SCOPE_CGO_CALL_METRIC();

    auto binary_set = static_cast<milvus::NamedBufferSet*>(c_binary_set);
    return static_cast<int>(binary_set->size());
}

void
GetBinarySetKeys(CBinarySet c_binary_set, void* data) {
    SCOPE_CGO_CALL_METRIC();

    auto binary_set = static_cast<milvus::NamedBufferSet*>(c_binary_set);
    const char** data_ = (const char**)data;
    std::size_t i = 0;
    for (auto it = binary_set->begin(); it != binary_set->end(); ++it, i++) {
        data_[i] = it->first.c_str();
    }
}

int
GetBinarySetValueSize(CBinarySet c_binary_set, const char* key) {
    SCOPE_CGO_CALL_METRIC();

    auto binary_set = static_cast<milvus::NamedBufferSet*>(c_binary_set);
    int ret = 0;
    try {
        AssertInfo(binary_set != nullptr, "BinarySet pointer was null");
        AssertInfo(key != nullptr, "BinarySet entry key was null");
        const auto entry = binary_set->find(key);
        AssertInfo(entry != binary_set->end(),
                   "BinarySet entry was not found: {}",
                   key);
        ret = static_cast<int>(entry->second.size);
    } catch (const std::exception&) {
    }
    return ret;
}

CStatus
CopyBinarySetValue(void* data, const char* key, CBinarySet c_binary_set) {
    SCOPE_CGO_CALL_METRIC();

    auto status = CStatus();
    auto binary_set = static_cast<milvus::NamedBufferSet*>(c_binary_set);
    try {
        AssertInfo(binary_set != nullptr, "BinarySet pointer was null");
        AssertInfo(key != nullptr, "BinarySet entry key was null");
        const auto entry = binary_set->find(key);
        AssertInfo(entry != binary_set->end(),
                   "BinarySet entry was not found: {}",
                   key);
        AssertInfo(data != nullptr || entry->second.size == 0,
                   "BinarySet copy destination was null for {} bytes",
                   entry->second.size);
        AssertInfo(entry->second.data != nullptr || entry->second.size == 0,
                   "BinarySet entry {} is a remote descriptor",
                   key);
        status.error_code = milvus::ErrorCode::Success;
        status.error_msg = "";
        if (entry->second.size != 0) {
            milvus::fastmem::FastMemcpy(
                data, entry->second.data.get(), entry->second.size);
        }
    } catch (std::exception& e) {
        status.error_code = milvus::ErrorCode::UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
