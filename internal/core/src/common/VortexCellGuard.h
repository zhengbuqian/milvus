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

#include <linux/falloc.h>
#include <fcntl.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "log/Log.h"

namespace milvus {

/// RAII guard that releases physical pages via PUNCH_HOLE on destruction.
///
/// When a vortex cell is loaded, its compressed segments are pwrite'd
/// into a shared memfd.  VortexCellGuard records those byte ranges.
/// When the cell is evicted (GroupChunk destroyed), the guard releases
/// the physical memory while keeping the memfd's logical size unchanged.
///
/// The memfd is owned by VortexFileHandle which outlives all cells.
class VortexCellGuard {
 public:
    /// @param fd      memfd descriptor (owned by VortexFileHandle)
    /// @param ranges  (offset, length) pairs to PUNCH_HOLE on destruction
    VortexCellGuard(int fd, std::vector<std::pair<off_t, size_t>> ranges)
        : fd_(fd), ranges_(std::move(ranges)) {
    }

    ~VortexCellGuard() {
        for (const auto& [offset, length] : ranges_) {
            int rc = fallocate(fd_,
                               FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                               offset,
                               length);
            if (rc != 0) {
                LOG_WARN(
                    "VortexCellGuard: PUNCH_HOLE failed at offset={} "
                    "length={}: {}",
                    offset,
                    length,
                    strerror(errno));
            }
        }
    }

    VortexCellGuard(const VortexCellGuard&) = delete;
    VortexCellGuard& operator=(const VortexCellGuard&) = delete;
    VortexCellGuard(VortexCellGuard&&) = default;
    VortexCellGuard& operator=(VortexCellGuard&&) = default;

 private:
    int fd_;
    std::vector<std::pair<off_t, size_t>> ranges_;
};

}  // namespace milvus
