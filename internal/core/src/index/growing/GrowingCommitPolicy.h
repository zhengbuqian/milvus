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

#include <atomic>
#include <chrono>
#include <cstdint>

// Commit cadence and watermark bookkeeping, shared by every appender BY
// COMPOSITION (§3 principle 2 — appenders do not inherit each other any more
// than readers do).
//
// See core_refactor/01-scalar-index.md §7 semantics 1: "SNAPSHOT PLUS
// WATERMARK, NO REAL-TIME PROMISE. tantivy's commit/reload is natively this
// model; today text match's commit lag is IMPLICIT, and the contract makes it an
// explicit watermark."
//
// RE-HOMED FROM: `TextMatchIndex`'s three growing members and one predicate —
// `commit_interval_in_ms_`, `last_commit_time_`, `mtx_`
// (`index/TextMatchIndex.h:113-116`) and `shouldTriggerCommit()`
// (`index/TextMatchIndex.cpp:346-352`), which `AddTextsGrowing` consults after
// every batch (`:291-294`). The only thing added is the WATERMARK ITSELF: today
// nothing records how many rows the last commit covered, which is precisely the
// implicitness §7 turns into `CommittedRows()`.

namespace milvus::index {

class GrowingCommitPolicy {
 public:
    using Clock = std::chrono::high_resolution_clock;

    explicit GrowingCommitPolicy(int64_t commit_interval_in_ms)
        : commit_interval_in_ms_(commit_interval_in_ms),
          last_commit_time_(Clock::now()) {
    }

    // True when `commit_interval_in_ms_` has elapsed since the last commit.
    // Re-home of `TextMatchIndex::shouldTriggerCommit`.
    bool
    ShouldCommit() const;

    // Called by the appender right after a successful commit/reload, with the
    // number of rows the new snapshot covers.
    void
    NoteCommitted(int64_t rows_covered);

    // §7's watermark: which row the current snapshot covers up to.
    // MONOTONICALLY NON-DECREASING — the appender must never publish a smaller
    // number, including after a failed commit (in which case it publishes
    // nothing and the previous snapshot stays current).
    int64_t
    CommittedRows() const {
        return committed_rows_.load(std::memory_order_acquire);
    }

    // Rows handed to `Append` so far, committed or not. `appended - committed`
    // is the tail §7 semantics 2 leaves to segcore/exec:
    // "[CommittedRows(), insert_barrier) is an EXECUTION POLICY of segcore/exec;
    // the index only reports the watermark." The index side deliberately carries
    // NO BIT expressing what to do about it — otherwise `ReaderCaps` starts
    // carrying product semantics.
    int64_t
    AppendedRows() const {
        return appended_rows_.load(std::memory_order_acquire);
    }

    void
    NoteAppended(int64_t rows) {
        appended_rows_.fetch_add(rows, std::memory_order_acq_rel);
    }

 private:
    const int64_t commit_interval_in_ms_;
    std::atomic<Clock::time_point> last_commit_time_;
    std::atomic<int64_t> committed_rows_{0};
    std::atomic<int64_t> appended_rows_{0};
};

}  // namespace milvus::index
