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

#include "index/contracts/query/IndexReader.h"

namespace milvus::index {

// Optional consuming capability for a completed artifact whose state can
// directly become a query reader without a storage round trip. A capability
// pointer obtained from an artifact is borrowed; conversion must go through the
// owning ConsumeIndexArtifact helper so the artifact shell stays alive. Once
// IntoReader is invoked, neither conversion nor serialization may be retried.
class ReaderConvertible {
 public:
    virtual ~ReaderConvertible() = default;

    virtual IndexReaderBasePtr
    IntoReader() && = 0;
};

}  // namespace milvus::index
