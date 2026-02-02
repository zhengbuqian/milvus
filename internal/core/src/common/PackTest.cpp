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

#include <gtest/gtest.h>

#include "common/Pack.h"

namespace milvus {

TEST(PackTest, IsUnifiedScalarIndexVersion) {
    // Version 0, 1, 2 should not be unified
    EXPECT_FALSE(IsUnifiedScalarIndexVersion(0));
    EXPECT_FALSE(IsUnifiedScalarIndexVersion(1));
    EXPECT_FALSE(IsUnifiedScalarIndexVersion(2));

    // Version 3+ should be unified
    EXPECT_TRUE(IsUnifiedScalarIndexVersion(3));
    EXPECT_TRUE(IsUnifiedScalarIndexVersion(4));
    EXPECT_TRUE(IsUnifiedScalarIndexVersion(100));
}

}  // namespace milvus
