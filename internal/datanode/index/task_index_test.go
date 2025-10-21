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

package index

import (
    "testing"

    "github.com/milvus-io/milvus/pkg/v2/util/paramtable"
    "github.com/stretchr/testify/assert"
)

func TestSetTantivyBundleIndexParam_Toggle(t *testing.T) {
    paramtable.Init()

    // true
    paramtable.Get().Save("common.tantivy.bundleIndexFile", "true")
    p := setTantivyBundleIndexParam(map[string]string{})
    assert.Equal(t, "true", p["tantivy_bundle_index_file"]) // expect true

    // false
    paramtable.Get().Save("common.tantivy.bundleIndexFile", "false")
    p2 := setTantivyBundleIndexParam(map[string]string{"x": "y"})
    assert.Equal(t, "false", p2["tantivy_bundle_index_file"]) // expect false

    // existing key should be overridden
    paramtable.Get().Save("common.tantivy.bundleIndexFile", "true")
    p3 := setTantivyBundleIndexParam(map[string]string{"tantivy_bundle_index_file": "false"})
    assert.Equal(t, "true", p3["tantivy_bundle_index_file"]) // overridden to true
}
