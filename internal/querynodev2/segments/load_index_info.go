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

package segments

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/load_index_c.h"
#include "common/binary_set_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LoadIndexInfo is a wrapper of the underlying C-structure C.CLoadIndexInfo
type LoadIndexInfo struct {
	cLoadIndexInfo C.CLoadIndexInfo
}

// newLoadIndexInfo returns a new LoadIndexInfo and error
func newLoadIndexInfo(ctx context.Context) (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo
	var status C.CStatus
	_, awaitErr := GetDynamicPool().Submit(func() (any, error) {
		status = C.NewLoadIndexInfo(&cLoadIndexInfo)
		return nil, nil
	}).Await()
	loadIndexInfo := &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}
	if awaitErr != nil {
		deleteLoadIndexInfo(loadIndexInfo)
		return nil, merr.Wrap(awaitErr, "execute NewLoadIndexInfo on dynamic pool")
	}
	if err := HandleCStatus(ctx, &status, "NewLoadIndexInfo failed"); err != nil {
		deleteLoadIndexInfo(loadIndexInfo)
		return nil, err
	}
	return loadIndexInfo, nil
}

// deleteLoadIndexInfo would delete C.CLoadIndexInfo
func deleteLoadIndexInfo(info *LoadIndexInfo) {
	if info == nil || info.cLoadIndexInfo == nil {
		return
	}
	handle := info.cLoadIndexInfo
	info.cLoadIndexInfo = nil
	completed, awaitErr := GetDynamicPool().Submit(func() (any, error) {
		C.DeleteLoadIndexInfo(handle)
		return true, nil
	}).Await()
	if deleted, ok := completed.(bool); ok && deleted {
		return
	}
	if awaitErr != nil {
		err := merr.Wrap(awaitErr, "execute DeleteLoadIndexInfo on dynamic pool")
		mlog.Warn(context.TODO(),
			"failed to delete load index info on dynamic pool, deleting synchronously",
			mlog.Err(err))
	}
	C.DeleteLoadIndexInfo(handle)
}

func (li *LoadIndexInfo) appendLoadIndexInfo(ctx context.Context, info *cgopb.LoadIndexInfo) error {
	marshaled, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	var data *C.uint8_t
	if len(marshaled) > 0 {
		data = (*C.uint8_t)(unsafe.Pointer(&marshaled[0]))
	}
	length := C.uint64_t(len(marshaled))
	var status C.CStatus
	_, awaitErr := GetDynamicPool().Submit(func() (any, error) {
		status = C.FinishLoadIndexInfo(li.cLoadIndexInfo, data, length)
		return nil, nil
	}).Await()
	runtime.KeepAlive(marshaled)
	if awaitErr != nil {
		return merr.Wrap(awaitErr, "execute FinishLoadIndexInfo on dynamic pool")
	}
	return HandleCStatus(ctx, &status, "FinishLoadIndexInfo failed")
}

func (li *LoadIndexInfo) setShard(ctx context.Context, shard string) error {
	if shard == "" {
		return nil
	}
	cShard := C.CString(shard)
	defer C.free(unsafe.Pointer(cShard))
	status := C.SetLoadIndexInfoShard(li.cLoadIndexInfo, cShard)
	return HandleCStatus(ctx, &status, "SetLoadIndexInfoShard failed")
}
