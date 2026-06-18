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

//go:build replace_index_e2e

package milvusclient_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func TestReplaceIndexScalarLoadedE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{Address: "127.0.0.1:19530"})
	if err != nil {
		t.Fatalf("connect milvus: %v", err)
	}
	defer cli.Close(ctx)

	waitServerReady(t, ctx, cli)

	collectionName := fmt.Sprintf("replace_idx_scalar_%d", time.Now().UnixNano())
	const (
		idField     = "id"
		scoreField  = "score"
		vectorField = "vector"
		dim         = 4
		rowCount    = 4096
	)

	_ = cli.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	defer func() {
		_ = cli.DropCollection(context.Background(), milvusclient.NewDropCollectionOption(collectionName))
	}()

	schema := entity.NewSchema().
		WithField(entity.NewField().WithName(idField).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(scoreField).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(vectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(dim))

	if err := cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema)); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	ids := make([]int64, 0, rowCount)
	scores := make([]int64, 0, rowCount)
	vectors := make([][]float32, 0, rowCount)
	for i := int64(0); i < rowCount; i++ {
		ids = append(ids, i)
		scores = append(scores, i%8)
		vectors = append(vectors, []float32{float32(i), float32(i + 1), float32(i % 3), 1})
	}

	if _, err := cli.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithInt64Column(idField, ids).
		WithInt64Column(scoreField, scores).
		WithFloatVectorColumn(vectorField, dim, vectors)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	flushTask, err := cli.Flush(ctx, milvusclient.NewFlushOption(collectionName))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := flushTask.Await(ctx); err != nil {
		t.Fatalf("await flush: %v", err)
	}

	vectorIndexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, vectorField, index.NewFlatIndex(entity.L2)).WithIndexName("vector_idx"))
	if err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	if err := vectorIndexTask.Await(ctx); err != nil {
		t.Fatalf("await vector index: %v", err)
	}

	oldScalarIndex := "score_old_idx"
	scalarIndexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, scoreField, index.NewInvertedIndex()).WithIndexName(oldScalarIndex))
	if err != nil {
		t.Fatalf("create old scalar index: %v", err)
	}
	if err := scalarIndexTask.Await(ctx); err != nil {
		t.Fatalf("await old scalar index: %v", err)
	}

	loadTask, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	if err := loadTask.Await(ctx); err != nil {
		t.Fatalf("await load: %v", err)
	}

	assertQueryAndSearchReady(t, ctx, cli, collectionName, scoreField, vectorField)

	searchStats, stopSearch := startContinuousSearch(t, ctx, cli, collectionName, scoreField, vectorField)
	defer stopSearch()
	waitContinuousSearchReady(t, ctx, searchStats)

	intermediateScalarIndex := "score_gen2_idx"
	newScalarIndex := "score_new_idx"
	searchStats.replacing.Store(true)
	_, err = cli.ReplaceIndex(ctx, milvusclient.NewReplaceIndexOption(collectionName, scoreField, index.NewBitmapIndex()).
		WithNewIndexName(intermediateScalarIndex))
	if err != nil {
		t.Fatalf("replace scalar index to intermediate generation: %v", err)
	}
	replaceTask, err := cli.ReplaceIndex(ctx, milvusclient.NewReplaceIndexOption(collectionName, scoreField, index.NewInvertedIndex()).
		WithNewIndexName(newScalarIndex))
	if err != nil {
		t.Fatalf("replace scalar index to final generation: %v", err)
	}
	if err := replaceTask.Await(ctx); err != nil {
		t.Fatalf("await final replacement index: %v", err)
	}
	searchStats.replacing.Store(false)

	desc, err := cli.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption(collectionName, newScalarIndex))
	if err != nil {
		t.Fatalf("describe replacement index: %v", err)
	}
	if desc.State != index.IndexState(commonpb.IndexState_Finished) {
		t.Fatalf("replacement index state = %v, want finished", desc.State)
	}

	assertQueryAndSearchReady(t, ctx, cli, collectionName, scoreField, vectorField)
	assertContinuousSearchPassed(t, searchStats)
}

type continuousSearchStats struct {
	errCh          chan error
	readyCh        chan struct{}
	stop           context.CancelFunc
	wg             sync.WaitGroup
	readyOnce      sync.Once
	replacing      atomic.Bool
	successes      atomic.Int64
	duringAttempts atomic.Int64
	duringSuccess  atomic.Int64
}

func startContinuousSearch(t *testing.T, ctx context.Context, cli *milvusclient.Client, collectionName string, scoreField string, vectorField string) (*continuousSearchStats, func()) {
	t.Helper()

	searchCtx, stop := context.WithCancel(ctx)
	stats := &continuousSearchStats{
		errCh:   make(chan error, 1),
		readyCh: make(chan struct{}),
		stop:    stop,
	}
	for workerID := 0; workerID < 4; workerID++ {
		stats.wg.Add(1)
		go func(workerID int) {
			defer stats.wg.Done()
			for {
				select {
				case <-searchCtx.Done():
					return
				default:
				}

				duringReplacement := stats.replacing.Load()
				if duringReplacement {
					stats.duringAttempts.Add(1)
				}
				searchResults, err := cli.Search(searchCtx, milvusclient.NewSearchOption(collectionName, 5, []entity.Vector{
					entity.FloatVector([]float32{0, 1, 0, 1}),
				}).
					WithANNSField(vectorField).
					WithFilter(fmt.Sprintf("%s >= 0", scoreField)).
					WithOutputFields("id", scoreField).
					WithConsistencyLevel(entity.ClStrong))
				if err != nil {
					sendContinuousSearchError(stats.errCh, fmt.Errorf("continuous search worker %d failed: %w", workerID, err))
					return
				}
				if len(searchResults) != 1 || searchResults[0].ResultCount == 0 {
					sendContinuousSearchError(stats.errCh, fmt.Errorf("continuous search worker %d returned empty results: %+v", workerID, searchResults))
					return
				}
				stats.successes.Add(1)
				if duringReplacement || stats.replacing.Load() {
					stats.duringSuccess.Add(1)
				}
				stats.readyOnce.Do(func() {
					close(stats.readyCh)
				})
			}
		}(workerID)
	}

	return stats, func() {
		stop()
		stats.wg.Wait()
	}
}

func waitContinuousSearchReady(t *testing.T, ctx context.Context, stats *continuousSearchStats) {
	t.Helper()

	select {
	case <-stats.readyCh:
	case err := <-stats.errCh:
		t.Fatalf("continuous search failed before replace: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatalf("continuous search did not become ready")
	case <-ctx.Done():
		t.Fatalf("continuous search readiness context done: %v", ctx.Err())
	}
}

func assertContinuousSearchPassed(t *testing.T, stats *continuousSearchStats) {
	t.Helper()

	select {
	case err := <-stats.errCh:
		t.Fatalf("continuous search failed during replace: %v", err)
	default:
	}
	if stats.successes.Load() == 0 {
		t.Fatalf("continuous search recorded no successful requests")
	}
	if stats.duringAttempts.Load() == 0 {
		t.Fatalf("continuous search recorded no attempts during replacement")
	}
	if stats.duringSuccess.Load() == 0 {
		t.Fatalf("continuous search recorded no successful requests during replacement")
	}
}

func sendContinuousSearchError(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func waitServerReady(t *testing.T, ctx context.Context, cli *milvusclient.Client) {
	t.Helper()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.After(2 * time.Minute)
	for {
		_, err := cli.GetServerVersion(ctx, milvusclient.NewGetServerVersionOption())
		if err == nil {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("server did not become ready: %v", err)
		case <-ctx.Done():
			t.Fatalf("server readiness context done: %v", ctx.Err())
		}
	}
}

func assertQueryAndSearchReady(t *testing.T, ctx context.Context, cli *milvusclient.Client, collectionName string, scoreField string, vectorField string) {
	t.Helper()

	queryResult, err := cli.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("%s >= 0", scoreField)).
		WithOutputFields("id", scoreField).
		WithConsistencyLevel(entity.ClStrong))
	if err != nil {
		t.Fatalf("query loaded collection: %v", err)
	}
	if queryResult.ResultCount == 0 {
		t.Fatalf("query returned no rows")
	}

	searchResults, err := cli.Search(ctx, milvusclient.NewSearchOption(collectionName, 5, []entity.Vector{
		entity.FloatVector([]float32{0, 1, 0, 1}),
	}).
		WithANNSField(vectorField).
		WithFilter(fmt.Sprintf("%s >= 0", scoreField)).
		WithOutputFields("id", scoreField).
		WithConsistencyLevel(entity.ClStrong))
	if err != nil {
		t.Fatalf("search loaded collection: %v", err)
	}
	if len(searchResults) != 1 || searchResults[0].ResultCount == 0 {
		t.Fatalf("search returned empty results: %+v", searchResults)
	}
}
