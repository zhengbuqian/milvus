package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

// TODO(SPARSE) sparse vector don't check for dim, but baseChecker does, thus not including baseChecker for now
type sparseFloatVectorBaseChecker struct{}

func (c sparseFloatVectorBaseChecker) StaticCheck(params map[string]string) error {
	if !CheckStrByValues(params, Metric, SparseMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", SparseMetrics)
	}

	return nil
}

func (c sparseFloatVectorBaseChecker) CheckTrain(params map[string]string) error {
	// TODO(SPARSE) check drop ratio in range
	return nil
}

func (c sparseFloatVectorBaseChecker) CheckValidDataType(dType schemapb.DataType) error {
	if dType != schemapb.DataType_SparseFloatVector {
		return fmt.Errorf("only sparse float vector is supported")
	}
	return nil
}

func (c sparseFloatVectorBaseChecker) SetDefaultMetricTypeIfNotExist(params map[string]string) {
	setDefaultIfNotExist(params, common.MetricTypeKey, SparseFloatVectorDefaultMetricType)
}

func newSparseFloatVectorBaseChecker() IndexChecker {
	return &sparseFloatVectorBaseChecker{}
}
