package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type hnswChecker struct {
	floatVectorBaseChecker
}

func (c hnswChecker) StaticCheck(params map[string]string) error {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return errOutOfRange(EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction)
	}
	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return errOutOfRange(HNSWM, HNSWMinM, HNSWMaxM)
	}
	if !CheckStrByValues(params, Metric, HnswMetrics) {
		return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], HnswMetrics)
	}
	return nil
}

func (c hnswChecker) CheckTrain(params map[string]string) error {
	if err := c.StaticCheck(params); err != nil {
		return err
	}
	return c.baseChecker.CheckTrain(params)
}

func (c hnswChecker) CheckValidDataType(dType schemapb.DataType) error {
	// TODO(SPARSE) we'll add sparse vector support in HNSW later in cardinal
	if dType != schemapb.DataType_FloatVector && dType != schemapb.DataType_BinaryVector && dType != schemapb.DataType_Float16Vector && dType != schemapb.DataType_BFloat16Vector {
		return fmt.Errorf("only support float vector or binary vector")
	}
	return nil
}

func newHnswChecker() IndexChecker {
	return &hnswChecker{}
}
