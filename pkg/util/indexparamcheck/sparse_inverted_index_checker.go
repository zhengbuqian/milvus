package indexparamcheck

type sparseInvertedIndexChecker struct {
	sparseFloatVectorBaseChecker
}

func (c sparseInvertedIndexChecker) CheckTrain(params map[string]string) error {
	return nil
}

func newSparseInvertedIndexChecker() *sparseInvertedIndexChecker {
	return &sparseInvertedIndexChecker{}
}
