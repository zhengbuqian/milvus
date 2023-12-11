package indexcgowrapper

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	keyRawArr = "key_raw_arr"
)

type Dataset struct {
	DType schemapb.DataType
	Data  map[string]interface{}
}

func GenFloatVecDataset(vectors []float32) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_FloatVector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenSparseFloatVecDataset(data *storage.SparseFloatVectorFieldData) *Dataset {
	res, err := typeutil.SparseFloatArrayToBytes(data.ToProto())
	if err != nil {
		// TODO(SPARSE) gracefully handle error
		panic(err)
	}
	return &Dataset{
		DType: schemapb.DataType_SparseFloatVector,
		Data: map[string]interface{}{
			keyRawArr: res,
		},
	}
}

func GenBinaryVecDataset(vectors []byte) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_BinaryVector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenDataset(data storage.FieldData) *Dataset {
	switch f := data.(type) {
	case *storage.BoolFieldData:
		return &Dataset{
			DType: schemapb.DataType_Bool,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int8FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int8,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int16FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int16,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int32FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int32,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int64FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int64,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.FloatFieldData:
		return &Dataset{
			DType: schemapb.DataType_Float,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.DoubleFieldData:
		return &Dataset{
			DType: schemapb.DataType_Double,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.StringFieldData:
		return &Dataset{
			DType: schemapb.DataType_String,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.BinaryVectorFieldData:
		return GenBinaryVecDataset(f.Data)
	case *storage.FloatVectorFieldData:
		return GenFloatVecDataset(f.Data)
	case *storage.SparseFloatVectorFieldData:
		return GenSparseFloatVecDataset(f)
	default:
		return &Dataset{
			DType: schemapb.DataType_None,
			Data:  nil,
		}
	}
}
