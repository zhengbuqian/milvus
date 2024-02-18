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

package parquet

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/samber/lo"
	"golang.org/x/exp/constraints"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type FieldReader struct {
	columnIndex  int
	columnReader *pqarrow.ColumnReader

	dim   int
	field *schemapb.FieldSchema
}

func NewFieldReader(ctx context.Context, reader *pqarrow.FileReader, columnIndex int, field *schemapb.FieldSchema) (*FieldReader, error) {
	columnReader, err := reader.GetColumn(ctx, columnIndex)
	if err != nil {
		return nil, err
	}

	var dim int64 = 1
	if typeutil.IsVectorType(field.GetDataType()) {
		dim, err = typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
	}

	cr := &FieldReader{
		columnIndex:  columnIndex,
		columnReader: columnReader,
		dim:          int(dim),
		field:        field,
	}
	return cr, nil
}

func (c *FieldReader) Next(count int64) (any, error) {
	switch c.field.GetDataType() {
	case schemapb.DataType_Bool:
		return ReadBoolData(c, count)
	case schemapb.DataType_Int8:
		return ReadIntegerOrFloatData[int8](c, count)
	case schemapb.DataType_Int16:
		return ReadIntegerOrFloatData[int16](c, count)
	case schemapb.DataType_Int32:
		return ReadIntegerOrFloatData[int32](c, count)
	case schemapb.DataType_Int64:
		return ReadIntegerOrFloatData[int64](c, count)
	case schemapb.DataType_Float:
		data, err := ReadIntegerOrFloatData[float32](c, count)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, nil
		}
		return data, typeutil.VerifyFloats32(data.([]float32))
	case schemapb.DataType_Double:
		data, err := ReadIntegerOrFloatData[float64](c, count)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, nil
		}
		return data, typeutil.VerifyFloats64(data.([]float64))
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return ReadStringData(c, count)
	case schemapb.DataType_JSON:
		// JSON field read data from string array Parquet
		data, err := ReadStringData(c, count)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, nil
		}
		byteArr := make([][]byte, 0)
		for _, str := range data.([]string) {
			var dummy interface{}
			err = json.Unmarshal([]byte(str), &dummy)
			if err != nil {
				return nil, err
			}
			byteArr = append(byteArr, []byte(str))
		}
		return byteArr, nil
	case schemapb.DataType_BinaryVector:
		return ReadBinaryData(c, count)
	case schemapb.DataType_FloatVector:
		arrayData, err := ReadIntegerOrFloatArrayData[float32](c, count)
		if err != nil {
			return nil, err
		}
		if arrayData == nil {
			return nil, nil
		}
		vectors := lo.Flatten(arrayData.([][]float32))
		return vectors, nil
	// TODO(SPARSE): add sparse support
	case schemapb.DataType_Array:
		data := make([]*schemapb.ScalarField, 0, count)
		elementType := c.field.GetElementType()
		switch elementType {
		case schemapb.DataType_Bool:
			boolArray, err := ReadBoolArrayData(c, count)
			if err != nil {
				return nil, err
			}
			if boolArray == nil {
				return nil, nil
			}
			for _, elementArray := range boolArray.([][]bool) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Int8:
			int8Array, err := ReadIntegerOrFloatArrayData[int32](c, count)
			if err != nil {
				return nil, err
			}
			if int8Array == nil {
				return nil, nil
			}
			for _, elementArray := range int8Array.([][]int32) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Int16:
			int16Array, err := ReadIntegerOrFloatArrayData[int32](c, count)
			if err != nil {
				return nil, err
			}
			if int16Array == nil {
				return nil, nil
			}
			for _, elementArray := range int16Array.([][]int32) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Int32:
			int32Array, err := ReadIntegerOrFloatArrayData[int32](c, count)
			if err != nil {
				return nil, err
			}
			if int32Array == nil {
				return nil, nil
			}
			for _, elementArray := range int32Array.([][]int32) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Int64:
			int64Array, err := ReadIntegerOrFloatArrayData[int64](c, count)
			if err != nil {
				return nil, err
			}
			if int64Array == nil {
				return nil, nil
			}
			for _, elementArray := range int64Array.([][]int64) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Float:
			float32Array, err := ReadIntegerOrFloatArrayData[float32](c, count)
			if err != nil {
				return nil, err
			}
			if float32Array == nil {
				return nil, nil
			}
			for _, elementArray := range float32Array.([][]float32) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Double:
			float64Array, err := ReadIntegerOrFloatArrayData[float64](c, count)
			if err != nil {
				return nil, err
			}
			if float64Array == nil {
				return nil, nil
			}
			for _, elementArray := range float64Array.([][]float64) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_VarChar, schemapb.DataType_String:
			stringArray, err := ReadStringArrayData(c, count)
			if err != nil {
				return nil, err
			}
			if stringArray == nil {
				return nil, nil
			}
			for _, elementArray := range stringArray.([][]string) {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: elementArray,
						},
					},
				})
			}
		default:
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type '%s' for array field '%s'",
				elementType.String(), c.field.GetName()))
		}
		return data, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type '%s' for field '%s'",
			c.field.GetDataType().String(), c.field.GetName()))
	}
}

func (c *FieldReader) Close() {}

func ReadBoolData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		chunkData := make([]bool, dataNums)
		boolReader, ok := chunk.(*array.Boolean)
		if !ok {
			return nil, WrapTypeErr("bool", chunk.DataType().Name(), pcr.field)
		}
		for i := 0; i < dataNums; i++ {
			chunkData[i] = boolReader.Value(i)
		}
		data = append(data, chunkData...)
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadIntegerOrFloatData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]T, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		chunkData := make([]T, dataNums)
		switch chunk.DataType().ID() {
		case arrow.INT8:
			int8Reader := chunk.(*array.Int8)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int8Reader.Value(i))
			}
		case arrow.INT16:
			int16Reader := chunk.(*array.Int16)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int16Reader.Value(i))
			}
		case arrow.INT32:
			int32Reader := chunk.(*array.Int32)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int32Reader.Value(i))
			}
		case arrow.INT64:
			int64Reader := chunk.(*array.Int64)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int64Reader.Value(i))
			}
		case arrow.FLOAT32:
			float32Reader := chunk.(*array.Float32)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(float32Reader.Value(i))
			}
		case arrow.FLOAT64:
			float64Reader := chunk.(*array.Float64)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(float64Reader.Value(i))
			}
		default:
			return nil, WrapTypeErr("integer|float", chunk.DataType().Name(), pcr.field)
		}
		data = append(data, chunkData...)
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadStringData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		chunkData := make([]string, dataNums)
		stringReader, ok := chunk.(*array.String)
		if !ok {
			return nil, WrapTypeErr("string", chunk.DataType().Name(), pcr.field)
		}
		for i := 0; i < dataNums; i++ {
			chunkData[i] = stringReader.Value(i)
		}
		data = append(data, chunkData...)
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadBinaryData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		switch chunk.DataType().ID() {
		case arrow.BINARY:
			binaryReader := chunk.(*array.Binary)
			for i := 0; i < dataNums; i++ {
				data = append(data, binaryReader.Value(i)...)
			}
		case arrow.LIST:
			listReader := chunk.(*array.List)
			if !isRegularVector(listReader.Offsets(), pcr.dim, true) {
				return nil, merr.WrapErrImportFailed("binary vector is irregular")
			}
			uint8Reader, ok := listReader.ListValues().(*array.Uint8)
			if !ok {
				return nil, WrapTypeErr("binary", listReader.ListValues().DataType().Name(), pcr.field)
			}
			for i := 0; i < uint8Reader.Len(); i++ {
				data = append(data, uint8Reader.Value(i))
			}
		default:
			return nil, WrapTypeErr("binary", chunk.DataType().Name(), pcr.field)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func isRegularVector(offsets []int32, dim int, isBinary bool) bool {
	if len(offsets) < 1 {
		return false
	}
	if isBinary {
		dim = dim / 8
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i]-offsets[i-1] != int32(dim) {
			return false
		}
	}
	return true
}

func ReadBoolArrayData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		boolReader, ok := listReader.ListValues().(*array.Boolean)
		if !ok {
			return nil, WrapTypeErr("boolArray", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]bool, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, boolReader.Value(int(j)))
			}
			data = append(data, elementData)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadIntegerOrFloatArrayData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]T, 0, count)

	getDataFunc := func(offsets []int32, getValue func(int) T) {
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]T, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, getValue(int(j)))
			}
			data = append(data, elementData)
		}
	}
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		if typeutil.IsVectorType(pcr.field.GetDataType()) &&
			!isRegularVector(offsets, pcr.dim, pcr.field.GetDataType() == schemapb.DataType_BinaryVector) {
			return nil, merr.WrapErrImportFailed("float vector is irregular")
		}
		valueReader := listReader.ListValues()
		switch valueReader.DataType().ID() {
		case arrow.INT8:
			int8Reader := valueReader.(*array.Int8)
			getDataFunc(offsets, func(i int) T {
				return T(int8Reader.Value(i))
			})
		case arrow.INT16:
			int16Reader := valueReader.(*array.Int16)
			getDataFunc(offsets, func(i int) T {
				return T(int16Reader.Value(i))
			})
		case arrow.INT32:
			int32Reader := valueReader.(*array.Int32)
			getDataFunc(offsets, func(i int) T {
				return T(int32Reader.Value(i))
			})
		case arrow.INT64:
			int64Reader := valueReader.(*array.Int64)
			getDataFunc(offsets, func(i int) T {
				return T(int64Reader.Value(i))
			})
		case arrow.FLOAT32:
			float32Reader := valueReader.(*array.Float32)
			getDataFunc(offsets, func(i int) T {
				return T(float32Reader.Value(i))
			})
		case arrow.FLOAT64:
			float64Reader := valueReader.(*array.Float64)
			getDataFunc(offsets, func(i int) T {
				return T(float64Reader.Value(i))
			})
		default:
			return nil, WrapTypeErr("integerArray|floatArray", chunk.DataType().Name(), pcr.field)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadStringArrayData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		stringReader, ok := listReader.ListValues().(*array.String)
		if !ok {
			return nil, WrapTypeErr("stringArray", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]string, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, stringReader.Value(int(j)))
			}
			data = append(data, elementData)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}
