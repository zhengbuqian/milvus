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

package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/function/validator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSearchInfoDetermineSearchTypeWithPluralGroupByFieldIDs(t *testing.T) {
	info := &SearchInfo{
		planInfo: &planpb.QueryInfo{
			GroupByFieldIds: []int64{101},
		},
	}

	assert.Equal(t, internalpb.SearchType_DEFAULT, info.DetermineSearchType(false))
}

func TestParseGroupByInfoLegacyFieldPrecedence(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "brand", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "category", DataType: schemapb.DataType_VarChar},
	}}

	info, err := parseGroupByInfo([]*commonpb.KeyValuePair{
		{Key: GroupByFieldKey, Value: "brand"},
		{Key: GroupByFieldsKey, Value: "category"},
	}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int64{101}, info.groupByFieldIds)
	assert.Equal(t, []string{"brand"}, info.groupByFieldNames)

	info, err = parseGroupByInfo([]*commonpb.KeyValuePair{
		{Key: GroupByFieldKey, Value: " "},
		{Key: GroupByFieldsKey, Value: "brand, category"},
	}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int64{101, 102}, info.groupByFieldIds)
	assert.Equal(t, []string{"brand", "category"}, info.groupByFieldNames)
}

func TestValidateCollectionName(t *testing.T) {
	assert.Nil(t, validateCollectionName("abc"))
	assert.Nil(t, validateCollectionName("_123abc"))
	assert.Nil(t, validateCollectionName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
		"abc ",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateCollectionName(name))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "name"))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "alias"))
	}
}

func TestValidateCollectionDescription(t *testing.T) {
	maxLength := Params.ProxyCfg.MaxCollectionDescriptionLength.GetAsInt()
	tests := []struct {
		name        string
		description string
		wantErr     bool
	}{
		{
			name:        "empty description",
			description: "",
		},
		{
			name:        "exactly max bytes",
			description: strings.Repeat("a", maxLength),
		},
		{
			name:        "over max bytes",
			description: strings.Repeat("a", maxLength+1),
			wantErr:     true,
		},
		{
			name:        "cjk rune count under max but byte length over max",
			description: strings.Repeat("中", maxLength/3+1),
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCollectionDescription(test.description)
			if test.wantErr {
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			assert.NoError(t, err)
		})
	}

	t.Run("uses refreshable paramtable limit", func(t *testing.T) {
		old := Params.ProxyCfg.MaxCollectionDescriptionLength.SwapTempValue("3")
		defer Params.ProxyCfg.MaxCollectionDescriptionLength.SwapTempValue(old)

		err := validateCollectionDescription("abcd")
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func TestValidateResourceGroupName(t *testing.T) {
	assert.Nil(t, ValidateResourceGroupName("abc"))
	assert.Nil(t, ValidateResourceGroupName("_123abc"))
	assert.Nil(t, ValidateResourceGroupName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, ValidateResourceGroupName(name))
	}
}

func TestNamespacePartitionRoutingHelpers(t *testing.T) {
	namespace := "tenant_partition"
	partitionModeSchema := &schemapb.CollectionSchema{
		EnableNamespace: true,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
		},
	}
	partitionKeyModeSchema := &schemapb.CollectionSchema{
		EnableNamespace: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: common.NamespaceFieldName, DataType: schemapb.DataType_VarChar, IsPartitionKey: true},
		},
	}

	t.Run("single partition name", func(t *testing.T) {
		partitionName, usedNamespacePartition, err := resolveNamespacePartitionName(partitionModeSchema, &namespace, "")
		require.NoError(t, err)
		assert.True(t, usedNamespacePartition)
		assert.Equal(t, namespace, partitionName)

		partitionName, usedNamespacePartition, err = resolveNamespacePartitionName(partitionModeSchema, &namespace, namespace)
		require.NoError(t, err)
		assert.True(t, usedNamespacePartition)
		assert.Equal(t, namespace, partitionName)

		_, _, err = resolveNamespacePartitionName(partitionModeSchema, &namespace, "other_partition")
		require.ErrorIs(t, err, merr.ErrParameterInvalid)

		emptyNamespace := ""
		_, _, err = resolveNamespacePartitionName(partitionModeSchema, &emptyNamespace, "")
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("partition name list", func(t *testing.T) {
		partitionNames, usedNamespacePartition, err := resolveNamespacePartitionNames(partitionModeSchema, &namespace, nil)
		require.NoError(t, err)
		assert.True(t, usedNamespacePartition)
		assert.Equal(t, []string{namespace}, partitionNames)

		partitionNames, usedNamespacePartition, err = resolveNamespacePartitionNames(partitionModeSchema, &namespace, []string{namespace})
		require.NoError(t, err)
		assert.True(t, usedNamespacePartition)
		assert.Equal(t, []string{namespace}, partitionNames)

		_, _, err = resolveNamespacePartitionNames(partitionModeSchema, &namespace, []string{"other_partition"})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)

		_, _, err = resolveNamespacePartitionNames(partitionModeSchema, &namespace, []string{namespace, "other_partition"})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("default mode keeps namespace as plan filter", func(t *testing.T) {
		partitionName, usedNamespacePartition, err := resolveNamespacePartitionName(partitionKeyModeSchema, &namespace, "")
		require.NoError(t, err)
		assert.False(t, usedNamespacePartition)
		assert.Empty(t, partitionName)

		partitionNames, usedNamespacePartition, err := resolveNamespacePartitionNames(partitionKeyModeSchema, &namespace, nil)
		require.NoError(t, err)
		assert.False(t, usedNamespacePartition)
		assert.Empty(t, partitionNames)

		assert.Same(t, &namespace, namespaceForPlan(partitionKeyModeSchema, &namespace))
		assert.Nil(t, namespaceForPlan(partitionModeSchema, &namespace))
	})
}

func TestAddNamespaceDataPartitionMode(t *testing.T) {
	namespace := "tenant_partition"
	schema := &schemapb.CollectionSchema{
		EnableNamespace: true,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
		},
	}
	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			Namespace: &namespace,
			NumRows:   3,
		},
	}

	err := addNamespaceData(schema, insertMsg)
	require.NoError(t, err)
	assert.Equal(t, namespace, insertMsg.GetPartitionName())
	assert.Empty(t, insertMsg.GetFieldsData())
}

func TestConvertHybridSearchToSearchCopiesNamespace(t *testing.T) {
	namespace := "tenant_partition"
	req := &milvuspb.HybridSearchRequest{
		DbName:         "default",
		CollectionName: "coll",
		Namespace:      &namespace,
		Requests: []*milvuspb.SearchRequest{
			{Dsl: "pk > 0"},
		},
	}

	searchReq := convertHybridSearchToSearch(req)
	require.NotNil(t, searchReq.Namespace)
	assert.Equal(t, namespace, searchReq.GetNamespace())
}

func TestValidateDatabaseName(t *testing.T) {
	assert.Nil(t, ValidateDatabaseName("dbname"))
	assert.Nil(t, ValidateDatabaseName("_123abc"))
	assert.Nil(t, ValidateDatabaseName("abc123_"))

	longName := make([]byte, 512)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.Error(t, ValidateDatabaseName(name))
	}
}

func TestValidatePartitionTag(t *testing.T) {
	assert.Nil(t, validatePartitionTag("abc", true))
	assert.Nil(t, validatePartitionTag("123abc", true))
	assert.Nil(t, validatePartitionTag("_123abc", true))
	assert.Nil(t, validatePartitionTag("abc123_", true))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validatePartitionTag(name, true))
	}

	assert.Nil(t, validatePartitionTag("ab cd", false))
	assert.Nil(t, validatePartitionTag("ab*", false))
}

func TestValidateFieldName(t *testing.T) {
	assert.Nil(t, validateFieldName("abc"))
	assert.Nil(t, validateFieldName("_123abc"))
	assert.Nil(t, validateFieldName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
		"True",
		"null",
		"Null",
		"NULL",
		"nUlL",
		"NuLL",
		"array_contains",
		"json_contains_any",
		"ARRAY_LENGTH",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateFieldName(name))
	}
}

// Regression for #49314: user-supplied field named __virtual_pk__ (or
// RowID / Timestamp) must be rejected at CreateCollection to keep the
// system namespace clean. Covers regular fields and struct-array
// fields (both the struct name and its inner fields).
func TestValidateReservedFieldNames(t *testing.T) {
	// Accepts non-reserved names.
	ok := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id"},
			{Name: "embedding"},
		},
	}
	assert.NoError(t, validateReservedFieldNames(ok))

	for _, reserved := range []string{
		common.VirtualPKFieldName,
		common.RowIDFieldName,
		common.TimeStampFieldName,
	} {
		s := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: reserved, DataType: schemapb.DataType_Int64},
			},
		}
		err := validateReservedFieldNames(s)
		assert.Error(t, err, "reserved name %q must be rejected", reserved)
		assert.Contains(t, err.Error(), "reserved")
	}

	// Struct-array field name collision.
	s := &schemapb.CollectionSchema{
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{Name: common.VirtualPKFieldName, Fields: []*schemapb.FieldSchema{{Name: "a"}}},
		},
	}
	assert.Error(t, validateReservedFieldNames(s))

	// Struct-array inner field name collision.
	s2 := &schemapb.CollectionSchema{
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{Name: "ok_struct", Fields: []*schemapb.FieldSchema{{Name: common.RowIDFieldName}}},
		},
	}
	assert.Error(t, validateReservedFieldNames(s2))
}

func TestValidateDimension(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "1",
			},
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema = &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "2",
			},
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: Params.ProxyCfg.MaxDimension.GetValue(),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	// invalid dim
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "-1",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension.GetAsInt32() + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_BinaryVector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "8",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(Params.ProxyCfg.MaxDimension.GetAsInt()),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "9",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "262145",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_Int8Vector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "200",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "201",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension.GetAsInt32() + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
}

func TestValidateVectorFieldMetricType(t *testing.T) {
	field1 := &schemapb.FieldSchema{
		Name:         "",
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	assert.Nil(t, validateVectorFieldMetricType(field1))
	field1.DataType = schemapb.DataType_FloatVector
	assert.NotNil(t, validateVectorFieldMetricType(field1))
	field1.IndexParams = []*commonpb.KeyValuePair{
		{
			Key:   "abcdefg",
			Value: "",
		},
	}
	assert.NotNil(t, validateVectorFieldMetricType(field1))
	field1.IndexParams = append(field1.IndexParams, &commonpb.KeyValuePair{
		Key:   common.MetricTypeKey,
		Value: "",
	})
	assert.Nil(t, validateVectorFieldMetricType(field1))
}

func TestValidateDuplicatedFieldName(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{Name: "abc"},
		{Name: "def"},
	}
	assert.Nil(t, validateDuplicatedFieldName(fields))
	fields = append(fields, &schemapb.FieldSchema{
		Name: "abc",
	})
	assert.NotNil(t, validateDuplicatedFieldName(fields))
}

func TestValidatePrimaryKey(t *testing.T) {
	boolField := &schemapb.FieldSchema{
		Name:         "boolField",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_Bool,
	}

	int64Field := &schemapb.FieldSchema{
		Name:         "int64Field",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_Int64,
	}

	VarCharField := &schemapb.FieldSchema{
		Name:         "VarCharField",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "100",
			},
		},
	}

	// test collection without pk field
	assert.Error(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField},
	}))

	// test collection with int64 field ad pk
	int64Field.IsPrimaryKey = true
	assert.Nil(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, int64Field},
	}))

	// test collection with varChar field as pk
	VarCharField.IsPrimaryKey = true
	assert.Nil(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, VarCharField},
	}))

	// test collection with multi pk field
	assert.Error(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, int64Field, VarCharField},
	}))

	// test collection with varChar field as primary and autoID = true
	VarCharField.AutoID = true
	assert.Nil(t, validatePrimaryKey(&schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{boolField, VarCharField},
	}))
}

func TestValidateFieldType(t *testing.T) {
	type testCase struct {
		dt       schemapb.DataType
		et       schemapb.DataType
		validate bool
	}
	cases := []testCase{
		{
			dt:       schemapb.DataType_Bool,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int8,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int16,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int32,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Int64,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Float,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Double,
			validate: true,
		},
		{
			dt:       schemapb.DataType_FloatVector,
			validate: true,
		},
		{
			dt:       schemapb.DataType_BinaryVector,
			validate: true,
		},
		{
			dt:       schemapb.DataType_None,
			validate: false,
		},
		{
			dt:       schemapb.DataType_VarChar,
			validate: true,
		},
		{
			dt:       schemapb.DataType_String,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Bool,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int8,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int16,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int32,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Int64,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Float,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Double,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_VarChar,
			validate: true,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_String,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_None,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_JSON,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_Array,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_FloatVector,
			validate: false,
		},
		{
			dt:       schemapb.DataType_Array,
			et:       schemapb.DataType_BinaryVector,
			validate: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.dt.String(), func(t *testing.T) {
			sch := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						DataType:    tc.dt,
						ElementType: tc.et,
					},
				},
			}
			err := validateFieldType(sch)
			if tc.validate {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateSchema(t *testing.T) {
	coll := &schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      false,
		Fields:      nil,
	}
	assert.NotNil(t, validateSchema(coll))

	pf1 := &schemapb.FieldSchema{
		Name:         "f1",
		FieldID:      100,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf1)
	assert.NotNil(t, validateSchema(coll))

	pf1.IsPrimaryKey = true
	assert.Nil(t, validateSchema(coll))

	pf1.DataType = schemapb.DataType_Int32
	assert.NotNil(t, validateSchema(coll))

	pf1.DataType = schemapb.DataType_Int64
	assert.Nil(t, validateSchema(coll))

	pf2 := &schemapb.FieldSchema{
		Name:         "f2",
		FieldID:      101,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf2)
	assert.NotNil(t, validateSchema(coll))

	pf2.IsPrimaryKey = false
	assert.Nil(t, validateSchema(coll))

	pf2.Name = "f1"
	assert.NotNil(t, validateSchema(coll))
	pf2.Name = "f2"
	assert.Nil(t, validateSchema(coll))

	pf2.FieldID = 100
	assert.NotNil(t, validateSchema(coll))

	pf2.FieldID = 101
	assert.Nil(t, validateSchema(coll))

	pf2.DataType = -1
	assert.NotNil(t, validateSchema(coll))

	pf2.DataType = schemapb.DataType_FloatVector
	assert.NotNil(t, validateSchema(coll))

	pf2.DataType = schemapb.DataType_Int64
	assert.Nil(t, validateSchema(coll))

	tp3Good := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "128",
		},
	}

	tp3Bad1 := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "asdfa",
		},
	}

	tp3Bad2 := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "-1",
		},
	}

	tp3Bad3 := []*commonpb.KeyValuePair{
		{
			Key:   "dimX",
			Value: "128",
		},
	}

	tp3Bad4 := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "128",
		},
		{
			Key:   common.DimKey,
			Value: "64",
		},
	}

	ip3Good := []*commonpb.KeyValuePair{
		{
			Key:   common.MetricTypeKey,
			Value: "IP",
		},
	}

	ip3Bad1 := []*commonpb.KeyValuePair{
		{
			Key:   common.MetricTypeKey,
			Value: "JACCARD",
		},
	}

	ip3Bad2 := []*commonpb.KeyValuePair{
		{
			Key:   common.MetricTypeKey,
			Value: "xxxxxx",
		},
	}

	ip3Bad3 := []*commonpb.KeyValuePair{
		{
			Key:   common.MetricTypeKey,
			Value: "L2",
		},
		{
			Key:   common.MetricTypeKey,
			Value: "IP",
		},
	}

	pf3 := &schemapb.FieldSchema{
		Name:         "f3",
		FieldID:      102,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   tp3Good,
		IndexParams:  ip3Good,
	}

	coll.Fields = append(coll.Fields, pf3)
	assert.Nil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad1
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad2
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad3
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Bad4
	assert.NotNil(t, validateSchema(coll))

	pf3.TypeParams = tp3Good
	assert.Nil(t, validateSchema(coll))

	pf3.IndexParams = ip3Bad1
	assert.NotNil(t, validateSchema(coll))

	pf3.IndexParams = ip3Bad2
	assert.NotNil(t, validateSchema(coll))

	pf3.IndexParams = ip3Bad3
	assert.NotNil(t, validateSchema(coll))

	pf3.IndexParams = ip3Good
	assert.Nil(t, validateSchema(coll))
}

func TestValidateMultipleVectorFields(t *testing.T) {
	// case1, no vector field
	schema1 := &schemapb.CollectionSchema{}
	assert.NoError(t, validateMultipleVectorFields(schema1))

	// case2, only one vector field
	schema2 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "case2",
				DataType: schemapb.DataType_FloatVector,
			},
		},
	}
	assert.NoError(t, validateMultipleVectorFields(schema2))

	// case3, multiple vectors
	schema3 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "case3_f",
				DataType: schemapb.DataType_FloatVector,
			},
			{
				Name:     "case3_b",
				DataType: schemapb.DataType_BinaryVector,
			},
		},
	}
	if enableMultipleVectorFields {
		assert.NoError(t, validateMultipleVectorFields(schema3))
	} else {
		assert.Error(t, validateMultipleVectorFields(schema3))
	}
}

func TestFillFieldIDBySchema(t *testing.T) {
	t.Run("column count mismatch", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{}
		schema := newSchemaInfo(collSchema)
		columns := []*schemapb.FieldData{
			{
				FieldName: "TestFillFieldIDBySchema",
			},
		}
		// Validation should fail due to column count mismatch
		assert.Error(t, validateFieldDataColumns(columns, schema))
	})

	t.Run("successful validation and fill", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "TestFillFieldIDBySchema",
					DataType: schemapb.DataType_Int64,
					FieldID:  1,
				},
			},
		}
		schema := newSchemaInfo(collSchema)
		columns := []*schemapb.FieldData{
			{
				FieldName: "TestFillFieldIDBySchema",
			},
		}
		// Validation should succeed
		assert.NoError(t, validateFieldDataColumns(columns, schema))
		// Fill properties should succeed
		assert.NoError(t, fillFieldPropertiesOnly(columns, schema))
		assert.Equal(t, "TestFillFieldIDBySchema", columns[0].FieldName)
		assert.Equal(t, schemapb.DataType_Int64, columns[0].Type)
		assert.Equal(t, int64(1), columns[0].FieldId)
	})

	t.Run("field not in schema", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "FieldA",
					DataType: schemapb.DataType_Int64,
					FieldID:  1,
				},
			},
		}
		schema := newSchemaInfo(collSchema)
		columns := []*schemapb.FieldData{
			{
				FieldName: "FieldB",
			},
		}
		// Validation should fail because FieldB is not in schema
		err := validateFieldDataColumns(columns, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not exist in collection schema")
	})
}

func TestValidateUsername(t *testing.T) {
	// only spaces
	res := ValidateUsername(" ")
	assert.Error(t, res)
	// starts with non-alphabet
	res = ValidateUsername("1abc")
	assert.Error(t, res)
	// length gt 32
	res = ValidateUsername("aaaaaaaaaabbbbbbbbbbccccccccccddddd")
	assert.Error(t, res)
	// illegal character which not alphabet, _, ., ., or number
	res = ValidateUsername("a1^7*),")
	assert.Error(t, res)
	// normal username that only contains alphabet, _, ., -, and number
	res = ValidateUsername("a.17_good-")
	assert.Nil(t, res)
}

func TestValidatePassword(t *testing.T) {
	// only spaces
	res := ValidatePassword("")
	assert.NotNil(t, res)
	//
	res = ValidatePassword("1abc")
	assert.NotNil(t, res)
	//
	res = ValidatePassword("a1^7*).,")
	assert.Nil(t, res)
	//
	res = ValidatePassword("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmnnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrsssssssssstttttttttttuuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwwxxxxxxxxxxyyyyyyyyyzzzzzzzzzzz")
	assert.Error(t, res)
}

func TestReplaceID2Name(t *testing.T) {
	srcStr := "collection 432682805904801793 has not been loaded to memory or load failed"
	dstStr := "collection default_collection has not been loaded to memory or load failed"
	assert.Equal(t, dstStr, ReplaceID2Name(srcStr, int64(432682805904801793), "default_collection"))
}

func TestValidateName(t *testing.T) {
	nameType := "Test"
	validNames := []string{
		"abc",
		"_123abc",
	}
	for _, name := range validNames {
		assert.Nil(t, validateName(name, nameType))
		assert.Nil(t, ValidateRoleName(name))
		assert.Nil(t, ValidateObjectName(name))
		assert.Nil(t, ValidateObjectType(name))
		assert.Nil(t, ValidatePrivilege(name))
	}

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		" ",
		"123abc",
		"$abc",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateName(name, nameType))
		assert.NotNil(t, ValidateRoleName(name))
		assert.NotNil(t, ValidateObjectType(name))
		assert.NotNil(t, ValidatePrivilege(name))
	}
	assert.NotNil(t, ValidateObjectName(" "))
	assert.NotNil(t, ValidateObjectName(string(longName)))
	assert.Nil(t, ValidateObjectName("*"))
}

func TestValidateRoleName_HyphenToggle(t *testing.T) {
	pt := paramtable.Get()

	pt.ProxyCfg.RoleNameValidationAllowedChars.SwapTempValue("$-")
	assert.Nil(t, ValidateRoleName("Admin-1"))
	assert.Nil(t, ValidateRoleName("_a-bc$1"))
	assert.NotNil(t, ValidateRoleName("-bad"))
	assert.NotNil(t, ValidateRoleName("1leading"))
	assert.NotNil(t, ValidateRoleName(""))
	assert.NotNil(t, ValidateRoleName("*"))

	pt.ProxyCfg.RoleNameValidationAllowedChars.SwapTempValue("$")
	assert.Nil(t, ValidateRoleName("Admin_1"))
	assert.Nil(t, ValidateRoleName("Admin$1"))
	assert.NotNil(t, ValidateRoleName("Admin-1"))
}

func TestIsDefaultRole(t *testing.T) {
	assert.Equal(t, true, IsDefaultRole(util.RoleAdmin))
	assert.Equal(t, true, IsDefaultRole(util.RolePublic))
	assert.Equal(t, false, IsDefaultRole("manager"))
}

func GetContext(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}

func GetContextWithDB(ctx context.Context, originValue string, dbName string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	dbKey := strings.ToLower(util.HeaderDBName)
	contextMap := map[string]string{
		authKey: authValue,
		dbKey:   dbName,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}

func TestGetCurUserFromContext(t *testing.T) {
	_, err := GetCurUserFromContext(context.Background())
	assert.Error(t, err)

	_, err = GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{})))
	assert.Error(t, err)

	_, err = GetCurUserFromContext(GetContext(context.Background(), "123456"))
	assert.Error(t, err)

	root := "root"
	password := "123456"
	username, err := GetCurUserFromContext(GetContext(context.Background(), fmt.Sprintf("%s%s%s", root, util.CredentialSeparator, password)))
	assert.NoError(t, err)
	assert.Equal(t, "root", username)
}

func TestGetCurDBNameFromContext(t *testing.T) {
	dbName := GetCurDBNameFromContextOrDefault(context.Background())
	assert.Equal(t, util.DefaultDBName, dbName)

	dbName = GetCurDBNameFromContextOrDefault(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{})))
	assert.Equal(t, util.DefaultDBName, dbName)

	dbNameKey := strings.ToLower(util.HeaderDBName)
	dbNameValue := "foodb"
	contextMap := map[string]string{
		dbNameKey: dbNameValue,
	}
	md := metadata.New(contextMap)

	dbName = GetCurDBNameFromContextOrDefault(metadata.NewIncomingContext(context.Background(), md))
	assert.Equal(t, dbNameValue, dbName)
}

func TestGetRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	privilege.ResetPrivilegeCacheForTest()
	_, err := GetRole("foo")
	assert.Error(t, err)

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status:    merr.Success(),
		UserRoles: []string{"root/role1", "root/admin", "root/role2", "foo/role1"},
	}, nil).Times(1)

	privilege.InitPrivilegeCache(ctx, mixcoord)

	roles, err := GetRole("root")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(roles))

	roles, err = GetRole("foo")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(roles))
}

func TestPasswordVerify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	username := "user-test00"
	password := "PasswordVerify"

	invokedCount := 0

	mockedRootCoord := NewMixCoordMock()
	mockedRootCoord.GetGetCredentialFunc = func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
		invokedCount++
		return nil, errors.New("get cred not found credential")
	}

	privilege.InitPrivilegeCache(ctx, mockedRootCoord)
	privilegeCache := privilege.GetPrivilegeCache()
	assert.False(t, passwordVerify(ctx, username, password, privilegeCache))
	assert.Equal(t, 1, invokedCount)

	// Sha256Password has not been filled into cache during establish connection firstly
	encryptedPwd, err := crypto.PasswordEncrypt(password)
	assert.NoError(t, err)
	privilegeCache.RemoveCredential(username)
	mockedRootCoord.GetGetCredentialFunc = func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
		invokedCount++
		return &rootcoordpb.GetCredentialResponse{
			Status:   merr.Success(),
			Username: username,
			Password: encryptedPwd,
		}, nil
	}

	assert.True(t, passwordVerify(ctx, username, password, privilegeCache))

	ret, err := privilegeCache.GetCredentialInfo(ctx, username)
	assert.NoError(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, username, ret.Username)
	assert.NotNil(t, ret.Sha256Password)
	assert.Equal(t, 2, invokedCount)

	// Sha256Password already exists within cache
	assert.True(t, passwordVerify(ctx, username, password, privilegeCache))
	assert.Equal(t, 2, invokedCount)
}

func Test_isCollectionIsLoaded(t *testing.T) {
	ctx := context.Background()
	t.Run("normal", func(t *testing.T) {
		collID := int64(1)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        successStatus,
			CollectionIDs: []int64{collID, 10, 100},
		}, nil)
		loaded, err := isCollectionLoaded(ctx, mixc, collID)
		assert.NoError(t, err)
		assert.True(t, loaded)
	})

	t.Run("error", func(t *testing.T) {
		collID := int64(1)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        successStatus,
			CollectionIDs: []int64{collID},
		}, errors.New("error"))
		loaded, err := isCollectionLoaded(ctx, mixc, collID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})

	t.Run("fail", func(t *testing.T) {
		collID := int64(1)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "fail reason",
			},
			CollectionIDs: []int64{collID},
		}, nil)
		loaded, err := isCollectionLoaded(ctx, mixc, collID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})
}

func Test_isPartitionIsLoaded(t *testing.T) {
	ctx := context.Background()
	t.Run("normal", func(t *testing.T) {
		collID := int64(1)
		partID := int64(2)
		mixc := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixc.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status:       merr.Success(),
			PartitionIDs: []int64{partID},
		}, nil)
		loaded, err := isPartitionLoaded(ctx, mixc, collID, partID)
		assert.NoError(t, err)
		assert.True(t, loaded)
	})

	t.Run("error", func(t *testing.T) {
		collID := int64(1)
		partID := int64(2)
		mixCoord := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixCoord.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixCoord.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status:       merr.Success(),
			PartitionIDs: []int64{partID},
		}, errors.New("error"))
		loaded, err := isPartitionLoaded(ctx, mixCoord, collID, partID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})

	t.Run("fail", func(t *testing.T) {
		collID := int64(1)
		partID := int64(2)
		mixCoord := &mocks.MockMixCoordClient{}
		successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mixCoord.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
		mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: successStatus,
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					Serviceable: []bool{true, true, true},
				},
			},
		}, nil)
		mixCoord.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "fail reason",
			},
			PartitionIDs: []int64{partID},
		}, nil)
		loaded, err := isPartitionLoaded(ctx, mixCoord, collID, partID)
		assert.Error(t, err)
		assert.False(t, loaded)
	})
}

func Test_InsertTaskcheckFieldsDataBySchema(t *testing.T) {
	paramtable.Init()
	mlog.Info(context.TODO(), "InsertTaskcheckFieldsDataBySchema", mlog.Bool("enable", Params.ProxyCfg.SkipAutoIDCheck.GetAsBool()))
	var err error

	t.Run("schema is empty, though won't happen in system", func(t *testing.T) {
		// won't happen in system
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields:      []*schemapb.FieldSchema{},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					DbName:         "TestInsertTask_checkFieldsDataBySchema",
					CollectionName: "TestInsertTask_checkFieldsDataBySchema",
					PartitionName:  "TestInsertTask_checkFieldsDataBySchema",
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 0)
	})

	t.Run("miss field", func(t *testing.T) {
		// schema has field, msg has no field.
		// schema is not Nullable or has set default_value
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "a",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("miss field is nullable or set default_value", func(t *testing.T) {
		// schema has fields, msg has no field.
		// schema is Nullable or set default_value
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,

				Fields: []*schemapb.FieldSchema{
					{
						Name:     "a",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
						Nullable: true,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
						DefaultValue: &schemapb.ValueField{
							Data: &schemapb.ValueField_LongData{
								LongData: 1,
							},
						},
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)
	})

	t.Run("schema has autoid pk", func(t *testing.T) {
		// schema has autoid pk
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 0)
	})

	t.Run("schema pk is not autoid, but not pass pk", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("pass more data field", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "c",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("duplicate field datas", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("not pk field, but autoid == true", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       true,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("has more than one pk", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})

	t.Run("pk can not set default value", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_checkFieldsDataBySchema",
				Description: "TestInsertTask_checkFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
						DefaultValue: &schemapb.ValueField{
							Data: &schemapb.ValueField_LongData{
								LongData: 1,
							},
						},
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, false)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
	})
	t.Run("normal when upsert", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "Test_CheckFieldsDataBySchema",
				Description: "Test_CheckFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       false,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       false,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, false)
		assert.NoError(t, err)

		task = insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "Test_CheckFieldsDataBySchema",
				Description: "Test_CheckFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:         "b",
						AutoID:       false,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}
		err = checkFieldsDataBySchema(task.schema, task.insertMsg, false)
		assert.NoError(t, err)
	})

	t.Run("skip the auto id", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_fillFieldsDataBySchema",
				Description: "TestInsertTask_fillFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.ErrorIs(t, merr.ErrParameterInvalid, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)

		paramtable.Get().Save(Params.ProxyCfg.SkipAutoIDCheck.Key, "true")
		task = insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestInsertTask_fillFieldsDataBySchema",
				Description: "TestInsertTask_fillFieldsDataBySchema",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "a",
						AutoID:       true,
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						Name:     "b",
						AutoID:   false,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "a",
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "b",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
		}

		err = checkFieldsDataBySchema(task.schema, task.insertMsg, true)
		assert.NoError(t, err)
		assert.Equal(t, len(task.insertMsg.FieldsData), 2)
		paramtable.Get().Reset(Params.ProxyCfg.SkipAutoIDCheck.Key)
	})
}

func Test_InsertTaskCheckPrimaryFieldData(t *testing.T) {
	// schema is empty, though won't happen in system
	// num_rows(0) should be greater than 0
	case1 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "TestInsertTask_checkPrimaryFieldData",
				CollectionName: "TestInsertTask_checkPrimaryFieldData",
				PartitionName:  "TestInsertTask_checkPrimaryFieldData",
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}

	_, err := checkPrimaryFieldData(case1.schema, case1.insertMsg)
	assert.NotEqual(t, nil, err)

	// the num of passed fields is less than needed
	case2 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type: schemapb.DataType_Int64,
					},
				},
				Version: msgpb.InsertDataVersion_RowBased,
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	_, err = checkPrimaryFieldData(case2.schema, case2.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == false, no primary field schema
	// primary field is not found
	case3 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "int64Field",
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "floatField",
					DataType: schemapb.DataType_Float,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{},
					{},
				},
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	_, err = checkPrimaryFieldData(case3.schema, case3.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == true, has primary field schema, but primary field data exist
	// can not assign primary field data when auto id enabled int64Field
	case4 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkPrimaryFieldData",
			Description: "TestInsertTask_checkPrimaryFieldData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "int64Field",
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "floatField",
					FieldID:  2,
					DataType: schemapb.DataType_Float,
				},
			},
		},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				RowData: []*commonpb.Blob{
					{},
					{},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_Int64,
						FieldName: "int64Field",
					},
				},
			},
		},
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
		},
	}
	case4.schema.Fields[0].IsPrimaryKey = true
	case4.schema.Fields[0].AutoID = true
	case4.insertMsg.FieldsData[0] = newScalarFieldData(case4.schema.Fields[0], case4.schema.Fields[0].Name, 10)
	_, err = checkPrimaryFieldData(case4.schema, case4.insertMsg)
	assert.NotEqual(t, nil, err)

	// autoID == true, has primary field schema, but DataType don't match
	// the data type of the data not matches the schema
	case4.schema.Fields[0].IsPrimaryKey = false
	case4.schema.Fields[1].IsPrimaryKey = true
	case4.schema.Fields[1].AutoID = true
	_, err = checkPrimaryFieldData(case4.schema, case4.insertMsg)
	assert.NotEqual(t, nil, err)
}

func Test_UpsertTaskCheckPrimaryFieldData(t *testing.T) {
	// num_rows(0) should be greater than 0
	t.Run("schema is empty, though won't happen in system", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields:      []*schemapb.FieldSchema{},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					DbName:         "TestUpsertTask_checkPrimaryFieldData",
					CollectionName: "TestUpsertTask_checkPrimaryFieldData",
					PartitionName:  "TestUpsertTask_checkPrimaryFieldData",
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	t.Run("the num of passed fields is less than needed", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "int64Field",
						FieldID:  1,
						DataType: schemapb.DataType_Int64,
					},
					{
						Name:     "floatField",
						FieldID:  2,
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_Int64,
							FieldName: "int64Field",
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	// autoID == false, no primary field schema
	t.Run("primary field is not found", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:     "int64Field",
						DataType: schemapb.DataType_Int64,
					},
					{
						Name:     "floatField",
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{},
						{},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	// primary field data is nil, GetPrimaryFieldData fail
	t.Run("primary field data is nil", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       false,
					},
					{
						Name:     "floatField",
						FieldID:  2,
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{},
						{},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	// only support DataType Int64 or VarChar as PrimaryField
	t.Run("primary field type wrong", func(t *testing.T) {
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "floatVectorField",
						FieldID:      1,
						DataType:     schemapb.DataType_FloatVector,
						AutoID:       true,
						IsPrimaryKey: true,
					},
					{
						Name:     "floatField",
						FieldID:  2,
						DataType: schemapb.DataType_Float,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_FloatVector,
							FieldName: "floatVectorField",
						},
						{
							Type:      schemapb.DataType_Int64,
							FieldName: "floatField",
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NotEqual(t, nil, err)
	})

	t.Run("upsert must assign pk", func(t *testing.T) {
		// autoid==true
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       true,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "int64Field",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NoError(t, nil, err)

		// autoid==false
		task = insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      false,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       false,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "int64Field",
							Type:      schemapb.DataType_Int64,
						},
					},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err = checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		assert.NoError(t, nil, err)
	})

	t.Run("will generate new pk when autoid == true", func(t *testing.T) {
		// autoid==true
		task := insertTask{
			schema: &schemapb.CollectionSchema{
				Name:        "TestUpsertTask_checkPrimaryFieldData",
				Description: "TestUpsertTask_checkPrimaryFieldData",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						Name:         "int64Field",
						FieldID:      1,
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
						AutoID:       true,
					},
				},
			},
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					RowData: []*commonpb.Blob{
						{},
					},
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "int64Field",
							Type:      schemapb.DataType_Int64,
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{2},
										},
									},
								},
							},
						},
					},
					RowIDs: []int64{1},
				},
			},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
			},
		}
		_, _, err := checkUpsertPrimaryFieldData(task.schema, task.insertMsg)
		newPK := task.insertMsg.FieldsData[0].GetScalars().GetLongData().GetData()
		assert.Equal(t, newPK, task.insertMsg.RowIDs)
		assert.NoError(t, nil, err)
	})
}

func Test_ParseGuaranteeTs(t *testing.T) {
	strongTs := typeutil.Timestamp(0)
	boundedTs := typeutil.Timestamp(2)
	tsNow := tsoutil.ComposeTSByTime(time.Now())
	tsMax := tsoutil.ComposeTSByTime(time.Now())

	assert.Equal(t, tsMax, parseGuaranteeTs(strongTs, tsMax))
	ratio := Params.CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
	assert.Equal(t, tsoutil.AddPhysicalDurationOnTs(tsMax, -ratio), parseGuaranteeTs(boundedTs, tsMax))
	assert.Equal(t, tsNow, parseGuaranteeTs(tsNow, tsMax))
}

func Test_ParseGuaranteeTsFromConsistency(t *testing.T) {
	strong := commonpb.ConsistencyLevel_Strong
	bounded := commonpb.ConsistencyLevel_Bounded
	eventually := commonpb.ConsistencyLevel_Eventually
	session := commonpb.ConsistencyLevel_Session
	customized := commonpb.ConsistencyLevel_Customized

	tsDefault := typeutil.Timestamp(0)
	tsEventually := typeutil.Timestamp(1)
	tsNow := tsoutil.ComposeTSByTime(time.Now())
	tsMax := tsoutil.ComposeTSByTime(time.Now())

	assert.Equal(t, tsMax, parseGuaranteeTsFromConsistency(tsDefault, tsMax, strong))
	ratio := Params.CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
	assert.Equal(t, tsoutil.AddPhysicalDurationOnTs(tsMax, -ratio), parseGuaranteeTsFromConsistency(tsDefault, tsMax, bounded))
	assert.Equal(t, tsNow, parseGuaranteeTsFromConsistency(tsNow, tsMax, session))
	assert.Equal(t, tsNow, parseGuaranteeTsFromConsistency(tsNow, tsMax, customized))
	assert.Equal(t, tsEventually, parseGuaranteeTsFromConsistency(tsDefault, tsMax, eventually))
}

func Test_NQLimit(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateNQLimit(16384))
	assert.Nil(t, validateNQLimit(1))
	assert.Error(t, validateNQLimit(16385))
	assert.Error(t, validateNQLimit(0))
}

func Test_TopKLimit(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateLimit(16384, false))
	assert.Nil(t, validateLimit(1, false))
	assert.Error(t, validateLimit(16385, false))
	assert.Error(t, validateLimit(0, false))
}

func Test_LargeTopKLimit(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.QuotaConfig.TopKLimit.Key, "100")
	Params.Save(Params.QuotaConfig.LargeTopKLimit.Key, "200")
	defer Params.Reset(Params.QuotaConfig.TopKLimit.Key)
	defer Params.Reset(Params.QuotaConfig.LargeTopKLimit.Key)

	assert.Nil(t, validateLimit(100, false))
	assert.Error(t, validateLimit(101, false))

	assert.Nil(t, validateLimit(200, true))
	assert.Nil(t, validateLimit(150, true))
	assert.Error(t, validateLimit(201, true))
	assert.Error(t, validateLimit(0, true))
}

func Test_MaxQueryResultWindow(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateMaxQueryResultWindow(0, 16384, false))
	assert.Nil(t, validateMaxQueryResultWindow(0, 1, false))
	assert.Error(t, validateMaxQueryResultWindow(0, 16385, false))
	assert.Error(t, validateMaxQueryResultWindow(0, 0, false))
	assert.Error(t, validateMaxQueryResultWindow(1, 0, false))

	Params.Save(Params.QuotaConfig.LargeMaxQueryResultWindow.Key, "1000000")
	defer Params.Reset(Params.QuotaConfig.LargeMaxQueryResultWindow.Key)
	assert.Nil(t, validateMaxQueryResultWindow(0, 16385, true))
	assert.Nil(t, validateMaxQueryResultWindow(0, 1000000, true))
	assert.Error(t, validateMaxQueryResultWindow(0, 1000001, true))
}

func Test_GetPartitionProgressFailed(t *testing.T) {
	qc := mocks.NewMockQueryCoordClient(t)
	qc.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Unexpected error",
		},
	}, nil)
	_, _, err := getPartitionProgress(context.TODO(), qc, &commonpb.MsgBase{}, []string{}, "", 1, "")
	assert.Error(t, err)
}

func TestErrWithLog(t *testing.T) {
	err := errors.New("test")
	assert.ErrorIs(t, ErrWithLog(nil, "foo", err), err)
	assert.ErrorIs(t, ErrWithLog(mlog.With(), "foo", err), err)
}

func Test_CheckDynamicFieldData(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		schema := newTestSchema()
		jsonFieldData := autoGenDynamicFieldData(schema, jsonData)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err = checkDynamicFieldData(schema, insertMsg)
		assert.NoError(t, err)
	})
	t.Run("key has $meta", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
			"$meta": "error key",
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		schema := newTestSchema()
		jsonFieldData := autoGenDynamicFieldData(schema, jsonData)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err = checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("key has static field name", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
			"Int64Field": "error key",
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		schema := newTestSchema()
		jsonFieldData := autoGenDynamicFieldData(schema, jsonData)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err = checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("disable dynamic schema", func(t *testing.T) {
		jsonData := make([][]byte, 0)
		data := map[string]interface{}{
			"bool":   true,
			"int":    100,
			"float":  1.2,
			"string": "abc",
			"json": map[string]interface{}{
				"int":   20,
				"array": []int{1, 2, 3},
			},
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		assert.NoError(t, err)
		jsonData = append(jsonData, jsonBytes)
		schema := newTestSchema()
		jsonFieldData := autoGenDynamicFieldData(schema, jsonData)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		schema.EnableDynamicField = false
		err = checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("json data is string", func(t *testing.T) {
		data := "abcdefg"
		schema := newTestSchema()
		jsonFieldData := autoGenDynamicFieldData(schema, [][]byte{[]byte(data)})
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{jsonFieldData},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err := checkDynamicFieldData(schema, insertMsg)
		assert.Error(t, err)
	})
	t.Run("no json data", func(t *testing.T) {
		schema := newTestSchema()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "collectionName",
				FieldsData:     []*schemapb.FieldData{},
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}
		err := checkDynamicFieldData(schema, insertMsg)
		assert.NoError(t, err)
	})
}

func Test_validateMaxCapacityPerRow(t *testing.T) {
	paramtable.Init()

	t.Run("normal case", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "100",
				},
				{
					Key:   common.MaxCapacityKey,
					Value: "10",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.NoError(t, err)
	})

	t.Run("no max capacity", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("max capacity not int", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "six",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("max capacity exceed max", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "4097",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("custom max capacity", func(t *testing.T) {
		paramtable.Init()
		err := paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxArrayCapacity.Key, "5000")
		assert.NoError(t, err)
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.MaxArrayCapacity.Key)

		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "5000",
				},
			},
		}

		err = validateMaxCapacityPerRow("collection", arrayField)
		assert.NoError(t, err)
	})
}

func TestAppendUserInfoForRPC(t *testing.T) {
	ctx := GetContext(context.Background(), "root:123456")
	ctx = AppendUserInfoForRPC(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	authorization, ok := md[strings.ToLower(util.HeaderAuthorize)]
	assert.True(t, ok)
	expectAuth := crypto.Base64Encode("root:root")
	assert.Equal(t, expectAuth, authorization[0])
}

func TestNewContextWithMetadata(t *testing.T) {
	t.Run("with username and dbName", func(t *testing.T) {
		ctx := context.Background()
		ctx = NewContextWithMetadata(ctx, "testuser", "testdb")

		md, ok := metadata.FromIncomingContext(ctx)
		assert.True(t, ok)

		// Check dbName
		dbNameKey := strings.ToLower(util.HeaderDBName)
		dbNameVal, ok := md[dbNameKey]
		assert.True(t, ok)
		assert.Equal(t, "testdb", dbNameVal[0])

		// Check authorization
		authKey := strings.ToLower(util.HeaderAuthorize)
		authVal, ok := md[authKey]
		assert.True(t, ok)
		expectedAuth := crypto.Base64Encode("testuser:testuser")
		assert.Equal(t, expectedAuth, authVal[0])
	})

	t.Run("with empty username", func(t *testing.T) {
		ctx := context.Background()
		ctx = NewContextWithMetadata(ctx, "", "testdb")

		md, ok := metadata.FromIncomingContext(ctx)
		assert.True(t, ok)

		// Check dbName is set
		dbNameKey := strings.ToLower(util.HeaderDBName)
		dbNameVal, ok := md[dbNameKey]
		assert.True(t, ok)
		assert.Equal(t, "testdb", dbNameVal[0])

		// Check authorization is not set
		authKey := strings.ToLower(util.HeaderAuthorize)
		_, ok = md[authKey]
		assert.False(t, ok)
	})

	t.Run("with empty dbName", func(t *testing.T) {
		ctx := context.Background()
		ctx = NewContextWithMetadata(ctx, "testuser", "")

		md, ok := metadata.FromIncomingContext(ctx)
		assert.True(t, ok)

		// Check authorization is set
		authKey := strings.ToLower(util.HeaderAuthorize)
		authVal, ok := md[authKey]
		assert.True(t, ok)
		expectedAuth := crypto.Base64Encode("testuser:testuser")
		assert.Equal(t, expectedAuth, authVal[0])
	})
}

func TestGetCostValue(t *testing.T) {
	t.Run("empty status", func(t *testing.T) {
		{
			cost := GetCostValue(&commonpb.Status{})
			assert.Equal(t, 0, cost)
		}

		{
			cost := GetCostValue(nil)
			assert.Equal(t, 0, cost)
		}
	})

	t.Run("wrong cost value style", func(t *testing.T) {
		cost := GetCostValue(&commonpb.Status{
			ExtraInfo: map[string]string{
				"report_value": "abc",
			},
		})
		assert.Equal(t, 0, cost)
	})

	t.Run("success", func(t *testing.T) {
		cost := GetCostValue(&commonpb.Status{
			ExtraInfo: map[string]string{
				"report_value": "100",
			},
		})
		assert.Equal(t, 100, cost)
	})
}

func TestValidateLoadFieldsList(t *testing.T) {
	type testCase struct {
		tag       string
		schema    *schemapb.CollectionSchema
		expectErr bool
	}

	rowIDField := &schemapb.FieldSchema{
		FieldID:  common.RowIDField,
		Name:     common.RowIDFieldName,
		DataType: schemapb.DataType_Int64,
	}
	timestampField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	pkField := &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	scalarField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
	}
	partitionKeyField := &schemapb.FieldSchema{
		FieldID:        common.StartOfUserFieldID + 2,
		Name:           "part_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	vectorField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 3,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "768"},
		},
	}
	dynamicField := &schemapb.FieldSchema{
		FieldID:   common.StartOfUserFieldID + 4,
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}
	clusteringKeyField := &schemapb.FieldSchema{
		FieldID:         common.StartOfUserFieldID + 5,
		Name:            common.MetaFieldName,
		DataType:        schemapb.DataType_Int32,
		IsClusteringKey: true,
	}

	addSkipLoadAttr := func(f *schemapb.FieldSchema, flag bool) *schemapb.FieldSchema {
		result := typeutil.Clone(f)
		result.TypeParams = append(f.TypeParams, &commonpb.KeyValuePair{
			Key:   common.FieldSkipLoadKey,
			Value: strconv.FormatBool(flag),
		})
		return result
	}

	testCases := []testCase{
		{
			tag: "default",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
					clusteringKeyField,
				},
			},
			expectErr: false,
		},
		{
			tag: "pk_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					addSkipLoadAttr(pkField, true),
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			expectErr: true,
		},
		{
			tag: "part_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					addSkipLoadAttr(pkField, true),
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
			},
			expectErr: true,
		},
		{
			tag: "vector_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					addSkipLoadAttr(vectorField, true),
					dynamicField,
				},
			},
			expectErr: true,
		},
		{
			tag: "clustering_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
					addSkipLoadAttr(clusteringKeyField, true),
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			err := validateLoadFieldsList(tc.schema)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateFunction(t *testing.T) {
	t.Run("Valid function schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.NoError(t, err)
	})

	t.Run("Normalize function output flags explicitly", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}

		err := validator.ValidateFunction(schema, "", true)
		assert.NoError(t, err)
		assert.False(t, schema.GetFields()[1].GetIsFunctionOutput())

		err = validator.NormalizeFunctionOutputFields(schema)
		assert.NoError(t, err)
		assert.True(t, schema.GetFields()[1].GetIsFunctionOutput())
	})

	t.Run("Valid external field may match function output field name", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{
					Name:          "input_field",
					DataType:      schemapb.DataType_VarChar,
					ExternalField: "output_field",
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "enable_analyzer", Value: "true"},
					},
				},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.NoError(t, err)
	})

	t.Run("Invalid function schema - duplicate function names", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate function name")
	})

	t.Run("Invalid function schema - input field not found", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"non_existent_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input field not found")
	})

	t.Run("Invalid function schema - output field not found", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"non_existent_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "output field not found")
	})

	t.Run("Invalid function schema - nullable BM25 output field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}, Nullable: true},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be nullable")
	})

	t.Run("Valid create schema function - nullable BM25 input with non-nullable output field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}, Nullable: true},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.NoError(t, err)
	})

	t.Run("Invalid function schema - output field is primary key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, IsPrimaryKey: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be primary key")
	})

	t.Run("Invalid function schema - output field is partition key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, IsPartitionKey: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be partition key or clustering key")
	})

	t.Run("Invalid function schema - output field is clustering key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, IsClusteringKey: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be partition key or clustering key")
	})

	t.Run("Invalid create schema function - non-nullable BM25 input with nullable output field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function output field cannot be nullable")
	})
}

func TestValidateModelFunction(t *testing.T) {
	t.Run("Valid model function schema", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
			return map[string]string{
				"mock.apikey": "mock",
			}
		}
		ts := embedding.CreateOpenAIEmbeddingServer()
		defer ts.Close()
		paramtable.Get().FunctionCfg.TextEmbeddingProviders.GetFunc = func() map[string]string {
			return map[string]string{
				"openai.url": ts.URL,
			}
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
				{
					Name: "output_dense_field", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					},
				},
				{
					Name: "output_dense_field2", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					},
				},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
				{
					Name:             "f1",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_dense_field"},
					Params: []*commonpb.KeyValuePair{
						{Key: "provider", Value: "openai"},
						{Key: "model_name", Value: "text-embedding-ada-002"},
						{Key: "credential", Value: "mock"},
						{Key: "dim", Value: "4"},
					},
				},
				{
					Name:             "f2",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_dense_field2"},
					Params: []*commonpb.KeyValuePair{
						{Key: "provider", Value: "unknown_provider"},
						{Key: "model_name", Value: "text-embedding-ada-002"},
						{Key: "credential", Value: "mock"},
						{Key: "dim", Value: "4"},
					},
				},
			},
		}
		err := validator.ValidateFunction(schema, "f1", false)
		assert.NoError(t, err)

		err = validator.ValidateFunction(schema, "f2", false)
		assert.Error(t, err)

		err = validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
	})

	t.Run("Invalid function schema - Invalid function info ", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "input_field", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
				{Name: "output_field", DataType: schemapb.DataType_SparseFloatVector},
				{Name: "output_dense_field", DataType: schemapb.DataType_FloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_field"},
				},
				{
					Name:             "text_embedding_func",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldNames:  []string{"input_field"},
					OutputFieldNames: []string{"output_dense_field"},
					Params: []*commonpb.KeyValuePair{
						{Key: "provider", Value: "UnkownProvider"},
						{Key: "model_name", Value: "text-embedding-ada-002"},
						{Key: "api_key", Value: "mock"},
						{Key: "url", Value: "mock_url"},
						{Key: "dim", Value: "4"},
					},
				},
			},
		}
		err := validator.ValidateFunction(schema, "", false)
		assert.Error(t, err)
	})
}

func TestValidateFunctionInputField(t *testing.T) {
	t.Run("Valid BM25 function input", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
			},
		}
		err := validator.CheckFunctionInputField(function, fields)
		assert.NoError(t, err)
	})

	t.Run("Invalid BM25 function input - wrong data type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Int64,
			},
		}
		err := validator.CheckFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid BM25 function input - analyzer not enabled", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "false"}},
			},
		}
		err := validator.CheckFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid BM25 function input - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
			},
			{
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
			},
		}
		err := validator.CheckFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Unknown function type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Unknown,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_VarChar,
			},
		}
		err := validator.CheckFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{}
		err := validator.CheckFunctionInputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - wrong type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Int64,
			},
		}
		err := validator.CheckFunctionInputField(function, fields)
		assert.Error(t, err)
	})
}

func TestValidateFunctionOutputField(t *testing.T) {
	t.Run("Valid BM25 function output", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_SparseFloatVector,
			},
		}
		err := validator.CheckFunctionOutputField(function, fields)
		assert.NoError(t, err)
	})

	t.Run("Invalid BM25 function output - wrong data type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Float,
			},
		}
		err := validator.CheckFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid BM25 function output - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_BM25,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_SparseFloatVector,
			},
			{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		err := validator.CheckFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Unknown function type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Unknown,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		err := validator.CheckFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - multiple fields", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{}
		err := validator.CheckFunctionOutputField(function, fields)
		assert.Error(t, err)
	})

	t.Run("Invalid TextEmbedding function input - wrong type", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_TextEmbedding,
		}
		fields := []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_Int64,
			},
		}
		err := validator.CheckFunctionOutputField(function, fields)
		assert.Error(t, err)
	})
}

func TestValidateFunctionBasicParams(t *testing.T) {
	t.Run("Valid function", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "validFunction",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1", "input2"},
			OutputFieldNames: []string{"output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.NoError(t, err)
	})

	t.Run("Empty function name", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty input field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyInputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{},
			OutputFieldNames: []string{"output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty output field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyOutputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty input field name", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyInputName",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1", ""},
			OutputFieldNames: []string{"output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Duplicate input field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "duplicateInputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1", "input1"},
			OutputFieldNames: []string{"output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty output field name", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "emptyOutputName",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1", ""},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Input field used as output", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "inputAsOutput",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"field1", "field2"},
			OutputFieldNames: []string{"field1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Duplicate output field names", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "duplicateOutputs",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1", "output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})

	t.Run("Empty text embedding params", func(t *testing.T) {
		function := &schemapb.FunctionSchema{
			Name:             "textEmbeddingParam",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"input1"},
			OutputFieldNames: []string{"output1"},
		}
		err := validator.CheckFunctionBasicParams(function)
		assert.Error(t, err)
	})
}

func TestComputeRecall(t *testing.T) {
	t.Run("normal case1", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 3,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "9", "8", "5", "3", "1"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.1},
			Topks:  []int64{2, 2, 2},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 3,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "10", "8", "5", "3", "1"},
					},
				},
			},
			Scores: []float32{1.1, 0.98, 0.8, 0.5, 0.3, 0.1},
			Topks:  []int64{2, 2, 2},
		}

		err := computeRecall(result1, gt)
		assert.NoError(t, err)
		assert.Equal(t, result1.Recalls[0], float32(0.5))
		assert.Equal(t, result1.Recalls[1], float32(1.0))
		assert.Equal(t, result1.Recalls[2], float32(1.0))
	})

	t.Run("normal case2", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 6, 5, 4, 1, 34, 23, 22, 20},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result1, gt)
		assert.NoError(t, err)
		assert.Equal(t, result1.Recalls[0], float32(0.6))
		assert.Equal(t, result1.Recalls[1], float32(0.8))
	})

	t.Run("not match size", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 1,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 6, 5, 4},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3},
			Topks:  []int64{5},
		}

		err := computeRecall(result1, gt)
		assert.Error(t, err)
	})

	t.Run("not match type1", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "10", "8", "5", "3", "1", "23", "22", "21", "20"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result1, gt)
		assert.Error(t, err)
	})

	t.Run("not match type2", func(t *testing.T) {
		result1 := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"11", "10", "8", "5", "3", "1", "23", "22", "21", "20"},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{11, 9, 8, 5, 3, 1, 34, 23, 22, 21},
					},
				},
			},
			Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result1, gt)
		assert.Error(t, err)
	})

	t.Run("empty result with nil ids", func(t *testing.T) {
		result := &schemapb.SearchResultData{
			NumQueries: 2,
			Topks:      []int64{0, 0},
		}
		gt := &schemapb.SearchResultData{
			NumQueries: 2,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
					},
				},
			},
			Scores: []float32{1.0, 0.9, 0.8, 0.7, 0.6, 1.0, 0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{5, 5},
		}

		err := computeRecall(result, gt)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Recalls))
		assert.Equal(t, float32(0), result.Recalls[0])
		assert.Equal(t, float32(0), result.Recalls[1])
	})

	t.Run("empty gt with nil ids", func(t *testing.T) {
		result := &schemapb.SearchResultData{
			NumQueries: 1,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{1, 2, 3},
					},
				},
			},
			Scores: []float32{1.0, 0.9, 0.8},
			Topks:  []int64{3},
		}
		gt := &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{0},
		}

		err := computeRecall(result, gt)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Recalls))
		assert.Equal(t, float32(0), result.Recalls[0])
	})

	t.Run("both empty results", func(t *testing.T) {
		result := &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{0},
		}
		gt := &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{0},
		}

		err := computeRecall(result, gt)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Recalls))
		assert.Equal(t, float32(0), result.Recalls[0])
	})
}

func TestCheckVarcharFormat(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_VarChar,
				FieldID:  100,
				TypeParams: []*commonpb.KeyValuePair{{
					Key:   common.EnableAnalyzerKey,
					Value: "true",
				}},
			},
			// skip field
			{
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	data := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{{
				FieldId: 100,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"valid string"},
							},
						},
					},
				},
			}},
		},
	}

	err := checkInputUtf8Compatiable(schema, data)
	assert.NoError(t, err)

	// invalid data
	invalidUTF8 := []byte{0xC0, 0xAF}
	data = &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{{
				FieldId: 100,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{string(invalidUTF8)},
							},
						},
					},
				},
			}},
		},
	}
	err = checkInputUtf8Compatiable(schema, data)
	assert.Error(t, err)
}

func BenchmarkCheckVarcharFormat(b *testing.B) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				DataType: schemapb.DataType_VarChar,
				FieldID:  100,
				TypeParams: []*commonpb.KeyValuePair{{
					Key:   common.EnableAnalyzerKey,
					Value: "true",
				}},
			},
			// skip field
			{
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	data := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{{
				FieldId: 100,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{strings.Repeat("a", 1024*1024)},
							},
						},
					},
				},
			}},
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		checkInputUtf8Compatiable(schema, data)
	}
}
