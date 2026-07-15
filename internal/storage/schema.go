package storage

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func ConvertToArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, field := range fields {
		if serdeMap[field.DataType].arrowType == nil {
			return nil, merr.WrapErrParameterInvalidMsg("unknown field data type [%s] for field [%s]", field.DataType, field.GetName())
		}
		var dim int
		switch field.DataType {
		case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector, schemapb.DataType_FloatVector:
			var err error
			dim, err = GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("dim not found in field [%s] params", field.GetName())
			}
		default:
			dim = 0
		}
		arrowFields = append(arrowFields, ConvertToArrowField(field, serdeMap[field.DataType].arrowType(dim)))
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

// FilterRowIDFromSchema returns a deep copy of the schema with RowID system field removed.
func FilterRowIDFromSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	filtered := proto.Clone(schema).(*schemapb.CollectionSchema)
	n := 0
	for _, f := range filtered.Fields {
		if f.FieldID != common.RowIDField {
			filtered.Fields[n] = f
			n++
		}
	}
	filtered.Fields = filtered.Fields[:n]
	return filtered
}

// overrideTextFieldsToBinary replaces utf8 arrow type with binary for TEXT fields.
// In manifest storage, TEXT fields use LOB spillover and store binary-encoded LOB references.
func overrideTextFieldsToBinary(schema *schemapb.CollectionSchema, arrowSchema *arrow.Schema) *arrow.Schema {
	return overrideTextFieldsToBinaryByFields(typeutil.GetAllFieldSchemas(schema), arrowSchema)
}

func overrideTextFieldsToBinaryByFields(allFields []*schemapb.FieldSchema, arrowSchema *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, arrowSchema.NumFields())
	changed := false
	for i := 0; i < arrowSchema.NumFields(); i++ {
		fields[i] = arrowSchema.Field(i)
		if i < len(allFields) && allFields[i].DataType == schemapb.DataType_Text {
			fields[i].Type = arrow.BinaryTypes.Binary
			changed = true
		}
	}
	if !changed {
		return arrowSchema
	}
	return arrow.NewSchema(fields, nil)
}

func ConvertToArrowField(field *schemapb.FieldSchema, dataType arrow.DataType, useFieldID bool) arrow.Field {
	f := arrow.Field{
		Type:     dataType,
		Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{strconv.Itoa(int(field.GetFieldID()))}),
		Nullable: field.GetNullable(),
	}
	// external field name has higher priority
	if field.GetExternalField() != "" {
		f.Name = field.GetExternalField()
	} else if useFieldID { // use fieldID as name when specified
		f.Name = fmt.Sprintf("%d", field.GetFieldID())
	} else {
		f.Name = field.GetName()
	}
	return f
}
