package indexparamcheck

import (
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	MinGramKey                   = "min_gram"
	MaxGramKey                   = "max_gram"
	NgramBuildModeKey            = "ngram_build_mode"
	NgramDirectSoftLimitBytesKey = "ngram_direct_soft_limit_bytes"
)

type NgramIndexChecker struct {
	scalarIndexChecker
}

func newNgramIndexChecker() *NgramIndexChecker {
	return &NgramIndexChecker{}
}

func (c *NgramIndexChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if dataType != schemapb.DataType_VarChar && dataType != schemapb.DataType_JSON {
		return merr.WrapErrParameterInvalidMsg("Ngram index can only be created on VARCHAR or JSON field")
	}

	// For JSON field with ngram index, json_cast_type must be VARCHAR
	if dataType == schemapb.DataType_JSON {
		castType, exists := params[common.JSONCastTypeKey]
		if !exists {
			return merr.WrapErrParameterMissingMsg("JSON field with ngram index must specify json_cast_type")
		}
		// Normalize cast type to uppercase for comparison
		castType = strings.ToUpper(strings.TrimSpace(castType))
		if castType != "VARCHAR" {
			return merr.WrapErrParameterInvalidMsg("JSON field with ngram index only supports VARCHAR cast type, got: %s", castType)
		}
	}

	minGramStr, minGramExist := params[MinGramKey]
	maxGramStr, maxGramExist := params[MaxGramKey]
	if !minGramExist || !maxGramExist {
		return merr.WrapErrParameterMissingMsg("Ngram index must specify both min_gram and max_gram")
	}

	minGram, err := strconv.Atoi(minGramStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("min_gram for Ngram index must be an integer, got: %s", minGramStr)
	}

	maxGram, err := strconv.Atoi(maxGramStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("max_gram for Ngram index must be an integer, got: %s", maxGramStr)
	}

	if minGram <= 0 || maxGram <= 0 || minGram > maxGram {
		return merr.WrapErrParameterInvalidMsg("invalid min_gram or max_gram value for Ngram index, min_gram: %d, max_gram: %d", minGram, maxGram)
	}

	if buildMode, ok := params[NgramBuildModeKey]; ok {
		switch buildMode {
		case "regular", "auto", "force_direct":
		default:
			return merr.WrapErrParameterInvalidMsg("invalid ngram_build_mode: %s", buildMode)
		}
	}

	if softLimit, ok := params[NgramDirectSoftLimitBytesKey]; ok {
		decimalDigitsOnly := softLimit != ""
		for _, digit := range softLimit {
			if digit < '0' || digit > '9' {
				decimalDigitsOnly = false
				break
			}
		}
		softLimitBytes, err := strconv.ParseUint(softLimit, 10, 64)
		if !decimalDigitsOnly || err != nil || softLimitBytes == 0 {
			return merr.WrapErrParameterInvalidMsg("ngram_direct_soft_limit_bytes must be a positive integer, got: %s", softLimit)
		}
	}

	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *NgramIndexChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	dType := field.GetDataType()
	if !typeutil.IsStringType(dType) && dType != schemapb.DataType_JSON {
		return merr.WrapErrParameterInvalidMsg("ngram index can only be created on VARCHAR or JSON field")
	}
	return nil
}
