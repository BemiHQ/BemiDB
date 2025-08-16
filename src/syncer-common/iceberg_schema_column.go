package syncerCommon

import (
	"encoding/csv"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/marcboeker/go-duckdb/v2"
)

type IcebergColumnType string

const (
	IcebergColumnTypeBoolean     IcebergColumnType = "boolean"
	IcebergColumnTypeString      IcebergColumnType = "string"
	IcebergColumnTypeInteger     IcebergColumnType = "int"
	IcebergColumnTypeDecimal     IcebergColumnType = "decimal"
	IcebergColumnTypeLong        IcebergColumnType = "long"
	IcebergColumnTypeFloat       IcebergColumnType = "float"
	IcebergColumnTypeDouble      IcebergColumnType = "double"
	IcebergColumnTypeDate        IcebergColumnType = "date"
	IcebergColumnTypeTime        IcebergColumnType = "time"
	IcebergColumnTypeTimeTz      IcebergColumnType = "timetz"
	IcebergColumnTypeTimestamp   IcebergColumnType = "timestamp"
	IcebergColumnTypeTimestampTz IcebergColumnType = "timestamptz"
	IcebergColumnTypeBinary      IcebergColumnType = "binary"

	BEMIDB_NULL_STRING = "BEMIDB_NULL"

	PARQUET_NESTED_FIELD_ID_PREFIX = 1000
	PARQUET_NAN                    = 0 // DuckDB crashes on NaN, libc++abi: terminating due to uncaught exception of type duckdb::InvalidConfigurationException: {"exception_type":"Invalid Configuration","exception_message":"Column float4_column lower bound deserialization failed: Failed to deserialize blob '' of size 0, attempting to produce value of type 'FLOAT'"}
)

type IcebergSchemaColumn struct {
	Config           *common.CommonConfig
	ColumnName       string
	ColumnType       IcebergColumnType
	Position         int
	NumericPrecision int
	NumericScale     int
	IsList           bool
	IsNullable       bool
}

type ParquetSchemaField struct {
	FieldId              string
	Name                 string
	Type                 string
	ConvertedType        string
	RepetitionType       string
	Scale                string
	Precision            string
	Length               string
	NestedType           string
	NestedConvertedType  string
	NestedRepetitionType string
	NestedFieldId        string
}

type IcebergSchemaField struct {
	Id       int         `json:"id"`
	Name     string      `json:"name"`
	Type     interface{} `json:"type"`
	Required bool        `json:"required"`
}

func (col *IcebergSchemaColumn) NormalizedColumnName() string {
	return strings.ReplaceAll(col.ColumnName, ",", "_") // Parquet doesn't allow commas in column names
}

func (col *IcebergSchemaColumn) QuotedColumnName() string {
	return `"` + col.NormalizedColumnName() + `"`
}

func (col *IcebergSchemaColumn) NormalizedPrecision() int {
	if col.NumericPrecision > TRINO_MAX_DECIMAL_PRECISION || col.NumericPrecision == 0 {
		return TRINO_MAX_DECIMAL_PRECISION
	}
	return col.NumericPrecision
}

func (col *IcebergSchemaColumn) NormalizedScale() int {
	if col.NumericPrecision == 0 {
		return TRINO_FALLBACK_DECIMAL_SCALE
	}
	return col.NumericScale
}

func (col *IcebergSchemaColumn) IcebergSchemaFieldMap() IcebergSchemaField {
	icebergSchemaField := IcebergSchemaField{
		Id:   col.Position,
		Name: col.NormalizedColumnName(),
	}

	switch col.ColumnType {
	case IcebergColumnTypeBoolean:
		icebergSchemaField.Type = "boolean"
	case IcebergColumnTypeString:
		icebergSchemaField.Type = "string"
	case IcebergColumnTypeInteger:
		icebergSchemaField.Type = "int"
	case IcebergColumnTypeDecimal:
		icebergSchemaField.Type = "decimal(" + common.IntToString(col.NormalizedPrecision()) + ", " + common.IntToString(col.NormalizedScale()) + ")"
	case IcebergColumnTypeLong:
		icebergSchemaField.Type = "long"
	case IcebergColumnTypeFloat:
		icebergSchemaField.Type = "float"
	case IcebergColumnTypeDouble:
		icebergSchemaField.Type = "double"
	case IcebergColumnTypeDate:
		icebergSchemaField.Type = "date"
	case IcebergColumnTypeTime, IcebergColumnTypeTimeTz:
		icebergSchemaField.Type = "time"
	case IcebergColumnTypeTimestamp:
		icebergSchemaField.Type = "timestamp"
	case IcebergColumnTypeTimestampTz:
		icebergSchemaField.Type = "timestamptz"
	case IcebergColumnTypeBinary:
		icebergSchemaField.Type = "binary"
	default:
		panic("Unsupported column type: " + string(col.ColumnType))
	}

	if col.IsNullable {
		icebergSchemaField.Required = false
	} else {
		icebergSchemaField.Required = true
	}

	if col.IsList {
		icebergSchemaField.Type = map[string]interface{}{
			"type":             "list",
			"element":          icebergSchemaField.Type,
			"element-id":       PARQUET_NESTED_FIELD_ID_PREFIX + col.Position,
			"element-required": false,
		}
	}

	return icebergSchemaField
}

func (col *IcebergSchemaColumn) DuckdbType() string {
	var duckdbType string
	switch col.ColumnType {
	case IcebergColumnTypeBoolean:
		duckdbType = "BOOLEAN"
	case IcebergColumnTypeString:
		duckdbType = "VARCHAR"
	case IcebergColumnTypeInteger:
		duckdbType = "INTEGER"
	case IcebergColumnTypeLong:
		duckdbType = "BIGINT"
	case IcebergColumnTypeDecimal:
		duckdbType = "DECIMAL(" + common.IntToString(col.NormalizedPrecision()) + ", " + common.IntToString(col.NormalizedScale()) + ")"
	case IcebergColumnTypeFloat:
		duckdbType = "FLOAT"
	case IcebergColumnTypeDouble:
		duckdbType = "DOUBLE"
	case IcebergColumnTypeDate:
		duckdbType = "DATE"
	case IcebergColumnTypeTime, IcebergColumnTypeTimeTz:
		duckdbType = "TIME"
	case IcebergColumnTypeTimestamp:
		duckdbType = "TIMESTAMP"
	case IcebergColumnTypeTimestampTz:
		duckdbType = "TIMESTAMP WITH TIME ZONE"
	case IcebergColumnTypeBinary:
		duckdbType = "BLOB"
	default:
		panic("Unsupported column type for DuckDB: " + string(col.ColumnType))
	}

	if col.IsList {
		return duckdbType + "[]"
	}
	return duckdbType
}

func (col *IcebergSchemaColumn) DuckdbValueFromCsv(value string) interface{} {
	if value == BEMIDB_NULL_STRING {
		return nil
	}

	if col.IsList {
		var values []interface{}

		csvString := strings.TrimPrefix(value, "{")
		csvString = strings.TrimSuffix(csvString, "}")
		if csvString == "" {
			return values
		}

		csvString = strings.ReplaceAll(csvString, "\\\"", "\"\"") // Replace escaped double quotes with double quotes according to CSV format rules
		csvReader := csv.NewReader(strings.NewReader(csvString))
		stringValues, err := csvReader.Read()
		common.PanicIfError(col.Config, err)

		for _, stringValue := range stringValues {
			primitiveValue := col.duckdbPrimitiveValueFromCsv(stringValue)
			values = append(values, primitiveValue)
		}
		return values
	}

	return col.duckdbPrimitiveValueFromCsv(value)
}

func (col *IcebergSchemaColumn) duckdbPrimitiveValueFromCsv(value string) interface{} {
	switch col.ColumnType {
	case IcebergColumnTypeBoolean:
		return value == "t"
	case IcebergColumnTypeString:
		return value
	case IcebergColumnTypeBinary:
		return []byte(value)
	case IcebergColumnTypeInteger:
		return int32(StringToInt(value))
	case IcebergColumnTypeLong:
		return StringToInt64(value)
	case IcebergColumnTypeFloat:
		floatValue := StringToFloat64(value)
		if math.IsNaN(floatValue) {
			return PARQUET_NAN
		}
		return float32(floatValue)
	case IcebergColumnTypeDouble:
		floatValue := StringToFloat64(value)
		if math.IsNaN(floatValue) {
			return PARQUET_NAN
		}
		return floatValue
	case IcebergColumnTypeDecimal:
		scale := col.NormalizedScale()
		parts := strings.Split(value, ".")
		integerPart := parts[0]

		// Pad fractional part with zeros if necessary
		var fractionalPart string
		if len(parts) == 1 {
			fractionalPart = strings.Repeat("0", scale)
		} else {
			fractionalPart = parts[1]
			if len(fractionalPart) < scale {
				fractionalPart += strings.Repeat("0", scale-len(fractionalPart))
			}
		}
		decimalValue := new(big.Int)
		decimalValue.SetString(integerPart+fractionalPart, 10)

		return duckdb.Decimal{
			Width: uint8(col.NormalizedPrecision()),
			Scale: uint8(scale),
			Value: decimalValue,
		}
	case IcebergColumnTypeDate:
		return StringDateToTime(value)
	case IcebergColumnTypeTime:
		parsedTime, err := time.Parse("15:04:05.999999", value)
		common.PanicIfError(col.Config, err)
		return parsedTime
	case IcebergColumnTypeTimeTz:
		parsedTime, err := time.Parse("15:04:05.999999-07", value)
		common.PanicIfError(col.Config, err)
		return parsedTime
	case IcebergColumnTypeTimestamp:
		parsedTimestamp, err := time.Parse("2006-01-02 15:04:05.999999", value)
		common.PanicIfError(col.Config, err)
		return parsedTimestamp
	case IcebergColumnTypeTimestampTz:
		parsedTimestamp, err := time.Parse("2006-01-02 15:04:05.999999-07:00", value)
		if err != nil {
			parsedTimestamp, err = time.Parse("2006-01-02 15:04:05.999999-07", value)
			common.PanicIfError(col.Config, err)
		}
		return parsedTimestamp
	}

	panic("Unsupported value: " + value + " for column type: " + string(col.ColumnType))
}
