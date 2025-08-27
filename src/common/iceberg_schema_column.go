package common

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
)

type IcebergColumnType string

const (
	IcebergColumnTypeBoolean   IcebergColumnType = "boolean"
	IcebergColumnTypeString    IcebergColumnType = "string"
	IcebergColumnTypeInteger   IcebergColumnType = "int"
	IcebergColumnTypeDecimal   IcebergColumnType = "decimal"
	IcebergColumnTypeLong      IcebergColumnType = "long"
	IcebergColumnTypeFloat     IcebergColumnType = "float"
	IcebergColumnTypeDouble    IcebergColumnType = "double"
	IcebergColumnTypeDate      IcebergColumnType = "date"
	IcebergColumnTypeTime      IcebergColumnType = "time"
	IcebergColumnTypeTimeTz    IcebergColumnType = "timetz"
	IcebergColumnTypeTimestamp IcebergColumnType = "timestamp"
	IcebergColumnTypeBinary    IcebergColumnType = "binary"

	BEMIDB_NULL_STRING = "BEMIDB_NULL"

	PARQUET_NAN                    = 0 // DuckDB crashes on NaN, libc++abi: terminating due to uncaught exception of type duckdb::InvalidConfigurationException: {"exception_type":"Invalid Configuration","exception_message":"Column float4_column lower bound deserialization failed: Failed to deserialize blob '' of size 0, attempting to produce value of type 'FLOAT'"}
	PARQUET_MAX_DECIMAL_PRECISION  = 38
	PARQUET_FALLBACK_DECIMAL_SCALE = 6
	PARQUET_NESTED_FIELD_ID_PREFIX = 1000

	PG_USER_DEFINED_PRIMITIVE_TYPE = "USER_DEFINED"
)

type IcebergSchemaColumn struct {
	Config                *CommonConfig
	ColumnName            string
	ColumnType            IcebergColumnType
	Position              int
	NumericPrecision      int
	NumericScale          int
	DatetimePrecision     int
	IsList                bool
	IsRequired            bool
	IsPartOfUniqueIndex   bool
	PgPrimitiveColumnType string
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
	if col.NumericPrecision > PARQUET_MAX_DECIMAL_PRECISION || col.NumericPrecision == 0 {
		return PARQUET_MAX_DECIMAL_PRECISION
	}
	return col.NumericPrecision
}

func (col *IcebergSchemaColumn) NormalizedScale() int {
	if col.NumericPrecision == 0 {
		return PARQUET_FALLBACK_DECIMAL_SCALE
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
		icebergSchemaField.Type = "decimal(" + IntToString(col.NormalizedPrecision()) + ", " + IntToString(col.NormalizedScale()) + ")"
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
	case IcebergColumnTypeBinary:
		icebergSchemaField.Type = "binary"
	default:
		panic("Unsupported column type: " + string(col.ColumnType))
	}

	icebergSchemaField.Required = col.IsRequired

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
		duckdbType = "DECIMAL(" + IntToString(col.NormalizedPrecision()) + ", " + IntToString(col.NormalizedScale()) + ")"
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
		PanicIfError(col.Config, err)

		for _, stringValue := range stringValues {
			primitiveValue := col.duckdbPrimitiveValueFromCsv(stringValue)
			values = append(values, primitiveValue)
		}
		return values
	}

	return col.duckdbPrimitiveValueFromCsv(value)
}

func (col *IcebergSchemaColumn) DuckdbValueFromJson(value any) interface{} {
	if value == nil {
		return nil
	}

	if col.IsList {
		var values []interface{}
		for _, itemValue := range value.([]any) {
			primitiveValue := col.duckdbPrimitiveValueFromJson(itemValue)
			values = append(values, primitiveValue)
		}
		return values
	}

	return col.duckdbPrimitiveValueFromJson(value)
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
		valueFloat := StringToFloat64(value)
		if math.IsNaN(valueFloat) {
			return PARQUET_NAN
		}
		return float32(valueFloat)
	case IcebergColumnTypeDouble:
		valueFloat := StringToFloat64(value)
		if math.IsNaN(valueFloat) {
			return PARQUET_NAN
		}
		return valueFloat
	case IcebergColumnTypeDecimal:
		switch col.PgPrimitiveColumnType {
		case "interval":
			microseconds := 0

			parts := strings.Split(value, " ")
			for i, part := range parts {
				if strings.HasPrefix(part, "year") {
					Panic(col.Config, "Year intervals are not supported yet")
				} else if strings.HasPrefix(part, "mon") {
					months := StringToInt(parts[i-1])
					microseconds += months * 30_437_500 * 24 * 60 * 60 // Approximation: 30.4375 days per month
				} else if strings.HasPrefix(part, "day") {
					days := StringToInt(parts[i-1])
					microseconds += days * 24 * 60 * 60 * 1_000_000
				} else if strings.Contains(part, ":") {
					timeParts := strings.Split(part, ":")
					hours := StringToInt(timeParts[0])
					minutes := StringToInt(timeParts[1])
					secondsParts := strings.Split(timeParts[2], ".")
					seconds := StringToInt(secondsParts[0])
					microseconds += (hours * 60 * 60 * 1_000_000) + (minutes * 60 * 1_000_000) + (seconds * 1_000_000)
					if len(secondsParts) > 1 && len(secondsParts[1]) == 6 {
						microseconds += StringToInt(secondsParts[1])
					}
				}
			}
			valueMicrosecondsString := IntToString(microseconds)
			return col.duckdbDecimal(valueMicrosecondsString)
		default:
			return col.duckdbDecimal(value)
		}
	case IcebergColumnTypeDate:
		return StringDateToTime(value)
	case IcebergColumnTypeTime:
		parsedTime, err := time.Parse("15:04:05.999999", value)
		PanicIfError(col.Config, err)
		return parsedTime
	case IcebergColumnTypeTimeTz:
		parsedTime, err := time.Parse("15:04:05.999999-07", value)
		PanicIfError(col.Config, err)
		return parsedTime
	case IcebergColumnTypeTimestamp:
		parsedTimestamp, err := time.Parse("2006-01-02 15:04:05.999999", value)
		if err != nil {
			parsedTimestamp, err = time.Parse("2006-01-02 15:04:05.999999-07:00", value)
			if err != nil {
				parsedTimestamp, err = time.Parse("2006-01-02 15:04:05.999999-07", value)
				PanicIfError(col.Config, err)
			}
		}
		return parsedTimestamp
	}

	panic("Unsupported value: " + value + " for column type: " + string(col.ColumnType))
}

func (col *IcebergSchemaColumn) duckdbPrimitiveValueFromJson(value any) interface{} {
	kind := reflect.TypeOf(value).Kind()

	switch col.ColumnType {
	case IcebergColumnTypeInteger,
		IcebergColumnTypeLong,
		IcebergColumnTypeFloat,
		IcebergColumnTypeDouble:
		switch kind {
		case reflect.Bool:
			if value.(bool) {
				return 1
			} else {
				return 0
			}
		case reflect.String:
			if value.(string) == "NaN" {
				return PARQUET_NAN
			}
		default:
			return value
		}
	case IcebergColumnTypeDecimal:
		valueString := Float64ToString(value.(float64))
		return col.duckdbDecimal(valueString)
	case IcebergColumnTypeBinary:
		return []byte("\\x" + value.(string))
	case IcebergColumnTypeBoolean:
		switch kind {
		case reflect.String:
			return value.(string) == "true"
		default:
			return value
		}
	case IcebergColumnTypeString:
		switch col.PgPrimitiveColumnType {
		case "bpchar":
			return strings.TrimRight(value.(string), " ")
		case "point":
			valueMap := value.(map[string]interface{})
			return "(" + Float64ToString(valueMap["x"].(float64)) + "," + Float64ToString(valueMap["y"].(float64)) + ")"
		case PG_USER_DEFINED_PRIMITIVE_TYPE:
			valueString := value.(string)
			valueDecodedHex, err := HexToString(valueString)
			if err == nil {
				return valueDecodedHex
			} else {
				return valueString
			}
		}

		switch kind {
		case reflect.Map:
			jsonBytes, err := json.Marshal(value)
			PanicIfError(col.Config, err)
			return string(jsonBytes)
		case reflect.Float64:
			return Float64ToString(value.(float64))
		default:
			return value
		}
	case IcebergColumnTypeDate:
		days := value.(float64)
		return time.Unix(0, 0).UTC().AddDate(0, 0, int(days))
	case IcebergColumnTypeTime:
		var nanoseconds int64
		if col.DatetimePrecision == 6 {
			nanoseconds = int64(value.(float64)) * 1_000
		} else {
			nanoseconds = int64(value.(float64)) * 1_000_000
		}
		valueTime := time.Unix(0, nanoseconds).UTC()
		return valueTime
	case IcebergColumnTypeTimeTz:
		valueString := value.(string)
		if valueString == "" {
			return nil
		}
		if strings.HasSuffix(valueString, "Z") {
			valueString = strings.TrimSuffix(valueString, "Z") + "-00"
		}
		parsedTime, err := time.Parse("15:04:05.999999-07", valueString)
		PanicIfError(col.Config, err)
		return parsedTime
	case IcebergColumnTypeTimestamp:
		switch kind {
		case reflect.String:
			valueString := value.(string)
			if valueString == "" {
				return nil
			}
			valueString = strings.Replace(valueString, " ", "T", 1) // Amplitude
			valueString = strings.TrimSuffix(valueString, "Z")
			parsedTimestamp, err := time.Parse("2006-01-02T15:04:05.999999", valueString)
			if err != nil {
				parsedTimestamp, err = time.Parse("2006-01-02T15:04:05.999999-07:00", valueString)
				if err != nil {
					parsedTimestamp, err = time.Parse("2006-01-02T15:04:05.999999-07", valueString)
					PanicIfError(col.Config, err)
				}
			}
			return parsedTimestamp
		case reflect.Float64:
			valueFloat := value.(float64)
			epoch := time.Unix(0, 0).UTC()
			if col.DatetimePrecision == 6 {
				microseconds := int64(valueFloat)
				return epoch.Add(time.Duration(microseconds) * time.Microsecond)
			}
			milliseconds := int64(valueFloat)
			return epoch.Add(time.Duration(milliseconds) * time.Millisecond)
		}
	}

	panic(fmt.Sprintf("Unsupported value: %v for column type: %s", value, col.ColumnType))
}

func (col *IcebergSchemaColumn) duckdbDecimal(value string) duckdb.Decimal {
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
}
