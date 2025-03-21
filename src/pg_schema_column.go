package main

import (
	"encoding/csv"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	PG_NULL_STRING = "BEMIDB_NULL"
	PG_TRUE        = "YES"
	PG_FALSE       = "FALSE"

	PG_DATA_TYPE_ARRAY = "ARRAY"

	PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED = "REQUIRED"
	PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL = "OPTIONAL"

	PARQUET_NAN                   = "NaN"
	PARQUET_MAX_DECIMAL_PRECISION = 38

	// 0000-01-01 00:00:00 +0000 UTC
	EPOCH_TIME_MS = -62167219200000
)

type PgSchemaColumn struct {
	ColumnName             string
	DataType               string
	UdtName                string
	IsNullable             string
	OrdinalPosition        string
	CharacterMaximumLength string
	NumericPrecision       string
	NumericScale           string
	DatetimePrecision      string
	Namespace              string
	PartOfPrimaryKey       bool
	config                 *Config
}

// Parquet doesn't allow commas in column names
func (pgSchemaColumn PgSchemaColumn) NormalizedColumnName() string {
	return strings.ReplaceAll(pgSchemaColumn.ColumnName, ",", "_")
}

func NewPgSchemaColumn(config *Config) *PgSchemaColumn {
	return &PgSchemaColumn{
		config: config,
	}
}

type ParquetSchemaField struct {
	Name                string
	Type                string
	RepetitionType      string
	FieldId             string
	Length              string
	ConvertedType       string
	Scale               string
	Precision           string
	NestedType          string
	NestedConvertedType string
}

type IcebergSchemaField struct {
	Id       int         `json:"id"`
	Name     string      `json:"name"`
	Type     interface{} `json:"type"`
	Required bool        `json:"required"`
}

func (pgSchemaColumn PgSchemaColumn) ToParquetSchemaFieldMap() map[string]interface{} {
	field := pgSchemaColumn.toParquetSchemaField()

	tagKeyVals := []string{
		"name=" + field.Name,
		"type=" + field.Type,
		"repetitiontype=" + field.RepetitionType,
		"fieldid=" + field.FieldId,
	}

	if field.Length != "" {
		tagKeyVals = append(tagKeyVals, "length="+field.Length)
	}
	if field.ConvertedType != "" {
		tagKeyVals = append(tagKeyVals, "convertedtype="+field.ConvertedType)
	}
	if field.Scale != "" {
		tagKeyVals = append(tagKeyVals, "scale="+field.Scale)
	}
	if field.Precision != "" {
		tagKeyVals = append(tagKeyVals, "precision="+field.Precision)
	}

	result := map[string]interface{}{
		"Tag": strings.Join(tagKeyVals, ", "),
	}

	if field.NestedType != "" {
		nestedTagKeyVals := []string{
			"name=element",
			"type=" + field.NestedType,
		}

		if field.NestedConvertedType != "" {
			nestedTagKeyVals = append(nestedTagKeyVals, "convertedtype="+field.NestedConvertedType)
		}

		result["Fields"] = []map[string]interface{}{
			{"Tag": strings.Join(nestedTagKeyVals, ", ")},
		}
	}

	return result
}

func (pgSchemaColumn PgSchemaColumn) ToIcebergSchemaFieldMap() IcebergSchemaField {
	icebergSchemaField := IcebergSchemaField{}

	id, err := StringToInt(pgSchemaColumn.OrdinalPosition)
	if err != nil {
		panic(err)
	}

	icebergSchemaField.Id = id
	icebergSchemaField.Name = pgSchemaColumn.NormalizedColumnName()

	if pgSchemaColumn.IsNullable == PG_TRUE {
		icebergSchemaField.Required = false
	} else {
		icebergSchemaField.Required = true
	}

	primitiveType := pgSchemaColumn.icebergPrimitiveType()
	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		icebergSchemaField.Type = map[string]interface{}{
			"type":             "list",
			"element":          primitiveType,
			"element-id":       pgSchemaColumn.OrdinalPosition,
			"element-required": false,
		}
	} else {
		icebergSchemaField.Type = primitiveType
	}

	return icebergSchemaField
}

func (pgSchemaColumn *PgSchemaColumn) FormatParquetValue(value string) interface{} {
	if value == PG_NULL_STRING {
		return nil
	}

	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		var values []interface{}

		csvString := strings.TrimPrefix(value, "{")
		csvString = strings.TrimSuffix(csvString, "}")
		if csvString == "" {
			return values
		}

		// Replace escaped double quotes with double quotes according to CSV format rules
		csvString = strings.ReplaceAll(csvString, "\\\"", "\"\"")
		csvReader := csv.NewReader(strings.NewReader(csvString))
		stringValues, err := csvReader.Read()
		PanicIfError(err, pgSchemaColumn.config)

		for _, stringValue := range stringValues {
			values = append(values, pgSchemaColumn.parquetPrimitiveValue(stringValue))
		}

		return values
	}

	return pgSchemaColumn.parquetPrimitiveValue(value)
}

func (pgSchemaColumn *PgSchemaColumn) toParquetSchemaField() ParquetSchemaField {
	primitiveType, primitiveConvertedType := pgSchemaColumn.parquetPrimitiveTypes()

	parquetSchemaField := ParquetSchemaField{
		Name:          pgSchemaColumn.NormalizedColumnName(),
		FieldId:       pgSchemaColumn.OrdinalPosition,
		Type:          primitiveType,
		ConvertedType: primitiveConvertedType,
	}

	// Set RepetitionType
	if pgSchemaColumn.IsNullable == PG_TRUE {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL
	} else {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED
	}

	// Set other field properties
	switch pgSchemaColumn.UdtName {
	case "numeric":
		scale, err := StringToInt(pgSchemaColumn.NumericScale)
		PanicIfError(err, pgSchemaColumn.config)
		precision, err := StringToInt(pgSchemaColumn.NumericPrecision)
		PanicIfError(err, pgSchemaColumn.config)
		if precision > PARQUET_MAX_DECIMAL_PRECISION {
			precision = PARQUET_MAX_DECIMAL_PRECISION
		} else if precision == 0 {
			precision = PARQUET_MAX_DECIMAL_PRECISION / 2
			scale = PARQUET_MAX_DECIMAL_PRECISION / 2
		}

		parquetSchemaField.Scale = IntToString(scale)
		parquetSchemaField.Precision = IntToString(precision)
		parquetSchemaField.Length = IntToString(scale + precision)
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
			parquetSchemaField.NestedType = parquetSchemaField.Type
			parquetSchemaField.NestedConvertedType = parquetSchemaField.ConvertedType
			parquetSchemaField.Type = "LIST"
			parquetSchemaField.ConvertedType = ""
		}
	}

	return parquetSchemaField
}

func (pgSchemaColumn *PgSchemaColumn) parquetPrimitiveValue(value string) interface{} {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "bit", "bytea", "jsonb", "json", "numeric", "uuid", "interval",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"tsvector", "xml", "pg_snapshot":
		return value
	case "bpchar":
		trimmedValue := strings.TrimRight(value, " ")
		return trimmedValue
	case "int2", "int4":
		intValue, err := StringToInt(value)
		PanicIfError(err, pgSchemaColumn.config)
		return int32(intValue)
	case "int8":
		intValue, err := strconv.ParseInt(value, 10, 64)
		PanicIfError(err, pgSchemaColumn.config)
		return intValue
	case "xid":
		intValue, err := strconv.ParseUint(value, 10, 32)
		PanicIfError(err, pgSchemaColumn.config)
		return intValue
	case "xid8":
		intValue, err := strconv.ParseUint(value, 10, 64)
		PanicIfError(err, pgSchemaColumn.config)
		return intValue
	case "float4":
		floatValue, err := strconv.ParseFloat(value, 32)
		PanicIfError(err, pgSchemaColumn.config)
		if math.IsNaN(floatValue) {
			return PARQUET_NAN
		}
		return float32(floatValue)
	case "float8":
		floatValue, err := strconv.ParseFloat(value, 64)
		PanicIfError(err, pgSchemaColumn.config)
		if math.IsNaN(floatValue) {
			return PARQUET_NAN
		}
		return floatValue
	case "bool":
		boolValue, err := strconv.ParseBool(value)
		PanicIfError(err, pgSchemaColumn.config)
		return boolValue
	case "timestamp":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999", value)
			PanicIfError(err, pgSchemaColumn.config)
			return parsedTime.UnixMicro()
		} else {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999", value)
			PanicIfError(err, pgSchemaColumn.config)
			return parsedTime.UnixMilli()
		}
	case "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999-07:00", value)
			if err != nil {
				parsedTime, err = time.Parse("2006-01-02 15:04:05.999999-07", value)
				PanicIfError(err, pgSchemaColumn.config)
			}
			return parsedTime.UnixMicro()
		} else {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999-07:00", value)
			if err != nil {
				parsedTime, err = time.Parse("2006-01-02 15:04:05.999-07", value)
				PanicIfError(err, pgSchemaColumn.config)
			}
			return parsedTime.UnixMilli()
		}
	case "time":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("15:04:05.999999", value)
			PanicIfError(err, pgSchemaColumn.config)
			return int64(-EPOCH_TIME_MS*1000 + parsedTime.UnixMicro())
		} else {
			parsedTime, err := time.Parse("15:04:05.999", value)
			PanicIfError(err, pgSchemaColumn.config)
			return -EPOCH_TIME_MS + parsedTime.UnixMilli()
		}
	case "timetz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("15:04:05.999999-07", value)
			PanicIfError(err, pgSchemaColumn.config)
			return int64(-EPOCH_TIME_MS*1000 + parsedTime.UnixMicro())
		} else {
			parsedTime, err := time.Parse("15:04:05.999-07", value)
			PanicIfError(err, pgSchemaColumn.config)
			return -EPOCH_TIME_MS + parsedTime.UnixMilli()
		}
	case "date":
		parsedTime, err := StringDateToTime(value)
		PanicIfError(err, pgSchemaColumn.config)
		return parsedTime.Unix() / 86400
	default:
		// User-defined types
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			return value
		}
	}

	panic("Unsupported PostgreSQL value: " + value)
}

func (pgSchemaColumn *PgSchemaColumn) parquetPrimitiveTypes() (primitiveType string, primitiveConvertedType string) {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "bpchar", "bit", "bytea", "interval", "jsonb", "json",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"uuid", // DuckDB doesn't support BLOB (FIXED_LEN_BYTE_ARRAY) -> UUID type casting, use a string instead
		"tsvector", "xml", "pg_snapshot":
		return "BYTE_ARRAY", "UTF8"
	case "date":
		return "INT32", "DATE"
	case "int2", "int4":
		return "INT32", ""
	case "int8":
		return "INT64", ""
	case "float4":
		return "FLOAT", ""
	case "float8":
		return "DOUBLE", ""
	case "numeric":
		return "FIXED_LEN_BYTE_ARRAY", "DECIMAL"
	case "xid":
		return "INT32", "UINT_32"
	case "xid8":
		return "INT64", "UINT_64"
	case "bool":
		return "BOOLEAN", ""
	case "time", "timetz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			return "INT64", "TIME_MICROS"
		} else {
			return "INT32", "TIME_MILLIS"
		}
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			return "INT64", "TIMESTAMP_MICROS"
		} else {
			return "INT64", "TIMESTAMP_MILLIS"
		}
	default:
		// User-defined types
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			return "BYTE_ARRAY", "UTF8"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}

func (pgSchemaColumn *PgSchemaColumn) icebergPrimitiveType() string {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "interval", "jsonb", "json", "bpchar", "bit",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"tsvector", "xml", "pg_snapshot":
		return "string"
	case "uuid":
		return "uuid"
	case "int2", "int4", "xid":
		return "int"
	case "int8", "xid8":
		return "long"
	case "float4", "float8":
		return "float"
	case "numeric":
		if pgSchemaColumn.NumericScale == "0" {
			return "decimal"
		}

		precision, err := StringToInt(pgSchemaColumn.NumericPrecision)
		PanicIfError(err, pgSchemaColumn.config)
		if precision > PARQUET_MAX_DECIMAL_PRECISION {
			precision = PARQUET_MAX_DECIMAL_PRECISION
		}
		return "decimal(" + IntToString(precision) + ", " + pgSchemaColumn.NumericScale + ")"
	case "bool":
		return "boolean"
	case "date":
		return "date"
	case "bytea":
		return "binary"
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "9" {
			return "timestamp_ns"
		} else {
			return "timestamp"
		}
	case "time", "timetz":
		return "time"
	default:
		// User-defined types
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			return "string"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}
