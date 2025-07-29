package main

import (
	"encoding/csv"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	PG_TRUE            = "YES"
	PG_DATA_TYPE_ARRAY = "ARRAY"
)

type PgSchemaColumn struct {
	ColumnName        string
	DataType          string
	UdtName           string
	IsNullable        string
	OrdinalPosition   string
	NumericPrecision  string
	NumericScale      string
	DatetimePrecision string
	Namespace         string
	PartOfPrimaryKey  bool
	Config            *Config
}

func NewPgSchemaColumn(config *Config) *PgSchemaColumn {
	return &PgSchemaColumn{
		Config: config,
	}
}

func (pgSchemaColumn *PgSchemaColumn) ToIcebergSchemaColumn() *common.IcebergSchemaColumn {
	var icebergColumnType common.IcebergColumnType

	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "bool":
		icebergColumnType = common.IcebergColumnTypeBoolean
	case "bit", "int2", "int4":
		icebergColumnType = common.IcebergColumnTypeInteger
	case "xid":
		icebergColumnType = common.IcebergColumnTypeLong
	case "int8", "xid8":
		icebergColumnType = common.IcebergColumnTypeDecimal
	case "float4":
		icebergColumnType = common.IcebergColumnTypeFloat
	case "float8":
		icebergColumnType = common.IcebergColumnTypeDouble
	case "numeric":
		icebergColumnType = common.IcebergColumnTypeDecimal
	case "date":
		icebergColumnType = common.IcebergColumnTypeDate
	case "time":
		icebergColumnType = common.IcebergColumnTypeTime
	case "timetz":
		icebergColumnType = common.IcebergColumnTypeTimeTz
	case "timestamp":
		icebergColumnType = common.IcebergColumnTypeTimestamp
	case "timestamptz":
		icebergColumnType = common.IcebergColumnTypeTimestampTz
	case "varchar", "char", "text", "jsonb", "json", "bpchar", "uuid",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"interval", "ltree", "tsvector", "xml", "pg_snapshot":
		icebergColumnType = common.IcebergColumnTypeString
	case "bytea":
		icebergColumnType = common.IcebergColumnTypeBinary
	default:
		// User-defined types -> VARCHAR
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			icebergColumnType = common.IcebergColumnTypeString
		} else {
			panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
		}
	}

	return &common.IcebergSchemaColumn{
		Config:           pgSchemaColumn.Config.BaseConfig,
		ColumnName:       pgSchemaColumn.ColumnName,
		ColumnType:       icebergColumnType,
		Position:         common.StringToInt(pgSchemaColumn.OrdinalPosition),
		NumericPrecision: common.StringToInt(pgSchemaColumn.NumericPrecision),
		NumericScale:     common.StringToInt(pgSchemaColumn.NumericScale),
		IsList:           pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY,
		IsNullable:       pgSchemaColumn.IsNullable == PG_TRUE,
	}
}

func (pgSchemaColumn *PgSchemaColumn) TrinoType() string {
	primitiveType := pgSchemaColumn.primitiveTrinoType()

	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		return "ARRAY(" + primitiveType + ")"
	}
	return primitiveType
}

func (pgSchemaColumn *PgSchemaColumn) JsonToTrinoValue(jsonValue interface{}) string {
	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		if jsonValue == nil {
			return "NULL"
		}

		var values []string

		for _, value := range jsonValue.([]interface{}) {
			values = append(values, pgSchemaColumn.jsonToPrimitiveTrinoValue(value))
		}

		return "ARRAY[" + strings.Join(values, ",") + "]"
	}

	return pgSchemaColumn.jsonToPrimitiveTrinoValue(jsonValue)
}

func (pgSchemaColumn *PgSchemaColumn) CsvToTrinoValue(csvValue string) string {
	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		if csvValue == common.BEMIDB_NULL_STRING {
			return "NULL"
		}

		csvString := strings.TrimPrefix(csvValue, "{")
		csvString = strings.TrimSuffix(csvString, "}")
		if csvString == "" {
			return "ARRAY[]"
		}

		// Replace escaped double quotes with double quotes according to CSV format rules
		csvString = strings.ReplaceAll(csvString, "\\\"", "\"\"")
		csvReader := csv.NewReader(strings.NewReader(csvString))
		stringValues, err := csvReader.Read()
		common.PanicIfError(pgSchemaColumn.Config.BaseConfig, err)

		var values []string
		for _, stringValue := range stringValues {
			values = append(values, pgSchemaColumn.csvToPrimitiveTrinoValue(stringValue))
		}
		return "ARRAY[" + strings.Join(values, ",") + "]"
	}

	return pgSchemaColumn.csvToPrimitiveTrinoValue(csvValue)
}

func (pgSchemaColumn *PgSchemaColumn) jsonToPrimitiveTrinoValue(jsonValue interface{}) string {
	if jsonValue == nil {
		return "NULL"
	}

	kind := reflect.TypeOf(jsonValue).Kind()

	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "bool":
		if jsonValue.(bool) {
			return "true"
		}
		return "false"
	case "bit":
		if jsonValue.(bool) {
			return "1"
		}
		return "0"
	case "int2", "int4", "xid":
		return common.Float64ToString(jsonValue.(float64))
	case "float4":
		if kind == reflect.String {
			return "nan()"
		}
		return common.Float64ToString(jsonValue.(float64))
	case "float8":
		if kind == reflect.String {
			return "nan()"
		}
		return "CAST('" + common.Float64ToString(jsonValue.(float64)) + "' AS DOUBLE)"
	case "int8", "xid8", "numeric":
		return "DECIMAL '" + common.Float64ToString(jsonValue.(float64)) + "'"
	case "date":
		return "DATE '1970-01-01' + INTERVAL '" + common.Float64ToString(jsonValue.(float64)) + "' DAY"
	case "timestamp":
		if pgSchemaColumn.DatetimePrecision == "6" {
			return "from_unixtime_nanos(" + common.Float64ToString(jsonValue.(float64)) + " * 1000)"
		}
		return "from_unixtime_nanos(" + common.Float64ToString(jsonValue.(float64)) + " * 1000000)"
	case "timestamptz":
		return "from_iso8601_timestamp_nanos('" + jsonValue.(string) + "')"
	case "time":
		if pgSchemaColumn.DatetimePrecision == "6" {
			microseconds := int64(jsonValue.(float64))
			hours := microseconds / 3_600_000_000
			microseconds %= 3_600_000_000
			minutes := microseconds / 60_000_000
			microseconds %= 60_000_000
			seconds := microseconds / 1_000_000
			microseconds %= 1_000_000
			return fmt.Sprintf("TIME '%02d:%02d:%02d.%06d'", int(hours), int(minutes), int(seconds), int(microseconds))
		}
		milliseconds := int64(jsonValue.(float64))
		hours := milliseconds / 3_600_000
		milliseconds %= 3_600_000
		minutes := milliseconds / 60_000
		milliseconds %= 60_000
		seconds := milliseconds / 1_000
		milliseconds %= 1_000
		return fmt.Sprintf("TIME '%02d:%02d:%02d.%03d'", int(hours), int(minutes), int(seconds), int(milliseconds))
	case "timetz":
		return "TIME '" + strings.TrimRight(jsonValue.(string), "Z") + "' AT TIME ZONE 'UTC'"
	case "interval":
		return "'" + common.Float64ToString(jsonValue.(float64)) + "us'"
	case "varchar", "char", "text", "jsonb", "json", "uuid",
		"line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"ltree", "tsvector", "xml", "pg_snapshot":
		return "'" + strings.ReplaceAll(jsonValue.(string), "'", "''") + "'"
	case "bytea":
		return "CAST('\\x" + jsonValue.(string) + "' AS VARBINARY)"
	case "bpchar":
		trimmedValue := strings.TrimRight(jsonValue.(string), " ")
		return "'" + strings.ReplaceAll(trimmedValue, "'", "''") + "'"
	case "point", "geometry":
		point := jsonValue.(map[string]interface{})
		if point["wkb"] == nil {
			return "NULL"
		}
		return "'" + common.Base64ToHex(point["wkb"].(string)) + "'"
	default:
		// User-defined types -> VARCHAR value
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			if kind == reflect.String {
				stringValue, err := common.HexToString(jsonValue.(string))
				if err == nil {
					return "'" + stringValue + "'"
				} else {
					// Fallback to regular string value if not a valid hex string
					return "'" + strings.ReplaceAll(jsonValue.(string), "'", "''") + "'"
				}
			} else {
				panic(fmt.Sprintf("Unsupported user-defined type %+v with value %v of type %T", pgSchemaColumn, jsonValue, jsonValue))
			}
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}

func (pgSchemaColumn *PgSchemaColumn) csvToPrimitiveTrinoValue(csvValue string) string {
	if csvValue == common.BEMIDB_NULL_STRING {
		return "NULL"
	}

	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "bool":
		if csvValue == "t" {
			return "TRUE"
		}
		return "FALSE"
	case "bit", "int2", "int4", "xid":
		return csvValue
	case "float4":
		if csvValue == "NaN" {
			return "nan()"
		}
		return csvValue
	case "float8":
		if csvValue == "NaN" {
			return "nan()"
		}
		return "CAST('" + csvValue + "' AS DOUBLE)"
	case "int8", "xid8", "numeric":
		return "DECIMAL '" + csvValue + "'"
	case "date":
		return "DATE '" + csvValue + "'"
	case "timestamp", "timestamptz":
		return "TIMESTAMP '" + csvValue + "'"
	case "time":
		return "TIME '" + csvValue + "'"
	case "timetz":
		var parsedTime time.Time
		var err error
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err = time.Parse("15:04:05.999999-07", csvValue)
		} else if pgSchemaColumn.DatetimePrecision == "3" {
			parsedTime, err = time.Parse("15:04:05.999-07", csvValue)
		}
		common.PanicIfError(pgSchemaColumn.Config.BaseConfig, err)

		parsedTime = parsedTime.In(time.UTC)
		return "TIME '" + parsedTime.Format("15:04:05.999999-07:00") + "' AT TIME ZONE 'UTC'"
	case "interval": // 1 mon 2 days 01:00:01.000001 -> '_us' (microseconds)
		microseconds := 0

		parts := strings.Split(csvValue, " ")
		for i, part := range parts {
			if strings.HasPrefix(part, "mon") {
				months := common.StringToInt(parts[i-1])
				microseconds += months * 30_437_500 * 24 * 60 * 60 // Approximation: 30.4375 days per month
			} else if strings.HasPrefix(part, "day") {
				days := common.StringToInt(parts[i-1])
				microseconds += days * 24 * 60 * 60 * 1_000_000
			} else if strings.Contains(part, ":") {
				timeParts := strings.Split(part, ":")
				hours := common.StringToInt(timeParts[0])
				minutes := common.StringToInt(timeParts[1])
				secondsParts := strings.Split(timeParts[2], ".")
				seconds := common.StringToInt(secondsParts[0])
				microseconds += (hours * 60 * 60 * 1_000_000) + (minutes * 60 * 1_000_000) + (seconds * 1_000_000)
				if len(secondsParts) > 1 && len(secondsParts[1]) == 6 {
					microseconds += common.StringToInt(secondsParts[1])
				}
			}
		}
		return "'" + common.IntToString(microseconds) + "us'"
	case "varchar", "char", "text", "jsonb", "json", "uuid",
		"line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"ltree", "tsvector", "xml", "pg_snapshot":
		return "'" + strings.ReplaceAll(csvValue, "'", "''") + "'"
	case "bytea":
		return "CAST('" + csvValue + "' AS VARBINARY)"
	case "bpchar":
		trimmedValue := strings.TrimRight(csvValue, " ")
		return "'" + strings.ReplaceAll(trimmedValue, "'", "''") + "'"
	case "point":
		return "to_hex(ST_AsBinary(ST_Point" + csvValue + "))" // to_hex(ST_AsBinary(ST_Point(1, 2)))
	// TODO "geometry":
	default:
		// User-defined types -> VARCHAR value
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			return "'" + strings.ReplaceAll(csvValue, "'", "''") + "'"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}

func (pgSchemaColumn *PgSchemaColumn) primitiveTrinoType() string {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "bool":
		return "BOOLEAN"
	case "bit", "int2", "int4":
		return "INTEGER"
	case "xid":
		return "BIGINT"
	case "int8", "xid8":
		return "DECIMAL(20, 0)"
	case "float4":
		return "REAL"
	case "float8":
		return "DOUBLE"
	case "numeric":
		scale := common.StringToInt(pgSchemaColumn.NumericScale)
		precision := common.StringToInt(pgSchemaColumn.NumericPrecision)
		if precision > common.TRINO_MAX_DECIMAL_PRECISION {
			precision = common.TRINO_MAX_DECIMAL_PRECISION
		} else if precision == 0 {
			precision = common.TRINO_MAX_DECIMAL_PRECISION
			scale = common.TRINO_MAX_DECIMAL_PRECISION / 2
		}
		return "DECIMAL(" + common.IntToString(precision) + ", " + common.IntToString(scale) + ")"
	case "date":
		return "DATE"
	case "time", "timetz": // TIME(6) WITH TIME ZONE in Iceberg is not supported by Trino
		return "TIME(6)"
	case "timestamp":
		return "TIMESTAMP(6)"
	case "timestamptz":
		return "TIMESTAMP(6) WITH TIME ZONE"
	case "varchar", "char", "text", "jsonb", "json", "bpchar", "uuid",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"interval", "ltree", "tsvector", "xml", "pg_snapshot":
		return "VARCHAR"
	case "bytea":
		return "VARBINARY"
	default:
		// User-defined types -> VARCHAR
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			return "VARCHAR"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}
