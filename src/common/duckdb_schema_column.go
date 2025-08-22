package common

import "strings"

const (
	DUCKDB_YES = "YES"
)

type DuckdbSchemaColumn struct {
	ColumnName        string
	DataType          string
	IsNullable        string
	OrdinalPosition   string
	NumericPrecision  string
	NumericScale      string
	DatetimePrecision string
	Config            *CommonConfig
}

func (column *DuckdbSchemaColumn) ToIcebergSchemaColumn() *IcebergSchemaColumn {
	icebergSchemaColumn := &IcebergSchemaColumn{
		Config:            column.Config,
		ColumnName:        column.ColumnName,
		Position:          StringToInt(column.OrdinalPosition),
		NumericPrecision:  StringToInt(column.NumericPrecision),
		NumericScale:      StringToInt(column.NumericScale),
		IsRequired:        column.IsNullable != DUCKDB_YES,
		DatetimePrecision: StringToInt(column.DatetimePrecision),
		IsList:            strings.HasSuffix(column.DataType, "[]"),
	}

	switch strings.TrimSuffix(column.DataType, "[]") {
	case "BOOLEAN":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeBoolean
	case "INTEGER":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeInteger
	case "BIGINT":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeLong
	case "FLOAT":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeFloat
	case "DOUBLE":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeDouble
	case "DATE":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeDate
	case "TIME":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeTime
	case "TIMESTAMP":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeTimestamp
	case "BLOB":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeBinary
	case "VARCHAR", "JSON":
		icebergSchemaColumn.ColumnType = IcebergColumnTypeString
	default:
		if strings.HasPrefix(column.DataType, "DECIMAL(") {
			icebergSchemaColumn.ColumnType = IcebergColumnTypeDecimal
		} else {
			panic("Unsupported DuckDB type: " + column.DataType)
		}
	}

	return icebergSchemaColumn
}
