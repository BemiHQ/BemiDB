package main

import (
	"strings"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	PG_TRUE            = "YES"
	PG_DATA_TYPE_ARRAY = "ARRAY"
)

type PgSchemaColumn struct {
	ColumnName          string
	DataType            string
	UdtName             string
	IsNullable          string
	OrdinalPosition     string
	NumericPrecision    string
	NumericScale        string
	DatetimePrecision   string
	Namespace           string
	IsPartOfUniqueIndex bool
	Config              *Config
}

func NewPgSchemaColumn(config *Config) *PgSchemaColumn {
	return &PgSchemaColumn{
		Config: config,
	}
}

func (pgSchemaColumn *PgSchemaColumn) ToIcebergSchemaColumn() *syncerCommon.IcebergSchemaColumn {
	icebergSchemaColumn := &syncerCommon.IcebergSchemaColumn{
		Config:                pgSchemaColumn.Config.CommonConfig,
		ColumnName:            pgSchemaColumn.ColumnName,
		Position:              syncerCommon.StringToInt(pgSchemaColumn.OrdinalPosition),
		NumericPrecision:      syncerCommon.StringToInt(pgSchemaColumn.NumericPrecision),
		NumericScale:          syncerCommon.StringToInt(pgSchemaColumn.NumericScale),
		IsList:                pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY,
		IsRequired:            pgSchemaColumn.IsNullable != PG_TRUE,
		IsPartOfUniqueIndex:   pgSchemaColumn.IsPartOfUniqueIndex,
		DatetimePrecision:     syncerCommon.StringToInt(pgSchemaColumn.DatetimePrecision),
		PgPrimitiveColumnType: strings.TrimLeft(pgSchemaColumn.UdtName, "_"),
	}

	switch icebergSchemaColumn.PgPrimitiveColumnType {
	case "bool":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeBoolean
	case "bit", "int2", "int4":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeInteger
	case "xid":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeLong
	case "int8", "xid8", "interval":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeDecimal
	case "float4":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeFloat
	case "float8":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeDouble
	case "numeric":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeDecimal
	case "date":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeDate
	case "time":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeTime
	case "timetz":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeTimeTz
	case "timestamp":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeTimestamp
	case "timestamptz":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeTimestampTz
	case "varchar", "char", "text", "jsonb", "json", "bpchar", "uuid",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"ltree", "tsvector", "xml", "pg_snapshot":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeString
	case "bytea":
		icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeBinary
	default:
		// User-defined types -> VARCHAR
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			icebergSchemaColumn.PgPrimitiveColumnType = syncerCommon.PG_USER_DEFINED_PRIMITIVE_TYPE
			icebergSchemaColumn.ColumnType = syncerCommon.IcebergColumnTypeString
		} else {
			panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
		}
	}

	return icebergSchemaColumn
}
