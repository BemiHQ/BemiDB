package postgres

import (
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
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

func (pgSchemaColumn *PgSchemaColumn) ToIcebergSchemaColumn() *common.IcebergSchemaColumn {
	pgPrimitiveColumnType := strings.TrimLeft(pgSchemaColumn.UdtName, "_")

	icebergSchemaColumn := &common.IcebergSchemaColumn{
		Config:              pgSchemaColumn.Config.CommonConfig,
		ColumnName:          pgSchemaColumn.ColumnName,
		Position:            common.StringToInt(pgSchemaColumn.OrdinalPosition),
		NumericPrecision:    common.StringToInt(pgSchemaColumn.NumericPrecision),
		NumericScale:        common.StringToInt(pgSchemaColumn.NumericScale),
		IsList:              pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY,
		IsRequired:          pgSchemaColumn.IsNullable != PG_TRUE,
		IsPartOfUniqueIndex: pgSchemaColumn.IsPartOfUniqueIndex,
		DatetimePrecision:   common.StringToInt(pgSchemaColumn.DatetimePrecision),
	}

	switch pgPrimitiveColumnType {
	case "bool":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeBoolean
	case "bit", "int2", "int4":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeInteger
	case "xid":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeLong
	case "int8", "xid8":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeDecimal
	case "interval":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeDecimal
		icebergSchemaColumn.LogicalColumnType = common.IcebergLogicalColumnTypeInterval
	case "float4":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeFloat
	case "float8":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeDouble
	case "numeric":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeDecimal
	case "date":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeDate
	case "time":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeTime
	case "timetz":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeTimeTz
	case "timestamp", "timestamptz":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeTimestamp
	case "jsonb", "json":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeString
		icebergSchemaColumn.LogicalColumnType = common.IcebergLogicalColumnTypeJson
	case "bpchar":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeString
		icebergSchemaColumn.LogicalColumnType = common.IcebergLogicalColumnTypeBpchar
	case "point":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeString
		icebergSchemaColumn.LogicalColumnType = common.IcebergLogicalColumnTypePoint
	case "varchar", "char", "text", "uuid",
		"line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"ltree", "tsvector", "xml", "pg_snapshot":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeString
	case "bytea":
		icebergSchemaColumn.ColumnType = common.IcebergColumnTypeBinary
	default:
		// User-defined types -> VARCHAR
		if pgSchemaColumn.Namespace != PG_SCHEMA_PG_CATALOG {
			icebergSchemaColumn.ColumnType = common.IcebergColumnTypeString
			icebergSchemaColumn.LogicalColumnType = common.IcebergLogicalColumnTypeUserDefined
		} else {
			panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
		}
	}

	return icebergSchemaColumn
}
