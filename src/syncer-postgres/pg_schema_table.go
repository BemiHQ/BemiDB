package main

import (
	"fmt"
	"strings"
)

type PgSchemaTable struct {
	Schema                 string
	Table                  string
	ParentPartitionedTable string
}

func (pgSchemaTable PgSchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, pgSchemaTable.Schema, pgSchemaTable.Table)
}

func (pgSchemaTable PgSchemaTable) ToConfigArg() string {
	return fmt.Sprintf(`%s.%s`, pgSchemaTable.Schema, pgSchemaTable.Table)
}

// public.table -> table
// CustomSchema.TableName -> custom_schema_table_name
func (pgSchemaTable PgSchemaTable) IcebergTableName() string {
	if pgSchemaTable.Schema != PG_SCHEMA_PUBLIC {
		return strings.ToLower(pgSchemaTable.Schema) + "_" + strings.ToLower(pgSchemaTable.Table)
	}
	return strings.ToLower(pgSchemaTable.Table)
}

func (pgSchemaTable PgSchemaTable) IcebergParentPartitionedTableName() string {
	if pgSchemaTable.Schema != PG_SCHEMA_PUBLIC {
		return pgSchemaTable.Schema + "_" + pgSchemaTable.ParentPartitionedTable
	}
	return pgSchemaTable.ParentPartitionedTable
}
