package main

import (
	"fmt"
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
// CustomSchema.TableName -> CustomSchemaTableName
func (pgSchemaTable PgSchemaTable) IcebergTableName() string {
	if pgSchemaTable.Schema != PG_SCHEMA_PUBLIC {
		return pgSchemaTable.Schema + "_" + pgSchemaTable.Table
	}
	return pgSchemaTable.Table
}

func (pgSchemaTable PgSchemaTable) IcebergParentPartitionedTableName() string {
	if pgSchemaTable.Schema != PG_SCHEMA_PUBLIC {
		return pgSchemaTable.Schema + "_" + pgSchemaTable.ParentPartitionedTable
	}
	return pgSchemaTable.ParentPartitionedTable
}
