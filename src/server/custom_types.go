package main

import (
	"fmt"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	set := make(Set[T])
	return set
}

func (set Set[T]) Add(item T) Set[T] {
	set[item] = struct{}{}
	return set
}

func (set Set[T]) AddAll(items []T) Set[T] {
	for _, item := range items {
		set.Add(item)
	}
	return set
}

func (set Set[T]) Contains(item T) bool {
	_, ok := set[item]
	return ok
}

func (set Set[T]) Values() []T {
	values := make([]T, 0, len(set))
	for val := range set {
		values = append(values, val)
	}

	return values
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type IcebergTableField struct {
	Name     string
	Type     string
	Required bool
	IsList   bool
}

func (tableField IcebergTableField) ToSql() string {
	sql := fmt.Sprintf(`"%s" %s`, tableField.Name, tableField.Type)

	if tableField.IsList {
		sql += "[]"
	}

	if tableField.Required {
		sql += " NOT NULL"
	}

	return sql
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type QuerySchemaTable struct {
	Schema string
	Table  string
	Alias  string
}

func NewQuerySchemaTableFromString(schemaTable string) QuerySchemaTable {
	parts := strings.Split(schemaTable, ".")

	qSchemaTable := QuerySchemaTable{
		Table: parts[len(parts)-1],
	}
	if len(parts) > 1 {
		qSchemaTable.Schema = parts[0]
	}

	if !StringContainsUpper(qSchemaTable.Schema) {
		qSchemaTable.Schema = strings.ReplaceAll(qSchemaTable.Schema, `"`, "")
	}
	if !StringContainsUpper(qSchemaTable.Table) {
		qSchemaTable.Table = strings.ReplaceAll(qSchemaTable.Table, `"`, "")
	}

	return qSchemaTable
}

func (qSchemaTable QuerySchemaTable) ToIcebergSchemaTable() IcebergSchemaTable {
	if qSchemaTable.Schema == "" {
		return IcebergSchemaTable{
			Schema: PG_SCHEMA_PUBLIC,
			Table:  qSchemaTable.Table,
		}
	}

	return IcebergSchemaTable{
		Schema: qSchemaTable.Schema,
		Table:  qSchemaTable.Table,
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type QuerySchemaFunction struct {
	Schema   string
	Function string
}
