package main

import (
	"context"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	PG_SCHEMA_PUBLIC = "public"
)

type SelectRemapperTable struct {
	parserTable         *QueryParserTable
	icebergSchemaTables []IcebergSchemaTable
	icebergReader       *IcebergReader
	duckdb              *Duckdb
	config              *Config
}

func NewSelectRemapperTable(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *SelectRemapperTable {
	remapper := &SelectRemapperTable{
		parserTable:   NewQueryParserTable(config),
		icebergReader: icebergReader,
		duckdb:        duckdb,
		config:        config,
	}
	remapper.reloadIceberSchemaTables()
	return remapper
}

// FROM / JOIN [TABLE]
func (remapper *SelectRemapperTable) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable
	qSchemaTable := parser.NodeToQuerySchemaTable(node)

	// pg_catalog.pg_statio_user_tables -> return nothing
	if parser.IsPgStatioUserTablesTable(qSchemaTable) {
		tableNode := parser.MakePgStatioUserTablesNode(qSchemaTable.Alias)
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_shadow -> return hard-coded credentials
	if parser.IsPgShadowTable(qSchemaTable) {
		tableNode := parser.MakePgShadowNode(remapper.config.User, remapper.config.EncryptedPassword, qSchemaTable.Alias)
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_roles -> return hard-coded role info
	if parser.IsPgRolesTable(qSchemaTable) {
		tableNode := parser.MakePgRolesNode(remapper.config.User, qSchemaTable.Alias)
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_shdescription -> return nothing
	if parser.IsPgShdescriptionTable(qSchemaTable) {
		tableNode := parser.MakePgShdescriptionNode(qSchemaTable.Alias)
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_class -> reload Iceberg tables
	if parser.IsPgClassTable(qSchemaTable) {
		remapper.reloadIceberSchemaTables()
		return node
	}

	// pg_catalog.pg_* other system tables -> return as is
	if parser.IsTableFromPgCatalog(qSchemaTable) {
		return node
	}

	// information_schema.tables -> reload Iceberg tables
	if parser.IsInformationSchemaTablesTable(qSchemaTable) {
		remapper.reloadIceberSchemaTables()
		return node
	}

	// information_schema.* other system tables -> return as is
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		return node
	}

	// iceberg.table -> FROM iceberg_scan('iceberg/schema/table/metadata/v1.metadata.json', skip_schema_inference = true)
	if qSchemaTable.Schema == "" {
		qSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}
	schemaTable := qSchemaTable.ToIcebergSchemaTable()
	if !remapper.icebergSchemaTableExists(schemaTable) {
		remapper.reloadIceberSchemaTables()
		if !remapper.icebergSchemaTableExists(schemaTable) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}
	icebergPath := remapper.icebergReader.MetadataFilePath(schemaTable)
	tableNode := parser.MakeIcebergTableNode(icebergPath)
	return remapper.overrideTable(node, tableNode)
}

// FROM PG_FUNCTION()
func (remapper *SelectRemapperTable) RemapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	// pg_catalog.pg_get_keywords() -> hard-coded keywords
	if remapper.parserTable.IsPgGetKeywordsFunction(node) {
		return remapper.parserTable.MakePgGetKeywordsNode(node)
	}

	// pg_show_all_settings() -> duckdb_settings()
	if remapper.parserTable.IsPgShowAllSettingsFunction(node) {
		return remapper.parserTable.MakePgShowAllSettingsNode(node)
	}

	// set_config() -> SET statement
	if remapper.parserTable.IsSetConfigFunction(node) {
		return remapper.parserTable.MakeSetConfigNode(node)
	}

	return node
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (remapper *SelectRemapperTable) RemapNestedTableFunction(funcCallNode *pgQuery.FuncCall) *pgQuery.FuncCall {
	// array_upper(values, 1) -> len(values)
	if remapper.parserTable.IsArrayUpperFunction(funcCallNode) {
		return remapper.parserTable.MakeArrayUpperNode(funcCallNode)
	}

	return funcCallNode
}

func (remapper *SelectRemapperTable) overrideTable(node *pgQuery.Node, fromClause *pgQuery.Node) *pgQuery.Node {
	node = fromClause
	return node
}

func (remapper *SelectRemapperTable) reloadIceberSchemaTables() {
	icebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	PanicIfError(err)

	ctx := context.Background()
	for _, icebergSchemaTable := range icebergSchemaTables {
		remapper.duckdb.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+icebergSchemaTable.String()+" (id INT)", nil)
	}

	remapper.icebergSchemaTables = icebergSchemaTables
}

func (remapper *SelectRemapperTable) icebergSchemaTableExists(schemaTable IcebergSchemaTable) bool {
	for _, icebergSchemaTable := range remapper.icebergSchemaTables {
		if icebergSchemaTable == schemaTable {
			return true
		}
	}
	return false
}
