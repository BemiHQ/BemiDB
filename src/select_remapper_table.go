package main

import (
	"context"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	PG_SCHEMA_PUBLIC = "public"

	PG_TABLE_PG_INHERITS           = "pg_inherits"
	PG_TABLE_PG_SHDESCRIPTION      = "pg_shdescription"
	PG_TABLE_PG_STATIO_USER_TABLES = "pg_statio_user_tables"
	PG_TABLE_PG_SHADOW             = "pg_shadow"
	PG_TABLE_PG_NAMESPACE          = "pg_namespace"
	PG_TABLE_PG_ROLES              = "pg_roles"
	PG_TABLE_PG_CLASS              = "pg_class"
	PG_TABLE_PG_EXTENSION          = "pg_extension"
	PG_TABLE_PG_REPLICATION_SLOTS  = "pg_replication_slots"
	PG_TABLE_PG_DATABASE           = "pg_database"
	PG_TABLE_PG_STAT_GSSAPI        = "pg_stat_gssapi"
	PG_TABLE_PG_AUTH_MEMBERS       = "pg_auth_members"
	PG_TABLE_PG_USER               = "pg_user"
	PG_TABLE_PG_STAT_ACTIVITY      = "pg_stat_activity"

	PG_TABLE_TABLES = "tables"
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

	// pg_catalog.pg_* system tables
	if parser.IsTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {
		case PG_TABLE_PG_SHADOW:
			// pg_catalog.pg_shadow -> return hard-coded credentials
			tableNode := parser.MakePgShadowNode(remapper.config.User, remapper.config.EncryptedPassword, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_ROLES:
			// pg_catalog.pg_roles -> return hard-coded role info
			tableNode := parser.MakePgRolesNode(remapper.config.User, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_CLASS:
			// pg_catalog.pg_class -> reload Iceberg tables
			remapper.reloadIceberSchemaTables()
			return node
		case PG_TABLE_PG_INHERITS:
			// pg_catalog.pg_inherits -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_INHERITS, PG_INHERITS_TABLE, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_SHDESCRIPTION:
			// pg_catalog.pg_shdescription -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_SHDESCRIPTION, PG_SHDESCRIPTION_TABLE, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_STATIO_USER_TABLES:
			// pg_catalog.pg_statio_user_tables -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_STATIO_USER_TABLES, PG_STATIO_USER_TABLES_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_EXTENSION:
			// pg_catalog.pg_extension -> return hard-coded extension info
			tableNode := parser.MakePgExtensionNode(qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_REPLICATION_SLOTS:
			// pg_replication_slots -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_REPLICATION_SLOTS, PG_REPLICATION_SLOTS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_DATABASE:
			// pg_catalog.pg_database -> return hard-coded database info
			tableNode := parser.MakePgDatabaseNode(remapper.config.Database, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_STAT_GSSAPI:
			// pg_catalog.pg_stat_gssapi -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_STAT_GSSAPI, PG_STAT_GSSAPI_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_AUTH_MEMBERS:
			// pg_catalog.pg_auth_members -> return empty table
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_AUTH_MEMBERS, PG_AUTH_MEMBERS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_USER:
			// pg_catalog.pg_user -> return hard-coded user info
			tableNode := parser.MakePgUserNode(remapper.config.User, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_STAT_ACTIVITY:
			// pg_stat_activity -> return empty table
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_STAT_ACTIVITY, PG_STAT_ACTIVITY_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		default:
			// pg_catalog.pg_* other system tables -> return as is
			return node
		}
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {
		case PG_TABLE_TABLES:
			// information_schema.tables -> reload Iceberg tables
			remapper.reloadIceberSchemaTables()
			return node
		default:
			// information_schema.* other system tables -> return as is
			return node
		}
	}

	// iceberg.table -> FROM iceberg_scan('iceberg/schema/table/metadata/v1.metadata.json', skip_schema_inference = true)
	if qSchemaTable.Schema == "" {
		qSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}
	schemaTable := qSchemaTable.ToIcebergSchemaTable()
	if (!remapper.icebergSchemaTableExists(schemaTable)) {
		remapper.reloadIceberSchemaTables()
		if (!remapper.icebergSchemaTableExists(schemaTable)) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}
	icebergPath := remapper.icebergReader.MetadataFilePath(schemaTable)
	tableNode := parser.MakeIcebergTableNode(icebergPath, qSchemaTable)
	return remapper.overrideTable(node, tableNode)
}

// FROM [PG_FUNCTION()]
func (remapper *SelectRemapperTable) RemapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable

	// pg_catalog.pg_get_keywords() -> hard-coded keywords
	if parser.IsPgGetKeywordsFunction(node) {
		return parser.MakePgGetKeywordsNode(node)
	}

	// pg_show_all_settings() -> duckdb_settings()
	if parser.IsPgShowAllSettingsFunction(node) {
		return parser.MakePgShowAllSettingsNode(node)
	}

	// pg_is_in_recovery() -> 'f'::bool
	if parser.IsPgIsInRecoveryFunction(node) {
		return parser.MakePgIsInRecoveryNode(node)
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

