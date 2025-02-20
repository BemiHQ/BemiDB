package main

import (
	"context"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var MAX_REDUNDANT_PG_NAMESPACE_OID = 1265

type QueryRemapperTable struct {
	parserTable         *ParserTable
	parserWhere         *ParserWhere
	parserFunction      *ParserFunction
	icebergSchemaTables Set[IcebergSchemaTable]
	icebergReader       *IcebergReader
	duckdb              *Duckdb
	config              *Config
}

func NewQueryRemapperTable(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *QueryRemapperTable {
	remapper := &QueryRemapperTable{
		parserTable:    NewParserTable(config),
		parserWhere:    NewParserWhere(config),
		parserFunction: NewParserFunction(config),
		icebergReader:  icebergReader,
		duckdb:         duckdb,
		config:         config,
	}
	remapper.reloadIceberSchemaTables()
	return remapper
}

func (remapper *QueryRemapperTable) NodeToQuerySchemaTable(node *pgQuery.Node) QuerySchemaTable {
	return remapper.parserTable.NodeToQuerySchemaTable(node)
}

// FROM / JOIN [TABLE]
func (remapper *QueryRemapperTable) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable
	qSchemaTable := parser.NodeToQuerySchemaTable(node)

	// pg_catalog.pg_* system tables
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// pg_catalog.pg_shadow -> return hard-coded credentials
		case PG_TABLE_PG_SHADOW:
			return parser.MakePgShadowNode(remapper.config.User, remapper.config.EncryptedPassword, qSchemaTable.Alias)

		// pg_catalog.pg_roles -> return hard-coded role info
		case PG_TABLE_PG_ROLES:
			return parser.MakePgRolesNode(remapper.config.User, qSchemaTable.Alias)

		// pg_catalog.pg_extension -> return hard-coded extension info
		case PG_TABLE_PG_EXTENSION:
			return parser.MakePgExtensionNode(qSchemaTable.Alias)

		// pg_catalog.pg_database -> return hard-coded database info
		case PG_TABLE_PG_DATABASE:
			return parser.MakePgDatabaseNode(remapper.config.Database, qSchemaTable.Alias)

		// pg_catalog.pg_user -> return hard-coded user info
		case PG_TABLE_PG_USER:
			return parser.MakePgUserNode(remapper.config.User, qSchemaTable.Alias)

		// pg_stat_user_tables -> return hard-coded table info
		case PG_TABLE_PG_STAT_USER_TABLES:
			remapper.reloadIceberSchemaTables()
			return parser.MakePgStatUserTablesNode(remapper.icebergSchemaTables, qSchemaTable.Alias)

		// pg_collation -> return hard-coded collation (encoding) info
		case PG_TABLE_PG_COLLATION:
			return parser.MakePgCollationNode(qSchemaTable.Alias)

		// pg_index -> returns (SELECT *, FALSE AS indnullsnotdistinct FROM pg_index)
		// DuckDB does not support indnullsnotdistinct column
		case PG_TABLE_PG_INDEX:
			return parser.MakePgIndexNode(qSchemaTable)

		// pg_catalog.pg_inherits -> return empty table
		case PG_TABLE_PG_INHERITS:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_INHERITS, PG_INHERITS_DEFINITION, qSchemaTable.Alias)

		// pg_catalog.pg_shdescription -> return empty table
		case PG_TABLE_PG_SHDESCRIPTION:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_SHDESCRIPTION, PG_SHDESCRIPTION_DEFINITION, qSchemaTable.Alias)

		// pg_catalog.pg_statio_user_tables -> return empty table
		case PG_TABLE_PG_STATIO_USER_TABLES:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_STATIO_USER_TABLES, PG_STATIO_USER_TABLES_DEFINITION, qSchemaTable.Alias)

		// pg_replication_slots -> return empty table
		case PG_TABLE_PG_REPLICATION_SLOTS:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_REPLICATION_SLOTS, PG_REPLICATION_SLOTS_DEFINITION, qSchemaTable.Alias)

		// pg_catalog.pg_stat_gssapi -> return empty table
		case PG_TABLE_PG_STAT_GSSAPI:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_STAT_GSSAPI, PG_STAT_GSSAPI_DEFINITION, qSchemaTable.Alias)

		// pg_catalog.pg_auth_members -> return empty table
		case PG_TABLE_PG_AUTH_MEMBERS:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_AUTH_MEMBERS, PG_AUTH_MEMBERS_DEFINITION, qSchemaTable.Alias)

		// pg_stat_activity -> return empty table
		case PG_TABLE_PG_STAT_ACTIVITY:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_STAT_ACTIVITY, PG_STAT_ACTIVITY_DEFINITION, qSchemaTable.Alias)

		// pg_views -> return empty table
		case PG_TABLE_PG_VIEWS:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_VIEWS, PG_VIEWS_DEFINITION, qSchemaTable.Alias)

		// pg_matviews -> return empty table
		case PG_TABLE_PG_MATVIEWS:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_MATVIEWS, PG_MATVIEWS_DEFINITION, qSchemaTable.Alias)

		// pg_opclass -> return empty table
		case PG_TABLE_PG_OPCLASS:
			return parser.MakeEmptyTableNode(PG_TABLE_PG_OPCLASS, PG_OPCLASS_DEFINITION, qSchemaTable.Alias)

		// pg_catalog.pg_* other system tables -> return as is
		default:
			// pg_catalog.pg_class -> reload Iceberg tables
			switch qSchemaTable.Table {
			case PG_TABLE_PG_CLASS:
				remapper.reloadIceberSchemaTables()
			}

			return node
		}
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {

		// information_schema.tables -> reload Iceberg tables
		case PG_TABLE_TABLES:
			remapper.reloadIceberSchemaTables()
			return node

		// information_schema.* other system tables -> return as is
		default:
			return node
		}
	}

	// public.table -> FROM iceberg_scan('path', skip_schema_inference = true) table
	// schema.table -> FROM iceberg_scan('path', skip_schema_inference = true) schema_table
	schemaTable := qSchemaTable.ToIcebergSchemaTable()
	if !remapper.icebergSchemaTables.Contains(schemaTable) { // Reload Iceberg tables if not found
		remapper.reloadIceberSchemaTables()
		if !remapper.icebergSchemaTables.Contains(schemaTable) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}
	icebergPath := remapper.icebergReader.MetadataFilePath(schemaTable) // iceberg/schema/table/metadata/v1.metadata.json
	return parser.MakeIcebergTableNode(icebergPath, qSchemaTable)
}

// FROM [PG_FUNCTION()]
func (remapper *QueryRemapperTable) RemapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable

	schemaFunction := parser.SchemaFunction(node)

	if remapper.isFunctionFromPgCatalog(schemaFunction) {
		switch {

		// pg_catalog.pg_get_keywords() -> hard-coded keywords
		case schemaFunction.Function == PG_FUNCTION_PG_GET_KEYWORDS:
			return parser.MakePgGetKeywordsNode(node)

		// pg_catalog.pg_show_all_settings() -> duckdb_settings()
		case schemaFunction.Function == PG_FUNCTION_PG_SHOW_ALL_SETTINGS:
			return parser.MakePgShowAllSettingsNode(node)

		// pg_catalog.pg_is_in_recovery() -> 'f'::bool
		case schemaFunction.Function == PG_FUNCTION_PG_IS_IN_RECOVERY:
			return parser.MakePgIsInRecoveryNode(node)
		}
	}

	return node
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (remapper *QueryRemapperTable) RemapNestedTableFunction(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)

	switch {

	// array_upper(values, 1) -> len(values)
	case schemaFunction.Function == PG_FUNCTION_ARRAY_UPPER:
		return remapper.parserTable.MakeArrayUpperNode(functionCall)

	default:
		return functionCall
	}
}

func (remapper *QueryRemapperTable) RemapWhereClauseForTable(qSchemaTable QuerySchemaTable, selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// FROM pg_catalog.pg_namespace -> FROM pg_catalog.pg_namespace WHERE oid > 1265
		case PG_TABLE_PG_NAMESPACE:
			alias := qSchemaTable.Alias
			if alias == "" {
				alias = PG_TABLE_PG_NAMESPACE
			}
			withoutDuckdbOidsWhereCondition := remapper.parserWhere.MakeIntEqualityExpressionNode("oid", ">", MAX_REDUNDANT_PG_NAMESPACE_OID, alias)
			remapper.parserWhere.AppendWhereCondition(selectStatement, withoutDuckdbOidsWhereCondition)

		// FROM pg_catalog.pg_statio_user_tables -> FROM pg_catalog.pg_statio_user_tables WHERE false
		case PG_TABLE_PG_STATIO_USER_TABLES:
			falseWhereCondition := remapper.parserWhere.MakeFalseConditionNode()
			return remapper.parserWhere.OverrideWhereCondition(selectStatement, falseWhereCondition)
		}
	}
	return selectStatement
}

func (remapper *QueryRemapperTable) reloadIceberSchemaTables() {
	newIcebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	PanicIfError(err)

	ctx := context.Background()
	for _, icebergSchemaTable := range newIcebergSchemaTables.Values() {
		if !remapper.icebergSchemaTables.Contains(icebergSchemaTable) {
			icebergTableFields, err := remapper.icebergReader.TableFields(icebergSchemaTable)
			PanicIfError(err)

			var sqlColumns []string
			for _, icebergTableField := range icebergTableFields {
				sqlColumns = append(sqlColumns, icebergTableField.ToSql())
			}

			_, err = remapper.duckdb.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+icebergSchemaTable.Schema, nil)
			PanicIfError(err)
			_, err = remapper.duckdb.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+icebergSchemaTable.String()+" ("+strings.Join(sqlColumns, ", ")+")", nil)
			PanicIfError(err)
		}
	}
	for _, icebergSchemaTable := range remapper.icebergSchemaTables.Values() {
		if !newIcebergSchemaTables.Contains(icebergSchemaTable) {
			_, err = remapper.duckdb.ExecContext(ctx, "DROP TABLE IF EXISTS "+icebergSchemaTable.String(), nil)
			PanicIfError(err)
		}
	}

	remapper.icebergSchemaTables = newIcebergSchemaTables
}

// System pg_* tables
func (remapper *QueryRemapperTable) isTableFromPgCatalog(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_PG_CATALOG ||
		(qSchemaTable.Schema == "" &&
			(PG_SYSTEM_TABLES.Contains(qSchemaTable.Table) || PG_SYSTEM_VIEWS.Contains(qSchemaTable.Table)) &&
			!remapper.icebergSchemaTables.Contains(qSchemaTable.ToIcebergSchemaTable()))
}

func (remapper *QueryRemapperTable) isFunctionFromPgCatalog(schemaFunction QuerySchemaFunction) bool {
	return schemaFunction.Schema == PG_SCHEMA_PG_CATALOG ||
		(schemaFunction.Schema == "" && PG_SYSTEM_FUNCTIONS.Contains(schemaFunction.Function))
}
