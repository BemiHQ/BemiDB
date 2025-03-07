package main

import (
	"context"
	"regexp"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var PG_CATALOG_TABLE_NAMES = Set[string]{}
var PG_INFORMATION_SCHEMA_TABLE_NAMES = Set[string]{}

func CreatePgCatalogTableQueries(config *Config) []string {
	result := []string{
		// Static empty tables
		"CREATE TABLE pg_inherits(inhrelid oid, inhparent oid, inhseqno int4, inhdetachpending bool)",
		"CREATE TABLE pg_shdescription(objoid oid, classoid oid, description text)",
		"CREATE TABLE pg_statio_user_tables(relid oid, schemaname text, relname text, heap_blks_read int8, heap_blks_hit int8, idx_blks_read int8, idx_blks_hit int8, toast_blks_read int8, toast_blks_hit int8, tidx_blks_read int8, tidx_blks_hit int8)",
		"CREATE TABLE pg_replication_slots(slot_name text, plugin text, slot_type text, datoid oid, database text, temporary bool, active bool, active_pid int4, xmin int8, catalog_xmin int8, restart_lsn text, confirmed_flush_lsn text, wal_status text, safe_wal_size int8, two_phase bool, conflicting bool)",
		"CREATE TABLE pg_stat_gssapi(pid int4, gss_authenticated bool, principal text, encrypted bool, credentials_delegated bool)",
		"CREATE TABLE pg_auth_members(oid text, roleid oid, member oid, grantor oid, admin_option bool, inherit_option bool, set_option bool)",
		"CREATE TABLE pg_stat_activity(datid oid, datname text, pid int4, usesysid oid, usename text, application_name text, client_addr inet, client_hostname text, client_port int4, backend_start timestamp, xact_start timestamp, query_start timestamp, state_change timestamp, wait_event_type text, wait_event text, state text, backend_xid int8, backend_xmin int8, query text, backend_type text)",
		"CREATE TABLE pg_views(schemaname text, viewname text, viewowner text, definition text)",
		"CREATE TABLE pg_matviews(schemaname text, matviewname text, matviewowner text, tablespace text, hasindexes bool, ispopulated bool, definition text)",
		"CREATE TABLE pg_opclass(oid oid, opcmethod oid, opcname text, opcnamespace oid, opcowner oid, opcfamily oid, opcintype oid, opcdefault bool, opckeytype oid)",
		"CREATE TABLE pg_policy(oid oid, polname text, polrelid oid, polcmd text, polpermissive bool, polroles oid, polqual text, polwithcheck text)",
		"CREATE TABLE pg_statistic_ext(oid oid, stxrelid oid, stxname text, stxnamespace oid, stxowner oid, stxstattarget int4, stxkeys oid, stxkind text, stxexprs text)",
		"CREATE TABLE pg_publication(oid oid, pubname text, pubowner oid, puballtables bool, pubinsert bool, pubupdate bool, pubdelete bool, pubtruncate bool, pubviaroot bool)",
		"CREATE TABLE pg_publication_rel(oid oid, prpubid oid, prrelid oid, prqual text, prattrs text)",

		// Dynamic tables
		// DuckDB doesn't handle dynamic view replacement properly
		"CREATE TABLE pg_stat_user_tables(relid oid, schemaname text, relname text, seq_scan int8, last_seq_scan timestamp, seq_tup_read int8, idx_scan int8, last_idx_scan timestamp, idx_tup_fetch int8, n_tup_ins int8, n_tup_upd int8, n_tup_del int8, n_tup_hot_upd int8, n_tup_newpage_upd int8, n_live_tup int8, n_dead_tup int8, n_mod_since_analyze int8, n_ins_since_vacuum int8, last_vacuum timestamp, last_autovacuum timestamp, last_analyze timestamp, last_autoanalyze timestamp, vacuum_count int8, autovacuum_count int8, analyze_count int8, autoanalyze_count int8)",

		// Static views
		"CREATE VIEW pg_shadow AS SELECT '" + config.User + "' AS usename, '10'::oid AS usesysid, FALSE AS usecreatedb, FALSE AS usesuper, TRUE AS userepl, FALSE AS usebypassrls, '" + config.EncryptedPassword + "' AS passwd, NULL::timestamp AS valuntil, NULL::text[] AS useconfig",
		"CREATE VIEW pg_roles AS SELECT '10'::oid AS oid, '" + config.User + "' AS rolname, TRUE AS rolsuper, TRUE AS rolinherit, TRUE AS rolcreaterole, TRUE AS rolcreatedb, TRUE AS rolcanlogin, FALSE AS rolreplication, -1 AS rolconnlimit, NULL::text AS rolpassword, NULL::timestamp AS rolvaliduntil, FALSE AS rolbypassrls, NULL::text[] AS rolconfig",
		"CREATE VIEW pg_extension AS SELECT '13823'::oid AS oid, 'plpgsql' AS extname, '10'::oid AS extowner, '11'::oid AS extnamespace, FALSE AS extrelocatable, '1.0'::text AS extversion, NULL::text[] AS extconfig, NULL::text[] AS extcondition",
		"CREATE VIEW pg_database AS SELECT '16388'::oid AS oid, '" + config.Database + "' AS datname, '10'::oid AS datdba, '6'::int4 AS encoding, 'c' AS datlocprovider, FALSE AS datistemplate, TRUE AS datallowconn, '-1'::int4 AS datconnlimit, '722'::int8 AS datfrozenxid, '1'::int4 AS datminmxid, '1663'::oid AS dattablespace, 'en_US.UTF-8' AS datcollate, 'en_US.UTF-8' AS datctype, 'en_US.UTF-8' AS datlocale, NULL::text AS daticurules, NULL::text AS datcollversion, NULL::text[] AS datacl",
		"CREATE VIEW pg_user AS SELECT '" + config.User + "' AS usename, '10'::oid AS usesysid, TRUE AS usecreatedb, TRUE AS usesuper, TRUE AS userepl, TRUE AS usebypassrls, '' AS passwd, NULL::timestamp AS valuntil, NULL::text[] AS useconfig",
		"CREATE VIEW pg_collation AS SELECT '100'::oid AS oid, 'default' AS collname, '11'::oid AS collnamespace, '10'::oid AS collowner, 'd' AS collprovider, TRUE AS collisdeterministic, '-1'::int4 AS collencoding, NULL::text AS collcollate, NULL::text AS collctype, NULL::text AS colliculocale, NULL::text AS collicurules, NULL::text AS collversion",

		// Dynamic views
		// DuckDB does not support indnullsnotdistinct column
		"CREATE VIEW pg_index AS SELECT *, FALSE AS indnullsnotdistinct FROM pg_catalog.pg_index",
		// Hide DuckDB's system and duplicate schemas
		"CREATE VIEW pg_namespace AS SELECT * FROM pg_catalog.pg_namespace WHERE oid > 1265",
		// DuckDB does not support relforcerowsecurity column
		"CREATE VIEW pg_class AS SELECT *, FALSE AS relforcerowsecurity FROM pg_catalog.pg_class",
	}
	PG_CATALOG_TABLE_NAMES = extractTableNames(result)
	return result
}

func CreateInformationSchemaTableQueries(config *Config) []string {
	result := []string{
		// Dynamic views
		// DuckDB does not support udt_catalog, udt_schema, udt_name
		`CREATE VIEW columns AS
		SELECT
			table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, interval_type, interval_precision, character_set_catalog, character_set_schema, character_set_name, collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name,
			'` + config.Database + `' AS udt_catalog,
			'pg_catalog' AS udt_schema,
			CASE data_type
				WHEN 'BIGINT' THEN 'int8'
				WHEN 'BIGINT[]' THEN '_int8'
				WHEN 'BLOB' THEN 'bytea'
				WHEN 'BLOB[]' THEN '_bytea'
				WHEN 'BOOLEAN' THEN 'bool'
				WHEN 'BOOLEAN[]' THEN '_bool'
				WHEN 'DATE' THEN 'date'
				WHEN 'DATE[]' THEN '_date'
				WHEN 'FLOAT' THEN 'float8'
				WHEN 'FLOAT[]' THEN '_float8'
				WHEN 'INTEGER' THEN 'int4'
				WHEN 'INTEGER[]' THEN '_int4'
				WHEN 'VARCHAR' THEN 'text'
				WHEN 'VARCHAR[]' THEN '_text'
				WHEN 'TIME' THEN 'time'
				WHEN 'TIME[]' THEN '_time'
				WHEN 'TIMESTAMP' THEN 'timestamp'
				WHEN 'TIMESTAMP[]' THEN '_timestamp'
				WHEN 'UUID' THEN 'uuid'
				WHEN 'UUID[]' THEN '_uuid'
				ELSE
					CASE
					WHEN starts_with(data_type, 'DECIMAL') THEN 'numeric'
					END
			END AS udt_name,
			scope_catalog, scope_schema, scope_name, maximum_cardinality, dtd_identifier, is_self_referencing, is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, identity_cycle, is_generated, generation_expression, is_updatable
		FROM information_schema.columns`,
	}
	PG_INFORMATION_SCHEMA_TABLE_NAMES = extractTableNames(result)
	return result
}

type QueryRemapperTable struct {
	parserTable         *ParserTable
	parserFunction      *ParserFunction
	remapperFunction    *QueryRemapperFunction
	icebergSchemaTables Set[IcebergSchemaTable]
	icebergReader       *IcebergReader
	duckdb              *Duckdb
	config              *Config
}

func NewQueryRemapperTable(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *QueryRemapperTable {
	remapper := &QueryRemapperTable{
		parserTable:      NewParserTable(config),
		parserFunction:   NewParserFunction(config),
		remapperFunction: NewQueryRemapperFunction(config),
		icebergReader:    icebergReader,
		duckdb:           duckdb,
		config:           config,
	}
	remapper.reloadIceberSchemaTables()
	duckdb.ExecInitFile()
	return remapper
}

// FROM / JOIN [TABLE]
func (remapper *QueryRemapperTable) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable
	qSchemaTable := parser.NodeToQuerySchemaTable(node)

	// pg_catalog.pg_* system tables
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// pg_class -> reload Iceberg tables
		case PG_TABLE_PG_CLASS:
			remapper.reloadIceberSchemaTables()

		// pg_stat_user_tables -> return Iceberg tables
		case PG_TABLE_PG_STAT_USER_TABLES:
			remapper.reloadIceberSchemaTables()
			remapper.upsertPgStatUserTables(remapper.icebergSchemaTables)
		}

		// pg_catalog.pg_table -> main.pg_table
		if PG_CATALOG_TABLE_NAMES.Contains(qSchemaTable.Table) {
			parser.RemapSchemaToMain(node)
			return node
		}

		// pg_catalog.pg_* other system tables -> return as is
		return node
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {
		// information_schema.tables -> reload Iceberg tables
		case PG_TABLE_TABLES:
			remapper.reloadIceberSchemaTables()
		}

		// information_schema.table -> main.table
		if PG_INFORMATION_SCHEMA_TABLE_NAMES.Contains(qSchemaTable.Table) {
			parser.RemapSchemaToMain(node)
			return node
		}

		// information_schema.* other system tables -> return as is
		return node
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

// FROM FUNCTION()
func (remapper *QueryRemapperTable) RemapTableFunctionCall(rangeFunction *pgQuery.RangeFunction) {
	schemaFunction := remapper.parserTable.TopLevelSchemaFunction(rangeFunction)
	remapper.parserTable.SetAliasIfNotExists(rangeFunction, schemaFunction.Function)

	for _, functionCall := range remapper.parserTable.TableFunctionCalls(rangeFunction) {
		remapper.remapperFunction.RemapFunctionCall(functionCall)
		remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion
	}
}

func (remapper *QueryRemapperTable) reloadIceberSchemaTables() {
	newIcebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	PanicIfError(err, remapper.config)

	ctx := context.Background()
	for _, icebergSchemaTable := range newIcebergSchemaTables.Values() {
		if !remapper.icebergSchemaTables.Contains(icebergSchemaTable) {
			icebergTableFields, err := remapper.icebergReader.TableFields(icebergSchemaTable)
			PanicIfError(err, remapper.config)

			var sqlColumns []string
			for _, icebergTableField := range icebergTableFields {
				sqlColumns = append(sqlColumns, icebergTableField.ToSql())
			}

			_, err = remapper.duckdb.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+icebergSchemaTable.Schema, nil)
			PanicIfError(err, remapper.config)
			_, err = remapper.duckdb.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+icebergSchemaTable.String()+" ("+strings.Join(sqlColumns, ", ")+")", nil)
			PanicIfError(err, remapper.config)
		}
	}
	for _, icebergSchemaTable := range remapper.icebergSchemaTables.Values() {
		if !newIcebergSchemaTables.Contains(icebergSchemaTable) {
			_, err = remapper.duckdb.ExecContext(ctx, "DROP TABLE IF EXISTS "+icebergSchemaTable.String(), nil)
			PanicIfError(err, remapper.config)
		}
	}

	remapper.icebergSchemaTables = newIcebergSchemaTables
}

func (remapper *QueryRemapperTable) upsertPgStatUserTables(icebergSchemaTables Set[IcebergSchemaTable]) {
	values := make([]string, len(icebergSchemaTables))
	for i, icebergSchemaTable := range icebergSchemaTables.Values() {
		values[i] = "('123456', '" + icebergSchemaTable.Schema + "', '" + icebergSchemaTable.Table + "', 0, NULL, 0, 0, NULL, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, NULL, NULL, NULL, NULL, 0, 0, 0, 0)"
	}

	err := remapper.duckdb.ExecTransactionContext(context.Background(), []string{
		"DELETE FROM pg_stat_user_tables",
		"INSERT INTO pg_stat_user_tables VALUES " + strings.Join(values, ", "),
	})
	PanicIfError(err, remapper.config)
}

// System pg_* tables
func (remapper *QueryRemapperTable) isTableFromPgCatalog(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_PG_CATALOG ||
		(qSchemaTable.Schema == "" &&
			(PG_SYSTEM_TABLES.Contains(qSchemaTable.Table) || PG_SYSTEM_VIEWS.Contains(qSchemaTable.Table)) &&
			!remapper.icebergSchemaTables.Contains(qSchemaTable.ToIcebergSchemaTable()))
}

func extractTableNames(tables []string) Set[string] {
	names := make(Set[string])
	re := regexp.MustCompile(`CREATE (TABLE|VIEW) (\w+)`)

	for _, table := range tables {
		matches := re.FindStringSubmatch(table)
		if len(matches) > 1 {
			names.Add(matches[2])
		}
	}

	return names
}
