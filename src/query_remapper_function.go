package main

import (
	"regexp"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var PG_CATALOG_MACRO_FUNCTION_NAMES = Set[string]{}
var PG_INFORMATION_SCHEMA_MACRO_FUNCTION_NAMES = Set[string]{}

func CreatePgCatalogMacroQueries(config *Config) []string {
	result := []string{
		// Functions
		"CREATE MACRO aclexplode(aclitem_array) AS json(aclitem_array)",
		"CREATE MACRO current_setting(setting_name) AS '', (setting_name, missing_ok) AS ''",
		"CREATE MACRO pg_backend_pid() AS 0",
		"CREATE MACRO pg_encoding_to_char(encoding_int) AS 'UTF8'",
		"CREATE MACRO pg_get_expr(pg_node_tree, relation_oid) AS pg_catalog.pg_get_expr(pg_node_tree, relation_oid), (pg_node_tree, relation_oid, pretty_bool) AS pg_catalog.pg_get_expr(pg_node_tree, relation_oid)",
		"CREATE MACRO pg_get_function_identity_arguments(func_oid) AS ''",
		"CREATE MACRO pg_get_indexdef(index_oid) AS '', (index_oid, column_int) AS '', (index_oid, column_int, pretty_bool) AS ''",
		"CREATE MACRO pg_get_partkeydef(table_oid) AS ''",
		"CREATE MACRO pg_get_userbyid(role_id) AS 'bemidb'",
		"CREATE MACRO pg_get_viewdef(view_oid) AS pg_catalog.pg_get_viewdef(view_oid), (view_oid, pretty_bool) AS pg_catalog.pg_get_viewdef(view_oid)",
		"CREATE MACRO pg_indexes_size(regclass) AS 0",
		"CREATE MACRO pg_is_in_recovery() AS false",
		"CREATE MACRO pg_table_size(regclass) AS 0",
		"CREATE MACRO pg_tablespace_location(tablespace_oid) AS ''",
		"CREATE MACRO pg_total_relation_size(regclass) AS 0",
		"CREATE MACRO quote_ident(text) AS '\"' || text || '\"'",
		"CREATE MACRO row_to_json(record) AS to_json(record), (record, pretty_bool) AS to_json(record)",
		"CREATE MACRO set_config(setting_name, new_value, is_local) AS new_value",
		"CREATE MACRO version() AS 'PostgreSQL " + PG_VERSION + ", compiled by BemiDB'",
		"CREATE MACRO pg_get_statisticsobjdef_columns(oid) AS NULL",
		`CREATE MACRO jsonb_extract_path_text(from_json, path_elems) AS
			CASE typeof(path_elems) LIKE '%[]'
			WHEN true THEN json_extract_path_text(from_json, path_elems)[1]::varchar
			ELSE json_extract_path_text(from_json, path_elems)::varchar
		END`,
		`CREATE MACRO json_build_object(k1, v1) AS json_object(k1, v1),
			(k1, v1, k2, v2) AS json_object(k1, v1, k2, v2),
			(k1, v1, k2, v2, k3, v3) AS json_object(k1, v1, k2, v2, k3, v3),
			(k1, v1, k2, v2, k3, v3, k4, v4) AS json_object(k1, v1, k2, v2, k3, v3, k4, v4)`,
		`CREATE MACRO array_upper(arr, dimension) AS
			CASE dimension
			WHEN 1 THEN len(arr)
			ELSE NULL
		END`,

		// Table functions
		"CREATE MACRO pg_is_in_recovery() AS TABLE SELECT false AS pg_is_in_recovery",
		`CREATE MACRO pg_show_all_settings() AS TABLE SELECT
			name,
			value AS setting,
			NULL::text AS unit,
			'Settings' AS category,
			description AS short_desc,
			NULL::text AS extra_desc,
			'user' AS context,
			input_type AS vartype,
			'default' AS source,
			NULL::int4 AS min_val,
			NULL::int4 AS max_val,
			NULL::text[] AS enumvals,
			value AS boot_val,
			value AS reset_val,
			NULL::text AS sourcefile,
			NULL::int4 AS sourceline,
			FALSE AS pending_restart
		FROM duckdb_settings()`,
		`CREATE MACRO pg_get_keywords() AS TABLE SELECT
			keyword_name AS word,
			'U' AS catcode,
			TRUE AS barelabel,
			keyword_category AS catdesc,
			'can be bare label' AS baredesc
		FROM duckdb_keywords()`,
	}
	PG_CATALOG_MACRO_FUNCTION_NAMES = extractMacroNames(result)
	return result
}

func CreateInformationSchemaMacroQueries(config *Config) []string {
	result := []string{
		"CREATE MACRO _pg_expandarray(arr) AS STRUCT_PACK(x := unnest(arr), n := unnest(generate_series(1, array_length(arr))))",
	}
	PG_INFORMATION_SCHEMA_MACRO_FUNCTION_NAMES = extractMacroNames(result)
	return result
}

var BUILTIN_DUCKDB_PG_FUNCTION_NAMES = NewSet([]string{
	"array_to_string",
})

type QueryRemapperFunction struct {
	parserFunction *ParserFunction
	config         *Config
}

func NewQueryRemapperFunction(config *Config) *QueryRemapperFunction {
	return &QueryRemapperFunction{
		parserFunction: NewParserFunction(config),
		config:         config,
	}
}

func (remapper *QueryRemapperFunction) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	return remapper.parserFunction.SchemaFunction(functionCall)
}

// FUNCTION(...) -> ANOTHER_FUNCTION(...)
func (remapper *QueryRemapperFunction) RemapFunctionCall(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	schemaFunction := remapper.SchemaFunction(functionCall)

	// Pre-defined macro functions
	switch schemaFunction.Schema {

	// pg_catalog.func() -> main.func()
	case PG_SCHEMA_PG_CATALOG, "":
		if PG_CATALOG_MACRO_FUNCTION_NAMES.Contains(schemaFunction.Function) || BUILTIN_DUCKDB_PG_FUNCTION_NAMES.Contains(schemaFunction.Function) {
			remapper.parserFunction.RemapSchemaToMain(functionCall)
			return schemaFunction
		}

	// information_schema.func() -> main.func()
	case PG_SCHEMA_INFORMATION_SCHEMA:
		if PG_INFORMATION_SCHEMA_MACRO_FUNCTION_NAMES.Contains(schemaFunction.Function) {
			remapper.parserFunction.RemapSchemaToMain(functionCall)
			return schemaFunction
		}
	}

	switch {

	// format('%s %1$s', str) -> printf('%1$s %1$s', str)
	case schemaFunction.Function == PG_FUNCTION_FORMAT:
		remapper.parserFunction.RemapFormatToPrintf(functionCall)
		return schemaFunction

	// encode(sha256(...), 'hex') -> sha256(...)
	case schemaFunction.Function == PG_FUNCTION_ENCODE:
		remapper.parserFunction.RemoveEncode(functionCall)
		return schemaFunction

	}

	return nil
}

func (remapper *QueryRemapperFunction) RemapNestedFunctionCalls(functionCall *pgQuery.FuncCall) {
	nestedFunctionCalls := remapper.parserFunction.NestedFunctionCalls(functionCall)
	if len(nestedFunctionCalls) == 0 {
		return
	}

	for _, nestedFunctionCall := range nestedFunctionCalls {
		if nestedFunctionCall == nil {
			continue
		}

		schemaFunction := remapper.RemapFunctionCall(nestedFunctionCall)
		if schemaFunction != nil {
			continue
		}

		remapper.RemapNestedFunctionCalls(nestedFunctionCall) // self-recursion
	}
}

func extractMacroNames(macros []string) Set[string] {
	names := make(Set[string])
	re := regexp.MustCompile(`CREATE MACRO (\w+)\(`)

	for _, macro := range macros {
		matches := re.FindStringSubmatch(macro)
		if len(matches) > 1 {
			names.Add(matches[1])
		}
	}

	return names
}
