package main

import (
	"regexp"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var CREATE_CUSTOM_MACRO_QUERIES = []string{
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
}
var CUSTOM_MACRO_PG_FUNCTION_NAMES = extractMacroNames(CREATE_CUSTOM_MACRO_QUERIES)

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

// FUNCTION(...) -> ANOTHER_FUNCTION(...)
func (remapper *QueryRemapperFunction) RemapFunctionCall(functionCall *pgQuery.FuncCall) *PgSchemaFunction {
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)

	if schemaFunction.Schema == PG_SCHEMA_PG_CATALOG &&
		(CUSTOM_MACRO_PG_FUNCTION_NAMES.Contains(schemaFunction.Function) ||
			BUILTIN_DUCKDB_PG_FUNCTION_NAMES.Contains(schemaFunction.Function)) {
		remapper.parserFunction.RemapSchemaToMain(functionCall)
		return &schemaFunction
	}

	switch schemaFunction.Function {

	// format('%s %1$s', str) -> printf('%1$s %1$s', str)
	case PG_FUNCTION_FORMAT:
		remapper.parserFunction.RemapFormatToPrintf(functionCall)
		return &schemaFunction
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
