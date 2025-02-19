package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME = map[string]ConstantDefinition{
	"pg_get_userbyid":                    {"bemidb", "text"},
	"pg_encoding_to_char":                {"UTF8", "text"},
	"pg_get_function_identity_arguments": {"", "text"},
	"pg_get_partkeydef":                  {"", "text"},
	"pg_tablespace_location":             {"", "text"},
	"pg_get_indexdef":                    {"", "text"},
	"pg_total_relation_size":             {"0", "int4"},
	"pg_table_size":                      {"0", "int4"},
	"pg_indexes_size":                    {"0", "int4"},
	"pg_backend_pid":                     {"0", "int4"},
	"pg_is_in_recovery":                  {"f", "bool"},
}

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

// PG_FUNCTION(...) -> CONSTANT
func (remapper *QueryRemapperFunction) RemapPgFunctionCallToConstantNode(functionCall *pgQuery.FuncCall) (*PgSchemaFunction, *pgQuery.Node) {
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)
	constantDefinion, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[schemaFunction.Function]
	if ok {
		return &schemaFunction, remapper.parserFunction.MakeConstantNode(constantDefinion)
	}

	return nil, nil
}

// FUNCTION(...args) -> ANOTHER_FUNCTION(...other_args) or FUNCTION(...other_args)
func (remapper *QueryRemapperFunction) RemapFunctionCallDynamically(functionCall *pgQuery.FuncCall) *PgSchemaFunction {
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)

	switch schemaFunction.Function {

	// quote_ident(str) -> concat("\""+str+"\"")
	case PG_FUNCTION_QUOTE_INDENT:
		remapper.parserFunction.RemapQuoteIdentToConcat(functionCall)
		return &schemaFunction

	// array_to_string(array, separator) -> main.array_to_string(array, separator)
	case PG_FUNCTION_ARRAY_TO_STRING:
		remapper.parserFunction.RemapArrayToString(functionCall)
		return &schemaFunction

	// row_to_json(col) -> to_json(col)
	case PG_FUNCTION_ROW_TO_JSON:
		remapper.parserFunction.RemapRowToJson(functionCall)
		return &schemaFunction

	// aclexplode(acl) -> json
	case PG_FUNCTION_ACLEXPLODE:
		remapper.parserFunction.RemapAclExplode(functionCall)
		return &schemaFunction

	// format('%s', str) -> printf('%1$s', str)
	case PG_FUNCTION_FORMAT:
		remapper.parserFunction.RemapFormatToPrintf(functionCall)
		return &schemaFunction

	// pg_catalog.pg_get_expr(pg_node_tree, relation_oid, pretty_bool) -> pg_catalog.pg_get_expr(pg_node_tree, relation_oid)
	case PG_FUNCTION_PG_GET_EXPR:
		if remapper.functionFromPgCatalog(schemaFunction) {
			remapper.parserFunction.RemoveThirdArgument(functionCall)
			return &schemaFunction
		}

	// pg_catalog.pg_get_viewdef(view_oid, pretty_bool) -> pg_catalog.pg_get_viewdef(view_oid)
	case PG_FUNCTION_PG_GET_VIEWDEF:
		if remapper.functionFromPgCatalog(schemaFunction) {
			remapper.parserFunction.RemoveSecondArgument(functionCall)
			return &schemaFunction
		}
	}

	return nil
}

func (remapper *QueryRemapperFunction) RemapNestedFunctionCalls(functionCall *pgQuery.FuncCall) {
	nestedFunctionCalls := remapper.parserFunction.NestedFunctionCalls(functionCall)
	if len(nestedFunctionCalls) == 0 {
		return
	}

	for i, nestedFunctionCall := range nestedFunctionCalls {
		if nestedFunctionCall == nil {
			continue
		}

		schemaFunction := remapper.RemapFunctionCallDynamically(nestedFunctionCall)
		if schemaFunction != nil {
			continue
		}

		_, constantNode := remapper.RemapPgFunctionCallToConstantNode(nestedFunctionCall)
		if constantNode != nil {
			remapper.parserFunction.OverrideFunctionCallArg(functionCall, i, constantNode)
			continue
		}

		remapper.RemapNestedFunctionCalls(nestedFunctionCall) // self-recursion
	}
}

func (remapper *QueryRemapperFunction) functionFromPgCatalog(schemaFunction PgSchemaFunction) bool {
	return schemaFunction.Schema == PG_SCHEMA_PG_CATALOG || schemaFunction.Schema == ""
}
