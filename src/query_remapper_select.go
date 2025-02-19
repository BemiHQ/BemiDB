package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRemapperSelect struct {
	parserSelect   *ParserSelect
	parserFunction *ParserFunction
	config         *Config
}

func NewQueryRemapperSelect(config *Config) *QueryRemapperSelect {
	return &QueryRemapperSelect{
		parserSelect:   NewParserSelect(config),
		parserFunction: NewParserFunction(config),
		config:         config,
	}
}

func (remapper *QueryRemapperSelect) RemapFunctionToConstant(functionCall *pgQuery.FuncCall) *pgQuery.Node {
	return remapper.parserFunction.RemapToConstant(functionCall)
}

// SELECT ...
func (remapper *QueryRemapperSelect) RemapSelect(targetNode *pgQuery.Node) *pgQuery.Node {
	// PG_FUNCTION().value
	newTargetNode := remapper.remappedInderectionFunctionCall(targetNode)
	if newTargetNode != nil {
		return newTargetNode
	}

	// FUNCTION()
	functionCall := remapper.parserFunction.FunctionCall(targetNode)
	if functionCall == nil {
		return targetNode
	}
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)

	renamedNameFunction := remapper.swappedFunction(functionCall)
	if renamedNameFunction != nil {
		functionCall = renamedNameFunction
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
	}

	remappedArgsFunction := remapper.remappedFunctionArgs(functionCall)
	if remappedArgsFunction != nil {
		functionCall = remappedArgsFunction
	}

	constantNode := remapper.parserFunction.RemapToConstant(functionCall)
	if constantNode != nil {
		remapper.parserSelect.OverrideTargetValue(targetNode, constantNode)
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
	}

	remapper.remapNestedFunctionCalls(functionCall) // recursion

	return targetNode
}

func (remapper *QueryRemapperSelect) remappedInderectionFunctionCall(targetNode *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserFunction

	functionCall := parser.InderectionFunctionCall(targetNode)
	if functionCall == nil {
		return nil
	}

	schemaFunction := parser.SchemaFunction(functionCall)

	switch {

	// (information_schema._pg_expandarray(array)).n -> unnest(anyarray) AS n
	case schemaFunction.Schema == PG_SCHEMA_INFORMATION_SCHEMA && schemaFunction.Function == PG_FUNCTION_PG_EXPANDARRAY:
		inderectionColumnName := parser.InderectionColumnName(targetNode)
		newTargetNode := parser.RemapInderectionToFunctionCall(targetNode, parser.RemapPgExpandArray(functionCall))
		remapper.parserSelect.SetDefaultTargetName(newTargetNode, inderectionColumnName)
		return newTargetNode

	default:
		return nil
	}
}

func (remapper *QueryRemapperSelect) swappedFunction(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)

	switch schemaFunction.Function {

	// quote_ident(str) -> concat("\""+str+"\"")
	case PG_FUNCTION_QUOTE_INDENT:
		return remapper.parserFunction.RemapQuoteIdentToConcat(functionCall)

	// array_to_string(array, separator) -> main.array_to_string(array, separator)
	case PG_FUNCTION_ARRAY_TO_STRING:
		return remapper.parserFunction.RemapArrayToString(functionCall)

	// row_to_json(col) -> to_json(col)
	case PG_FUNCTION_ROW_TO_JSON:
		return remapper.parserFunction.RemapRowToJson(functionCall)

	// aclexplode(acl) -> json
	case PG_FUNCTION_ACLEXPLODE:
		return remapper.parserFunction.RemapAclExplode(functionCall)

	// format('%s', str) -> printf('%1$s', str)
	case PG_FUNCTION_FORMAT:
		return remapper.parserFunction.RemapFormatToPrintf(functionCall)

	default:
		return nil
	}
}

func (remapper *QueryRemapperSelect) remappedFunctionArgs(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)

	// pg_catalog.pg_get_expr(pg_node_tree, relation_oid, pretty_bool) -> pg_catalog.pg_get_expr(pg_node_tree, relation_oid)
	if (schemaFunction.Schema == PG_SCHEMA_PG_CATALOG || schemaFunction.Schema == "") && schemaFunction.Function == PG_FUNCTION_PG_GET_EXPR {
		return remapper.parserFunction.RemoveThirdArgument(functionCall)
	}

	// pg_catalog.pg_get_viewdef(view_oid, pretty_bool) -> pg_catalog.pg_get_viewdef(view_oid)
	if (schemaFunction.Schema == PG_SCHEMA_PG_CATALOG || schemaFunction.Schema == "") && schemaFunction.Function == PG_FUNCTION_PG_GET_VIEWDEF {
		return remapper.parserFunction.RemoveSecondArgument(functionCall)
	}

	return nil
}

func (remapper *QueryRemapperSelect) remapNestedFunctionCalls(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	nestedFunctionCalls := remapper.parserFunction.NestedFunctionCalls(functionCall)

	for i, nestedFunctionCall := range nestedFunctionCalls {
		if nestedFunctionCall == nil {
			continue
		}

		renamedFunctionCall := remapper.swappedFunction(nestedFunctionCall)
		if renamedFunctionCall != nil {
			nestedFunctionCall = renamedFunctionCall
		}

		constantNode := remapper.parserFunction.RemapToConstant(nestedFunctionCall)
		if constantNode != nil {
			remapper.parserFunction.OverrideFunctionCallArg(functionCall, i, constantNode)
		}

		remapper.remapNestedFunctionCalls(nestedFunctionCall) // self-recursion
	}

	return functionCall
}
