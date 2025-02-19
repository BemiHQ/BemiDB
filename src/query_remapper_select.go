package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRemapperSelect struct {
	remapperFunction *QueryRemapperFunction
	parserSelect     *ParserSelect
	parserFunction   *ParserFunction
	config           *Config
}

func NewQueryRemapperSelect(config *Config) *QueryRemapperSelect {
	return &QueryRemapperSelect{
		remapperFunction: NewQueryRemapperFunction(config),
		parserSelect:     NewParserSelect(config),
		parserFunction:   NewParserFunction(config),
		config:           config,
	}
}

// SELECT ...
func (remapper *QueryRemapperSelect) RemapSelect(targetNode *pgQuery.Node) *pgQuery.Node {
	// FUNCTION().value
	newTargetNode := remapper.remappedInderectionFunctionCall(targetNode)
	if newTargetNode != nil {
		return newTargetNode
	}

	// FUNCTION()
	functionCall := remapper.parserFunction.FunctionCall(targetNode)
	if functionCall == nil {
		return targetNode
	}

	// PG_FUNCTION(...) -> CONSTANT
	schemaFunction, constantNode := remapper.remapperFunction.RemapPgFunctionCallToConstantNode(functionCall)
	if schemaFunction != nil {
		remapper.parserSelect.OverrideTargetValue(targetNode, constantNode)
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
		return targetNode
	}

	// FUNCTION(...args) -> ANOTHER_FUNCTION(...other_args) or FUNCTION(...other_args)
	schemaFunction = remapper.remapperFunction.RemapFunctionCallDynamically(functionCall)
	if schemaFunction != nil {
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
		return targetNode
	}

	// function(NESTED_FUNCTION(...), ...)
	remapper.remapperFunction.RemapNestedFunctionCalls(functionCall)

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
