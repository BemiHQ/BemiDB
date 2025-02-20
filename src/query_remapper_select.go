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
func (remapper *QueryRemapperSelect) RemapSelectFunctions(targetNode *pgQuery.Node) *pgQuery.Node {
	functionCall := remapper.parserFunction.InderectionFunctionCall(targetNode)
	if functionCall == nil {
		functionCall = remapper.parserFunction.FunctionCall(targetNode)
		if functionCall == nil {
			return targetNode
		}
	}

	// FUNCTION(...) -> ANOTHER_FUNCTION(...)
	schemaFunction := remapper.remapperFunction.RemapFunctionCall(functionCall)
	if schemaFunction != nil {
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
		return targetNode
	}

	// function(NESTED_FUNCTION(...), ...)
	remapper.remapperFunction.RemapNestedFunctionCalls(functionCall)

	return targetNode
}
