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

// SELECT FUNCTION(...) -> SELECT FUNCTION(...) AS FUNCTION
func (remapper *QueryRemapperSelect) SetDefaultTargetNameToFunctionName(targetNode *pgQuery.Node) *pgQuery.Node {
	functionCall := remapper.parserFunction.FunctionCall(targetNode)
	if functionCall != nil {
		schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)
		// FUNCTION(...) -> FUNCTION(...) AS FUNCTION
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
		return targetNode
	}

	indirectionName := remapper.parserFunction.IndirectionName(targetNode)
	if indirectionName != "" {
		// (FUNCTION()).n -> (FUNCTION()).n AS n
		remapper.parserSelect.SetDefaultTargetName(targetNode, indirectionName)
		return targetNode
	}

	return targetNode
}
