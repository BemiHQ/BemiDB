package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRemapperWhere struct {
	remapperFunction *QueryRemapperFunction
	parserWhere      *ParserWhere
	config           *Config
}

func NewQueryRemapperWhere(config *Config) *QueryRemapperWhere {
	return &QueryRemapperWhere{
		remapperFunction: NewQueryRemapperFunction(config),
		parserWhere:      NewParserWhere(config),
		config:           config,
	}
}

func (remapper *QueryRemapperWhere) RemapWhereExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node) *pgQuery.SelectStmt {
	functionCall := remapper.parserWhere.FunctionCall(node)
	if functionCall == nil {
		return selectStatement
	}

	// PG_FUNCTION(...) -> CONSTANT
	_, constantNode := remapper.remapperFunction.RemapPgFunctionCallToConstantNode(functionCall)
	if constantNode == nil {
		return selectStatement
	}
	node.Node = constantNode.Node

	return selectStatement
}
