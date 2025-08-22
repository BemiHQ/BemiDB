package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type QueryRemapperSelect struct {
	parserSelect   *ParserSelect
	parserFunction *ParserFunction
	parserAExpr    *ParserAExpr
	config         *Config
}

func NewQueryRemapperSelect(config *Config) *QueryRemapperSelect {
	return &QueryRemapperSelect{
		parserSelect:   NewParserSelect(config),
		parserFunction: NewParserFunction(config),
		parserAExpr:    NewParserAExpr(config),
		config:         config,
	}
}

// SELECT FUNCTION(...) -> SELECT FUNCTION(...) AS FUNCTION
func (remapper *QueryRemapperSelect) SetDefaultTargetName(targetNode *pgQuery.Node) *pgQuery.Node {
	aExpr := remapper.parserAExpr.AExprFromTargetNode(targetNode)
	if aExpr != nil {
		operatorName := remapper.parserAExpr.OperatorName(aExpr)

		// [column]->'value' or [column]->>'value' -> column_value
		if operatorName == "->" || operatorName == "->>" {
			columnName := remapper.parserAExpr.LeftColumnRefName(aExpr)
			valueName := remapper.parserAExpr.RightAConstValue(aExpr)

			if columnName != "" && valueName != "" {
				remapper.parserSelect.SetDefaultTargetName(targetNode, columnName+"_"+valueName)
				return targetNode
			}
		}
	}

	functionCall := remapper.parserFunction.FunctionCall(targetNode)
	if functionCall != nil {
		schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)
		functionName := schemaFunction.Function

		// FUNCTION(...) -> FUNCTION(...) AS FUNCTION
		remapper.parserSelect.SetDefaultTargetName(targetNode, functionName)
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
