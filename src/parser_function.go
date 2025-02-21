package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserFunction struct {
	config *Config
	utils  *ParserUtils
}

func NewParserFunction(config *Config) *ParserFunction {
	return &ParserFunction{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserFunction) FunctionCall(targetNode *pgQuery.Node) *pgQuery.FuncCall {
	return targetNode.GetResTarget().Val.GetFuncCall()
}

func (parser *ParserFunction) InderectionFunctionCall(targetNode *pgQuery.Node) *pgQuery.FuncCall {
	indirection := targetNode.GetResTarget().Val.GetAIndirection()
	if indirection != nil && indirection.Arg.GetFuncCall() != nil {
		return indirection.Arg.GetFuncCall()
	}

	return nil
}

func (parser *ParserFunction) NestedFunctionCalls(functionCall *pgQuery.FuncCall) []*pgQuery.FuncCall {
	nestedFunctionCalls := []*pgQuery.FuncCall{}

	for _, arg := range functionCall.Args {
		nestedFunctionCalls = append(nestedFunctionCalls, arg.GetFuncCall())
	}

	return nestedFunctionCalls
}

func (parser *ParserFunction) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	return parser.utils.SchemaFunction(functionCall)
}

// pg_catalog.func() -> main.func()
func (parser *ParserFunction) RemapSchemaToMain(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	switch len(functionCall.Funcname) {
	case 1:
		functionCall.Funcname = append([]*pgQuery.Node{pgQuery.MakeStrNode(DUCKDB_SCHEMA_MAIN)}, functionCall.Funcname...)
	case 2:
		functionCall.Funcname[0] = pgQuery.MakeStrNode(DUCKDB_SCHEMA_MAIN)
	}

	return functionCall
}

// format('%s %1$s', str) -> printf('%1$s %1$s', str)
func (parser *ParserFunction) RemapFormatToPrintf(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	format := functionCall.Args[0].GetAConst().GetSval().Sval
	for i := range functionCall.Args[1:] {
		format = strings.Replace(format, "%s", "%"+IntToString(i+1)+"$s", 1)
	}

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("printf")}
	functionCall.Args[0] = pgQuery.MakeAConstStrNode(format, 0)
	return functionCall
}
