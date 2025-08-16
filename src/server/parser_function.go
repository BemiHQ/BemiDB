package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"

	"github.com/BemiHQ/BemiDB/src/common"
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

func (parser *ParserFunction) FirstArgumentToString(functionCall *pgQuery.FuncCall) string {
	if len(functionCall.Args) < 1 {
		return ""
	}
	return functionCall.Args[0].GetAConst().GetSval().Sval
}

// n from (FUNCTION()).n
func (parser *ParserFunction) IndirectionName(targetNode *pgQuery.Node) string {
	indirection := targetNode.GetResTarget().Val.GetAIndirection()
	if indirection != nil {
		return indirection.Indirection[0].GetString_().Sval
	}

	return ""
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
	format := parser.FirstArgumentToString(functionCall)
	for i := range functionCall.Args[1:] {
		format = strings.Replace(format, "%s", "%"+common.IntToString(i+1)+"$s", 1)
	}

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("printf")}
	functionCall.Args[0] = pgQuery.MakeAConstStrNode(format, 0)
	return functionCall
}

// encode(sha256(...), 'hex') -> sha256(...)
func (parser *ParserFunction) RemoveEncode(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	firstArg := functionCall.Args[0]
	nestedFunctionCall := firstArg.GetFuncCall()
	schemaFunction := parser.utils.SchemaFunction(nestedFunctionCall)
	if schemaFunction.Function != "sha256" {
		return
	}

	secondArg := functionCall.Args[1]
	var format string
	if secondArg.GetAConst() != nil {
		format = secondArg.GetAConst().GetSval().Sval
	} else if secondArg.GetTypeCast() != nil {
		format = secondArg.GetTypeCast().Arg.GetAConst().GetSval().Sval
	}
	if format != "hex" {
		return
	}

	functionCall.Funcname = nestedFunctionCall.Funcname
	functionCall.Args = nestedFunctionCall.Args
}
