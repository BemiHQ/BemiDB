package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME = map[string]string{
	"version":                            "PostgreSQL " + PG_VERSION + ", compiled by Bemi",
	"pg_get_userbyid":                    "bemidb",
	"pg_get_function_identity_arguments": "",
	"pg_total_relation_size":             "0",
	"pg_table_size":                      "0",
	"pg_indexes_size":                    "0",
	"pg_get_partkeydef":                  "",
	"pg_tablespace_location":             "",
	"pg_encoding_to_char":                "UTF8",
	"pg_backend_pid":                     "0",
	"pg_is_in_recovery":                  "f",
	"current_setting":                    "",
	"aclexplode":                         "",
	"pg_get_indexdef":                    "",
}

type ParserFunction struct {
	config *Config
	utils  *ParserUtils
}

func NewParserFunction(config *Config) *ParserFunction {
	return &ParserFunction{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserFunction) RemapToConstant(functionCall *pgQuery.FuncCall) *pgQuery.Node {
	schemaFunction := parser.SchemaFunction(functionCall)
	constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[schemaFunction.Function]
	if ok {
		return pgQuery.MakeAConstStrNode(constant, 0)
	}

	return nil
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

func (parser *ParserFunction) InderectionColumnName(targetNode *pgQuery.Node) string {
	return targetNode.GetResTarget().Val.GetAIndirection().Indirection[0].GetString_().Sval
}

func (parser *ParserFunction) NestedFunctionCalls(functionCall *pgQuery.FuncCall) []*pgQuery.FuncCall {
	nestedFunctionCalls := []*pgQuery.FuncCall{}

	for _, arg := range functionCall.Args {
		nestedFunctionCalls = append(nestedFunctionCalls, arg.GetFuncCall())
	}

	return nestedFunctionCalls
}

func (parser *ParserFunction) SchemaFunction(functionCall *pgQuery.FuncCall) PgSchemaFunction {
	return parser.utils.SchemaFunction(functionCall)
}

// quote_ident(str) -> concat("\""+str+"\"")
func (parser *ParserFunction) RemapQuoteIdentToConcat(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname[0] = pgQuery.MakeStrNode("concat")
	argConstant := functionCall.Args[0].GetAConst()
	if argConstant != nil {
		str := argConstant.GetSval().Sval
		str = "\"" + str + "\""
		functionCall.Args[0] = pgQuery.MakeAConstStrNode(str, 0)
	}

	return functionCall
}

func (parser *ParserFunction) RemoveThirdArgument(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	if len(functionCall.Args) > 2 {
		functionCall.Args = functionCall.Args[:2]
	}

	return functionCall
}

func (parser *ParserFunction) RemoveSecondArgument(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	if len(functionCall.Args) > 1 {
		functionCall.Args = functionCall.Args[:1]
	}

	return functionCall
}

// row_to_json() -> to_json()
func (parser *ParserFunction) RemapRowToJson(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("to_json")}
	return functionCall
}

// information_schema._pg_expandarray(array) -> unnest(anyarray)
func (parser *ParserFunction) RemapPgExpandArray(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("unnest")}
	return functionCall
}

// (...).n -> func() AS n
func (parser *ParserFunction) RemapInderectionToFunctionCall(targetNode *pgQuery.Node, functionCall *pgQuery.FuncCall) *pgQuery.Node {
	targetNode.GetResTarget().Val = &pgQuery.Node{Node: &pgQuery.Node_FuncCall{FuncCall: functionCall}}
	return targetNode
}

// array_to_string() -> main.array_to_string()
func (parser *ParserFunction) RemapArrayToString(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{
		pgQuery.MakeStrNode("main"),
		pgQuery.MakeStrNode("array_to_string"),
	}
	return functionCall
}

// aclexplode() -> json()
func (parser *ParserFunction) RemapAclExplode(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("json")}
	return functionCall
}

// format('%s', str) -> printf('%1$s', str)
func (parser *ParserFunction) RemapFormatToPrintf(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	format := functionCall.Args[0].GetAConst().GetSval().Sval
	for i := range functionCall.Args[1:] {
		format = strings.Replace(format, "%s", "%"+IntToString(i+1)+"$s", 1)
	}

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("printf")}
	functionCall.Args[0] = pgQuery.MakeAConstStrNode(format, 0)
	return functionCall
}

func (parser *ParserFunction) OverrideFunctionCallArg(functionCall *pgQuery.FuncCall, index int, node *pgQuery.Node) {
	functionCall.Args[index] = node
}
