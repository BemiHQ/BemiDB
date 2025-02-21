package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserUtils struct {
	config *Config
}

func NewParserUtils(config *Config) *ParserUtils {
	return &ParserUtils{config: config}
}

func (utils *ParserUtils) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	switch len(functionCall.Funcname) {
	case 1:
		return &QuerySchemaFunction{
			Schema:   "",
			Function: functionCall.Funcname[0].GetString_().Sval,
		}
	case 2:
		return &QuerySchemaFunction{
			Schema:   functionCall.Funcname[0].GetString_().Sval,
			Function: functionCall.Funcname[1].GetString_().Sval,
		}
	default:
		panic("Invalid function call")
	}
}

func (utils *ParserUtils) MakeSubselectFromNode(qSchemaTable QuerySchemaTable, targetList []*pgQuery.Node, fromNode *pgQuery.Node) *pgQuery.Node {
	alias := qSchemaTable.Alias
	if alias == "" {
		if qSchemaTable.Schema == PG_SCHEMA_PUBLIC || qSchemaTable.Schema == "" {
			alias = qSchemaTable.Table
		} else {
			alias = qSchemaTable.Schema + "_" + qSchemaTable.Table
		}
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList: targetList,
							FromClause: []*pgQuery.Node{fromNode},
						},
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
				},
			},
		},
	}
}

func (utils *ParserUtils) MakeAConstBoolNode(val bool) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_AConst{
			AConst: &pgQuery.A_Const{
				Val: &pgQuery.A_Const_Boolval{
					Boolval: &pgQuery.Boolean{
						Boolval: val,
					},
				},
				Isnull:   false,
				Location: 0,
			},
		},
	}
}

func (utils *ParserUtils) MakeNullNode() *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_AConst{
			AConst: &pgQuery.A_Const{
				Isnull: true,
			},
		},
	}
}
