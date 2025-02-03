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

func (utils *ParserUtils) SchemaFunction(functionCall *pgQuery.FuncCall) PgSchemaFunction {
	switch len(functionCall.Funcname) {
	case 1:
		return PgSchemaFunction{
			Schema:   "",
			Function: functionCall.Funcname[0].GetString_().Sval,
		}
	case 2:
		return PgSchemaFunction{
			Schema:   functionCall.Funcname[0].GetString_().Sval,
			Function: functionCall.Funcname[1].GetString_().Sval,
		}
	default:
		panic("Invalid function call")
	}
}

func (utils *ParserUtils) MakeSubselectWithRowsNode(tableName string, tableDef TableDefinition, rowsValues [][]string, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(tableDef.Columns))
	for i, col := range tableDef.Columns {
		columnNodes[i] = pgQuery.MakeStrNode(col.Name)
	}

	selectStmt := &pgQuery.SelectStmt{}

	for _, row := range rowsValues {
		var rowList []*pgQuery.Node
		for i, val := range row {
			colType := tableDef.Columns[i].Type
			constNode := utils.makeTypedConstNode(val, colType)
			rowList = append(rowList, constNode)
		}
		selectStmt.ValuesLists = append(selectStmt.ValuesLists,
			&pgQuery.Node{Node: &pgQuery.Node_List{List: &pgQuery.List{Items: rowList}}})
	}

	if alias == "" {
		alias = tableName
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: selectStmt,
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
					Colnames:  columnNodes,
				},
			},
		},
	}
}

func (utils *ParserUtils) MakeSubselectWithoutRowsNode(tableName string, tableDef TableDefinition, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(tableDef.Columns))
	for i, col := range tableDef.Columns {
		columnNodes[i] = pgQuery.MakeStrNode(col.Name)
	}

	targetList := make([]*pgQuery.Node, len(tableDef.Columns))
	for i, col := range tableDef.Columns {
		nullNode := &pgQuery.Node{
			Node: &pgQuery.Node_AConst{
				AConst: &pgQuery.A_Const{
					Isnull: true,
				},
			},
		}
		typedNullNode := utils.MakeTypeCastNode(nullNode, col.Type)
		targetList[i] = pgQuery.MakeResTargetNodeWithVal(typedNullNode, 0)
	}

	if alias == "" {
		alias = tableName
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList:  targetList,
							WhereClause: utils.MakeAConstBoolNode(false),
						},
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
					Colnames:  columnNodes,
				},
			},
		},
	}
}

func (utils *ParserUtils) MakeSubselectFromNode(tableName string, targetList []*pgQuery.Node, fromNode *pgQuery.Node, alias string) *pgQuery.Node {
	if alias == "" {
		alias = tableName
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

func (utils *ParserUtils) makeTypedConstNode(val string, pgType string) *pgQuery.Node {
	if val == "NULL" {
		return &pgQuery.Node{
			Node: &pgQuery.Node_AConst{
				AConst: &pgQuery.A_Const{
					Isnull: true,
				},
			},
		}
	}

	constNode := pgQuery.MakeAConstStrNode(val, 0)

	return utils.MakeTypeCastNode(constNode, pgType)
}

func (utils *ParserUtils) MakeTypeCastNode(arg *pgQuery.Node, typeName string) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_TypeCast{
			TypeCast: &pgQuery.TypeCast{
				Arg: arg,
				TypeName: &pgQuery.TypeName{
					Names: []*pgQuery.Node{
						pgQuery.MakeStrNode(typeName),
					},
					Location: 0,
				},
			},
		},
	}
}
