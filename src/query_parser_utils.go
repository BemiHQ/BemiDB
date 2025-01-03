package main

import (
	"strconv"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryParserUtils struct {
	config *Config
}

func NewQueryParserUtils(config *Config) *QueryParserUtils {
	return &QueryParserUtils{config: config}
}

func (utils *QueryParserUtils) MakeSubselectWithRowsNode(tableName string, columns []string, rowsValues [][]string, alias string) *pgQuery.Node {
	parserType := NewQueryParserType(utils.config)

	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, column := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(column)
	}

	selectStmt := &pgQuery.SelectStmt{}

	for _, row := range rowsValues {
		var rowList []*pgQuery.Node
		for _, val := range row {
			constNode := pgQuery.MakeAConstStrNode(val, 0)

			// Cast TRUE/FALSE to bool
			switch strings.ToLower(val) {
			case "true", "false":
				constNode = parserType.MakeCaseTypeCastNode(constNode, "bool")
			default:
				// If it's an integer, cast to int8; otherwise leave it as varchar
				if _, err := strconv.ParseInt(val, 10, 64); err == nil {
					constNode = parserType.MakeCaseTypeCastNode(constNode, "int8")
				}
			}

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

func (utils *QueryParserUtils) MakeSubselectWithoutRowsNode(tableName string, columns []string, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, column := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(column)
	}

	targetList := make([]*pgQuery.Node, len(columns))
	for i, _ := range columns {
		targetList[i] = pgQuery.MakeResTargetNodeWithVal(
			utils.MakeAConstBoolNode(false),
			0,
		)
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

func (utils *QueryParserUtils) MakeSubselectFromNode(tableName string, targetList []*pgQuery.Node, fromNode *pgQuery.Node, alias string) *pgQuery.Node {
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

func (utils *QueryParserUtils) MakeAConstBoolNode(val bool) *pgQuery.Node {
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
