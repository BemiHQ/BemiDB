package main

import (
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
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
			if val == "NULL" {
				constNode := &pgQuery.Node{
					Node: &pgQuery.Node_AConst{
						AConst: &pgQuery.A_Const{
							Isnull: true,
						},
					},
				}
				rowList = append(rowList, constNode)
			} else {
				constNode := pgQuery.MakeAConstStrNode(val, 0)
				if _, err := strconv.ParseInt(val, 10, 64); err == nil {
					constNode = parserType.MakeCaseTypeCastNode(constNode, "int8")
				} else {
					valLower := strings.ToLower(val)
					if valLower == "true" || valLower == "false" {
						constNode = parserType.MakeCaseTypeCastNode(constNode, "bool")
					}
				}
				rowList = append(rowList, constNode)
			}
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

func (utils *QueryParserUtils) MakeSubselectWithTypedRows(tableName string, columns []PgColumnDef, rowsValues [][]string, alias string) *pgQuery.Node {
	parserType := NewQueryParserType(utils.config)

	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, col := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(col.Name)
	}

	selectStmt := &pgQuery.SelectStmt{}

	for _, row := range rowsValues {
		var rowList []*pgQuery.Node
		for i, val := range row {
			if val == "NULL" {
				constNode := &pgQuery.Node{
					Node: &pgQuery.Node_AConst{
						AConst: &pgQuery.A_Const{
							Isnull: true,
						},
					},
				}
				rowList = append(rowList, constNode)
			} else {
				constNode := pgQuery.MakeAConstStrNode(val, 0)
				// Convert pgtype OID to type name
				typeName := pgtypeOIDToTypeName(columns[i].PgType)
				constNode = parserType.MakeCaseTypeCastNode(constNode, typeName)
				rowList = append(rowList, constNode)
			}
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

func pgtypeOIDToTypeName(oid uint32) string {
	switch oid {
	case pgtype.OIDOID:
		return "oid"
	case pgtype.NameOID:
		return "name"
	case pgtype.TextOID:
		return "text"
	case pgtype.BoolOID:
		return "bool"
	case pgtype.Int4OID:
		return "int4"
	case pgtype.Int8OID:
		return "int8"
	case pgtype.Float8OID:
		return "float8"
	case pgtype.VarcharOID:
		return "varchar"
	case pgtype.TimestampOID:
		return "timestamp"
	case pgtype.TimestamptzOID:
		return "timestamptz"
	case pgtype.IntervalOID:
		return "interval"
	case pgtype.TextArrayOID:
		return "text[]"
	default:
		return "text"
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
