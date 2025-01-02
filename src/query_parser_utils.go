package main

import (
	"strconv"
	"github.com/jackc/pgx/v5/pgtype"
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

// Add a new struct to track column metadata
type ColumnDef struct {
	Name    string
	TypeOID uint32
	Value   string
}

type QueryParserUtils struct {
	config *Config
}

func NewQueryParserUtils(config *Config) *QueryParserUtils {
	return &QueryParserUtils{config: config}
}

func (utils *QueryParserUtils) MakeSubselectWithRowsNode(tableName string, columnDefs []ColumnDef, alias string) *pgQuery.Node {
	parserType := NewQueryParserType(utils.config)

	columnNodes := make([]*pgQuery.Node, len(columnDefs))
	for i, col := range columnDefs {
		columnNodes[i] = pgQuery.MakeStrNode(col.Name)
	}

	selectStmt := &pgQuery.SelectStmt{}

	// Create a single row from the column values
	var rowList []*pgQuery.Node
	for _, col := range columnDefs {
		constNode := pgQuery.MakeAConstStrNode(col.Value, 0)
		if col.TypeOID == uint32(pgtype.OIDOID) {
			constNode = parserType.MakeCaseTypeCastNode(constNode, "oid")
		} else if _, err := strconv.ParseInt(col.Value, 10, 64); err == nil {
			constNode = parserType.MakeCaseTypeCastNode(constNode, "int8")
		}
		rowList = append(rowList, constNode)
	}

	selectStmt.ValuesLists = append(selectStmt.ValuesLists,
		&pgQuery.Node{Node: &pgQuery.Node_List{List: &pgQuery.List{Items: rowList}}})

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

func (utils *QueryParserUtils) MakeSubselectWithoutRowsNode(tableName string, columns []ColumnDef, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, col := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(col.Name)
	}

	targetList := make([]*pgQuery.Node, len(columns))
	for i := range columns {
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
