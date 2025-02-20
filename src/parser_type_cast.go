package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserTypeCast struct {
	utils  *ParserUtils
	config *Config
}

func NewParserTypeCast(config *Config) *ParserTypeCast {
	return &ParserTypeCast{utils: NewParserUtils(config), config: config}
}

func (parser *ParserTypeCast) TypeCast(node *pgQuery.Node) *pgQuery.TypeCast {
	if node.GetTypeCast() == nil {
		return nil
	}

	typeCast := node.GetTypeCast()
	if len(typeCast.TypeName.Names) == 0 {
		return nil
	}

	return typeCast
}

func (parser *ParserTypeCast) TypeName(typeCast *pgQuery.TypeCast) string {
	typeNameNode := typeCast.TypeName
	typeName := typeNameNode.Names[0].GetString_().Sval

	if typeNameNode.ArrayBounds != nil {
		return typeName + "[]"
	}

	return typeName
}

func (parser *ParserTypeCast) ArgStringValue(typeCast *pgQuery.TypeCast) string {
	return typeCast.Arg.GetAConst().GetSval().Sval
}

func (parser *ParserTypeCast) MakeListValueFromArray(node *pgQuery.Node) *pgQuery.Node {
	arrayStr := node.GetAConst().GetSval().Sval
	arrayStr = strings.Trim(arrayStr, "{}")
	elements := strings.Split(arrayStr, ",")

	funcCall := &pgQuery.FuncCall{
		Funcname: []*pgQuery.Node{
			pgQuery.MakeStrNode("list_value"),
		},
	}

	for _, elem := range elements {
		funcCall.Args = append(funcCall.Args,
			pgQuery.MakeAConstStrNode(elem, 0))
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_FuncCall{
			FuncCall: funcCall,
		},
	}
}

// SELECT c.oid
// FROM pg_class c
// JOIN pg_namespace n ON n.oid = c.relnamespace
// WHERE n.nspname = 'schema' AND c.relname = 'table'
func (parser *ParserTypeCast) MakeSubselectOidBySchemaTableArg(argumentNode *pgQuery.Node) *pgQuery.Node {
	targetNode := pgQuery.MakeResTargetNodeWithVal(
		pgQuery.MakeColumnRefNode([]*pgQuery.Node{
			pgQuery.MakeStrNode("c"),
			pgQuery.MakeStrNode("oid"),
		}, 0),
		0,
	)

	joinNode := pgQuery.MakeJoinExprNode(
		pgQuery.JoinType_JOIN_INNER,
		pgQuery.MakeFullRangeVarNode("", "pg_class", "c", 0),
		pgQuery.MakeFullRangeVarNode("", "pg_namespace", "n", 0),
		pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{
				pgQuery.MakeStrNode("="),
			},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("n"),
				pgQuery.MakeStrNode("oid"),
			}, 0),
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("c"),
				pgQuery.MakeStrNode("relnamespace"),
			}, 0),
			0,
		),
	)

	value := argumentNode.GetAConst().GetSval().Sval
	qSchemaTable := NewQuerySchemaTableFromString(value)
	if qSchemaTable.Schema == "" {
		qSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}

	whereNode := pgQuery.MakeBoolExprNode(
		pgQuery.BoolExprType_AND_EXPR,
		[]*pgQuery.Node{
			pgQuery.MakeAExprNode(
				pgQuery.A_Expr_Kind_AEXPR_OP,
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("="),
				},
				pgQuery.MakeColumnRefNode([]*pgQuery.Node{
					pgQuery.MakeStrNode("n"),
					pgQuery.MakeStrNode("nspname"),
				}, 0),
				pgQuery.MakeAConstStrNode(qSchemaTable.Schema, 0),
				0,
			),
			pgQuery.MakeAExprNode(
				pgQuery.A_Expr_Kind_AEXPR_OP,
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("="),
				},
				pgQuery.MakeColumnRefNode([]*pgQuery.Node{
					pgQuery.MakeStrNode("c"),
					pgQuery.MakeStrNode("relname"),
				}, 0),
				pgQuery.MakeAConstStrNode(qSchemaTable.Table, 0),
				0,
			),
		},
		0,
	)

	return &pgQuery.Node{
		Node: &pgQuery.Node_SubLink{
			SubLink: &pgQuery.SubLink{
				SubLinkType: pgQuery.SubLinkType_EXPR_SUBLINK,
				Subselect: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList:  []*pgQuery.Node{targetNode},
							FromClause:  []*pgQuery.Node{joinNode},
							WhereClause: whereNode,
						},
					},
				},
			},
		},
	}

}
