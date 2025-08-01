package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
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
	if typeCast == nil {
		return ""
	}

	typeNameNode := typeCast.TypeName
	var typeNames []string

	for _, name := range typeNameNode.Names {
		typeNames = append(typeNames, name.GetString_().Sval)
	}

	typeName := strings.Join(typeNames, ".")

	if typeNameNode.ArrayBounds != nil {
		typeName += "[]"
	}

	return typeName
}

func (parser *ParserTypeCast) NestedTypeCast(typeCast *pgQuery.TypeCast) *pgQuery.TypeCast {
	return parser.TypeCast(typeCast.Arg)
}

// "value" COLLATE pg_catalog.default -> "value"
func (parser *ParserTypeCast) RemovedDefaultCollateClause(node *pgQuery.Node) *pgQuery.Node {
	collname := node.GetCollateClause().Collname

	if len(collname) == 2 && collname[0].GetString_().Sval == "pg_catalog" && collname[1].GetString_().Sval == "default" {
		return node.GetCollateClause().Arg
	}

	return node
}

func (parser *ParserTypeCast) ArgStringValue(typeCast *pgQuery.TypeCast) string {
	return typeCast.Arg.GetAConst().GetSval().Sval
}

// pg_catalog.[type] -> [type]
func (parser *ParserTypeCast) RemovePgCatalog(typeCast *pgQuery.TypeCast) {
	if typeCast != nil && len(typeCast.TypeName.Names) == 2 && typeCast.TypeName.Names[0].GetString_().Sval == PG_SCHEMA_PG_CATALOG {
		typeCast.TypeName.Names = typeCast.TypeName.Names[1:]
	}
}

func (parser *ParserTypeCast) SetTypeCastArg(typeCast *pgQuery.TypeCast, arg *pgQuery.Node) {
	typeCast.Arg = arg
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

	if argumentNode.GetAConst() == nil {
		// NOTE: ::regclass::oid on non-constants is not fully supported yet
		return parser.utils.MakeNullNode()
	}

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
