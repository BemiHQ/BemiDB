package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserAExpr struct {
	config *Config
	utils  *ParserUtils
}

func NewParserAExpr(config *Config) *ParserAExpr {
	return &ParserAExpr{
		config: config,
		utils:  NewParserUtils(config),
	}
}

func (parser *ParserAExpr) AExpr(node *pgQuery.Node) *pgQuery.A_Expr {
	return node.GetAExpr()
}

// = ANY({schema_information}) -> IN (schema_information)
func (parser *ParserAExpr) ConvertedRightAnyToIn(node *pgQuery.Node) *pgQuery.Node {
	aExpr := parser.AExpr(node)

	if aExpr.Kind != pgQuery.A_Expr_Kind_AEXPR_OP_ANY {
		return node
	}

	if aExpr.Rexpr.GetAConst() == nil {
		// NOTE: ... = ANY() on non-constants is not fully supported yet
		return parser.utils.MakeNullNode()
	}

	arrayStr := aExpr.Rexpr.GetAConst().GetSval().Sval
	arrayStr = strings.Trim(arrayStr, "{}")
	values := strings.Split(arrayStr, ",")

	items := make([]*pgQuery.Node, len(values))
	for i, value := range values {
		value = strings.Trim(value, " ")
		items[i] = &pgQuery.Node{
			Node: &pgQuery.Node_AConst{
				AConst: &pgQuery.A_Const{
					Val: &pgQuery.A_Const_Sval{
						Sval: &pgQuery.String{
							Sval: value,
						},
					},
					Location: 0,
				},
			},
		}
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_AExpr{
			AExpr: &pgQuery.A_Expr{
				Kind:  pgQuery.A_Expr_Kind_AEXPR_IN,
				Name:  []*pgQuery.Node{{Node: &pgQuery.Node_String_{String_: &pgQuery.String{Sval: "="}}}},
				Lexpr: aExpr.Lexpr,
				Rexpr: &pgQuery.Node{
					Node: &pgQuery.Node_List{
						List: &pgQuery.List{
							Items: items,
						},
					},
				},
				Location: aExpr.Location,
			},
		},
	}
}

// pg_catalog.[operator] -> [operator]
func (parser *ParserAExpr) RemovePgCatalog(node *pgQuery.Node) {
	aExpr := parser.AExpr(node)

	if aExpr == nil || aExpr.Kind != pgQuery.A_Expr_Kind_AEXPR_OP {
		return
	}

	if len(aExpr.Name) == 2 && aExpr.Name[0].GetString_().Sval == PG_SCHEMA_PG_CATALOG {
		aExpr.Name = aExpr.Name[1:] // Remove the first element
	}
}
