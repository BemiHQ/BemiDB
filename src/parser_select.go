package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserSelect struct {
	config *Config
	utils  *ParserUtils
}

func NewParserSelect(config *Config) *ParserSelect {
	return &ParserSelect{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserSelect) SetDefaultTargetName(targetNode *pgQuery.Node, name string) {
	target := targetNode.GetResTarget()

	if target.Name == "" {
		target.Name = name
	}
}

// = ANY({schema_information}) -> IN (schema_information)
func (parser *ParserSelect) ConvertRightAnyToIn(aExpr *pgQuery.A_Expr) *pgQuery.Node {
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
