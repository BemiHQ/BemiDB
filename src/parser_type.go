package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserType struct {
	config *Config
}

func NewParserType(config *Config) *ParserType {
	return &ParserType{config: config}
}

func (parser *ParserType) MakeTypeCastNode(arg *pgQuery.Node, typeName string) *pgQuery.Node {
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

func (parser *ParserType) inferNodeType(node *pgQuery.Node) string {
	if typeCast := node.GetTypeCast(); typeCast != nil {
		return typeCast.TypeName.Names[0].GetString_().Sval
	}

	if aConst := node.GetAConst(); aConst != nil {
		switch {
		case aConst.GetBoolval() != nil:
			return "boolean"
		case aConst.GetIval() != nil:
			return "int8"
		case aConst.GetSval() != nil:
			return "text"
		}
	}
	return ""
}

func (parser *ParserType) MakeCaseTypeCastNode(arg *pgQuery.Node, typeName string) *pgQuery.Node {
	if existingType := parser.inferNodeType(arg); existingType == typeName {
		return arg
	}
	return parser.MakeTypeCastNode(arg, typeName)
}

func (parser *ParserType) RemapTypeCast(node *pgQuery.Node) *pgQuery.Node {
	if node.GetTypeCast() != nil {
		typeCast := node.GetTypeCast()
		if len(typeCast.TypeName.Names) > 0 {
			typeName := typeCast.TypeName.Names[0].GetString_().Sval
			if typeName == "regclass" {
				return typeCast.Arg
			}

			if typeName == "text" {
				return parser.MakeListValueFromArray(typeCast.Arg)
			}
		}
	}
	return node
}

func (parser *ParserType) MakeListValueFromArray(node *pgQuery.Node) *pgQuery.Node {
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
