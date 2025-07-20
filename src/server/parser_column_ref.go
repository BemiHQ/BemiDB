package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserColumnRef struct {
	config *Config
}

func NewParserColumnRef(config *Config) *ParserColumnRef {
	return &ParserColumnRef{config: config}
}

func (parser *ParserColumnRef) FieldNames(node *pgQuery.Node) []string {
	columnRef := node.GetColumnRef()
	if columnRef == nil {
		return nil
	}

	fieldNames := make([]string, 0)
	for _, field := range columnRef.Fields {
		if field.GetString_() == nil {
			return nil
		}
		fieldNames = append(fieldNames, field.GetString_().Sval)
	}
	return fieldNames
}

func (parser *ParserColumnRef) SetFields(node *pgQuery.Node, fields []string) {
	columnRef := node.GetColumnRef()

	columnRef.Fields = make([]*pgQuery.Node, len(fields))
	for i, field := range fields {
		columnRef.Fields[i] = pgQuery.MakeStrNode(field)
	}
}

// expression -> NOT expression
func (parser *ParserColumnRef) NotBooleanExpression(node *pgQuery.Node) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_BoolExpr{
			BoolExpr: &pgQuery.BoolExpr{
				Boolop: pgQuery.BoolExprType_NOT_EXPR,
				Args:   []*pgQuery.Node{node},
			},
		},
	}
}
