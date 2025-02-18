package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserWhere struct {
	config *Config
	utils  *ParserUtils
}

func NewParserWhere(config *Config) *ParserWhere {
	return &ParserWhere{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserWhere) FunctionCall(whereNode *pgQuery.Node) *pgQuery.FuncCall {
	return whereNode.GetFuncCall()
}

// WHERE column OPERATOR(>, <, ...) value
func (parser *ParserWhere) MakeIntEqualityExpressionNode(column string, operator string, value int, alias string) *pgQuery.Node {
	columnRefNodes := []*pgQuery.Node{pgQuery.MakeStrNode(column)}
	if alias != "" {
		columnRefNodes = []*pgQuery.Node{pgQuery.MakeStrNode(alias), pgQuery.MakeStrNode(column)}
	}

	return pgQuery.MakeAExprNode(
		pgQuery.A_Expr_Kind_AEXPR_OP,
		[]*pgQuery.Node{pgQuery.MakeStrNode(operator)},
		pgQuery.MakeColumnRefNode(columnRefNodes, 0),
		pgQuery.MakeAConstIntNode(int64(value), 0),
		0,
	)
}

// WHERE false
func (parser *ParserWhere) MakeFalseConditionNode() *pgQuery.Node {
	return parser.utils.MakeAConstBoolNode(false)
}

func (parser *ParserWhere) AppendWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
	whereClause := selectStatement.WhereClause

	if whereClause == nil {
		selectStatement.WhereClause = whereCondition
	} else if whereClause.GetBoolExpr() != nil {
		boolExpr := whereClause.GetBoolExpr()
		if boolExpr.Boolop.String() == "AND_EXPR" {
			selectStatement.WhereClause.GetBoolExpr().Args = append(boolExpr.Args, whereCondition)
		}
	} else if whereClause.GetAExpr() != nil {
		selectStatement.WhereClause = pgQuery.MakeBoolExprNode(
			pgQuery.BoolExprType_AND_EXPR,
			[]*pgQuery.Node{whereClause, whereCondition},
			0,
		)
	}
	return selectStatement
}

func (parser *ParserWhere) OverrideWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
	selectStatement.WhereClause = whereCondition
	return selectStatement
}
