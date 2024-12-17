package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var KNOWN_SET_STATEMENTS = NewSet([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"timezone",                    // SET SESSION timezone TO 'UTC'
	"extra_float_digits",          // SET extra_float_digits = 3
	"application_name",            // SET application_name = 'psql'
})

type SelectRemapper struct {
	parserTable    *QueryParserTable
	remapperTable  *SelectRemapperTable
	remapperWhere  *SelectRemapperWhere
	remapperSelect *SelectRemapperSelect
	icebergReader  *IcebergReader
	duckdb         *Duckdb
	config         *Config
}

func NewSelectRemapper(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *SelectRemapper {
	return &SelectRemapper{
		parserTable:    NewQueryParserTable(config),
		remapperTable:  NewSelectRemapperTable(config, icebergReader, duckdb),
		remapperWhere:  NewSelectRemapperWhere(config),
		remapperSelect: NewSelectRemapperSelect(config),
		icebergReader:  icebergReader,
		duckdb:         duckdb,
		config:         config,
	}
}

// SET ... (no-op)
func (selectRemapper *SelectRemapper) RemapSetStatement(stmt *pgQuery.RawStmt) *pgQuery.RawStmt {
	setStatement := stmt.Stmt.GetVariableSetStmt()

	if !KNOWN_SET_STATEMENTS.Contains(setStatement.Name) {
		LogWarn(selectRemapper.config, "Unsupported SET ", setStatement.Name, ":", setStatement)
	}

	stmt.Stmt.GetVariableSetStmt().Name = "schema"
	stmt.Stmt.GetVariableSetStmt().Args = []*pgQuery.Node{
		pgQuery.MakeAConstStrNode(PG_SCHEMA_PUBLIC, 0),
	}

	return stmt
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (selectRemapper *SelectRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	selectStatement = selectRemapper.remapTypeCastsInSelect(selectStatement)

	// CASE
	if hasCaseExpr := selectRemapper.hasCaseExpressions(selectStatement); hasCaseExpr {
		selectRemapper.traceTreeTraversal("CASE expressions", indentLevel)
		return selectRemapper.remapCaseExpressions(selectStatement, indentLevel)
	}

	// UNION
	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		selectRemapper.traceTreeTraversal("UNION left", indentLevel)
		leftSelectStatement := selectStatement.Larg
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // self-recursion

		selectRemapper.traceTreeTraversal("UNION right", indentLevel)
		rightSelectStatement := selectStatement.Rarg
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // self-recursion

		return selectStatement
	}

	// JOIN
	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		// SELECT
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)                                                         // recursive
		selectStatement.FromClause[0] = selectRemapper.remapJoinExpressions(selectStatement, selectStatement.FromClause[0], indentLevel+1) // recursive with self-recursion
		return selectStatement
	}

	// WHERE
	if selectStatement.WhereClause != nil {
		selectStatement = selectRemapper.remapWhereExpression(selectStatement, selectStatement.WhereClause, indentLevel)
	}

	// FROM
	if len(selectStatement.FromClause) > 0 {
		// SELECT
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel) // recursive

		// FROM
		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				// WHERE
				selectRemapper.traceTreeTraversal("WHERE statements", indentLevel)
				qSchemaTable := selectRemapper.parserTable.NodeToQuerySchemaTable(fromNode)
				selectStatement = selectRemapper.remapperWhere.RemapWhere(qSchemaTable, selectStatement)
				// TABLE
				selectRemapper.traceTreeTraversal("FROM table", indentLevel)
				selectStatement.FromClause[i] = selectRemapper.remapperTable.RemapTable(fromNode)
			} else if fromNode.GetRangeSubselect() != nil {
				// FROM (SELECT ...)
				selectRemapper.traceTreeTraversal("FROM subselect", indentLevel)
				subSelectStatement := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				subSelectStatement = selectRemapper.remapSelectStatement(subSelectStatement, indentLevel+1) // self-recursion
			}

			// FROM PG_FUNCTION()
			if fromNode.GetRangeFunction() != nil {
				selectStatement.FromClause[i] = selectRemapper.remapTableFunction(fromNode, indentLevel+1) // recursive
			}
		}
		return selectStatement
	}

	selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel) // recursive
	return selectStatement
}

func (selectRemapper *SelectRemapper) hasCaseExpressions(selectStatement *pgQuery.SelectStmt) bool {
	for _, target := range selectStatement.TargetList {
		if target.GetResTarget().Val.GetCaseExpr() != nil {
			return true
		}
	}
	return false
}

func (selectRemapper *SelectRemapper) remapCaseExpressions(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	for _, target := range selectStatement.TargetList {
		if caseExpr := target.GetResTarget().Val.GetCaseExpr(); caseExpr != nil {
			for _, when := range caseExpr.Args {
				if whenClause := when.GetCaseWhen(); whenClause != nil {
					if whenClause.Expr != nil {
						if aExpr := whenClause.Expr.GetAExpr(); aExpr != nil {
							if subLink := aExpr.Lexpr.GetSubLink(); subLink != nil {
								selectRemapper.traceTreeTraversal("CASE WHEN left", indentLevel+1)
								subSelect := subLink.Subselect.GetSelectStmt()
								subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
							}
							if subLink := aExpr.Rexpr.GetSubLink(); subLink != nil {
								selectRemapper.traceTreeTraversal("CASE WHEN right", indentLevel+1)
								subSelect := subLink.Subselect.GetSelectStmt()
								subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
							}
						}
					}

					if whenClause.Result != nil {
						if subLink := whenClause.Result.GetSubLink(); subLink != nil {
							selectRemapper.traceTreeTraversal("CASE THEN", indentLevel+1)
							subSelect := subLink.Subselect.GetSelectStmt()
							subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
						}
					}
				}
			}

			if caseExpr.Defresult != nil {
				if subLink := caseExpr.Defresult.GetSubLink(); subLink != nil {
					selectRemapper.traceTreeTraversal("CASE ELSE", indentLevel+1)
					subSelect := subLink.Subselect.GetSelectStmt()
					subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
				}
			}
		}
	}
	return selectStatement
}

// FROM PG_FUNCTION()
func (selectRemapper *SelectRemapper) remapTableFunction(fromNode *pgQuery.Node, indentLevel int) *pgQuery.Node {
	selectRemapper.traceTreeTraversal("FROM function()", indentLevel)

	fromNode = selectRemapper.remapperTable.RemapTableFunction(fromNode)
	if fromNode.GetRangeFunction() == nil {
		return fromNode
	}

	for _, funcNode := range fromNode.GetRangeFunction().Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			funcCallNode := funcItemNode.GetFuncCall()
			if funcCallNode == nil {
				continue
			}
			funcCallNode = selectRemapper.remapTableFunctionArgs(funcCallNode, indentLevel+1) // recursive
		}
	}

	return fromNode
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (selectRemapper *SelectRemapper) remapTableFunctionArgs(funcCallNode *pgQuery.FuncCall, indentLevel int) *pgQuery.FuncCall {
	selectRemapper.traceTreeTraversal("FROM nested_function()", indentLevel)

	for i, argNode := range funcCallNode.GetArgs() {
		nestedFunctionCall := argNode.GetFuncCall()
		if nestedFunctionCall == nil {
			continue
		}

		nestedFunctionCall = selectRemapper.remapperTable.RemapNestedTableFunction(nestedFunctionCall)
		nestedFunctionCall = selectRemapper.remapTableFunctionArgs(nestedFunctionCall, indentLevel+1) // recursive

		funcCallNode.Args[i].Node = &pgQuery.Node_FuncCall{FuncCall: nestedFunctionCall}
	}

	return funcCallNode
}

func (selectRemapper *SelectRemapper) remapTypeCastsInSelect(selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	// WHERE [CONDITION]
	if selectStatement.WhereClause != nil {
		selectStatement.WhereClause = selectRemapper.remapTypeCastsInNode(selectStatement.WhereClause)
	}

	// FROM / JOIN [TABLE] and VALUES
	if len(selectStatement.FromClause) > 0 {
		for _, fromNode := range selectStatement.FromClause {
			if fromNode.GetJoinExpr() != nil {
				joinExpr := fromNode.GetJoinExpr()
				if joinExpr.Quals != nil {
					joinExpr.Quals = selectRemapper.remapTypeCastsInNode(joinExpr.Quals)
				}
			}
			// Subqueries
			if fromNode.GetRangeSubselect() != nil {
				subSelect := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				selectRemapper.remapTypeCastsInSelect(subSelect)
			}
		}
	}

	// VALUES list
	if len(selectStatement.ValuesLists) > 0 {
		for i, valuesList := range selectStatement.ValuesLists {
			for j, value := range valuesList.GetList().Items {
				selectStatement.ValuesLists[i].GetList().Items[j] = selectRemapper.remapTypeCastsInNode(value)
			}
		}
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapTypeCastsInNode(node *pgQuery.Node) *pgQuery.Node {
	if node == nil {
		return nil
	}

	// Direct typecast
	if node.GetTypeCast() != nil {
		return selectRemapper.remapTypecast(node)
	}

	// Handle CASE expressions
	if node.GetCaseExpr() != nil {
		caseExpr := node.GetCaseExpr()
		// Handle WHEN clauses
		for i, when := range caseExpr.Args {
			whenClause := when.GetCaseWhen()
			if whenClause.Result != nil {
				whenClause.Result = selectRemapper.remapTypeCastsInNode(whenClause.Result)
			}
			caseExpr.Args[i] = when
		}
		// Handle ELSE clause
		if caseExpr.Defresult != nil {
			caseExpr.Defresult = selectRemapper.remapTypeCastsInNode(caseExpr.Defresult)
		}
	}

	// AND/OR expressions
	if node.GetBoolExpr() != nil {
		boolExpr := node.GetBoolExpr()
		for i, arg := range boolExpr.Args {
			boolExpr.Args[i] = selectRemapper.remapTypeCastsInNode(arg)
		}
	}

	// Comparison expressions
	if node.GetAExpr() != nil {
		aExpr := node.GetAExpr()
		if aExpr.Lexpr != nil {
			aExpr.Lexpr = selectRemapper.remapTypeCastsInNode(aExpr.Lexpr)
		}
		if aExpr.Rexpr != nil {
			aExpr.Rexpr = selectRemapper.remapTypeCastsInNode(aExpr.Rexpr)
		}
	}

	// IN expressions
	if node.GetList() != nil {
		list := node.GetList()
		for i, item := range list.Items {
			list.Items[i] = selectRemapper.remapTypeCastsInNode(item)
		}
	}

	return node
}

func (selectRemapper *SelectRemapper) remapWhereExpression(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.SelectStmt {
	selectRemapper.traceTreeTraversal("WHERE expression", indentLevel)

	if aExpr := node.GetAExpr(); aExpr != nil {
		if aExpr.Lexpr != nil {
			selectRemapper.traceTreeTraversal("WHERE expression left", indentLevel+1)
			selectStatement = selectRemapper.remapWhereExpression(selectStatement, aExpr.Lexpr, indentLevel+1)
		}
		if aExpr.Rexpr != nil {
			selectRemapper.traceTreeTraversal("WHERE expression right", indentLevel+1)
			selectStatement = selectRemapper.remapWhereExpression(selectStatement, aExpr.Rexpr, indentLevel+1)
		}
	}

	if funcCall := node.GetFuncCall(); funcCall != nil {
		if constantNode := selectRemapper.remapperSelect.remappedToConstant(funcCall); constantNode != nil {
			node.Node = constantNode.Node
		}
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapJoinExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	selectRemapper.traceTreeTraversal("JOIN left", indentLevel)
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = selectRemapper.remapJoinExpressions(selectStatement, leftJoinNode, indentLevel+1) // self-recursion
	} else if leftJoinNode.GetRangeVar() != nil {
		// WHERE
		selectRemapper.traceTreeTraversal("WHERE left", indentLevel+1)
		qSchemaTable := selectRemapper.parserTable.NodeToQuerySchemaTable(leftJoinNode)
		selectStatement = selectRemapper.remapperWhere.RemapWhere(qSchemaTable, selectStatement)
		// TABLE
		selectRemapper.traceTreeTraversal("TABLE left", indentLevel+1)
		leftJoinNode = selectRemapper.remapperTable.RemapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Larg = leftJoinNode

	selectRemapper.traceTreeTraversal("JOIN right", indentLevel)
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = selectRemapper.remapJoinExpressions(selectStatement, rightJoinNode, indentLevel+1) // self-recursion
	} else if rightJoinNode.GetRangeVar() != nil {
		// WHERE
		selectRemapper.traceTreeTraversal("WHERE right", indentLevel+1)
		qSchemaTable := selectRemapper.parserTable.NodeToQuerySchemaTable(rightJoinNode)
		selectStatement = selectRemapper.remapperWhere.RemapWhere(qSchemaTable, selectStatement)
		// TABLE
		selectRemapper.traceTreeTraversal("TABLE right", indentLevel+1)
		rightJoinNode = selectRemapper.remapperTable.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Rarg = rightJoinNode

	return node
}

func (selectRemapper *SelectRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	selectRemapper.traceTreeTraversal("SELECT statements", indentLevel+1)

	for i, targetNode := range selectStatement.TargetList {
		targetNode = selectRemapper.remapperSelect.RemapSelect(targetNode)

		// Recursively remap sub-selects
		subSelectStatement := selectRemapper.remapperSelect.SubselectStatement(targetNode)
		if subSelectStatement != nil {
			subSelectStatement = selectRemapper.remapSelect(subSelectStatement, indentLevel+1) // self-recursion
		}

		selectStatement.TargetList[i] = targetNode
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapTypecast(node *pgQuery.Node) *pgQuery.Node {
	if node.GetTypeCast() != nil {
		typeCast := node.GetTypeCast()
		if len(typeCast.TypeName.Names) > 0 {
			typeName := typeCast.TypeName.Names[0].GetString_().Sval
			if typeName == "regclass" {
				return typeCast.Arg
			}

			if typeName == "text" {
				arrayStr := typeCast.Arg.GetAConst().GetSval().Sval
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
		}
	}
	return node
}

func (selectRemapper *SelectRemapper) traceTreeTraversal(label string, indentLevel int) {
	LogTrace(selectRemapper.config, strings.Repeat(">", indentLevel), label)
}
