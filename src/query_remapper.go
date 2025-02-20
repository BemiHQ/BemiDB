package main

import (
	"errors"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var SUPPORTED_SET_STATEMENTS = NewSet([]string{
	"timezone", // SET SESSION timezone TO 'UTC'
})

var KNOWN_SET_STATEMENTS = NewSet([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"extra_float_digits",          // SET extra_float_digits = 3
	"application_name",            // SET application_name = 'psql'
	"datestyle",                   // SET datestyle TO 'ISO'
	"session characteristics",     // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED
})

var FALLBACK_QUERY_TREE, _ = pgQuery.Parse(FALLBACK_SQL_QUERY)
var FALLBACK_SET_QUERY_TREE, _ = pgQuery.Parse("SET schema TO public")

type QueryRemapper struct {
	parserTypeCast   *ParserTypeCast
	remapperTable    *QueryRemapperTable
	remapperTypeCast *QueryRemapperTypeCast
	remapperFunction *QueryRemapperFunction
	remapperSelect   *QueryRemapperSelect
	remapperShow     *QueryRemapperShow
	icebergReader    *IcebergReader
	duckdb           *Duckdb
	config           *Config
}

func NewQueryRemapper(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *QueryRemapper {
	return &QueryRemapper{
		parserTypeCast:   NewParserTypeCast(config),
		remapperTable:    NewQueryRemapperTable(config, icebergReader, duckdb),
		remapperTypeCast: NewQueryRemapperTypeCast(config),
		remapperFunction: NewQueryRemapperFunction(config),
		remapperSelect:   NewQueryRemapperSelect(config),
		remapperShow:     NewQueryRemapperShow(config),
		icebergReader:    icebergReader,
		duckdb:           duckdb,
		config:           config,
	}
}

func (remapper *QueryRemapper) RemapStatements(statements []*pgQuery.RawStmt) ([]*pgQuery.RawStmt, error) {
	// Empty query
	if len(statements) == 0 {
		return statements, nil
	}

	for i, stmt := range statements {
		LogTrace(remapper.config, "Remapping statement #"+IntToString(i+1))

		node := stmt.Stmt

		switch {
		// Empty statement
		case node == nil:
			return nil, errors.New("empty statement")

		// SELECT
		case node.GetSelectStmt() != nil:
			remappedSelect := remapper.remapSelectStatement(stmt.Stmt.GetSelectStmt(), 1)
			stmt.Stmt = &pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: remappedSelect}}
			statements[i] = stmt

		// SET
		case node.GetVariableSetStmt() != nil:
			statements[i] = remapper.remapSetStatement(stmt)

		// DISCARD ALL
		case node.GetDiscardStmt() != nil:
			statements[i] = FALLBACK_QUERY_TREE.Stmts[0]

		// SHOW
		case node.GetVariableShowStmt() != nil:
			statements[i] = remapper.remapperShow.RemapShowStatement(stmt)

		// Unsupported query
		default:
			LogDebug(remapper.config, "Query tree:", stmt, node)
			return nil, errors.New("unsupported query type")
		}
	}

	return statements, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// SET ... (no-op)
func (remapper *QueryRemapper) remapSetStatement(stmt *pgQuery.RawStmt) *pgQuery.RawStmt {
	setStatement := stmt.Stmt.GetVariableSetStmt()

	if SUPPORTED_SET_STATEMENTS.Contains(strings.ToLower(setStatement.Name)) {
		return stmt
	}

	if !KNOWN_SET_STATEMENTS.Contains(strings.ToLower(setStatement.Name)) {
		LogWarn(remapper.config, "Unknown SET ", setStatement.Name, ":", setStatement)
	}

	return FALLBACK_SET_QUERY_TREE.Stmts[0]
}

func (remapper *QueryRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	// UNION
	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		remapper.traceTreeTraversal("UNION left", indentLevel)
		leftSelectStatement := selectStatement.Larg
		remapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // self-recursion

		remapper.traceTreeTraversal("UNION right", indentLevel)
		rightSelectStatement := selectStatement.Rarg
		remapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // self-recursion
	}

	// JOIN
	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		selectStatement.FromClause[0] = remapper.remapJoinExpressions(selectStatement, selectStatement.FromClause[0], indentLevel+1) // recursion
	}

	// WHERE
	if selectStatement.WhereClause != nil {
		selectStatement.WhereClause = remapper.remapTypeCastsInNode(selectStatement.WhereClause)                    // recursion
		selectStatement = remapper.remapWhereExpressions(selectStatement, selectStatement.WhereClause, indentLevel) // recursion
	}

	// WITH
	if selectStatement.WithClause != nil {
		remapper.traceTreeTraversal("WITH CTE's", indentLevel)
		for _, cte := range selectStatement.WithClause.Ctes {
			if cteSelect := cte.GetCommonTableExpr().Ctequery.GetSelectStmt(); cteSelect != nil {
				remapper.remapSelectStatement(cteSelect, indentLevel+1) // self-recursion
			}
		}
	}

	// FROM
	if len(selectStatement.FromClause) > 0 {
		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				remapper.traceTreeTraversal("WHERE statements", indentLevel)
				// FROM [TABLE]
				remapper.traceTreeTraversal("FROM table", indentLevel)
				selectStatement.FromClause[i] = remapper.remapperTable.RemapTable(fromNode)
				qSchemaTable := remapper.remapperTable.NodeToQuerySchemaTable(fromNode)
				selectStatement = remapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
			} else if fromNode.GetRangeSubselect() != nil {
				// FROM (SELECT ...)
				remapper.traceTreeTraversal("FROM subselect", indentLevel)
				subSelectStatement := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				remapper.remapSelectStatement(subSelectStatement, indentLevel+1) // self-recursion
			} else if fromNode.GetRangeFunction() != nil {
				// FROM PG_FUNCTION()
				selectStatement.FromClause[i] = remapper.remapTableFunction(fromNode, indentLevel) // recursion
			}
		}
	}

	// SELECT
	selectStatement = remapper.remapSelect(selectStatement, indentLevel) // recursion

	return selectStatement
}

// CASE ...
func (remapper *QueryRemapper) remapCaseExpression(caseExpr *pgQuery.CaseExpr, indentLevel int) *pgQuery.CaseExpr {
	for _, when := range caseExpr.Args {
		if whenClause := when.GetCaseWhen(); whenClause != nil {
			// WHEN ...
			if whenClause.Expr != nil {
				if aExpr := whenClause.Expr.GetAExpr(); aExpr != nil {
					if aExpr.Kind == pgQuery.A_Expr_Kind_AEXPR_OP_ANY {
						whenClause.Expr = remapper.remapperSelect.parserSelect.ConvertAnyToIn(aExpr)
					}
					if subLink := aExpr.Lexpr.GetSubLink(); subLink != nil {
						remapper.traceTreeTraversal("CASE->WHEN left", indentLevel+1)
						subSelect := subLink.Subselect.GetSelectStmt()
						remapper.remapSelectStatement(subSelect, indentLevel+1)
					}
					if subLink := aExpr.Rexpr.GetSubLink(); subLink != nil {
						remapper.traceTreeTraversal("CASE->WHEN right", indentLevel+1)
						subSelect := subLink.Subselect.GetSelectStmt()
						remapper.remapSelectStatement(subSelect, indentLevel+1)
					}
				}
			}

			// THEN ...
			if whenClause.Result != nil {
				typeName := remapper.parserTypeCast.inferNodeType(whenClause.Result)
				if typeName != "" {
					whenClause.Result = remapper.parserTypeCast.MakeCaseTypeCastNode(whenClause.Result, typeName)
				}
				if subLink := whenClause.Result.GetSubLink(); subLink != nil {
					remapper.traceTreeTraversal("CASE->THEN", indentLevel+1)
					subSelect := subLink.Subselect.GetSelectStmt()
					remapper.remapSelectStatement(subSelect, indentLevel+1)
				}
				if functionCall := whenClause.Result.GetFuncCall(); functionCall != nil {
					remapper.traceTreeTraversal("CASE->THEN function", indentLevel+1)
					remapper.remapperFunction.RemapFunctionCall(functionCall)
					remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion
				}
			}
		}
	}

	// ELSE ...
	if caseExpr.Defresult != nil {
		typeName := remapper.parserTypeCast.inferNodeType(caseExpr.Defresult)
		if typeName != "" {
			caseExpr.Defresult = remapper.parserTypeCast.MakeCaseTypeCastNode(caseExpr.Defresult, typeName)
		}
		if subLink := caseExpr.Defresult.GetSubLink(); subLink != nil {
			remapper.traceTreeTraversal("CASE->ELSE", indentLevel+1)
			subSelect := subLink.Subselect.GetSelectStmt()
			remapper.remapSelectStatement(subSelect, indentLevel+1)
		}
		if functionCall := caseExpr.Defresult.GetFuncCall(); functionCall != nil {
			remapper.traceTreeTraversal("CASE->ELSE function", indentLevel+1)
			remapper.remapperFunction.RemapFunctionCall(functionCall)
			remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion
		}
	}

	return caseExpr
}

// FROM PG_FUNCTION()
func (remapper *QueryRemapper) remapTableFunction(fromNode *pgQuery.Node, indentLevel int) *pgQuery.Node {
	remapper.traceTreeTraversal("FROM function()", indentLevel)

	fromNode = remapper.remapperTable.RemapTableFunction(fromNode)
	if fromNode.GetRangeFunction() == nil {
		return fromNode
	}

	for _, funcNode := range fromNode.GetRangeFunction().Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			funcCallNode := funcItemNode.GetFuncCall()
			if funcCallNode == nil {
				continue
			}
			remapper.remapTableFunctionArgs(funcCallNode, indentLevel+1) // recursion
		}
	}

	return fromNode
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (remapper *QueryRemapper) remapTableFunctionArgs(funcCallNode *pgQuery.FuncCall, indentLevel int) *pgQuery.FuncCall {
	remapper.traceTreeTraversal("FROM nested_function()", indentLevel)

	for i, argNode := range funcCallNode.GetArgs() {
		nestedFunctionCall := argNode.GetFuncCall()
		if nestedFunctionCall == nil {
			continue
		}

		nestedFunctionCall = remapper.remapperTable.RemapNestedTableFunction(nestedFunctionCall)
		nestedFunctionCall = remapper.remapTableFunctionArgs(nestedFunctionCall, indentLevel+1) // recursion

		funcCallNode.Args[i].Node = &pgQuery.Node_FuncCall{FuncCall: nestedFunctionCall}
	}

	return funcCallNode
}

func (remapper *QueryRemapper) remapTypeCastsInNode(node *pgQuery.Node) *pgQuery.Node {
	if node == nil {
		return nil
	}

	// Direct typecast
	if node.GetTypeCast() != nil {
		return remapper.remapperTypeCast.RemapTypeCast(node)
	}

	// Handle CASE expressions
	if node.GetCaseExpr() != nil {
		caseExpr := node.GetCaseExpr()
		// Handle WHEN clauses
		for i, when := range caseExpr.Args {
			whenClause := when.GetCaseWhen()
			if whenClause.Result != nil {
				whenClause.Result = remapper.remapTypeCastsInNode(whenClause.Result) // self-recursion
			}
			caseExpr.Args[i] = when
		}
		// Handle ELSE clause
		if caseExpr.Defresult != nil {
			caseExpr.Defresult = remapper.remapTypeCastsInNode(caseExpr.Defresult) // self-recursion
		}
	}

	// AND/OR expressions
	if node.GetBoolExpr() != nil {
		boolExpr := node.GetBoolExpr()
		for i, arg := range boolExpr.Args {
			boolExpr.Args[i] = remapper.remapTypeCastsInNode(arg) // self-recursion
		}
	}

	// Comparison expressions
	if node.GetAExpr() != nil {
		aExpr := node.GetAExpr()
		if aExpr.Lexpr != nil {
			aExpr.Lexpr = remapper.remapTypeCastsInNode(aExpr.Lexpr) // self-recursion
		}
		if aExpr.Rexpr != nil {
			aExpr.Rexpr = remapper.remapTypeCastsInNode(aExpr.Rexpr) // self-recursion
		}
	}

	// IN expressions
	if node.GetList() != nil {
		list := node.GetList()
		for i, item := range list.Items {
			list.Items[i] = remapper.remapTypeCastsInNode(item) // self-recursion
		}
	}

	return node
}

func (remapper *QueryRemapper) remapJoinExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	remapper.traceTreeTraversal("JOIN left", indentLevel)
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = remapper.remapJoinExpressions(selectStatement, leftJoinNode, indentLevel+1) // self-recursion
	} else if leftJoinNode.GetRangeVar() != nil {
		// WHERE
		remapper.traceTreeTraversal("WHERE left", indentLevel+1)
		qSchemaTable := remapper.remapperTable.NodeToQuerySchemaTable(leftJoinNode)
		selectStatement = remapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
		// TABLE
		remapper.traceTreeTraversal("TABLE left", indentLevel+1)
		leftJoinNode = remapper.remapperTable.RemapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		remapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Larg = leftJoinNode

	remapper.traceTreeTraversal("JOIN right", indentLevel)
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = remapper.remapJoinExpressions(selectStatement, rightJoinNode, indentLevel+1) // self-recursion
	} else if rightJoinNode.GetRangeVar() != nil {
		// WHERE
		remapper.traceTreeTraversal("WHERE right", indentLevel+1)
		qSchemaTable := remapper.remapperTable.NodeToQuerySchemaTable(rightJoinNode)
		remapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
		// TABLE
		remapper.traceTreeTraversal("TABLE right", indentLevel+1)
		rightJoinNode = remapper.remapperTable.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		remapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Rarg = rightJoinNode

	remapper.traceTreeTraversal("JOIN on", indentLevel)
	if node.GetJoinExpr().Quals != nil {
		node.GetJoinExpr().Quals = remapper.remapTypeCastsInNode(node.GetJoinExpr().Quals) // recursion

		// DuckDB doesn't support non-INNER JOINs with ON clauses that reference columns from outer tables:
		//   SELECT (
		//     SELECT 1 AS test FROM (SELECT 1 AS inner_val) LEFT JOIN (SELECT NULL) ON inner_val = *outer_val*
		//   ) FROM (SELECT 1 AS outer_val)
		//   > "Non-inner join on correlated columns not supported"
		//
		// References:
		// - https://github.com/duckdb/duckdb/blob/f6ae05d0a23cae549c6f612026eda27130fe1600/src/planner/joinside.cpp#L63
		// - https://github.com/duckdb/duckdb/discussions/16012
		if node.GetJoinExpr().Jointype != pgQuery.JoinType_JOIN_INNER {
			// Change the JOIN type to INNER in some cases like: ON ... = indclass[i] (sent via Postico)
			if indentLevel > 2 && node.GetJoinExpr().Quals.GetAExpr() != nil && node.GetJoinExpr().Quals.GetAExpr().Rexpr.GetAIndirection() != nil {
				rightIndirectionColumnRef := node.GetJoinExpr().Quals.GetAExpr().Rexpr.GetAIndirection().Arg.GetColumnRef().Fields[0].GetString_().Sval
				if rightIndirectionColumnRef == "indclass" {
					node.GetJoinExpr().Jointype = pgQuery.JoinType_JOIN_INNER
				}
			}
		}
	}

	return node
}

func (remapper *QueryRemapper) remapWhereExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.SelectStmt {
	remapper.traceTreeTraversal("WHERE statements", indentLevel)

	boolExpr := node.GetBoolExpr()
	if boolExpr != nil {
		for _, arg := range boolExpr.Args {
			selectStatement = remapper.remapWhereExpressions(selectStatement, arg, indentLevel+1) // self-recursion
		}
	}

	subLink := node.GetSubLink()
	if subLink != nil {
		subSelect := subLink.Subselect.GetSelectStmt()
		remapper.remapSelectStatement(subSelect, indentLevel+1) // recursion
	}

	aExpr := node.GetAExpr()
	if aExpr != nil {
		if aExpr.Lexpr != nil {
			selectStatement = remapper.remapWhereExpressions(selectStatement, aExpr.Lexpr, indentLevel+1) // self-recursion
		}
		if aExpr.Rexpr != nil {
			selectStatement = remapper.remapWhereExpressions(selectStatement, aExpr.Rexpr, indentLevel+1) // self-recursion
		}
	}

	return selectStatement
}

// SELECT ...
func (remapper *QueryRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	remapper.traceTreeTraversal("SELECT statements", indentLevel)

	// SELECT ...
	for targetNodeIdx, targetNode := range selectStatement.TargetList {
		if targetNode.GetResTarget().Val.GetCaseExpr() != nil {
			// CASE
			caseExpression := targetNode.GetResTarget().Val.GetCaseExpr()
			remapper.remapCaseExpression(caseExpression, indentLevel) // recursion
		} else if targetNode.GetResTarget().Val.GetSubLink() != nil {
			// Nested SELECT
			subLink := targetNode.GetResTarget().Val.GetSubLink()
			subSelect := subLink.Subselect.GetSelectStmt()
			remapper.remapSelectStatement(subSelect, indentLevel+1) // recursion

			// DuckDB doesn't work with ORDER BY in ARRAY subqueries:
			//   SELECT ARRAY(SELECT 1 FROM pg_enum ORDER BY enumsortorder)
			//   > Referenced column "enumsortorder" not found in FROM clause!
			//
			// Remove ORDER BY from ARRAY subqueries
			if subLink.SubLinkType == pgQuery.SubLinkType_ARRAY_SUBLINK && subSelect.SortClause != nil {
				subSelect.SortClause = nil
			}
		} else if targetNode.GetResTarget().Val.GetCoalesceExpr() != nil {
			// COALESCE(value1, value2, ...)
			coalesceExpr := targetNode.GetResTarget().Val.GetCoalesceExpr()
			for _, arg := range coalesceExpr.Args {
				if arg.GetSubLink() != nil {
					// Nested SELECT
					subSelect := arg.GetSubLink().Subselect.GetSelectStmt()
					remapper.remapSelectStatement(subSelect, indentLevel+1) // recursion
				}
			}
		} else {
			targetNode.GetResTarget().Val = remapper.remapTypeCastsInNode(targetNode.GetResTarget().Val) // recursion
		}

		targetNode = remapper.remapperSelect.RemapSelect(targetNode)
		selectStatement.TargetList[targetNodeIdx] = targetNode
	}

	// VALUES (...)
	if len(selectStatement.ValuesLists) > 0 {
		for i, valuesList := range selectStatement.ValuesLists {
			for j, value := range valuesList.GetList().Items {
				selectStatement.ValuesLists[i].GetList().Items[j] = remapper.remapTypeCastsInNode(value) // recursion
			}
		}
	}

	return selectStatement
}

func (remapper *QueryRemapper) traceTreeTraversal(label string, indentLevel int) {
	LogTrace(remapper.config, strings.Repeat(">", indentLevel), label)
}
