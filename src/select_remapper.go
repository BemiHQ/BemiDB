package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME = map[string]string{
	"version":                            "PostgreSQL " + PG_VERSION + ", compiled by Bemi",
	"pg_get_userbyid":                    "bemidb",
	"pg_get_function_identity_arguments": "",
	"pg_total_relation_size":             "0",
	"pg_table_size":                      "0",
	"pg_indexes_size":                    "0",
	"pg_get_partkeydef":                  "",
}

var KNOWN_SET_STATEMENTS = NewSet([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"timezone",                    // SET SESSION timezone TO 'UTC'
})

type SelectRemapper struct {
	queryParser   *QueryParser
	tableRemapper *SelectTableRemapper
	icebergReader *IcebergReader
	config        *Config
}

func NewSelectRemapper(config *Config, icebergReader *IcebergReader) *SelectRemapper {
	queryParser := NewQueryParser(config)

	return &SelectRemapper{
		queryParser:   queryParser,
		tableRemapper: NewSelectTableRemapper(config, queryParser, icebergReader),
		icebergReader: icebergReader,
		config:        config,
	}
}

func (selectRemapper *SelectRemapper) RemapQueryTreeWithSelect(queryTree *pgQuery.ParseResult) *pgQuery.ParseResult {
	selectStatement := queryTree.Stmts[0].Stmt.GetSelectStmt()
	selectStatement = selectRemapper.remapSelectStatement(selectStatement, 0)

	return queryTree
}

// No-op
func (selectRemapper *SelectRemapper) RemapQueryTreeWithSet(queryTree *pgQuery.ParseResult) *pgQuery.ParseResult {
	setStatement := queryTree.Stmts[0].Stmt.GetVariableSetStmt()

	if !KNOWN_SET_STATEMENTS.Contains(setStatement.Name) {
		LogWarn(selectRemapper.config, "Unsupported SET ", setStatement.Name, ":", setStatement)
	}

	queryTree.Stmts[0].Stmt.GetVariableSetStmt().Name = "schema"
	queryTree.Stmts[0].Stmt.GetVariableSetStmt().Args = []*pgQuery.Node{
		pgQuery.MakeAConstStrNode("main", 0),
	}

	return queryTree
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (selectRemapper *SelectRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	selectStatement = selectRemapper.remapTypeCastsInSelect(selectStatement)

	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" UNION left")
		leftSelectStatement := selectStatement.Larg
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1)

		LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" UNION right")
		rightSelectStatement := selectStatement.Rarg
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1)

		return selectStatement
	}

	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)
		selectRemapper.remapJoinExpressions(selectStatement.FromClause[0], indentLevel)
		return selectStatement
	}

	if len(selectStatement.FromClause) > 0 {
		if selectStatement.FromClause[0].GetRangeVar() != nil {
			selectStatement = selectRemapper.remapWhere(selectStatement)
		}
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)
		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" SELECT statement")
				selectStatement.FromClause[i] = selectRemapper.tableRemapper.RemapTable(fromNode)
			} else if fromNode.GetRangeSubselect() != nil {
				selectRemapper.remapSelectStatement(fromNode.GetRangeSubselect().Subquery.GetSelectStmt(), indentLevel+1)
			}

			if fromNode.GetRangeFunction() != nil {
				selectStatement.FromClause[i] = selectRemapper.remapTableFunction(fromNode)
			}
		}
		return selectStatement
	}

	selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)
	return selectStatement
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

func (selectRemapper *SelectRemapper) remapJoinExpressions(node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" JOIN left")
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = selectRemapper.remapJoinExpressions(leftJoinNode, indentLevel+1)
	} else if leftJoinNode.GetRangeVar() != nil {
		leftJoinNode = selectRemapper.tableRemapper.RemapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1)
	}

	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" JOIN right")
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = selectRemapper.remapJoinExpressions(rightJoinNode, indentLevel+1)
	} else if rightJoinNode.GetRangeVar() != nil {
		rightJoinNode = selectRemapper.tableRemapper.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1)
	}

	return node
}

// WHERE [CONDITION]
func (selectRemapper *SelectRemapper) remapWhere(selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	schemaTable := selectRemapper.queryParser.NodeToSchemaTable(selectStatement.FromClause[0])

	// FROM pg_catalog.pg_namespace => FROM pg_catalog.pg_namespace WHERE nspname != 'main'
	if selectRemapper.queryParser.IsPgNamespaceTable(schemaTable) {
		withoutMainSchemaWhereCondition := selectRemapper.queryParser.MakeStringExpressionNode("nspname", "!=", "main")
		return selectRemapper.appendWhereCondition(selectStatement, withoutMainSchemaWhereCondition)
	}

	// FROM pg_catalog.pg_statio_user_tables -> return nothing
	if selectRemapper.queryParser.IsPgStatioUserTablesTable(schemaTable) {
		falseWhereCondition := selectRemapper.queryParser.MakeAConstBoolNode(false)
		selectStatement = selectRemapper.overrideWhereCondition(selectStatement, falseWhereCondition)
		return selectStatement
	}

	return selectStatement
}

// FROM [PG_FUNCTION()]
func (selectRemapper *SelectRemapper) remapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	for _, funcf := range node.GetRangeFunction().Functions {
		for _, item := range funcf.GetList().Items {
			functionCall := item.GetFuncCall()
			if len(functionCall.Funcname) == 2 {
				schema := functionCall.Funcname[0].GetString_().Sval
				functionName := functionCall.Funcname[1].GetString_().Sval

				// pg_catalog.pg_get_keywords() -> hard-coded keywords
				if selectRemapper.queryParser.IsPgGetKeywordsFunction(schema, functionName) {
					return selectRemapper.queryParser.MakePgGetKeywordsNode()
				}
			}
		}
	}
	return node
}

func (selectRemapper *SelectRemapper) appendWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
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

func (selectRemapper *SelectRemapper) overrideWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
	selectStatement.WhereClause = whereCondition
	return selectStatement
}

// SELECT [PG_FUNCTION()]
func (selectRemapper *SelectRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" SELECT functions")

	for _, targetItem := range selectStatement.TargetList {
		target := targetItem.GetResTarget()
		if target.Val.GetFuncCall() != nil {
			functionCall := target.Val.GetFuncCall()
			originalFunctionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval

			renamedFunctionCall := selectRemapper.remappedFunctionName(functionCall)
			if renamedFunctionCall != nil {
				functionCall = renamedFunctionCall
				if target.Name == "" {
					target.Name = originalFunctionName
				}
			}

			constantNode := selectRemapper.remappedConstantNode(functionCall)
			if constantNode != nil {
				target.Val = constantNode
				if target.Name == "" {
					target.Name = originalFunctionName
				}
			}

			functionCall = selectRemapper.remapFunctionCallArgs(functionCall, indentLevel+1)
		} else if target.Val.GetSubLink() != nil {
			subSelectStatement := target.Val.GetSubLink().Subselect.GetSelectStmt()
			subSelectStatement = selectRemapper.remapSelect(subSelectStatement, indentLevel+1)
		}
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapFunctionCallArgs(functionCall *pgQuery.FuncCall, indentLevel int) *pgQuery.FuncCall {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" SELECT function args")

	for i, arg := range functionCall.Args {
		if arg.GetFuncCall() != nil {
			argFunctionCall := arg.GetFuncCall()

			renamedFunctionCall := selectRemapper.remappedFunctionName(argFunctionCall)
			if renamedFunctionCall != nil {
				argFunctionCall = renamedFunctionCall
			}

			constantNode := selectRemapper.remappedConstantNode(argFunctionCall)
			if constantNode != nil {
				functionCall.Args[i] = constantNode
			}
			argFunctionCall = selectRemapper.remapFunctionCallArgs(argFunctionCall, indentLevel+1)
		}
	}

	return functionCall
}

func (selectRemapper *SelectRemapper) remappedFunctionName(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval

	if selectRemapper.queryParser.IsQuoteIdentFunction(functionName) {
		functionCall.Funcname[0] = pgQuery.MakeStrNode("concat")
		argConstant := functionCall.Args[0].GetAConst()
		if argConstant != nil {
			str := argConstant.GetSval().Sval
			str = "\"" + str + "\""
			functionCall.Args[0] = pgQuery.MakeAConstStrNode(str, 0)
		}

		return functionCall
	}

	return nil
}

func (selectRemapper *SelectRemapper) remappedConstantNode(functionCall *pgQuery.FuncCall) *pgQuery.Node {
	functionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval
	constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[functionName]
	if ok {
		return pgQuery.MakeAConstStrNode(constant, 0)
	}

	return nil
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
