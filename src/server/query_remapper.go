package main

import (
	"errors"
	"fmt"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"

	"github.com/BemiHQ/BemiDB/src/common"
)

var SUPPORTED_SET_STATEMENTS = common.NewSet[string]().AddAll([]string{
	"timezone", // SET SESSION timezone TO 'UTC'
})

var KNOWN_SET_STATEMENTS = common.NewSet[string]().AddAll([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"extra_float_digits",          // SET extra_float_digits = 3
	"application_name",            // SET application_name = 'psql'
	"datestyle",                   // SET datestyle TO 'ISO'
	"session characteristics",     // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED
})

var NOOP_QUERY_TREE, _ = pgQuery.Parse("SET TimeZone = 'UTC'")

type QueryRemapper struct {
	parserTypeCast     *ParserTypeCast
	remapperTable      *QueryRemapperTable
	remapperExpression *QueryRemapperExpression
	remapperFunction   *QueryRemapperFunction
	remapperSelect     *QueryRemapperSelect
	remapperShow       *QueryRemapperShow
	IcebergReader      *IcebergReader
	IcebergWriter      *IcebergWriter
	config             *Config
}

func NewQueryRemapper(config *Config, icebergReader *IcebergReader, icebergWriter *IcebergWriter, serverDuckdbClient *common.DuckdbClient) *QueryRemapper {
	return &QueryRemapper{
		parserTypeCast:     NewParserTypeCast(config),
		remapperTable:      NewQueryRemapperTable(config, icebergReader, serverDuckdbClient),
		remapperExpression: NewQueryRemapperExpression(config),
		remapperFunction:   NewQueryRemapperFunction(config, icebergReader),
		remapperSelect:     NewQueryRemapperSelect(config),
		remapperShow:       NewQueryRemapperShow(config),
		IcebergReader:      icebergReader,
		IcebergWriter:      icebergWriter,
		config:             config,
	}
}

func (remapper *QueryRemapper) ParseAndRemapQuery(query string) ([]string, []string, error) {
	queryTree, err := pgQuery.Parse(query)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't parse query: %s. %w", query, err)
	}

	if strings.HasSuffix(query, INSPECT_SQL_COMMENT) {
		common.LogDebug(remapper.config.CommonConfig, queryTree.Stmts)
	}

	var originalQueryStatements []string
	for _, stmt := range queryTree.Stmts {
		originalQueryStatement, err := pgQuery.Deparse(&pgQuery.ParseResult{Stmts: []*pgQuery.RawStmt{stmt}})
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't deparse query: %s. %w", query, err)
		}
		originalQueryStatements = append(originalQueryStatements, originalQueryStatement)
	}

	remappedStatements, err := remapper.remapStatements(queryTree.Stmts)
	if err != nil {
		return nil, nil, err
	}

	var queryStatements []string
	for _, remappedStatement := range remappedStatements {
		queryStatement, err := pgQuery.Deparse(&pgQuery.ParseResult{Stmts: []*pgQuery.RawStmt{remappedStatement}})
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't deparse remapped query: %s. %w", query, err)
		}
		queryStatements = append(queryStatements, queryStatement)
	}

	return queryStatements, originalQueryStatements, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (remapper *QueryRemapper) remapStatements(statements []*pgQuery.RawStmt) ([]*pgQuery.RawStmt, error) {
	// Empty query
	if len(statements) == 0 {
		return statements, nil
	}

	for i, stmt := range statements {
		common.LogTrace(remapper.config.CommonConfig, "Remapping statement #"+common.IntToString(i+1))

		node := stmt.Stmt

		switch {
		// Empty statement
		case node == nil:
			return nil, errors.New("empty statement")

		// SELECT
		case node.GetSelectStmt() != nil:
			selectStatement := node.GetSelectStmt()
			remapper.remapSelectStatement(selectStatement, 1)
			stmt.Stmt = &pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: selectStatement}}
			statements[i] = stmt

		// SET
		case node.GetVariableSetStmt() != nil:
			statements[i] = remapper.remapSetStatement(stmt)

		// DISCARD ALL
		case node.GetDiscardStmt() != nil:
			statements[i] = NOOP_QUERY_TREE.Stmts[0]

		// SHOW
		case node.GetVariableShowStmt() != nil:
			statements[i] = remapper.remapperShow.RemapShowStatement(stmt)

		// BEGIN
		case node.GetTransactionStmt() != nil:
			statements[i] = NOOP_QUERY_TREE.Stmts[0]

		// CREATE MATERIALIZED VIEW
		case node.GetCreateTableAsStmt() != nil:
			err := remapper.createMaterializedView(node)
			if err != nil {
				return nil, err
			}
			statements[i] = NOOP_QUERY_TREE.Stmts[0]

		// DROP MATERIALIZED VIEW
		case node.GetDropStmt() != nil && node.GetDropStmt().RemoveType == pgQuery.ObjectType_OBJECT_MATVIEW:
			err := remapper.dropMaterializedViewFromNode(node)
			if err != nil {
				return nil, err
			}
			statements[i] = NOOP_QUERY_TREE.Stmts[0]

		// REFRESH MATERIALIZED VIEW
		case node.GetRefreshMatViewStmt() != nil:
			err := remapper.refreshMaterializedViewFromNode(node)
			if err != nil {
				return nil, err
			}
			statements[i] = NOOP_QUERY_TREE.Stmts[0]

		// Unsupported query
		default:
			common.LogDebug(remapper.config.CommonConfig, "Query tree:", stmt, node)
			return nil, errors.New("unsupported query type")
		}
	}

	return statements, nil
}

// SET ... (no-op)
func (remapper *QueryRemapper) remapSetStatement(stmt *pgQuery.RawStmt) *pgQuery.RawStmt {
	setStatement := stmt.Stmt.GetVariableSetStmt()

	if SUPPORTED_SET_STATEMENTS.Contains(strings.ToLower(setStatement.Name)) {
		return stmt
	}

	if !KNOWN_SET_STATEMENTS.Contains(strings.ToLower(setStatement.Name)) {
		common.LogWarn(remapper.config.CommonConfig, "Unknown SET ", setStatement.Name, ":", setStatement)
	}

	return NOOP_QUERY_TREE.Stmts[0]
}

func (remapper *QueryRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) {
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
		remapper.traceTreeTraversal("WHERE statements", indentLevel)

		if remapper.removeWhereClause(selectStatement.WhereClause) {
			selectStatement.WhereClause = nil
		} else {
			selectStatement.WhereClause = remapper.remappedExpressions(selectStatement.WhereClause, indentLevel) // recursion
		}
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
				// FROM [TABLE]
				remapper.traceTreeTraversal("FROM table", indentLevel)
				selectStatement.FromClause[i] = remapper.remapperTable.RemapTable(fromNode)
			} else if fromNode.GetRangeSubselect() != nil {
				// FROM (SELECT ...)
				remapper.traceTreeTraversal("FROM subselect", indentLevel)
				subSelectStatement := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				remapper.remapSelectStatement(subSelectStatement, indentLevel+1) // self-recursion
			} else if fromNode.GetRangeFunction() != nil {
				// FROM PG_FUNCTION()
				remapper.traceTreeTraversal("FROM function()", indentLevel)
				remapper.remapperTable.RemapTableFunctionCall(fromNode.GetRangeFunction()) // recursion
			}
		}
	}

	// ORDER BY
	if selectStatement.SortClause != nil {
		remapper.traceTreeTraversal("ORDER BY statements", indentLevel)
		for _, sortNode := range selectStatement.SortClause {
			sortNode.GetSortBy().Node = remapper.remappedExpressions(sortNode.GetSortBy().Node, indentLevel) // recursion
		}
	}

	// GROUP BY
	if selectStatement.GroupClause != nil {
		remapper.traceTreeTraversal("GROUP BY statements", indentLevel)
		for i, groupNode := range selectStatement.GroupClause {
			selectStatement.GroupClause[i] = remapper.remappedExpressions(groupNode, indentLevel) // recursion
		}
	}

	// SELECT
	remapper.remapSelect(selectStatement, indentLevel) // recursion
}

func (remapper *QueryRemapper) remapJoinExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	remapper.traceTreeTraversal("JOIN left", indentLevel)
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = remapper.remapJoinExpressions(selectStatement, leftJoinNode, indentLevel+1) // self-recursion
	} else if leftJoinNode.GetRangeVar() != nil {
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
		// TABLE
		remapper.traceTreeTraversal("TABLE right", indentLevel+1)
		rightJoinNode = remapper.remapperTable.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		remapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Rarg = rightJoinNode

	if quals := node.GetJoinExpr().Quals; quals != nil {
		remapper.traceTreeTraversal("JOIN on", indentLevel)
		node.GetJoinExpr().Quals = remapper.remappedExpressions(quals, indentLevel) // recursion

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

func (remapper *QueryRemapper) remappedExpressions(node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	// CASE
	caseExpression := node.GetCaseExpr()
	if caseExpression != nil {
		remapper.remapCaseExpression(caseExpression, indentLevel) // recursion
	}

	// OR/AND
	boolExpr := node.GetBoolExpr()
	if boolExpr != nil {
		for i, arg := range boolExpr.Args {
			boolExpr.Args[i] = remapper.remappedExpressions(arg, indentLevel+1) // self-recursion
		}
	}

	// COALESCE(value1, value2, ...)
	coalesceExpr := node.GetCoalesceExpr()
	if coalesceExpr != nil {
		for _, arg := range coalesceExpr.Args {
			if arg.GetSubLink() != nil {
				// Nested SELECT
				subSelect := arg.GetSubLink().Subselect.GetSelectStmt()
				remapper.remapSelectStatement(subSelect, indentLevel+1) // recursion
			}
		}
	}

	// Nested SELECT
	subLink := node.GetSubLink()
	if subLink != nil {
		subSelect := subLink.Subselect.GetSelectStmt()
		remapper.remapSelectStatement(subSelect, indentLevel+1) // recursion
	}

	// Comparison
	aExpr := node.GetAExpr()
	if aExpr != nil {
		node = remapper.remapperExpression.RemappedExpression(node)
		if aExpr.Lexpr != nil {
			aExpr.Lexpr = remapper.remappedExpressions(aExpr.Lexpr, indentLevel+1) // self-recursion
		}
		if aExpr.Rexpr != nil {
			aExpr.Rexpr = remapper.remappedExpressions(aExpr.Rexpr, indentLevel+1) // self-recursion
		}
	}

	// IS NULL
	nullTest := node.GetNullTest()
	if nullTest != nil {
		nullTest.Arg = remapper.remappedExpressions(nullTest.Arg, indentLevel+1) // self-recursion
	}

	// IN
	list := node.GetList()
	if list != nil {
		for i, item := range list.Items {
			if item != nil {
				list.Items[i] = remapper.remapperExpression.RemappedExpression(item)
			}
		}
	}

	// FUNCTION(...)
	functionCall := node.GetFuncCall()
	if functionCall != nil {
		remapper.remapperFunction.RemapFunctionCall(functionCall)
		remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion

		for i, arg := range functionCall.Args {
			if arg != nil {
				functionCall.Args[i] = remapper.remappedExpressions(arg, indentLevel+1) // self-recursion
			}
		}

		if functionCall.AggFilter != nil && functionCall.AggFilter.GetNullTest() != nil {
			functionCall.AggFilter.GetNullTest().Arg = remapper.remappedExpressions(functionCall.AggFilter.GetNullTest().Arg, indentLevel+1) // self-recursion
		}
	}

	// (FUNCTION()).n
	indirectionFunctionCall := node.GetAIndirection()
	if indirectionFunctionCall != nil {
		functionCall := indirectionFunctionCall.Arg.GetFuncCall()
		if functionCall != nil {
			remapper.remapperFunction.RemapFunctionCall(functionCall)
			remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion
		}
	}

	return remapper.remapperExpression.RemappedExpression(node)
}

// CASE ...
func (remapper *QueryRemapper) remapCaseExpression(caseExpr *pgQuery.CaseExpr, indentLevel int) {
	for _, when := range caseExpr.Args {
		if whenClause := when.GetCaseWhen(); whenClause != nil {
			// WHEN ...
			if whenClause.Expr != nil {
				whenClause.Expr = remapper.remappedExpressions(whenClause.Expr, indentLevel+1) // recursion
			}

			// THEN ...
			if whenClause.Result != nil {
				whenClause.Result = remapper.remappedExpressions(whenClause.Result, indentLevel+1) // recursion
			}
		}
	}

	// ELSE ...
	if caseExpr.Defresult != nil {
		caseExpr.Defresult = remapper.remappedExpressions(caseExpr.Defresult, indentLevel+1) // recursion
	}
}

// SELECT ...
func (remapper *QueryRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	remapper.traceTreeTraversal("SELECT statements", indentLevel)

	// SELECT ...
	for targetNodeIdx, targetNode := range selectStatement.TargetList {
		targetNode = remapper.remapperSelect.SetDefaultTargetName(targetNode)

		valNode := targetNode.GetResTarget().Val
		if valNode != nil {
			targetNode.GetResTarget().Val = remapper.remappedExpressions(valNode, indentLevel) // recursion
		}

		// Nested SELECT
		if valNode.GetSubLink() != nil {
			subLink := valNode.GetSubLink()
			subSelect := subLink.Subselect.GetSelectStmt()

			// DuckDB doesn't work with ORDER BY in ARRAY subqueries:
			//   SELECT ARRAY(SELECT 1 FROM pg_enum ORDER BY enumsortorder)
			//   > Referenced column "enumsortorder" not found in FROM clause!
			//
			// Remove ORDER BY from ARRAY subqueries
			if subLink.SubLinkType == pgQuery.SubLinkType_ARRAY_SUBLINK && subSelect.SortClause != nil {
				subSelect.SortClause = nil
			}
		}

		selectStatement.TargetList[targetNodeIdx] = targetNode
	}

	// VALUES (...)
	if len(selectStatement.ValuesLists) > 0 {
		for i, valuesList := range selectStatement.ValuesLists {
			for j, value := range valuesList.GetList().Items {
				if value != nil {
					selectStatement.ValuesLists[i].GetList().Items[j] = remapper.remapperExpression.RemappedExpression(value)
				}
			}
		}
	}

	return selectStatement
}

// Fix the query sent by psql "\d [table]": ... WHERE attrelid = pr.prrelid AND attnum = prattrs[s] ...
// DuckDB fails with the following errors:
//
// 1) INTERNAL Error: Failed to bind column reference "prrelid" [93.3] (bindings: {#[101.0], #[101.1], #[101.2]}) This error signals an assertion failure within DuckDB. This usually occurs due to unexpected conditions or errors in the program's logic.
// 2) Binder Error: No function matches the given name and argument types 'array_extract(VARCHAR, STRUCT(generate_series BIGINT))'. You might need to add explicit type casts.
func (remapper *QueryRemapper) removeWhereClause(whereClause *pgQuery.Node) bool {
	boolExpr := whereClause.GetBoolExpr()
	if boolExpr == nil || boolExpr.Boolop != pgQuery.BoolExprType_AND_EXPR || len(boolExpr.Args) != 2 {
		return false
	}

	arg1 := boolExpr.Args[0].GetAExpr()
	if arg1 == nil || arg1.Kind != pgQuery.A_Expr_Kind_AEXPR_OP || arg1.Name[0].GetString_().Sval != "=" {
		return false
	}

	arg2 := boolExpr.Args[1].GetAExpr()
	if arg2 == nil || arg2.Kind != pgQuery.A_Expr_Kind_AEXPR_OP || arg2.Name[0].GetString_().Sval != "=" {
		return false
	}

	arg1LColRef := arg1.Lexpr.GetColumnRef()
	if arg1LColRef == nil || len(arg1LColRef.Fields) != 1 || arg1LColRef.Fields[0].GetString_().Sval != "attrelid" {
		return false
	}

	arg1RColRef := arg1.Rexpr.GetColumnRef()
	if arg1RColRef == nil || len(arg1RColRef.Fields) != 2 || arg1RColRef.Fields[0].GetString_().Sval != "pr" || arg1RColRef.Fields[1].GetString_().Sval != "prrelid" {
		return false
	}

	arg2LColRef := arg2.Lexpr.GetColumnRef()
	if arg2LColRef == nil || len(arg2LColRef.Fields) != 1 || arg2LColRef.Fields[0].GetString_().Sval != "attnum" {
		return false
	}

	arg2RIndir := arg2.Rexpr.GetAIndirection()
	if arg2RIndir == nil || arg2RIndir.Arg.GetColumnRef() == nil || len(arg2RIndir.Arg.GetColumnRef().Fields) != 1 || arg2RIndir.Arg.GetColumnRef().Fields[0].GetString_().Sval != "prattrs" {
		return false
	}
	if len(arg2RIndir.Indirection) != 1 || arg2RIndir.Indirection[0].GetAIndices() == nil || arg2RIndir.Indirection[0].GetAIndices().Uidx.GetColumnRef() == nil || len(arg2RIndir.Indirection[0].GetAIndices().Uidx.GetColumnRef().Fields) != 1 || arg2RIndir.Indirection[0].GetAIndices().Uidx.GetColumnRef().Fields[0].GetString_().Sval != "s" {
		return false
	}

	return true
}

func (remapper *QueryRemapper) createMaterializedView(node *pgQuery.Node) error {
	// Extract the schema and table names
	icebergSchemaTable := common.IcebergSchemaTable{
		Schema: node.GetCreateTableAsStmt().Into.Rel.Schemaname,
		Table:  node.GetCreateTableAsStmt().Into.Rel.Relname,
	}
	if icebergSchemaTable.Schema == "" {
		icebergSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}

	// Extract the definition of the materialized view
	definitionSelectStmt := node.GetCreateTableAsStmt().Query.GetSelectStmt()
	definitionRawStmt := &pgQuery.RawStmt{Stmt: &pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: definitionSelectStmt}}}
	definition, err := pgQuery.Deparse(&pgQuery.ParseResult{Stmts: []*pgQuery.RawStmt{definitionRawStmt}})
	if err != nil {
		return fmt.Errorf("couldn't read definition of CREATE MATERIALIZED VIEW: %w", err)
	}

	// Store the materialized view in the catalog
	err = remapper.IcebergWriter.CreateMaterializedView(icebergSchemaTable, definition)
	if err != nil {
		if strings.HasPrefix(err.Error(), "ERROR: duplicate key value violates unique constraint") {
			return fmt.Errorf("relation %s already exists", icebergSchemaTable.String())
		} else {
			return fmt.Errorf("couldn't create materialized view: %w", err)
		}
	}

	// Refresh the materialized view if it is not a "CREATE MATERIALIZED VIEW ... WITH NO DATA" statement
	if !node.GetCreateTableAsStmt().Into.SkipData {
		queryStatements, _, err := remapper.ParseAndRemapQuery(definition)
		if err != nil {
			deleteErr := remapper.IcebergWriter.DropMaterializedView(icebergSchemaTable)
			if deleteErr != nil {
				return fmt.Errorf("couldn't remap definition of CREATE MATERIALIZED VIEW: %w (%w)", err, deleteErr)
			}
			return fmt.Errorf("couldn't remap definition of CREATE MATERIALIZED VIEW: %w", err)
		}

		err = remapper.IcebergWriter.RefreshMaterializedView(icebergSchemaTable, queryStatements[0])
		if err != nil {
			deleteErr := remapper.IcebergWriter.DropMaterializedView(icebergSchemaTable)
			if deleteErr != nil {
				return fmt.Errorf("couldn't refresh materialized view: %w (%w)", err, deleteErr)
			}
			return fmt.Errorf("couldn't refresh materialized view: %w", err)
		}
	}

	return nil
}

func (remapper *QueryRemapper) dropMaterializedViewFromNode(node *pgQuery.Node) error {
	var icebergSchemaTable common.IcebergSchemaTable
	nodeItems := node.GetDropStmt().Objects[0].GetList().Items
	if len(nodeItems) == 2 {
		icebergSchemaTable = common.IcebergSchemaTable{
			Schema: nodeItems[0].GetString_().Sval,
			Table:  nodeItems[1].GetString_().Sval,
		}
	} else {
		icebergSchemaTable = common.IcebergSchemaTable{
			Schema: PG_SCHEMA_PUBLIC,
			Table:  nodeItems[0].GetString_().Sval,
		}
	}

	// Delete the materialized view from the catalog
	err := remapper.IcebergWriter.DropMaterializedView(icebergSchemaTable)
	if err != nil {
		return err
	}

	return nil
}

func (remapper *QueryRemapper) refreshMaterializedViewFromNode(node *pgQuery.Node) error {
	icebergSchemaTable := common.IcebergSchemaTable{
		Schema: node.GetRefreshMatViewStmt().Relation.Schemaname,
		Table:  node.GetRefreshMatViewStmt().Relation.Relname,
	}
	if icebergSchemaTable.Schema == "" {
		icebergSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}

	materializedView, err := remapper.IcebergReader.MaterializedView(icebergSchemaTable)
	if err != nil {
		return err
	}

	queryStatements, _, err := remapper.ParseAndRemapQuery(materializedView.Definition)
	if err != nil {
		return fmt.Errorf("couldn't remap definition of REFRESH MATERIALIZED VIEW: %w", err)
	}

	if node.GetRefreshMatViewStmt().Concurrent {
		go func() {
			err := remapper.IcebergWriter.RefreshMaterializedView(icebergSchemaTable, queryStatements[0])
			if err != nil {
				common.LogError(remapper.config.CommonConfig, "couldn't refresh materialized view concurrently: %s", err)
			}
		}()
	} else {
		err = remapper.IcebergWriter.RefreshMaterializedView(icebergSchemaTable, queryStatements[0])
		if err != nil {
			return fmt.Errorf("couldn't refresh materialized view: %w", err)
		}
	}

	return nil
}

func (remapper *QueryRemapper) traceTreeTraversal(label string, indentLevel int) {
	common.LogTrace(remapper.config.CommonConfig, strings.Repeat(">", indentLevel), label)
}
