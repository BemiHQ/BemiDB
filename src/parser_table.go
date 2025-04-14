package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserTable struct {
	config *Config
	utils  *ParserUtils
}

func NewParserTable(config *Config) *ParserTable {
	return &ParserTable{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserTable) NodeToQuerySchemaTable(node *pgQuery.Node) QuerySchemaTable {
	rangeVar := node.GetRangeVar()
	var alias string

	if rangeVar.Alias != nil {
		alias = rangeVar.Alias.Aliasname
	}

	return QuerySchemaTable{
		Schema: rangeVar.Schemaname,
		Table:  rangeVar.Relname,
		Alias:  alias,
	}
}

func (parser *ParserTable) RemapSchemaToMain(node *pgQuery.Node) {
	node.GetRangeVar().Schemaname = DUCKDB_SCHEMA_MAIN
}

// Other information_schema.* tables
func (parser *ParserTable) IsTableFromInformationSchema(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_INFORMATION_SCHEMA
}

// public.table -> FROM iceberg_scan('path', skip_schema_inference = true) table
// schema.table -> FROM iceberg_scan('path', skip_schema_inference = true) schema_table
func (parser *ParserTable) MakeIcebergTableNode(tablePath string, qSchemaTable QuerySchemaTable) *pgQuery.Node {
	node := pgQuery.MakeSimpleRangeFunctionNode([]*pgQuery.Node{
		pgQuery.MakeListNode([]*pgQuery.Node{
			pgQuery.MakeFuncCallNode(
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("iceberg_scan"),
				},
				[]*pgQuery.Node{
					pgQuery.MakeAConstStrNode(
						tablePath,
						0,
					),
					pgQuery.MakeAExprNode(
						pgQuery.A_Expr_Kind_AEXPR_OP,
						[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
						pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("skip_schema_inference")}, 0),
						parser.utils.MakeAConstBoolNode(true),
						0,
					),
				},
				0,
			),
		}),
	})

	// DuckDB doesn't support aliases on iceberg_scan() functions, so we need to wrap it in a nested select that can have an alias
	selectStarNode := pgQuery.MakeResTargetNodeWithVal(
		pgQuery.MakeColumnRefNode(
			[]*pgQuery.Node{pgQuery.MakeAStarNode()},
			0,
		),
		0,
	)
	return parser.utils.MakeSubselectFromNode(qSchemaTable, []*pgQuery.Node{selectStarNode}, node)
}

func (parser *ParserTable) TopLevelSchemaFunction(rangeFunction *pgQuery.RangeFunction) *QuerySchemaFunction {
	if len(rangeFunction.Functions) == 0 || len(rangeFunction.Functions[0].GetList().Items) == 0 {
		return nil
	}

	functionNode := rangeFunction.Functions[0].GetList().Items[0]
	if functionNode.GetFuncCall() == nil {
		return nil // E.g., system PG calls like "... FROM user" => sqlvalue_function:{op:SVFOP_USER}
	}

	return parser.utils.SchemaFunction(functionNode.GetFuncCall())
}

func (parser *ParserTable) TableFunctionCalls(rangeFunction *pgQuery.RangeFunction) []*pgQuery.FuncCall {
	functionCalls := []*pgQuery.FuncCall{}

	for _, funcNode := range rangeFunction.Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			functionCall := funcItemNode.GetFuncCall()
			if functionCall != nil {
				functionCalls = append(functionCalls, functionCall)
			}
		}
	}

	return functionCalls
}

func (parser *ParserTable) SetAliasIfNotExists(rangeFunction *pgQuery.RangeFunction, alias string) {
	if rangeFunction.GetAlias() != nil {
		return
	}

	rangeFunction.Alias = &pgQuery.Alias{Aliasname: alias}
}
