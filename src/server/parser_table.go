package main

import (
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type QueryToIcebergTable struct {
	QuerySchemaTable QuerySchemaTable
	IcebergTablePath string
}

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

// public.table -> (SELECT * FROM iceberg_scan('path')) table
// schema.table -> (SELECT * FROM iceberg_scan('path')) schema_table
// public.table -> (SELECT permitted, columns FROM iceberg_scan('path')) table
// public.table -> (SELECT NULL WHERE FALSE) table
// public.table t -> (SELECT * FROM iceberg_scan('path')) t
func (parser *ParserTable) MakeIcebergTableNode(queryToIcebergTable QueryToIcebergTable, permissions *map[string][]string) *pgQuery.Node {
	var query string
	if permissions == nil {
		query = "SELECT * FROM iceberg_scan('" + queryToIcebergTable.IcebergTablePath + "')"
	} else if columnNames, allowed := (*permissions)[queryToIcebergTable.QuerySchemaTable.ToIcebergSchemaTable().ToArg()]; allowed {
		quotedColumnNames := make([]string, len(columnNames))
		for i, columnName := range columnNames {
			quotedColumnNames[i] = "\"" + columnName + "\""
		}
		query = "SELECT " + strings.Join(quotedColumnNames, ", ") + " FROM iceberg_scan('" + queryToIcebergTable.IcebergTablePath + "')"
	} else {
		query = "SELECT NULL WHERE FALSE"
	}

	return parser.makeSubselectNode(query, queryToIcebergTable.QuerySchemaTable)
}

// information_schema.tables -> (SELECT * FROM main.tables) information_schema_tables
// information_schema.tables -> (SELECT * FROM main.tables WHERE table_schema || '.' || table_name IN ('permitted.table')) information_schema_tables
// information_schema.tables t -> (SELECT * FROM main.tables) t
func (parser *ParserTable) MakeInformationSchemaTablesNode(qSchemaTable QuerySchemaTable, permissions *map[string][]string) *pgQuery.Node {
	query := "SELECT * FROM main.tables"

	if permissions != nil {
		quotedSchemaTableNames := []string{}
		for schemaTable := range *permissions {
			quotedSchemaTableNames = append(quotedSchemaTableNames, "'"+schemaTable+"'")
		}
		query += " WHERE table_schema || '.' || table_name IN (" + strings.Join(quotedSchemaTableNames, ", ") + ")"
	}

	return parser.makeSubselectNode(query, qSchemaTable)
}

// information_schema.columns -> (SELECT * FROM main.columns) information_schema_columns
// information_schema.columns -> (SELECT * FROM main.columns WHERE (table_schema || '.' || table_name IN ('permitted.table') AND column_name IN ('permitted', 'columns')) OR ...) information_schema_columns
// information_schema.columns c -> (SELECT * FROM main.columns) c
func (parser *ParserTable) MakeInformationSchemaColumnsNode(qSchemaTable QuerySchemaTable, permissions *map[string][]string) *pgQuery.Node {
	query := "SELECT * FROM main.columns"

	if permissions != nil {
		conditions := []string{}
		for schemaTable, columnNames := range *permissions {
			quotedColumnNames := []string{}
			for _, columnName := range columnNames {
				quotedColumnNames = append(quotedColumnNames, "'"+columnName+"'")
			}
			conditions = append(conditions, "(table_schema || '.' || table_name = '"+schemaTable+"' AND column_name IN ("+strings.Join(quotedColumnNames, ", ")+"))")
		}
		query += " WHERE " + strings.Join(conditions, " OR ")
	}

	return parser.makeSubselectNode(query, qSchemaTable)
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

func (parser *ParserTable) Alias(rangeFunction *pgQuery.RangeFunction) string {
	if rangeFunction.GetAlias() != nil {
		return rangeFunction.GetAlias().Aliasname
	}

	return ""
}

func (parser *ParserTable) SetAlias(rangeFunction *pgQuery.RangeFunction, alias string, columnName string) {
	rangeFunction.Alias = &pgQuery.Alias{
		Aliasname: alias,
		Colnames:  []*pgQuery.Node{pgQuery.MakeStrNode(columnName)},
	}
}

func (parser *ParserTable) SetAliasIfNotExists(rangeFunction *pgQuery.RangeFunction, alias string) {
	if rangeFunction.GetAlias() != nil {
		return
	}

	rangeFunction.Alias = &pgQuery.Alias{Aliasname: alias}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// (query) AS qSchemaTable
func (parser *ParserTable) makeSubselectNode(query string, qSchemaTable QuerySchemaTable) *pgQuery.Node {
	queryTree, err := pgQuery.Parse(query)
	common.PanicIfError(parser.config.CommonConfig, err)

	alias := qSchemaTable.Alias
	if alias == "" {
		if qSchemaTable.Schema == PG_SCHEMA_PUBLIC || qSchemaTable.Schema == "" {
			alias = qSchemaTable.Table
		} else {
			alias = qSchemaTable.Schema + "_" + qSchemaTable.Table
		}
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: queryTree.Stmts[0].Stmt.GetSelectStmt(),
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
				},
			},
		},
	}
}
