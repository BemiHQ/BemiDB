package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRemapperExpression struct {
	parserTypeCast  *ParserTypeCast
	parserColumnRef *ParserColumnRef
	parserAExpr     *ParserAExpr
	config          *Config
}

func NewQueryRemapperExpression(config *Config) *QueryRemapperExpression {
	remapper := &QueryRemapperExpression{
		parserTypeCast:  NewParserTypeCast(config),
		parserColumnRef: NewParserColumnRef(config),
		parserAExpr:     NewParserAExpr(config),
		config:          config,
	}
	return remapper
}

func (remapper *QueryRemapperExpression) RemappedExpression(node *pgQuery.Node) *pgQuery.Node {
	node = remapper.remappedTypeCast(node)
	node = remapper.remappedArithmeticExpression(node)
	node = remapper.remappedCollateClause(node)
	node = remapper.remappedNullColumnExpression(node)
	remapper.remapColumnReference(node)

	return node
}

// value::type or CAST(value AS type)
func (remapper *QueryRemapperExpression) remappedTypeCast(node *pgQuery.Node) *pgQuery.Node {
	typeCast := remapper.parserTypeCast.TypeCast(node)
	if typeCast == nil {
		return node
	}

	remapper.parserTypeCast.RemovePgCatalog(typeCast)
	typeName := remapper.parserTypeCast.TypeName(typeCast)

	switch typeName {
	case "text[]":
		// '{a,b,c}'::text[] -> ARRAY['a', 'b', 'c']
		return remapper.parserTypeCast.MakeListValueFromArray(typeCast.Arg)
	case "regproc":
		// 'schema.function_name'::regproc -> 'function_name'
		nameParts := strings.Split(remapper.parserTypeCast.ArgStringValue(typeCast), ".")
		return pgQuery.MakeAConstStrNode(nameParts[len(nameParts)-1], 0)
	case "regclass":
		// 'schema.table'::regclass -> SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'schema' AND c.relname = 'table'
		return remapper.parserTypeCast.MakeSubselectOidBySchemaTableArg(typeCast.Arg)
	case "oid":
		// 'schema.table'::regclass::oid -> SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'schema' AND c.relname = 'table'
		nestedTypeCast := remapper.parserTypeCast.NestedTypeCast(typeCast)
		remapper.parserTypeCast.RemovePgCatalog(nestedTypeCast)
		nestedTypeName := remapper.parserTypeCast.TypeName(nestedTypeCast)
		if nestedTypeName != "regclass" {
			return node
		}
		return remapper.parserTypeCast.MakeSubselectOidBySchemaTableArg(nestedTypeCast.Arg)
	case "text":
		// value::(regtype|regnamespace|regclass)::text -> value::text
		nestedTypeCast := remapper.parserTypeCast.NestedTypeCast(typeCast)
		remapper.parserTypeCast.RemovePgCatalog(nestedTypeCast)
		nestedTypeName := remapper.parserTypeCast.TypeName(nestedTypeCast)
		if nestedTypeName != "regtype" && nestedTypeName != "regnamespace" && nestedTypeName != "regclass" {
			return node
		}
		remapper.parserTypeCast.SetTypeCastArg(typeCast, nestedTypeCast.Arg)
	}

	return node
}

func (remapper *QueryRemapperExpression) remappedArithmeticExpression(node *pgQuery.Node) *pgQuery.Node {
	aExpr := remapper.parserAExpr.AExpr(node)
	if aExpr == nil {
		return node
	}

	// = ANY({schema_information}) -> IN (schema_information)
	node = remapper.parserAExpr.ConvertedRightAnyToIn(node)

	// pg_catalog.[operator] -> [operator]
	remapper.parserAExpr.RemovePgCatalog(node)

	return node
}

// DuckDB v2 doesn't return results with "WHERE [column] IS NOT NULL"
// * [column] IS NOT NULL -> NOT [column] IS NULL
// * [column] IS NULL -> NOT [column] IS NOT NULL
func (remapper *QueryRemapperExpression) remappedNullColumnExpression(node *pgQuery.Node) *pgQuery.Node {
	nullTest := node.GetNullTest()
	if nullTest == nil || nullTest.Arg == nil || nullTest.Arg.GetColumnRef() == nil {
		return node
	}

	if nullTest.Nulltesttype == pgQuery.NullTestType_IS_NOT_NULL {
		nullTest.Nulltesttype = pgQuery.NullTestType_IS_NULL
	} else if nullTest.Nulltesttype == pgQuery.NullTestType_IS_NULL {
		nullTest.Nulltesttype = pgQuery.NullTestType_IS_NOT_NULL
	}

	return remapper.parserColumnRef.NotBooleanExpression(node)
}

// public.table.column -> table.column
// schema.table.column -> schema_table.column
func (remapper *QueryRemapperExpression) remapColumnReference(node *pgQuery.Node) {
	fieldNames := remapper.parserColumnRef.FieldNames(node)
	if fieldNames == nil || len(fieldNames) != 3 {
		return
	}

	schema := fieldNames[0]
	if schema == PG_SCHEMA_PG_CATALOG || schema == PG_SCHEMA_INFORMATION_SCHEMA {
		return
	}

	table := fieldNames[1]
	column := fieldNames[2]
	if schema == PG_SCHEMA_PUBLIC {
		remapper.parserColumnRef.SetFields(node, []string{table, column})
		return
	}

	remapper.parserColumnRef.SetFields(node, []string{schema + "_" + table, column})
}

// "value" COLLATE pg_catalog.default -> "value"
func (remapper *QueryRemapperExpression) remappedCollateClause(node *pgQuery.Node) *pgQuery.Node {
	if node.GetCollateClause() == nil {
		return node
	}

	return remapper.parserTypeCast.RemovedDefaultCollateClause(node)
}
