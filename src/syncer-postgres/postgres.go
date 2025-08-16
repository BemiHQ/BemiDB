package main

import (
	"context"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	PG_SCHEMA_PUBLIC     = "public"
	PG_SCHEMA_PG_CATALOG = "pg_catalog"

	PG_CONNECTION_TIMEOUT = 30 * time.Second
	PG_SESSION_TIMEOUT    = "2h"
)

type Postgres struct {
	PostgresClient *common.PostgresClient
	Config         *Config
}

func NewPostgres(config *Config) *Postgres {
	postgresClient := common.NewPostgresClient(config.CommonConfig, config.DatabaseUrl)

	_, err := postgresClient.Exec(context.Background(), "SET SESSION statement_timeout = '"+PG_SESSION_TIMEOUT+"'")
	common.PanicIfError(config.CommonConfig, err)

	return &Postgres{
		Config:         config,
		PostgresClient: postgresClient,
	}
}

func (postgres *Postgres) Close() {
	postgres.PostgresClient.Close()
}

func (postgres *Postgres) Schemas() []string {
	var schemas []string

	schemasRows, err := postgres.PostgresClient.Query(
		context.Background(),
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'pg_toast', 'information_schema')",
	)
	common.PanicIfError(postgres.Config.CommonConfig, err)
	defer schemasRows.Close()

	for schemasRows.Next() {
		var schema string
		err = schemasRows.Scan(&schema)
		common.PanicIfError(postgres.Config.CommonConfig, err)
		schemas = append(schemas, schema)
	}

	return schemas
}

func (postgres *Postgres) SchemaTables(schema string) []PgSchemaTable {
	var pgSchemaTables []PgSchemaTable

	tablesRows, err := postgres.PostgresClient.Query(
		context.Background(),
		`
		SELECT pg_class.relname AS table, COALESCE(parent.relname, '') AS parent_partitioned_table
		FROM pg_class
		JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
		LEFT JOIN pg_inherits ON pg_inherits.inhrelid = pg_class.oid
		LEFT JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
		WHERE pg_namespace.nspname = $1 AND pg_class.relkind = 'r' AND has_table_privilege(pg_class.oid, 'SELECT')
		`,
		schema,
	)
	common.PanicIfError(postgres.Config.CommonConfig, err)
	defer tablesRows.Close()

	for tablesRows.Next() {
		pgSchemaTable := PgSchemaTable{Schema: schema}
		err = tablesRows.Scan(&pgSchemaTable.Table, &pgSchemaTable.ParentPartitionedTable)
		common.PanicIfError(postgres.Config.CommonConfig, err)
		pgSchemaTables = append(pgSchemaTables, pgSchemaTable)
	}

	return pgSchemaTables
}

func (postgres *Postgres) PgSchemaColumns(pgSchemaTable PgSchemaTable) []PgSchemaColumn {
	var pgSchemaColumns []PgSchemaColumn

	rows, err := postgres.PostgresClient.Query(
		context.Background(),
		`SELECT
			columns.column_name,
			columns.data_type,
			columns.udt_name,
			columns.is_nullable,
			columns.ordinal_position,
			COALESCE(columns.numeric_precision, 0),
			COALESCE(columns.numeric_scale, 0),
			COALESCE(columns.datetime_precision, 0),
			pg_namespace.nspname,
			CASE WHEN pk.constraint_name IS NOT NULL THEN true ELSE false END
		FROM information_schema.columns
		JOIN pg_type ON pg_type.typname = columns.udt_name
		JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
		LEFT JOIN (
			SELECT
				tc.constraint_name,
				kcu.column_name,
				kcu.table_schema,
				kcu.table_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
				AND tc.table_name = kcu.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
		) pk ON pk.column_name = columns.column_name AND pk.table_schema = columns.table_schema AND pk.table_name = columns.table_name
		WHERE columns.table_schema = $1 AND columns.table_name = $2 AND columns.is_generated = 'NEVER'
		ORDER BY columns.ordinal_position`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
	)
	common.PanicIfError(postgres.Config.CommonConfig, err)
	defer rows.Close()

	for rows.Next() {
		pgSchemaColumn := NewPgSchemaColumn(postgres.Config)
		err = rows.Scan(
			&pgSchemaColumn.ColumnName,
			&pgSchemaColumn.DataType,
			&pgSchemaColumn.UdtName,
			&pgSchemaColumn.IsNullable,
			&pgSchemaColumn.OrdinalPosition,
			&pgSchemaColumn.NumericPrecision,
			&pgSchemaColumn.NumericScale,
			&pgSchemaColumn.DatetimePrecision,
			&pgSchemaColumn.Namespace,
			&pgSchemaColumn.PartOfPrimaryKey,
		)
		common.PanicIfError(postgres.Config.CommonConfig, err)
		pgSchemaColumns = append(pgSchemaColumns, *pgSchemaColumn)
	}

	return pgSchemaColumns
}
