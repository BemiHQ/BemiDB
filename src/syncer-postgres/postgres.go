package main

import (
	"context"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	PG_SCHEMA_PUBLIC     = "public"
	PG_SCHEMA_PG_CATALOG = "pg_catalog"

	PG_CONNECTION_TIMEOUT = 30 * time.Second
	PG_SESSION_TIMEOUT    = "2h"

	POSTGRES_MAX_RETRY_COUNT = 1
)

type Postgres struct {
	PostgresClient *common.PostgresClient
	Config         *Config
}

func NewPostgres(config *Config) *Postgres {
	postgres := Postgres{Config: config}
	postgres.Reconnect()
	return &postgres
}

func (postgres *Postgres) Close() {
	postgres.PostgresClient.Close()
}

func (postgres *Postgres) ReplicationSlotExists(slotName string) bool {
	var slotExists bool
	err := postgres.PostgresClient.QueryRow(context.Background(), "SELECT TRUE FROM pg_replication_slots WHERE slot_name = '"+slotName+"'").Scan(&slotExists)
	if err != nil && err.Error() == "no rows in result set" {
		return false
	}
	common.PanicIfError(postgres.Config.CommonConfig, err)
	return slotExists
}

func (postgres *Postgres) CreateReplicationSlot(slotName string) {
	_, err := postgres.PostgresClient.Exec(context.Background(), "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName)
	common.PanicIfError(postgres.Config.CommonConfig, err)
}

func (postgres *Postgres) Schemas() []string {
	var schemas []string

	schemasRows, err := postgres.PostgresClient.Query(
		context.Background(),
		`SELECT schema_name
		FROM information_schema.schemata
		WHERE
			schema_name NOT IN ('pg_catalog', 'pg_toast', 'information_schema') AND
			has_schema_privilege(current_user, schema_name, 'USAGE')`,
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
		WHERE
			pg_namespace.nspname = $1 AND
			pg_class.relkind = 'r' AND
			has_table_privilege(pg_class.oid, 'SELECT')
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

func (postgres *Postgres) PgSchemaColumns(pgSchemaTable PgSchemaTable, retryCount ...int) []PgSchemaColumn {
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
			pg_namespace.nspname
		FROM information_schema.columns
		JOIN pg_type ON pg_type.typname = columns.udt_name
		JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
		WHERE columns.table_schema = $1 AND columns.table_name = $2
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
		)
		common.PanicIfError(postgres.Config.CommonConfig, err)
		pgSchemaColumns = append(pgSchemaColumns, *pgSchemaColumn)
	}

	var joinedUniqueColumnNames string
	err = postgres.PostgresClient.QueryRow(
		context.Background(),
		`SELECT array_to_string(array_agg(a.attname), ',') as columns
		FROM pg_class t
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN unnest(ix.indkey) WITH ORDINALITY AS c(colnum, ordinality) ON true
		JOIN pg_attribute a ON t.oid = a.attrelid AND a.attnum = c.colnum
		WHERE ix.indisunique = true AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1) AND t.relname = $2
		GROUP BY t.relname, ix.indexrelid
		ORDER BY
			array_length(array_agg(a.attname), 1),
			CASE WHEN array_to_string(array_agg(a.attname), ', ') ILIKE '%id' THEN 0 ELSE 1 END,
			array_to_string(array_agg(a.attname), ',')`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
	).Scan(&joinedUniqueColumnNames)
	if err != nil {
		if err.Error() == "no rows in result set" {
			joinedUniqueColumnNames = ""
		} else {
			if strings.Contains(err.Error(), "terminating connection due to conflict with recovery (SQLSTATE 40001)") ||
				strings.Contains(err.Error(), "failed to deallocate cached statement(s): conn closed") {
				currentRetryCount := 0
				if len(retryCount) > 0 {
					currentRetryCount = retryCount[0]
				}

				if currentRetryCount < POSTGRES_MAX_RETRY_COUNT {
					common.LogWarn(postgres.Config.CommonConfig, "Retrying PgSchemaColumns() for table "+pgSchemaTable.String()+" due to failure:", err)
					postgres.Reconnect()
					return postgres.PgSchemaColumns(pgSchemaTable, currentRetryCount+1)
				}
			}
			common.PanicIfError(postgres.Config.CommonConfig, err)
		}
	}

	uniqueColumnNames := common.NewSet[string]()
	uniqueColumnNames.AddAll(strings.Split(joinedUniqueColumnNames, ","))
	if uniqueColumnNames.IsEmpty() {
		common.Panic(postgres.Config.CommonConfig, "No unique columns found for table "+pgSchemaTable.String())
	} else {
		common.LogInfo(postgres.Config.CommonConfig, "Unique columns for table "+pgSchemaTable.String()+":", joinedUniqueColumnNames)
	}

	for i := range pgSchemaColumns {
		pgSchemaColumns[i].IsPartOfUniqueIndex = uniqueColumnNames.Contains(pgSchemaColumns[i].ColumnName)
	}

	return pgSchemaColumns
}

func (postgres *Postgres) Reconnect() {
	if postgres.PostgresClient != nil {
		postgres.Close()
	}
	postgres.PostgresClient = common.NewPostgresClient(postgres.Config.CommonConfig, postgres.Config.DatabaseUrl)

	_, err := postgres.PostgresClient.Exec(context.Background(), "SET SESSION statement_timeout = '"+PG_SESSION_TIMEOUT+"'")
	common.PanicIfError(postgres.Config.CommonConfig, err)

	var isStandby bool
	err = postgres.PostgresClient.QueryRow(context.Background(), "SELECT pg_is_in_recovery()").Scan(&isStandby)
	common.PanicIfError(postgres.Config.CommonConfig, err)

	if isStandby {
		_, err = postgres.PostgresClient.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
		common.PanicIfError(postgres.Config.CommonConfig, err)
	} else {
		_, err = postgres.PostgresClient.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
		common.PanicIfError(postgres.Config.CommonConfig, err)
	}
}
