package main

import (
	"context"
	"strings"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
	"github.com/jackc/pgx/v5"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"
)

type SyncerFullRefresh struct {
	Config *Config
	Utils  *SyncerUtils
}

func NewSyncerFullRefresh(config *Config) *SyncerFullRefresh {
	return &SyncerFullRefresh{
		Config: config,
		Utils:  NewSyncerUtils(config),
	}
}

func (syncer *SyncerFullRefresh) Sync(postgres *Postgres, trino *common.Trino, pgSchemaTables []PgSchemaTable) {
	_, err := postgres.Conn.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
	common.PanicIfError(syncer.Config.BaseConfig, err)

	icebergTableNames := common.NewSet[string]()

	for _, pgSchemaTable := range pgSchemaTables {
		pgSchemaColumns := postgres.PgSchemaColumns(pgSchemaTable)

		common.LogInfo(syncer.Config.BaseConfig, "Syncing table:", pgSchemaTable.String()+"...")
		syncer.syncTable(postgres, trino, pgSchemaTable, pgSchemaColumns)

		icebergTableNames.Add(pgSchemaTable.IcebergTableName())
	}

	syncer.Utils.DropOldTables(trino, icebergTableNames)
}

func (syncer *SyncerFullRefresh) syncTable(postgres *Postgres, trino *common.Trino, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn) {
	// Create a capped buffer read and written in parallel
	cappedBuffer := common.NewCappedBuffer(syncer.Config.BaseConfig, MAX_IN_MEMORY_BUFFER_SIZE)

	// Copy from PG to cappedBuffer in a separate goroutine in parallel
	go func() {
		syncer.copyFromPgTable(postgres.Conn, pgSchemaTable, cappedBuffer)
	}()

	// Read from cappedBuffer and write to Iceberg
	syncer.writeToIceberg(trino, pgSchemaTable, pgSchemaColumns, cappedBuffer)
}

func (syncer *SyncerFullRefresh) writeToIceberg(trino *common.Trino, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn, cappedBuffer *common.CappedBuffer) {
	columnSchemas := []string{}
	for _, pgSchemaColumn := range pgSchemaColumns {
		columnSchemas = append(columnSchemas, `"`+pgSchemaColumn.ColumnName+"\" "+pgSchemaColumn.TrinoType())
	}

	syncingQuotedTrinoTablePath := trino.Schema() + `."` + pgSchemaTable.IcebergTableName() + TEMP_TABLE_SUFFIX_SYNCING + `"`
	deletingQuotedTrinoTablePath := trino.Schema() + `."` + pgSchemaTable.IcebergTableName() + TEMP_TABLE_SUFFIX_DELETING + `"`
	newQuotedTrinoTablePath := trino.Schema() + `."` + pgSchemaTable.IcebergTableName() + `"`

	syncer.recreateTable(trino, syncingQuotedTrinoTablePath, "("+strings.Join(columnSchemas, ",")+")")

	syncer.Utils.InsertFromCappedBuffer(trino, syncingQuotedTrinoTablePath, pgSchemaTable, pgSchemaColumns, cappedBuffer)

	syncer.swapTable(trino, syncingQuotedTrinoTablePath, deletingQuotedTrinoTablePath, newQuotedTrinoTablePath)

	common.LogInfo(syncer.Config.BaseConfig, "Compacting...")
	trino.CompactTable(newQuotedTrinoTablePath)
}

func (syncer *SyncerFullRefresh) copyFromPgTable(copyConn *pgx.Conn, pgSchemaTable PgSchemaTable, cappedBuffer *common.CappedBuffer) {
	copySql := "COPY " + pgSchemaTable.String() + " TO STDOUT WITH CSV HEADER NULL '" + PG_NULL_STRING + "'"
	result, err := copyConn.PgConn().CopyTo(context.Background(), cappedBuffer, copySql)
	common.PanicIfError(syncer.Config.BaseConfig, err)

	common.LogInfo(syncer.Config.BaseConfig, "Copied", result.RowsAffected(), "row(s) into", pgSchemaTable.String())
	cappedBuffer.Close()
}

func (syncer *SyncerFullRefresh) recreateTable(trino *common.Trino, syncingQuotedTrinoTablePath string, columnSchemasStatement string) {
	ctx := context.Background()

	_, err := trino.ExecContext(ctx, "DROP TABLE IF EXISTS "+syncingQuotedTrinoTablePath)
	common.PanicIfError(syncer.Config.BaseConfig, err)

	_, err = trino.ExecContext(ctx, "CREATE TABLE "+syncingQuotedTrinoTablePath+columnSchemasStatement)
	common.PanicIfError(syncer.Config.BaseConfig, err)
}

func (syncer *SyncerFullRefresh) swapTable(trino *common.Trino, syncingQuotedTrinoTablePath string, deletingQuotedTrinoTablePath string, newQuotedTrinoTablePath string) {
	ctx := context.Background()

	_, err := trino.ExecContext(ctx, "DROP TABLE IF EXISTS "+deletingQuotedTrinoTablePath)
	common.PanicIfError(syncer.Config.BaseConfig, err)

	_, err = trino.ExecContext(ctx, "ALTER TABLE IF EXISTS "+newQuotedTrinoTablePath+" RENAME TO "+deletingQuotedTrinoTablePath)
	common.PanicIfError(syncer.Config.BaseConfig, err)

	_, err = trino.ExecContext(ctx, "ALTER TABLE "+syncingQuotedTrinoTablePath+" RENAME TO "+newQuotedTrinoTablePath)
	common.PanicIfError(syncer.Config.BaseConfig, err)
}
