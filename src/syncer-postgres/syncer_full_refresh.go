package main

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"
)

type SyncerFullRefresh struct {
	Config    *Config
	Utils     *SyncerUtils
	StorageS3 *common.StorageS3
}

func NewSyncerFullRefresh(config *Config) *SyncerFullRefresh {
	return &SyncerFullRefresh{
		Config:    config,
		Utils:     NewSyncerUtils(config),
		StorageS3: common.NewStorageS3(config.BaseConfig),
	}
}

func (syncer *SyncerFullRefresh) Sync(postgres *Postgres, pgSchemaTables []PgSchemaTable) {
	_, err := postgres.Conn.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
	common.PanicIfError(syncer.Config.BaseConfig, err)

	icebergTableNames := common.NewSet[string]()

	for _, pgSchemaTable := range pgSchemaTables {
		pgSchemaColumns := postgres.PgSchemaColumns(pgSchemaTable)

		common.LogInfo(syncer.Config.BaseConfig, "Syncing table:", pgSchemaTable.String()+"...")
		syncer.syncTable(postgres, pgSchemaTable, pgSchemaColumns)

		icebergTableNames.Add(pgSchemaTable.IcebergTableName())
	}

	syncer.Utils.DeleteOldTables(syncer.StorageS3, icebergTableNames)
}

func (syncer *SyncerFullRefresh) syncTable(postgres *Postgres, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn) {
	// Create a capped buffer read and written in parallel
	cappedBuffer := common.NewCappedBuffer(syncer.Config.BaseConfig, MAX_IN_MEMORY_BUFFER_SIZE)

	// Copy from PG to cappedBuffer in a separate goroutine in parallel
	go func() {
		syncer.copyFromPgTable(postgres.Conn, pgSchemaTable, cappedBuffer)
	}()

	// Read from cappedBuffer and write to Iceberg
	syncer.writeToIceberg(pgSchemaTable, pgSchemaColumns, cappedBuffer)
}

func (syncer *SyncerFullRefresh) writeToIceberg(pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn, cappedBuffer *common.CappedBuffer) {
	icebergSchemaColumns := make([]*common.IcebergSchemaColumn, len(pgSchemaColumns))
	for i, pgSchemaColumn := range pgSchemaColumns {
		icebergSchemaColumns[i] = pgSchemaColumn.ToIcebergSchemaColumn()
	}

	// Delete -syncing table
	syncingIcebergTable := common.NewIcebergTable(syncer.Config.BaseConfig, syncer.StorageS3, pgSchemaTable.IcebergTableName()+TEMP_TABLE_SUFFIX_SYNCING)
	syncingIcebergTable.DeleteIfExists()

	// Write and create -syncing table
	icebergWriter := common.NewIcebergWriter(syncer.Config.BaseConfig, syncer.StorageS3, icebergSchemaColumns)
	syncingIcebergTable.GenerateS3TablePath()
	syncer.Utils.ReplaceFromCappedBuffer(icebergWriter, syncingIcebergTable, cappedBuffer)
	syncingIcebergTable.Create()

	// Delete -deleting table
	deletingIcebergTable := common.NewIcebergTable(syncer.Config.BaseConfig, syncer.StorageS3, pgSchemaTable.IcebergTableName()+TEMP_TABLE_SUFFIX_DELETING)
	deletingIcebergTable.DeleteIfExists()

	// Rename table to -deleting
	icebergTable := common.NewIcebergTable(syncer.Config.BaseConfig, syncer.StorageS3, pgSchemaTable.IcebergTableName())
	icebergTable.Rename(deletingIcebergTable.Name)

	// Rename -syncing to table
	syncingIcebergTable.Rename(pgSchemaTable.IcebergTableName())

	// Delete -deleting table
	deletingIcebergTable.DeleteIfExists()
}

func (syncer *SyncerFullRefresh) copyFromPgTable(copyConn *pgx.Conn, pgSchemaTable PgSchemaTable, cappedBuffer *common.CappedBuffer) {
	copySql := "COPY " + pgSchemaTable.String() + " TO STDOUT WITH CSV HEADER NULL '" + common.BEMIDB_NULL_STRING + "'"
	result, err := copyConn.PgConn().CopyTo(context.Background(), cappedBuffer, copySql)
	common.PanicIfError(syncer.Config.BaseConfig, err)

	common.LogInfo(syncer.Config.BaseConfig, "Copied", result.RowsAffected(), "rows from", pgSchemaTable.String())
	cappedBuffer.Close()
}
