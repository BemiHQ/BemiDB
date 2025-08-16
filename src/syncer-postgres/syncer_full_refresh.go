package main

import (
	"context"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"
)

type SyncerFullRefresh struct {
	Config    *Config
	Utils     *SyncerUtils
	StorageS3 *syncerCommon.StorageS3
}

func NewSyncerFullRefresh(config *Config) *SyncerFullRefresh {
	return &SyncerFullRefresh{
		Config:    config,
		Utils:     NewSyncerUtils(config),
		StorageS3: syncerCommon.NewStorageS3(config.CommonConfig),
	}
}

func (syncer *SyncerFullRefresh) Sync(postgres *Postgres, pgSchemaTables []PgSchemaTable) {
	_, err := postgres.PostgresClient.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
	common.PanicIfError(syncer.Config.CommonConfig, err)

	icebergTableNames := common.NewSet[string]()

	for _, pgSchemaTable := range pgSchemaTables {
		pgSchemaColumns := postgres.PgSchemaColumns(pgSchemaTable)

		common.LogInfo(syncer.Config.CommonConfig, "Syncing table:", pgSchemaTable.String()+"...")
		syncer.syncTable(postgres, pgSchemaTable, pgSchemaColumns)

		icebergTableNames.Add(pgSchemaTable.IcebergTableName())
	}

	syncer.Utils.DeleteOldTables(syncer.StorageS3, icebergTableNames)
}

func (syncer *SyncerFullRefresh) syncTable(postgres *Postgres, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn) {
	// Create a capped buffer read and written in parallel
	cappedBuffer := syncerCommon.NewCappedBuffer(syncer.Config.CommonConfig, MAX_IN_MEMORY_BUFFER_SIZE)

	// Copy from PG to cappedBuffer in a separate goroutine in parallel
	go func() {
		syncer.copyFromPgTable(postgres, pgSchemaTable, cappedBuffer)
	}()

	// Read from cappedBuffer and write to Iceberg
	syncer.writeToIceberg(pgSchemaTable, pgSchemaColumns, cappedBuffer)
}

func (syncer *SyncerFullRefresh) writeToIceberg(pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn, cappedBuffer *syncerCommon.CappedBuffer) {
	icebergSchemaColumns := make([]*syncerCommon.IcebergSchemaColumn, len(pgSchemaColumns))
	for i, pgSchemaColumn := range pgSchemaColumns {
		icebergSchemaColumns[i] = pgSchemaColumn.ToIcebergSchemaColumn()
	}

	// Delete -syncing table
	syncingIcebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.Config.DestinationSchemaName, pgSchemaTable.IcebergTableName()+TEMP_TABLE_SUFFIX_SYNCING)
	syncingIcebergTable.DeleteIfExists()

	// Write and create -syncing table
	icebergWriter := syncerCommon.NewIcebergWriter(syncer.Config.CommonConfig, syncer.StorageS3, icebergSchemaColumns)
	syncingIcebergTable.GenerateS3TablePath()
	syncer.Utils.ReplaceFromCappedBuffer(icebergWriter, syncingIcebergTable, cappedBuffer)
	syncingIcebergTable.Create()

	// Delete -deleting table
	deletingIcebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.Config.DestinationSchemaName, pgSchemaTable.IcebergTableName()+TEMP_TABLE_SUFFIX_DELETING)
	deletingIcebergTable.DeleteIfExists()

	// Rename table to -deleting
	icebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.Config.DestinationSchemaName, pgSchemaTable.IcebergTableName())
	icebergTable.Rename(deletingIcebergTable.TableName)

	// Rename -syncing to table
	syncingIcebergTable.Rename(pgSchemaTable.IcebergTableName())

	// Delete -deleting table
	deletingIcebergTable.DeleteIfExists()
}

func (syncer *SyncerFullRefresh) copyFromPgTable(postgres *Postgres, pgSchemaTable PgSchemaTable, cappedBuffer *syncerCommon.CappedBuffer) {
	copySql := "COPY " + pgSchemaTable.String() + " TO STDOUT WITH CSV HEADER NULL '" + syncerCommon.BEMIDB_NULL_STRING + "'"
	result, err := postgres.PostgresClient.Copy(cappedBuffer, copySql)
	common.PanicIfError(syncer.Config.CommonConfig, err)

	common.LogInfo(syncer.Config.CommonConfig, "Copied", result.RowsAffected(), "rows from", pgSchemaTable.String())
	cappedBuffer.Close()
}
