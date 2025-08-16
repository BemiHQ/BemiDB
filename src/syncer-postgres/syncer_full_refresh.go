package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"
)

type SyncerFullRefresh struct {
	Config       *Config
	Utils        *SyncerUtils
	StorageS3    *syncerCommon.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncerFullRefresh(config *Config, utils *SyncerUtils, storageS3 *syncerCommon.StorageS3, duckdbClient *common.DuckdbClient) *SyncerFullRefresh {
	return &SyncerFullRefresh{
		Config:       config,
		Utils:        utils,
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (syncer *SyncerFullRefresh) Sync(postgres *Postgres, pgSchemaTables []PgSchemaTable) {
	icebergTableNames := common.NewSet[string]()

	for _, pgSchemaTable := range pgSchemaTables {
		pgSchemaColumns := postgres.PgSchemaColumns(pgSchemaTable)

		common.LogInfo(syncer.Config.CommonConfig, "Syncing table:", pgSchemaTable.String()+"...")
		syncer.syncTable(postgres, pgSchemaTable, pgSchemaColumns)

		icebergTableNames.Add(pgSchemaTable.IcebergTableName())
	}

	syncer.Utils.DeleteOldTables(icebergTableNames)
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
	// Delete -syncing table
	syncingIcebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncer.Config.DestinationSchemaName, pgSchemaTable.IcebergTableName()+TEMP_TABLE_SUFFIX_SYNCING)
	syncingIcebergTable.DeleteIfExists()

	// Insert and create -syncing table
	icebergSchemaColumns := make([]*syncerCommon.IcebergSchemaColumn, len(pgSchemaColumns))
	for i, pgSchemaColumn := range pgSchemaColumns {
		icebergSchemaColumns[i] = pgSchemaColumn.ToIcebergSchemaColumn()
	}
	icebergWriter := syncerCommon.NewIcebergWriter(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, icebergSchemaColumns, 1)
	icebergWriter.InsertFromCsvCappedBuffer(syncingIcebergTable, cappedBuffer)

	// Delete -deleting table
	deletingIcebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncer.Config.DestinationSchemaName, pgSchemaTable.IcebergTableName()+TEMP_TABLE_SUFFIX_DELETING)
	deletingIcebergTable.DeleteIfExists()

	// Rename table to -deleting
	icebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncer.Config.DestinationSchemaName, pgSchemaTable.IcebergTableName())
	icebergTable.Rename(deletingIcebergTable.TableName)

	// Rename -syncing to table
	syncingIcebergTable.Rename(pgSchemaTable.IcebergTableName())

	// Delete -deleting table
	deletingIcebergTable.DeleteIfExists()
}

func (syncer *SyncerFullRefresh) copyFromPgTable(postgres *Postgres, pgSchemaTable PgSchemaTable, cappedBuffer *syncerCommon.CappedBuffer) {
	copySql := "COPY (SELECT * FROM " + pgSchemaTable.String() + ") TO STDOUT WITH CSV HEADER NULL '" + syncerCommon.BEMIDB_NULL_STRING + "'"
	result, err := postgres.PostgresClient.Copy(cappedBuffer, copySql)
	common.PanicIfError(syncer.Config.CommonConfig, err)

	common.LogInfo(syncer.Config.CommonConfig, "Copied", result.RowsAffected(), "rows from", pgSchemaTable.String())
	cappedBuffer.Close()
}
