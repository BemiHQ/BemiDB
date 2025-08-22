package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type SyncerFullRefresh struct {
	Config       *Config
	Utils        *SyncerUtils
	StorageS3    *common.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncerFullRefresh(config *Config, utils *SyncerUtils, storageS3 *common.StorageS3, duckdbClient *common.DuckdbClient) *SyncerFullRefresh {
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
	cappedBuffer := common.NewCappedBuffer(syncer.Config.CommonConfig, MAX_IN_MEMORY_BUFFER_SIZE)

	// Copy from PG to cappedBuffer in a separate goroutine in parallel
	go func() {
		syncer.copyFromPgTable(postgres, pgSchemaTable, cappedBuffer)
	}()

	// Read from cappedBuffer and write to Iceberg
	syncer.writeToIceberg(pgSchemaTable, pgSchemaColumns, cappedBuffer)
}

func (syncer *SyncerFullRefresh) writeToIceberg(pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn, cappedBuffer *common.CappedBuffer) {
	// Delete -syncing table
	syncingIcebergSchemaTable := common.IcebergSchemaTable{Schema: syncer.Config.DestinationSchemaName, Table: pgSchemaTable.IcebergTableName() + common.TEMP_TABLE_SUFFIX_SYNCING}
	syncingIcebergTable := common.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncingIcebergSchemaTable)
	syncingIcebergTable.DropIfExists()

	// Insert and create -syncing table
	icebergSchemaColumns := make([]*common.IcebergSchemaColumn, len(pgSchemaColumns))
	for i, pgSchemaColumn := range pgSchemaColumns {
		icebergSchemaColumns[i] = pgSchemaColumn.ToIcebergSchemaColumn()
	}
	icebergTableWriter := common.NewIcebergTableWriter(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncingIcebergTable, icebergSchemaColumns, 1)
	icebergTableWriter.InsertFromCsvCappedBuffer(cappedBuffer)

	// Delete -deleting table
	deletingIcebergSchemaTable := common.IcebergSchemaTable{Schema: syncer.Config.DestinationSchemaName, Table: pgSchemaTable.IcebergTableName() + common.TEMP_TABLE_SUFFIX_DELETING}
	deletingIcebergTable := common.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, deletingIcebergSchemaTable)
	deletingIcebergTable.DropIfExists()

	// Rename table to -deleting
	icebergSchemaTable := common.IcebergSchemaTable{Schema: syncer.Config.DestinationSchemaName, Table: pgSchemaTable.IcebergTableName()}
	icebergTable := common.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, icebergSchemaTable)
	icebergTable.Rename(deletingIcebergSchemaTable.Table)

	// Rename -syncing to table
	syncingIcebergTable.Rename(pgSchemaTable.IcebergTableName())

	// Delete -deleting table
	deletingIcebergTable.DropIfExists()
}

func (syncer *SyncerFullRefresh) copyFromPgTable(postgres *Postgres, pgSchemaTable PgSchemaTable, cappedBuffer *common.CappedBuffer) {
	copySql := "COPY (SELECT * FROM " + pgSchemaTable.String() + ") TO STDOUT WITH CSV HEADER NULL '" + common.BEMIDB_NULL_STRING + "'"
	result, err := postgres.PostgresClient.Copy(cappedBuffer, copySql)
	common.PanicIfError(syncer.Config.CommonConfig, err)

	common.LogInfo(syncer.Config.CommonConfig, "Copied", result.RowsAffected(), "rows from", pgSchemaTable.String())
	cappedBuffer.Close()
}
