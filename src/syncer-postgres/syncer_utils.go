package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

var (
	MAX_IN_MEMORY_BUFFER_SIZE = 32 * 1024 * 1024 // 32 MB

	COMPACT_AFTER_INSERT_BATCH_COUNT = 40 // Compact the table after every N insert batches
)

type SyncerUtils struct {
	Config       *Config
	StorageS3    *syncerCommon.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncerUtils(config *Config, storageS3 *syncerCommon.StorageS3, duckdbClient *common.DuckdbClient) *SyncerUtils {
	return &SyncerUtils{
		Config:       config,
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (utils *SyncerUtils) ShouldSyncTable(pgSchemaTable PgSchemaTable) bool {
	if utils.Config.IncludeSchemas != nil && !utils.Config.IncludeSchemas.Contains(pgSchemaTable.Schema) {
		return false
	}

	if utils.Config.IncludeTables != nil && !utils.Config.IncludeTables.Contains(pgSchemaTable.ToConfigArg()) {
		return false
	}

	if utils.Config.ExcludeTables != nil && utils.Config.ExcludeTables.Contains(pgSchemaTable.ToConfigArg()) {
		return false
	}

	return true
}

func (utils *SyncerUtils) DeleteOldTables(keepIcebergTableNames common.Set[string]) {
	icebergCatalog := syncerCommon.NewIcebergCatalog(utils.Config.CommonConfig, utils.Config.DestinationSchemaName)
	icebergTableNames := icebergCatalog.TableNames()

	for _, icebergTableName := range icebergTableNames.Values() {
		if keepIcebergTableNames.Contains(icebergTableName) {
			continue
		}

		common.LogInfo(utils.Config.CommonConfig, "Deleting old Iceberg table: "+icebergTableName)
		icebergTable := syncerCommon.NewIcebergTable(utils.Config.CommonConfig, utils.StorageS3, utils.DuckdbClient, utils.Config.DestinationSchemaName, icebergTableName)
		icebergTable.DeleteIfExists()
	}
}
