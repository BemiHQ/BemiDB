package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type SyncerUtils struct {
	Config       *Config
	StorageS3    *common.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncerUtils(config *Config, storageS3 *common.StorageS3, duckdbClient *common.DuckdbClient) *SyncerUtils {
	return &SyncerUtils{
		Config:       config,
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (utils *SyncerUtils) ShouldSyncTable(pgSchemaTable PgSchemaTable) bool {
	if utils.Config.IncludeTables != nil && !utils.Config.IncludeTables.Contains(pgSchemaTable.ToConfigArg()) {
		return false
	}

	if utils.Config.ExcludeTables != nil && utils.Config.ExcludeTables.Contains(pgSchemaTable.ToConfigArg()) {
		return false
	}

	return true
}

func (utils *SyncerUtils) DeleteOldTables(keepIcebergTableNames common.Set[string]) {
	icebergCatalog := common.NewIcebergCatalog(utils.Config.CommonConfig)
	icebergTableNames := icebergCatalog.SchemaTableNames(utils.Config.DestinationSchemaName)

	for _, icebergTableName := range icebergTableNames.Values() {
		if keepIcebergTableNames.Contains(icebergTableName) {
			continue
		}

		common.LogInfo(utils.Config.CommonConfig, "Deleting old Iceberg table: "+icebergTableName)
		icebergSchemaTable := common.IcebergSchemaTable{Schema: utils.Config.DestinationSchemaName, Table: icebergTableName}
		icebergTable := common.NewIcebergTable(utils.Config.CommonConfig, utils.StorageS3, utils.DuckdbClient, icebergSchemaTable)
		icebergTable.DropIfExists()
	}
}
