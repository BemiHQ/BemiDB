package main

import (
	"net/url"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

type Syncer struct {
	Config       *Config
	Utils        *SyncerUtils
	StorageS3    *syncerCommon.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncer(config *Config) *Syncer {
	storageS3 := syncerCommon.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, syncerCommon.DUCKDB_BOOT_QUERIES)

	return &Syncer{
		Config:       config,
		Utils:        NewSyncerUtils(config, storageS3, duckdbClient),
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (syncer *Syncer) Sync() {
	syncerCommon.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-postgres-start", syncer.name())

	postgres := NewPostgres(syncer.Config)
	defer postgres.Close()

	pgSchemaTables := syncer.pgSchemaTables(postgres)

	switch syncer.Config.SyncMode {
	case SyncModeCDC:
		common.LogInfo(syncer.Config.CommonConfig, "Starting CDC sync...")
		panic("CDC is not supported")
	case SyncModeIncremental:
		common.LogInfo(syncer.Config.CommonConfig, "Starting incremental sync...")
		panic("Incremental sync is not supported")
	case SyncModeFullRefresh:
		common.LogInfo(syncer.Config.CommonConfig, "Starting full-refresh sync...")
		NewSyncerFullRefresh(syncer.Config, syncer.Utils, syncer.StorageS3, syncer.DuckdbClient).Sync(postgres, pgSchemaTables)
	}

	syncerCommon.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-postgres-finish", syncer.name())
}

func (syncer *Syncer) pgSchemaTables(postgres *Postgres) []PgSchemaTable {
	pgSchemaTables := make([]PgSchemaTable, 0)
	for _, schema := range postgres.Schemas() {
		for _, pgSchemaTable := range postgres.SchemaTables(schema) {
			if !syncer.Utils.ShouldSyncTable(pgSchemaTable) {
				continue
			}
			pgSchemaTables = append(pgSchemaTables, pgSchemaTable)
		}
	}
	return pgSchemaTables
}

func (syncer *Syncer) name() string {
	url, err := url.Parse(syncer.Config.DatabaseUrl)
	if err != nil {
		return ""
	}

	return url.Hostname()
}
