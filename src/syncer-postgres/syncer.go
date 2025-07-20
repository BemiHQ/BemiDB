package main

import (
	"net/url"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

type Syncer struct {
	Config *Config
	Utils  *SyncerUtils
}

func NewSyncer(config *Config) *Syncer {
	return &Syncer{
		Config: config,
		Utils:  NewSyncerUtils(config),
	}
}

func (syncer *Syncer) Sync() {
	common.SendAnonymousAnalytics(syncer.Config.BaseConfig, "syncer-postgres-start", syncer.name())

	postgres := NewPostgres(syncer.Config)
	defer postgres.Close()

	trino := common.NewTrino(syncer.Config.BaseConfig)
	defer trino.Close()

	trino.CreateSchemaIfNotExists()

	pgSchemaTables := syncer.pgSchemaTables(postgres, trino)

	switch syncer.Config.SyncMode {
	case SyncModeCDC:
		common.LogInfo(syncer.Config.BaseConfig, "Starting CDC sync...")
		panic("CDC is not supported")
	case SyncModeIncremental:
		common.LogInfo(syncer.Config.BaseConfig, "Starting incremental sync...")
		panic("Incremental sync is not supported")
	case SyncModeFullRefresh:
		common.LogInfo(syncer.Config.BaseConfig, "Starting full-refresh sync...")
		NewSyncerFullRefresh(syncer.Config).Sync(postgres, trino, pgSchemaTables)
	}

	common.SendAnonymousAnalytics(syncer.Config.BaseConfig, "syncer-postgres-finish", syncer.name())
}

func (syncer *Syncer) pgSchemaTables(postgres *Postgres, trino *common.Trino) []PgSchemaTable {
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
