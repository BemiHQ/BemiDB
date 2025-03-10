package main

import (
	"github.com/jackc/pgx/v5"
)

type SyncerIncremental struct {
	config        *Config
	icebergWriter *IcebergWriter
}

func NewSyncerIncremental(config *Config, icebergWriter *IcebergWriter) *SyncerIncremental {
	return &SyncerIncremental{
		config:        config,
		icebergWriter: icebergWriter,
	}
}

func (syncer *SyncerIncremental) SyncPgTable(pgSchemaTable PgSchemaTable, structureConn *pgx.Conn, copyConn *pgx.Conn) {
}
