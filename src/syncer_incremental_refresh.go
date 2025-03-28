package main

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type SyncerIncrementalRefresh struct {
	config        *Config
	icebergWriter *IcebergWriter
}

func NewSyncerIncrementalRefresh(config *Config, icebergWriter *IcebergWriter) *SyncerIncrementalRefresh {
	return &SyncerIncrementalRefresh{
		config:        config,
		icebergWriter: icebergWriter,
	}
}

func (syncer *SyncerIncrementalRefresh) SyncPgTable(pgSchemaTable PgSchemaTable, internalTableMetadata InternalTableMetadata, dynamicRowCountPerBatch int, structureConn *pgx.Conn, copyConn *pgx.Conn) {
	// Create a capped buffer read and written in parallel
	cappedBuffer := NewCappedBuffer(MAX_IN_MEMORY_BUFFER_SIZE, syncer.config)

	var waitGroup sync.WaitGroup

	// Copy from PG to cappedBuffer in a separate goroutine in parallel
	waitGroup.Add(1)
	go func() {
		syncer.copyFromPgTable(pgSchemaTable, internalTableMetadata, copyConn, cappedBuffer, &waitGroup)
	}()

	// Ping PG using structureConn in a separate goroutine in parallel to keep the connection alive
	waitGroup.Add(1)
	stopPingChannel := make(chan struct{})
	go func() {
		syncer.pingPg(structureConn, &stopPingChannel, &waitGroup)
	}()

	// Read the header to get the column names
	csvReader := csv.NewReader(cappedBuffer)
	csvHeader, err := csvReader.Read()
	PanicIfError(syncer.config, err)

	schemaTable := pgSchemaTable.ToIcebergSchemaTable()
	pgSchemaColumns := syncer.pgTableSchemaColumns(structureConn, pgSchemaTable, csvHeader)
	reachedEnd := false
	totalRowCount := 0

	// Write to Iceberg in a separate goroutine in parallel
	LogInfo(syncer.config, "Writing incrementally to Iceberg...")
	syncer.icebergWriter.WriteIncrementally(schemaTable, pgSchemaColumns, dynamicRowCountPerBatch, MAX_PARQUET_PAYLOAD_THRESHOLD, func() [][]string {
		if reachedEnd {
			return [][]string{}
		}

		var rows [][]string
		for {
			row, err := csvReader.Read()

			if err == io.EOF {
				reachedEnd = true
				break
			}
			if err != nil {
				PanicIfError(syncer.config, err)
			}

			rows = append(rows, row)
			if len(rows) >= dynamicRowCountPerBatch {
				break
			}
		}

		totalRowCount += len(rows)
		LogDebug(syncer.config, "Writing", totalRowCount, "rows to Parquet...")
		runtime.GC() // To reduce Parquet Go memory leakage

		return rows
	})

	close(stopPingChannel) // Stop the pingPg goroutine
	waitGroup.Wait()       // Wait for the Read goroutine to finish
}

func (syncer *SyncerIncrementalRefresh) pgTableSchemaColumns(conn *pgx.Conn, pgSchemaTable PgSchemaTable, csvHeader []string) []PgSchemaColumn {
	if len(csvHeader) == 0 {
		PanicIfError(syncer.config, errors.New("couldn't read data from "+pgSchemaTable.String()))
	}

	var pgSchemaColumns []PgSchemaColumn

	rows, err := conn.Query(
		context.Background(),
		`SELECT
			columns.column_name,
			columns.data_type,
			columns.udt_name,
			columns.is_nullable,
			columns.ordinal_position,
			COALESCE(columns.character_maximum_length, 0),
			COALESCE(columns.numeric_precision, 0),
			COALESCE(columns.numeric_scale, 0),
			COALESCE(columns.datetime_precision, 0),
			pg_namespace.nspname,
			CASE WHEN pk.constraint_name IS NOT NULL THEN true ELSE false END
		FROM information_schema.columns
		JOIN pg_type ON pg_type.typname = columns.udt_name
		JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
		LEFT JOIN (
			SELECT
				tc.constraint_name,
				kcu.column_name,
				kcu.table_schema,
				kcu.table_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
				AND tc.table_name = kcu.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
		) pk ON pk.column_name = columns.column_name AND pk.table_schema = columns.table_schema AND pk.table_name = columns.table_name
		WHERE columns.table_schema = $1 AND columns.table_name = $2
		ORDER BY array_position($3, columns.column_name)`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
		csvHeader,
	)
	PanicIfError(syncer.config, err)
	defer rows.Close()

	for rows.Next() {
		pgSchemaColumn := NewPgSchemaColumn(syncer.config)
		err = rows.Scan(
			&pgSchemaColumn.ColumnName,
			&pgSchemaColumn.DataType,
			&pgSchemaColumn.UdtName,
			&pgSchemaColumn.IsNullable,
			&pgSchemaColumn.OrdinalPosition,
			&pgSchemaColumn.CharacterMaximumLength,
			&pgSchemaColumn.NumericPrecision,
			&pgSchemaColumn.NumericScale,
			&pgSchemaColumn.DatetimePrecision,
			&pgSchemaColumn.Namespace,
			&pgSchemaColumn.PartOfPrimaryKey,
		)
		PanicIfError(syncer.config, err)
		pgSchemaColumns = append(pgSchemaColumns, *pgSchemaColumn)
	}

	return pgSchemaColumns
}

func (syncer *SyncerIncrementalRefresh) copyFromPgTable(pgSchemaTable PgSchemaTable, internalTableMetadata InternalTableMetadata, copyConn *pgx.Conn, cappedBuffer *CappedBuffer, waitGroup *sync.WaitGroup) {
	LogInfo(syncer.config, "Reading from Postgres:", pgSchemaTable.String()+"...")
	result, err := copyConn.PgConn().CopyTo(
		context.Background(),
		cappedBuffer,
		"COPY (SELECT * FROM "+pgSchemaTable.String()+" WHERE xmin::text::bigint > "+internalTableMetadata.XminMaxString()+" OR xmin::text::bigint < "+internalTableMetadata.XminMinString()+") TO STDOUT WITH CSV HEADER NULL '"+PG_NULL_STRING+"'",
	)
	PanicIfError(syncer.config, err)
	LogInfo(syncer.config, "Copied", result.RowsAffected(), "row(s)...")

	cappedBuffer.Close()
	waitGroup.Done()
}

func (syncer *SyncerIncrementalRefresh) pingPg(conn *pgx.Conn, stopPingChannel *chan struct{}, waitGroup *sync.WaitGroup) {
	ticker := time.NewTicker(PING_PG_INTERVAL_SECONDS * time.Second)

	for {
		select {
		case <-*stopPingChannel:
			LogDebug(syncer.config, "Stopping the ping...")
			waitGroup.Done()
			ticker.Stop()
			return
		case <-ticker.C:
			LogDebug(syncer.config, "Pinging the database...")
			_, err := conn.Exec(context.Background(), "SELECT 1")
			PanicIfError(syncer.config, err)
		}
	}
}
