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

func (syncer *SyncerIncremental) SyncPgTable(pgSchemaTable PgSchemaTable, internalTableMetadata InternalTableMetadata, rowCountPerBatch int, structureConn *pgx.Conn, copyConn *pgx.Conn) {
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
	PanicIfError(err, syncer.config)

	schemaTable := pgSchemaTable.ToIcebergSchemaTable()
	pgSchemaColumns := syncer.pgTableSchemaColumns(structureConn, pgSchemaTable, csvHeader)
	reachedEnd := false
	totalRowCount := 0

	// Write to Iceberg in a separate goroutine in parallel
	LogInfo(syncer.config, "Writing to Iceberg...")
	syncer.icebergWriter.Append(schemaTable, pgSchemaColumns, func() [][]string {
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
				PanicIfError(err, syncer.config)
			}

			rows = append(rows, row)
			if len(rows) >= rowCountPerBatch {
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

func (syncer *SyncerIncremental) pgTableSchemaColumns(conn *pgx.Conn, pgSchemaTable PgSchemaTable, csvHeader []string) []PgSchemaColumn {
	if len(csvHeader) == 0 {
		PanicIfError(errors.New("couldn't read data from "+pgSchemaTable.String()), syncer.config)
	}

	var pgSchemaColumns []PgSchemaColumn

	rows, err := conn.Query(
		context.Background(),
		`SELECT
			column_name,
			data_type,
			udt_name,
			is_nullable,
			ordinal_position,
			COALESCE(character_maximum_length, 0),
			COALESCE(numeric_precision, 0),
			COALESCE(numeric_scale, 0),
			COALESCE(datetime_precision, 0),
			pg_namespace.nspname
		FROM information_schema.columns
		JOIN pg_type ON pg_type.typname = udt_name
		JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY array_position($3, column_name)`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
		csvHeader,
	)
	PanicIfError(err, syncer.config)
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
		)
		PanicIfError(err, syncer.config)
		pgSchemaColumns = append(pgSchemaColumns, *pgSchemaColumn)
	}

	return pgSchemaColumns
}

func (syncer *SyncerIncremental) copyFromPgTable(pgSchemaTable PgSchemaTable, internalTableMetadata InternalTableMetadata, copyConn *pgx.Conn, cappedBuffer *CappedBuffer, waitGroup *sync.WaitGroup) {
	LogInfo(syncer.config, "Reading from Postgres:", pgSchemaTable.String()+"...")
	result, err := copyConn.PgConn().CopyTo(
		context.Background(),
		cappedBuffer,
		"COPY (SELECT * FROM "+pgSchemaTable.String()+" WHERE xmin::text::bigint > "+internalTableMetadata.XminMaxString()+" OR xmin::text::bigint < "+internalTableMetadata.XminMinString()+") TO STDOUT WITH CSV HEADER NULL '"+PG_NULL_STRING+"'",
	)
	PanicIfError(err, syncer.config)
	LogInfo(syncer.config, "Copied", result.RowsAffected(), "row(s)...")

	cappedBuffer.Close()
	waitGroup.Done()
}

func (syncer *SyncerIncremental) pingPg(conn *pgx.Conn, stopPingChannel *chan struct{}, waitGroup *sync.WaitGroup) {
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
			PanicIfError(err, syncer.config)
		}
	}
}
