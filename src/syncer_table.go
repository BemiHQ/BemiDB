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

const (
	REFRESH_MODE_FULL             = "FULL"
	REFRESH_MODE_FULL_IN_PROGRESS = "FULL_IN_PROGRESS"
	REFRESH_MODE_INCREMENTAL      = "INCREMENTAL"
)

type SyncerTable struct {
	config *Config
}

func NewSyncerTable(config *Config) *SyncerTable {
	return &SyncerTable{config: config}
}

func (syncer *SyncerTable) SyncPgTable(pgSchemaTable PgSchemaTable, structureConn *pgx.Conn, copyConn *pgx.Conn, existingInternalTableMetadata InternalTableMetadata, incrementalRefresh bool) {
	// If there is the previous internal metadata and (we perform an incremental refresh or perform a full refresh with the previous full-in-progress mode)
	continuedRefresh := (existingInternalTableMetadata.XminMin != nil && existingInternalTableMetadata.XminMax != nil) &&
		(incrementalRefresh || existingInternalTableMetadata.LastRefreshMode == REFRESH_MODE_FULL_IN_PROGRESS)

	// Create a capped buffer read and written in parallel
	cappedBuffer := NewCappedBuffer(MAX_IN_MEMORY_BUFFER_SIZE, syncer.config)
	var waitGroup sync.WaitGroup

	// Copy from PG to cappedBuffer in a separate goroutine in parallel ------------------------------------------------
	waitGroup.Add(1)
	go func() {
		syncer.copyFromPgTable(pgSchemaTable, copyConn, existingInternalTableMetadata, continuedRefresh, cappedBuffer, &waitGroup)
	}()

	// Ping PG using structureConn in a separate goroutine in parallel to keep the connection alive --------------------
	stopPingChannel := make(chan struct{})
	waitGroup.Add(1)
	go func() {
		syncer.pingPg(structureConn, &stopPingChannel, &waitGroup)
	}()

	// Read from cappedBuffer and write to Iceberg ---------------------------------------------------------------------

	// Identify the batch size dynamically based on the table stats
	dynamicRowCountPerBatch := syncer.calculatedynamicRowCountPerBatch(pgSchemaTable, structureConn)
	LogDebug(syncer.config, "Row count per batch: ", dynamicRowCountPerBatch, "Continued refresh:", continuedRefresh, "Incremental refresh:", incrementalRefresh)

	// Read the header to get the column information
	csvReader := csv.NewReader(cappedBuffer)
	csvHeaders, err := csvReader.Read()
	PanicIfError(syncer.config, err)
	csvHeaders = csvHeaders[:len(csvHeaders)-1] // Ignore the last column (xmin)
	pgSchemaColumns := syncer.pgTableSchemaColumns(structureConn, pgSchemaTable, csvHeaders)

	icebergWriter := NewIcebergWriterTable(
		syncer.config,
		pgSchemaTable.ToIcebergSchemaTable(),
		pgSchemaColumns,
		dynamicRowCountPerBatch,
		MAX_PARQUET_PAYLOAD_THRESHOLD,
		continuedRefresh,
	)

	xminMin := existingInternalTableMetadata.XminMin
	reachedEnd := false
	totalRowCount := 0

	// Write to Iceberg in a separate goroutine in parallel
	LogInfo(syncer.config, "Writing incrementally to Iceberg...")
	icebergWriter.Write(func() ([][]string, InternalTableMetadata) {
		var newInternalTableMetadata InternalTableMetadata
		var rows [][]string

		if reachedEnd {
			return rows, newInternalTableMetadata
		}

		for {
			row, err := csvReader.Read()

			if err == io.EOF {
				reachedEnd = true
				break
			}
			if err != nil {
				PanicIfError(syncer.config, err)
			}

			xmin, err := StringToUint32(row[len(row)-1])
			PanicIfError(syncer.config, err)
			if xminMin == nil {
				xminMin = &xmin // New global min xmin
			}
			newInternalTableMetadata.XminMin = xminMin
			newInternalTableMetadata.XminMax = &xmin

			row = row[:len(row)-1] // Ignore the last column (xmin)
			rows = append(rows, row)

			if len(rows) >= dynamicRowCountPerBatch {
				break
			}
		}

		totalRowCount += len(rows)
		LogDebug(syncer.config, "Writing", totalRowCount, "total rows to Parquet files...")
		runtime.GC() // To reduce Parquet Go memory leakage

		newInternalTableMetadata.LastSyncedAt = time.Now().Unix()
		if incrementalRefresh {
			newInternalTableMetadata.LastRefreshMode = REFRESH_MODE_INCREMENTAL
		} else {
			newInternalTableMetadata.LastRefreshMode = REFRESH_MODE_FULL_IN_PROGRESS
		}

		return rows, newInternalTableMetadata
	})

	close(stopPingChannel) // Stop the pingPg goroutine
	waitGroup.Wait()       // Wait for the Read goroutine to finish
}

func (syncer *SyncerTable) pgTableSchemaColumns(conn *pgx.Conn, pgSchemaTable PgSchemaTable, csvHeaders []string) []PgSchemaColumn {
	if len(csvHeaders) == 0 {
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
		csvHeaders,
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

func (syncer *SyncerTable) copyFromPgTable(
	pgSchemaTable PgSchemaTable,
	copyConn *pgx.Conn,
	internalTableMetadata InternalTableMetadata,
	continuedRefresh bool,
	cappedBuffer *CappedBuffer,
	waitGroup *sync.WaitGroup,
) {
	LogInfo(syncer.config, "Reading from Postgres:", pgSchemaTable.String()+"...")

	if continuedRefresh {
		result, err := copyConn.PgConn().CopyTo(
			context.Background(),
			cappedBuffer,
			"COPY (SELECT *, xmin::text::bigint AS xmin FROM "+pgSchemaTable.String()+" WHERE xmin::text::bigint > "+internalTableMetadata.XminMaxString()+" OR xmin::text::bigint < "+internalTableMetadata.XminMinString()+" ORDER BY xmin::text::bigint ASC) "+
				"TO STDOUT WITH CSV HEADER NULL '"+PG_NULL_STRING+"'",
		)
		PanicIfError(syncer.config, err)
		LogInfo(syncer.config, "Copied", result.RowsAffected(), "row(s)...")
	} else {
		result, err := copyConn.PgConn().CopyTo(
			context.Background(),
			cappedBuffer,
			"COPY (SELECT *, xmin::text::bigint AS xmin FROM "+pgSchemaTable.String()+" ORDER BY xmin::text::bigint ASC) "+
				"TO STDOUT WITH CSV HEADER NULL '"+PG_NULL_STRING+"'",
		)
		PanicIfError(syncer.config, err)
		LogInfo(syncer.config, "Copied", result.RowsAffected(), "row(s)...")
	}

	cappedBuffer.Close()
	waitGroup.Done()
}

func (syncer *SyncerTable) calculatedynamicRowCountPerBatch(pgSchemaTable PgSchemaTable, conn *pgx.Conn) int {
	var tableSize int64
	var rowCount int64

	err := conn.QueryRow(
		context.Background(),
		`
		SELECT
			pg_total_relation_size(c.oid) AS table_size,
			CASE
				WHEN c.reltuples >= 0 THEN c.reltuples::bigint
				ELSE (SELECT count(*) FROM `+pgSchemaTable.String()+`)
			END AS row_count
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
	).Scan(&tableSize, &rowCount)
	PanicIfError(syncer.config, err)
	LogDebug(syncer.config, "Table size:", tableSize, "Row count:", rowCount)

	if tableSize == 0 || rowCount == 0 {
		return 1
	}

	rowSize := tableSize / rowCount
	dynamicRowCountPerBatch := int(MAX_PG_ROWS_BATCH_SIZE / rowSize)
	if dynamicRowCountPerBatch == 0 {
		return 1
	}

	return dynamicRowCountPerBatch
}

func (syncer *SyncerTable) pingPg(conn *pgx.Conn, stopPingChannel *chan struct{}, waitGroup *sync.WaitGroup) {
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
