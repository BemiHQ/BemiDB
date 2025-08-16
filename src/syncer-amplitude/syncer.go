package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	PAGINATION_TIME_INTERVAL = time.Hour
	AMPLITUDE_DATA_DELAY     = time.Hour // Amplitude data is available for export after an up to 2-hour delay (1 full hour + truncated). https://amplitude.com/docs/apis/analytics/export
)

type Syncer struct {
	Config    *Config
	Amplitude *Amplitude
}

func NewSyncer(config *Config) *Syncer {
	return &Syncer{
		Config:    config,
		Amplitude: NewAmplitude(config),
	}
}

func (syncer *Syncer) Sync() {
	syncerCommon.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-amplitude-start", syncer.name())

	trino := syncerCommon.NewTrino(syncer.Config.CommonConfig, syncer.Config.TrinoConfig, syncer.Config.DestinationSchemaName)
	defer trino.Close()

	trino.CreateSchemaIfNotExists()
	trino.CreateTableIfNotExists(EVENTS_TABLE_NAME, EventsTableSchema())

	quotedTablePath := trino.Schema() + `."` + EVENTS_TABLE_NAME + `"`

	lastSyncedTime := syncer.findLastCursorValue(trino)
	if lastSyncedTime.IsZero() {
		lastSyncedTime = syncer.Config.StartDate
	} else {
		common.LogInfo(syncer.Config.CommonConfig, "Last synced time found:", lastSyncedTime)
		syncer.deleteHourFromTable(trino, quotedTablePath, lastSyncedTime)
	}

	now := time.Now().UTC()
	endOfSyncWindow := now.Add(-AMPLITUDE_DATA_DELAY).Truncate(time.Hour)
	common.LogInfo(syncer.Config.CommonConfig, "Starting incremental sync from", lastSyncedTime, "to", endOfSyncWindow)

	for t := lastSyncedTime; t.Before(endOfSyncWindow); t = t.Add(PAGINATION_TIME_INTERVAL) {
		startTime := t
		endTime := t.Add(PAGINATION_TIME_INTERVAL - time.Hour) // -1 hour to ensure we don't overlap (Amplitude uses an inclusive end time)

		events, err := syncer.Amplitude.Export(startTime, endTime)
		if err != nil && strings.Contains(err.Error(), "Raw data files were not found.") {
			common.LogInfo(syncer.Config.CommonConfig, "No data found for the time range", startTime, "to", endTime, "- will retry later.")
			break
		}
		common.PanicIfError(syncer.Config.CommonConfig, err)

		if len(events) > 0 {
			syncer.insertEvents(trino, quotedTablePath, events)
		}

		// Compact every 24 hours
		if t.Hour() == 23 {
			common.LogInfo(syncer.Config.CommonConfig, "Compacting...")
			trino.CompactTable(quotedTablePath)
		}
	}

	common.LogInfo(syncer.Config.CommonConfig, "Compacting...")
	trino.CompactTable(quotedTablePath)

	syncerCommon.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-amplitude-finish", syncer.name())
}

func (syncer *Syncer) findLastCursorValue(trino *syncerCommon.Trino) time.Time {
	var nullString sql.NullString
	query := `SELECT CAST(max("server_upload_time") AS VARCHAR) FROM ` + trino.Schema() + `."` + EVENTS_TABLE_NAME + `"`
	err := trino.QueryRowContext(
		context.Background(),
		query,
	).Scan(&nullString)

	if err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}
		}
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "Table not found") {
			common.LogInfo(syncer.Config.CommonConfig, "Table", EVENTS_TABLE_NAME, "does not exist yet. Starting from scratch.")
			return time.Time{}
		}
		common.PanicIfError(syncer.Config.CommonConfig, err)
	}

	if !nullString.Valid || nullString.String == "" {
		return time.Time{}
	}

	return syncerCommon.StringMsToUtcTime(nullString.String).Truncate(time.Hour)
}

func (syncer *Syncer) insertEvents(trino *syncerCommon.Trino, quotedTrinoTablePath string, events []Event) {
	if len(events) == 0 {
		return
	}
	ctx := context.Background()
	quotedColumnNames := QuotedEventsTableColumnNames()

	insertSqlPrefix := "INSERT INTO " + quotedTrinoTablePath + "(" + quotedColumnNames + ")" + " VALUES "
	currentSql := insertSqlPrefix
	currentRowCount := 0

	for _, event := range events {
		rowValuesStatement := event.ToTrinoRow()
		if rowValuesStatement == "" {
			continue
		}

		currentRowCount++
		if len(currentSql)+len(rowValuesStatement)+1 < syncerCommon.TRINO_MAX_QUERY_LENGTH { // +1 for the comma
			if currentSql != insertSqlPrefix {
				currentSql += ","
			}
			currentSql += rowValuesStatement
		} else {
			_, err := trino.ExecContext(ctx, currentSql)
			common.PanicIfError(syncer.Config.CommonConfig, err)
			common.LogInfo(syncer.Config.CommonConfig, "Inserted", currentRowCount, "rows into table:", quotedTrinoTablePath)
			currentSql = insertSqlPrefix + rowValuesStatement
			currentRowCount = 1
		}
	}

	if currentSql != insertSqlPrefix {
		_, err := trino.ExecContext(ctx, currentSql)
		common.PanicIfError(syncer.Config.CommonConfig, err)
		common.LogInfo(syncer.Config.CommonConfig, "Inserted", currentRowCount, "rows into table:", quotedTrinoTablePath)
	}
}

func (syncer *Syncer) deleteHourFromTable(trino *syncerCommon.Trino, quotedTablePath string, startTime time.Time) {
	result, err := trino.ExecContext(
		context.Background(),
		`DELETE FROM `+quotedTablePath+` WHERE "server_upload_time" >= TIMESTAMP '`+syncerCommon.TimeToUtcStringMs(startTime)+`'`,
	)
	common.PanicIfError(syncer.Config.CommonConfig, err)

	rowCount, err := result.RowsAffected()
	common.PanicIfError(syncer.Config.CommonConfig, err)
	common.LogInfo(syncer.Config.CommonConfig, "Deleted", rowCount, "rows after", startTime)
}

func (syncer *Syncer) name() string {
	return syncer.Config.ApiKey[:5] + "..."
}
