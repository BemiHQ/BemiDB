package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

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
	common.SendAnonymousAnalytics(syncer.Config.BaseConfig, "syncer-amplitude-start", syncer.name())

	trino := common.NewTrino(syncer.Config.BaseConfig)
	defer trino.Close()

	trino.CreateSchemaIfNotExists()
	trino.CreateTableIfNotExists(EVENTS_TABLE_NAME, EventsTableSchema())

	quotedTablePath := trino.Schema() + `."` + EVENTS_TABLE_NAME + `"`

	lastSyncedTime := syncer.findLastCursorValue(trino)
	if lastSyncedTime.IsZero() {
		lastSyncedTime = syncer.Config.StartDate
	} else {
		common.LogInfo(syncer.Config.BaseConfig, "Last synced time found:", lastSyncedTime)
		syncer.deleteHourFromTable(trino, quotedTablePath, lastSyncedTime)
	}

	now := time.Now().UTC()
	endOfSyncWindow := now.Add(-AMPLITUDE_DATA_DELAY).Truncate(time.Hour)
	common.LogInfo(syncer.Config.BaseConfig, "Starting incremental sync from", lastSyncedTime, "to", endOfSyncWindow)

	for t := lastSyncedTime; t.Before(endOfSyncWindow); t = t.Add(PAGINATION_TIME_INTERVAL) {
		startTime := t
		endTime := t.Add(PAGINATION_TIME_INTERVAL - time.Hour) // -1 hour to ensure we don't overlap (Amplitude uses an inclusive end time)

		events, err := syncer.Amplitude.Export(startTime, endTime)
		if err != nil && strings.Contains(err.Error(), "Raw data files were not found.") {
			common.LogInfo(syncer.Config.BaseConfig, "No data found for the time range", startTime, "to", endTime, "- will retry later.")
			break
		}
		common.PanicIfError(syncer.Config.BaseConfig, err)

		if len(events) > 0 {
			syncer.insertEvents(trino, quotedTablePath, events)
		}

		// Compact every 24 hours
		if t.Hour() == 23 {
			common.LogInfo(syncer.Config.BaseConfig, "Compacting...")
			trino.CompactTable(quotedTablePath)
		}
	}

	common.LogInfo(syncer.Config.BaseConfig, "Compacting...")
	trino.CompactTable(quotedTablePath)

	common.SendAnonymousAnalytics(syncer.Config.BaseConfig, "syncer-amplitude-finish", syncer.name())
}

func (syncer *Syncer) findLastCursorValue(trino *common.Trino) time.Time {
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
			common.LogInfo(syncer.Config.BaseConfig, "Table", EVENTS_TABLE_NAME, "does not exist yet. Starting from scratch.")
			return time.Time{}
		}
		common.PanicIfError(syncer.Config.BaseConfig, err)
	}

	if !nullString.Valid || nullString.String == "" {
		return time.Time{}
	}

	return common.StringMsToUtcTime(nullString.String).Truncate(time.Hour)
}

func (syncer *Syncer) insertEvents(trino *common.Trino, quotedTrinoTablePath string, events []Event) {
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
		if len(currentSql)+len(rowValuesStatement)+1 < common.TRINO_MAX_QUERY_LENGTH { // +1 for the comma
			if currentSql != insertSqlPrefix {
				currentSql += ","
			}
			currentSql += rowValuesStatement
		} else {
			_, err := trino.ExecContext(ctx, currentSql)
			common.PanicIfError(syncer.Config.BaseConfig, err)
			common.LogInfo(syncer.Config.BaseConfig, "Inserted", currentRowCount, "rows into table:", quotedTrinoTablePath)
			currentSql = insertSqlPrefix + rowValuesStatement
			currentRowCount = 1
		}
	}

	if currentSql != insertSqlPrefix {
		_, err := trino.ExecContext(ctx, currentSql)
		common.PanicIfError(syncer.Config.BaseConfig, err)
		common.LogInfo(syncer.Config.BaseConfig, "Inserted", currentRowCount, "rows into table:", quotedTrinoTablePath)
	}
}

func (syncer *Syncer) deleteHourFromTable(trino *common.Trino, quotedTablePath string, startTime time.Time) {
	result, err := trino.ExecContext(
		context.Background(),
		`DELETE FROM `+quotedTablePath+` WHERE "server_upload_time" >= TIMESTAMP '`+common.TimeToUtcStringMs(startTime)+`'`,
	)
	common.PanicIfError(syncer.Config.BaseConfig, err)

	rowCount, err := result.RowsAffected()
	common.PanicIfError(syncer.Config.BaseConfig, err)
	common.LogInfo(syncer.Config.BaseConfig, "Deleted", rowCount, "rows after", startTime)
}

func (syncer *Syncer) name() string {
	return syncer.Config.ApiKey[:5] + "..."
}
