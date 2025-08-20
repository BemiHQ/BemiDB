package main

import (
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	PAGINATION_TIME_INTERVAL = time.Hour
	AMPLITUDE_DATA_DELAY     = time.Hour // Amplitude data is available for export after an up to 2-hour delay (1 full hour + truncated). https://amplitude.com/docs/apis/analytics/export

	CURSOR_COLUMN_NAME = "server_upload_time"

	MAX_IN_MEMORY_BUFFER_SIZE = 32 * 1024 * 1024 // 32 MB
	COMPRESSION_FACTOR        = 2                // 1 GB uncompressed data x 2 = ~90MB compressed data
)

type Syncer struct {
	Config       *Config
	Amplitude    *Amplitude
	StorageS3    *syncerCommon.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncer(config *Config) *Syncer {
	storageS3 := syncerCommon.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, syncerCommon.DUCKDB_BOOT_QUERIES)

	return &Syncer{
		Config:       config,
		Amplitude:    NewAmplitude(config),
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (syncer *Syncer) Sync() {
	syncerCommon.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-amplitude-start", syncer.name())

	cappedBuffer := syncerCommon.NewCappedBuffer(syncer.Config.CommonConfig, MAX_IN_MEMORY_BUFFER_SIZE)
	jsonQueueWriter := syncerCommon.NewJsonQueueWriter(cappedBuffer)

	icebergTable := syncerCommon.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, syncer.Config.DestinationSchemaName, EVENTS_TABLE_NAME)
	cursorValue := icebergTable.LastCursorValue(CURSOR_COLUMN_NAME)

	lastSyncedTime := syncer.Config.StartDate
	if cursorValue.StringValue != "" {
		lastSyncedTime = syncerCommon.StringMsToUtcTime(cursorValue.StringValue).Truncate(time.Hour).Add(time.Hour) // add 1 hour to ensure we don't overlap
	}
	now := time.Now().UTC()
	endOfSyncWindow := now.Add(-AMPLITUDE_DATA_DELAY).Truncate(time.Hour)
	common.LogInfo(syncer.Config.CommonConfig, "Starting incremental sync from", lastSyncedTime, "to", endOfSyncWindow)

	// Copy from Amplitude to cappedBuffer in a separate goroutine in parallel
	go func() {
		for t := lastSyncedTime; t.Before(endOfSyncWindow); t = t.Add(PAGINATION_TIME_INTERVAL) {
			startTime := t
			endTime := t.Add(PAGINATION_TIME_INTERVAL - time.Hour) // -1 hour to ensure we don't overlap (Amplitude uses an inclusive end time)

			err := syncer.Amplitude.Export(jsonQueueWriter, startTime, endTime)
			if err != nil {
				if strings.Contains(err.Error(), "Raw data files were not found.") || strings.Contains(err.Error(), "404: Not Found") {
					common.LogInfo(syncer.Config.CommonConfig, "No data found for the time range", startTime, "to", endTime, "- will retry later.")
					break
				}
			}
			common.PanicIfError(syncer.Config.CommonConfig, err)
		}
		common.LogInfo(syncer.Config.CommonConfig, "Finished exporting data from Amplitude.")
		jsonQueueWriter.Close()
	}()

	// Read from cappedBuffer and write to Iceberg
	icebergSchemaColumns := EventIcebergSchemaColumns(syncer.Config.CommonConfig)
	icebergWriter := syncerCommon.NewIcebergWriter(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, icebergSchemaColumns, COMPRESSION_FACTOR)
	icebergWriter.AppendFromJsonCappedBuffer(icebergTable, cursorValue, cappedBuffer)

	syncerCommon.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-amplitude-finish", syncer.name())
}

func (syncer *Syncer) name() string {
	return syncer.Config.ApiKey[:5] + "..."
}
