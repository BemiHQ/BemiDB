package main

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"io"
	"strings"

	goDuckdb "github.com/marcboeker/go-duckdb/v2"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

var (
	MAX_IN_MEMORY_BUFFER_SIZE     = 32 * 1024 * 1024   // 32 MB
	MAX_ICEBERG_WRITER_BATCH_SIZE = 1024 * 1024 * 1024 // 1 GB

	COMPACT_AFTER_INSERT_BATCH_COUNT = 40 // Compact the table after every N insert batches
)

type SyncerUtils struct {
	Config *Config
}

func NewSyncerUtils(config *Config) *SyncerUtils {
	return &SyncerUtils{
		Config: config,
	}
}

func (utils *SyncerUtils) ShouldSyncTable(pgSchemaTable PgSchemaTable) bool {
	if utils.Config.IncludeSchemas != nil && !utils.Config.IncludeSchemas.Contains(pgSchemaTable.Schema) {
		return false
	}

	if utils.Config.IncludeTables != nil && !utils.Config.IncludeTables.Contains(pgSchemaTable.ToConfigArg()) {
		return false
	}

	if utils.Config.ExcludeTables != nil && utils.Config.ExcludeTables.Contains(pgSchemaTable.ToConfigArg()) {
		return false
	}

	return true
}

func (utils *SyncerUtils) CreateTableIfNotExists(trino *common.Trino, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn) {
	columnSchemas := []string{}
	for _, pgSchemaColumn := range pgSchemaColumns {
		columnSchemas = append(columnSchemas, `"`+pgSchemaColumn.ColumnName+"\" "+pgSchemaColumn.TrinoType())
	}

	trino.CreateTableIfNotExists(pgSchemaTable.IcebergTableName(), "("+strings.Join(columnSchemas, ",")+")")
}

func (utils *SyncerUtils) DropOldTables(trino *common.Trino, keepIcebergTableNames common.Set[string]) {
	ctx := context.Background()
	trinoTableNames := make([]string, 0)

	rows, err := trino.QueryContext(ctx, "SHOW TABLES FROM "+trino.Schema())
	common.PanicIfError(utils.Config.BaseConfig, err)

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		common.PanicIfError(utils.Config.BaseConfig, err)
		trinoTableNames = append(trinoTableNames, tableName)
	}

	for _, trinoTableName := range trinoTableNames {
		if keepIcebergTableNames.Contains(trinoTableName) {
			continue
		}

		common.LogInfo(utils.Config.BaseConfig, "Dropping old table: "+trinoTableName)
		_, err = trino.ExecContext(ctx, "DROP TABLE IF EXISTS "+trino.Schema()+`."`+trinoTableName+`"`)
		common.PanicIfError(utils.Config.BaseConfig, err)
	}
}

func (utils *SyncerUtils) DeleteOldTables(storageS3 *common.StorageS3, keepIcebergTableNames common.Set[string]) {
	icebergCatalog := common.NewIcebergCatalog(utils.Config.BaseConfig)
	icebergTableNames := icebergCatalog.TableNames()

	for _, icebergTableName := range icebergTableNames.Values() {
		if keepIcebergTableNames.Contains(icebergTableName) {
			continue
		}

		common.LogInfo(utils.Config.BaseConfig, "Deleting old Iceberg table: "+icebergTableName)
		icebergTable := common.NewIcebergTable(utils.Config.BaseConfig, storageS3, icebergTableName)
		icebergTable.DeleteIfExists()
	}
}

func (utils *SyncerUtils) InsertFromCappedBuffer(trino *common.Trino, quotedTrinoTablePath string, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn, cappedBuffer *common.CappedBuffer) {
	ctx := context.Background()
	csvReader := csv.NewReader(cappedBuffer)
	_, err := csvReader.Read() // Read the header row
	common.PanicIfError(utils.Config.BaseConfig, err)

	quotedColumnNames := []string{}
	for _, pgSchemaColumn := range pgSchemaColumns {
		quotedColumnNames = append(quotedColumnNames, `"`+pgSchemaColumn.ColumnName+`"`)
	}

	insertSqlPrefix := "INSERT INTO " + quotedTrinoTablePath + "(" + strings.Join(quotedColumnNames, ",") + ")" + " VALUES "
	currentSql := insertSqlPrefix
	currentRowCount := 0
	batchCount := 0

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			common.PanicIfError(utils.Config.BaseConfig, err)
		}

		rowValues := []string{}
		for i, pgSchemaColumn := range pgSchemaColumns {
			rowValues = append(rowValues, pgSchemaColumn.CsvToTrinoValue(row[i]))
		}
		rowValuesStatement := "(" + strings.Join(rowValues, ",") + ")"

		currentRowCount++
		if len(currentSql)+len(rowValuesStatement)+1 < common.TRINO_MAX_QUERY_LENGTH { // +1 for the comma
			if currentSql != insertSqlPrefix {
				currentSql += ","
			}
			currentSql += rowValuesStatement
		} else {
			_, err := trino.ExecContext(ctx, currentSql)
			common.PanicIfError(utils.Config.BaseConfig, err)
			common.LogInfo(utils.Config.BaseConfig, "Inserted", currentRowCount, "rows into table:", pgSchemaTable.String())
			currentSql = insertSqlPrefix + rowValuesStatement
			currentRowCount = 1
			batchCount++
			if batchCount%COMPACT_AFTER_INSERT_BATCH_COUNT == 0 {
				common.LogInfo(utils.Config.BaseConfig, "Compacting table:", pgSchemaTable.String(), "after", batchCount, "insert batches")
				trino.CompactTable(quotedTrinoTablePath)
			}
		}
	}

	if currentSql != insertSqlPrefix {
		_, err := trino.ExecContext(ctx, currentSql)
		common.PanicIfError(utils.Config.BaseConfig, err)
		common.LogInfo(utils.Config.BaseConfig, "Inserted", currentRowCount, "rows into table:", pgSchemaTable.String())
	}
}

func (utils *SyncerUtils) ReplaceFromCappedBuffer(icebergWriter *common.IcebergWriter, icebergTable *common.IcebergTable, cappedBuffer *common.CappedBuffer) {
	csvReader := csv.NewReader(cappedBuffer)
	_, err := csvReader.Read() // Read the header row
	common.PanicIfError(utils.Config.BaseConfig, err)

	icebergWriter.Write(icebergTable.GeneratedS3TablePath, func(appender *goDuckdb.Appender) (rowCount int, reachedEnd bool) {
		var loadedSize int

		for {
			row, err := csvReader.Read()
			if err == io.EOF {
				reachedEnd = true
				break
			}
			if err != nil {
				common.PanicIfError(utils.Config.BaseConfig, err)
			}

			values := make([]driver.Value, len(icebergWriter.IcebergSchemaColumns))
			for i, icebergSchemaColumn := range icebergWriter.IcebergSchemaColumns {
				values[i] = icebergSchemaColumn.DuckdbValueFromCsv(row[i])
				loadedSize += len(row[i])
			}
			common.LogTrace(utils.Config.BaseConfig, "DuckDB appending values:", values)

			err = appender.AppendRow(values...)
			common.PanicIfError(utils.Config.BaseConfig, err)

			rowCount++
			if loadedSize >= MAX_ICEBERG_WRITER_BATCH_SIZE {
				common.LogDebug(utils.Config.BaseConfig, "Reached batch size limit")
				break
			}
		}

		common.LogInfo(utils.Config.BaseConfig, "Loaded", rowCount, "rows")
		return rowCount, reachedEnd
	})
}
