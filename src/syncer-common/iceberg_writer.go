package syncerCommon

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"io"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	MAX_ICEBERG_WRITER_BATCH_SIZE = 1024 * 1024 * 1024 // 1 GB
	MAX_PARQUET_FILE_SIZE         = 100 * 1024 * 1024  // 100 MB
)

type IcebergWriter struct {
	Config               *common.CommonConfig
	IcebergSchemaColumns []*IcebergSchemaColumn
	StorageS3            *StorageS3
	DuckdbClient         *common.DuckdbClient
	CompressionFactor    int64
}

func NewIcebergWriter(
	config *common.CommonConfig,
	storageS3 *StorageS3,
	duckdbClient *common.DuckdbClient,
	icebergSchemaColumns []*IcebergSchemaColumn,
	compressionFactor int64,
) *IcebergWriter {
	return &IcebergWriter{
		Config:               config,
		IcebergSchemaColumns: icebergSchemaColumns,
		StorageS3:            storageS3,
		DuckdbClient:         duckdbClient,
		CompressionFactor:    compressionFactor,
	}
}

func (writer *IcebergWriter) InsertFromCsvCappedBuffer(icebergTable *IcebergTable, cappedBuffer *CappedBuffer) {
	csvReader := csv.NewReader(cappedBuffer)
	_, err := csvReader.Read() // Read the header row
	common.PanicIfError(writer.Config, err)

	writer.insertRows(icebergTable, func(duckdbTableName string, loadedSize int64) (int, bool) {
		return writer.loadCsvRows(duckdbTableName, csvReader, loadedSize)
	})
}

func (writer *IcebergWriter) AppendFromCsvCappedBuffer(icebergTable *IcebergTable, cursorValue CursorValue, cappedBuffer *CappedBuffer) {
	metadataFileS3Path := icebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, insert for the first time
		writer.InsertFromCsvCappedBuffer(icebergTable, cappedBuffer)
		return
	}

	csvReader := csv.NewReader(cappedBuffer)
	_, err := csvReader.Read() // Read the header row
	common.PanicIfError(writer.Config, err)

	writer.appendRows(metadataFileS3Path, cursorValue, func(duckdbTableName string, loadedSize int64) (int, bool) {
		return writer.loadCsvRows(duckdbTableName, csvReader, loadedSize)
	})
}

func (writer *IcebergWriter) InsertFromJsonCappedBuffer(icebergTable *IcebergTable, cappedBuffer *CappedBuffer) {
	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	writer.insertRows(icebergTable, func(duckdbTableName string, loadedSize int64) (int, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergWriter) AppendFromJsonCappedBuffer(icebergTable *IcebergTable, cursorValue CursorValue, cappedBuffer *CappedBuffer) {
	metadataFileS3Path := icebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, insert for the first time
		writer.InsertFromJsonCappedBuffer(icebergTable, cappedBuffer)
		return
	}

	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	writer.appendRows(metadataFileS3Path, cursorValue, func(duckdbTableName string, loadedSize int64) (int, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergWriter) UpdateFromJsonCappedBuffer(icebergTable *IcebergTable, cappedBuffer *CappedBuffer) {
	metadataFileS3Path := icebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, insert for the first time
		writer.InsertFromJsonCappedBuffer(icebergTable, cappedBuffer)
		return
	}

	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	uniqueIndexColumnNames := writer.UniqueIndexColumnNames(icebergTable)
	writer.updateRows(metadataFileS3Path, uniqueIndexColumnNames, func(duckdbTableName string, loadedSize int64) (int, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergWriter) DeleteFromJsonCappedBuffer(icebergTable *IcebergTable, cappedBuffer *CappedBuffer) {
	metadataFileS3Path := icebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, do nothing
		return
	}

	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	uniqueIndexColumnNames := writer.UniqueIndexColumnNames(icebergTable)
	writer.deleteRows(metadataFileS3Path, uniqueIndexColumnNames, func(duckdbTableName string, loadedSize int64) (int, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergWriter) UniqueIndexColumnNames(icebergTable *IcebergTable) []string {
	uniqueIndexColumnNames := []string{}
	for _, icebergSchemaColumn := range writer.IcebergSchemaColumns {
		if icebergSchemaColumn.IsPartOfUniqueIndex {
			uniqueIndexColumnNames = append(uniqueIndexColumnNames, icebergSchemaColumn.ColumnName)
		}
	}
	if len(uniqueIndexColumnNames) == 0 {
		common.Panic(writer.Config, "Cannot update Iceberg table without unique index columns: "+icebergTable.String())
	}
	return uniqueIndexColumnNames
}

func (writer *IcebergWriter) insertRows(icebergTable *IcebergTable, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int, reachedEnd bool)) {
	tableS3Path := icebergTable.GenerateTableS3Path()
	dataS3Path := tableS3Path + "/data"
	metadataS3Path := tableS3Path + "/metadata"

	var newParquetCount int
	var lastSequenceNumber int
	var firstNewParquetFileUuid string
	newManifestListItemsSortedDesc := []ManifestListItem{}
	var newManifestListFile ManifestListFile

	for {
		tempDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempDuckdbTableName, 0)
		if loadedRowCount == 0 && newParquetCount > 0 {
			break
		}

		// Create parquet
		newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, loadedRowCount)
		common.LogDebug(writer.Config, "Parquet file with", newParquetFile.RecordCount, "record(s) created at:", newParquetFile.Key)
		if firstNewParquetFileUuid == "" {
			firstNewParquetFileUuid = newParquetFile.Uuid
		}

		// Create manifest
		lastSequenceNumber++
		newManifestFile := writer.StorageS3.CreateManifest(metadataS3Path, newParquetFile)
		newManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: newManifestFile}
		newManifestListItemsSortedDesc = append([]ManifestListItem{newManifestListItem}, newManifestListItemsSortedDesc...)

		// Create manifest list
		previousManifestListFile := newManifestListFile
		newManifestListFile = writer.StorageS3.CreateManifestList(metadataS3Path, firstNewParquetFileUuid, newManifestListItemsSortedDesc)

		// Create metadata
		writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{newManifestListFile})

		newParquetCount++
		common.LogInfo(writer.Config, "Written", newParquetFile.RecordCount, "records in Parquet file #"+common.IntToString(newParquetCount), "("+writer.formattedParquetFileSize(newParquetFile.Size)+")")

		// Create table
		if newParquetCount == 1 {
			icebergTable.Create(tableS3Path)
		}

		// Delete previous manifest list file
		if previousManifestListFile.Key != "" {
			writer.deleteObject(previousManifestListFile.Key)
		}

		if reachedEnd {
			break
		}
	}
}

func (writer *IcebergWriter) appendRows(metadataFileS3Path string, cursorValue CursorValue, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int, reachedEnd bool)) {
	tableS3Path := strings.Split(metadataFileS3Path, "/metadata/")[0]
	metadataS3Path := tableS3Path + "/metadata"
	dataS3Path := tableS3Path + "/data"

	existingManifestListFile := writer.StorageS3.LastManifestListFile(metadataFileS3Path)
	existingManifestListItemsSortedDesc := writer.StorageS3.ManifestListItems(existingManifestListFile)
	existingLastManifestListItem := existingManifestListItemsSortedDesc[0]
	existingLastParquetFileS3Path, lastParquetFileSize := writer.StorageS3.ParquetFileInfo(existingLastManifestListItem.ManifestFile)

	var newParquetCount int
	lastSequenceNumber := existingLastManifestListItem.SequenceNumber
	firstNewParquetFileUuid := existingManifestListFile.Uuid()
	newManifestListItemsSortedDesc := existingManifestListItemsSortedDesc

	objectsToDeleteKeys := []string{}
	replaceLastExistingParquetFile := lastParquetFileSize < MAX_PARQUET_FILE_SIZE
	if replaceLastExistingParquetFile {
		objectsToDeleteKeys = append(objectsToDeleteKeys, existingLastManifestListItem.ManifestFile.Key)
		objectsToDeleteKeys = append(objectsToDeleteKeys, writer.StorageS3.S3Client.ObjectKey(existingLastParquetFileS3Path))
		newManifestListItemsSortedDesc = newManifestListItemsSortedDesc[1:] // Remove the last item to replace it
	}

	for {
		tempDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

		var initialLoadedRowCount int64
		if newParquetCount == 0 && replaceLastExistingParquetFile {
			writer.insertToDuckdbTableFromParquet(tempDuckdbTableName, existingLastParquetFileS3Path, cursorValue)
			initialLoadedRowCount = lastParquetFileSize * (MAX_ICEBERG_WRITER_BATCH_SIZE * writer.CompressionFactor / MAX_PARQUET_FILE_SIZE)
		}

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempDuckdbTableName, initialLoadedRowCount)
		if loadedRowCount == 0 {
			if newParquetCount == 0 {
				return // no rows to append in the first batch
			} else {
				break // no more rows to append
			}
		}

		// Create parquet
		newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, loadedRowCount)
		common.LogDebug(writer.Config, "Parquet file with", newParquetFile.RecordCount, "record(s) created at:", newParquetFile.Key)
		if firstNewParquetFileUuid == "" {
			firstNewParquetFileUuid = newParquetFile.Uuid
		}

		// Create manifest
		lastSequenceNumber++
		newManifestFile := writer.StorageS3.CreateManifest(metadataS3Path, newParquetFile)
		newManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: newManifestFile}
		newManifestListItemsSortedDesc = append([]ManifestListItem{newManifestListItem}, newManifestListItemsSortedDesc...)

		newParquetCount++
		common.LogInfo(writer.Config, "Written", newParquetFile.RecordCount, "records in Parquet file #"+common.IntToString(newParquetCount), "("+writer.formattedParquetFileSize(newParquetFile.Size)+")")

		if reachedEnd {
			break
		}
	}

	// Create manifest list
	newManifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, firstNewParquetFileUuid, newManifestListItemsSortedDesc)

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{newManifestListFile})

	// Delete old files
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListFile.Key)
	for _, key := range objectsToDeleteKeys {
		writer.deleteObject(key)
	}
}

func (writer *IcebergWriter) updateRows(metadataFileS3Path string, uniqueIndexColumnNames []string, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int, reachedEnd bool)) {
	tableS3Path := strings.Split(metadataFileS3Path, "/metadata/")[0]
	metadataS3Path := tableS3Path + "/metadata"
	dataS3Path := tableS3Path + "/data"

	existingManifestListFile := writer.StorageS3.LastManifestListFile(metadataFileS3Path)
	manifestListItemsSortedDesc := writer.StorageS3.ManifestListItems(existingManifestListFile)
	firstNewParquetFileUuid := existingManifestListFile.Uuid()

	objectsToDeleteKeys := []string{}
	for {
		tempUpdatedRowsDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempUpdatedRowsDuckdbTableName)

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempUpdatedRowsDuckdbTableName, 0)
		if loadedRowCount == 0 {
			if len(objectsToDeleteKeys) == 0 {
				return // no rows to update in the first batch
			} else {
				break // no more rows to update
			}
		}

		for i, manifestListItem := range manifestListItemsSortedDesc {
			parquetFileS3Path, _ := writer.StorageS3.ParquetFileInfo(manifestListItem.ManifestFile)
			if !writer.hasOverlappingRowsInParquet(tempUpdatedRowsDuckdbTableName, parquetFileS3Path, uniqueIndexColumnNames) {
				continue
			}

			objectsToDeleteKeys = append(objectsToDeleteKeys, writer.StorageS3.S3Client.ObjectKey(parquetFileS3Path))
			objectsToDeleteKeys = append(objectsToDeleteKeys, manifestListItem.ManifestFile.Key)

			tempDuckdbTableName := writer.createTempDuckdbTable()
			defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

			writer.insertToDuckdbTableFromParquetWithoutOverlappingDuckdbTableRows(tempDuckdbTableName, tempUpdatedRowsDuckdbTableName, parquetFileS3Path, uniqueIndexColumnNames)
			writer.insertToDuckdbTableFromDuckdbTableThatOverlapWithParquetRows(tempDuckdbTableName, tempUpdatedRowsDuckdbTableName, parquetFileS3Path, uniqueIndexColumnNames)

			// Create parquet
			newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, loadedRowCount)
			common.LogDebug(writer.Config, "Parquet file with", newParquetFile.RecordCount, "record(s) created at:", newParquetFile.Key)

			// Create manifest
			newManifestFile := writer.StorageS3.CreateManifest(metadataS3Path, newParquetFile)
			manifestListItemsSortedDesc[i].ManifestFile = newManifestFile
		}

		if reachedEnd {
			break
		}
	}

	if len(objectsToDeleteKeys) == 0 {
		return // no overlapping rows found
	}

	// Create manifest list
	newManifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, firstNewParquetFileUuid, manifestListItemsSortedDesc)

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{newManifestListFile})

	// Delete old files
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListFile.Key)
	for _, key := range objectsToDeleteKeys {
		writer.deleteObject(key)
	}
}

func (writer *IcebergWriter) deleteRows(metadataFileS3Path string, uniqueIndexColumnNames []string, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int, reachedEnd bool)) {
	tableS3Path := strings.Split(metadataFileS3Path, "/metadata/")[0]
	metadataS3Path := tableS3Path + "/metadata"
	dataS3Path := tableS3Path + "/data"

	existingManifestListFile := writer.StorageS3.LastManifestListFile(metadataFileS3Path)
	manifestListItemsSortedDesc := writer.StorageS3.ManifestListItems(existingManifestListFile)
	firstNewParquetFileUuid := existingManifestListFile.Uuid()

	objectsToDeleteKeys := []string{}
	for {
		tempDeletedRowsDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempDeletedRowsDuckdbTableName)

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempDeletedRowsDuckdbTableName, 0)
		if loadedRowCount == 0 {
			if len(objectsToDeleteKeys) == 0 {
				return // no rows to delete in the first batch
			} else {
				break // no more rows to delete
			}
		}

		for i, manifestListItem := range manifestListItemsSortedDesc {
			parquetFileS3Path, _ := writer.StorageS3.ParquetFileInfo(manifestListItem.ManifestFile)
			if !writer.hasOverlappingRowsInParquet(tempDeletedRowsDuckdbTableName, parquetFileS3Path, uniqueIndexColumnNames) {
				continue
			}

			objectsToDeleteKeys = append(objectsToDeleteKeys, writer.StorageS3.S3Client.ObjectKey(parquetFileS3Path))
			objectsToDeleteKeys = append(objectsToDeleteKeys, manifestListItem.ManifestFile.Key)

			tempDuckdbTableName := writer.createTempDuckdbTable()
			defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

			writer.insertToDuckdbTableFromParquetWithoutOverlappingDuckdbTableRows(tempDuckdbTableName, tempDeletedRowsDuckdbTableName, parquetFileS3Path, uniqueIndexColumnNames)

			// Create parquet
			newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, loadedRowCount)
			common.LogDebug(writer.Config, "Parquet file with", newParquetFile.RecordCount, "record(s) created at:", newParquetFile.Key)

			// Create manifest
			newManifestFile := writer.StorageS3.CreateManifest(metadataS3Path, newParquetFile)
			manifestListItemsSortedDesc[i].ManifestFile = newManifestFile
		}

		if reachedEnd {
			break
		}
	}

	if len(objectsToDeleteKeys) == 0 {
		return // no overlapping rows found
	}

	// Create manifest list
	newManifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, firstNewParquetFileUuid, manifestListItemsSortedDesc)

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{newManifestListFile})

	// Delete old files
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListFile.Key)
	for _, key := range objectsToDeleteKeys {
		writer.deleteObject(key)
	}
}

func (writer *IcebergWriter) createTempDuckdbTable() string {
	tableName := "temp_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	columnSchemas := make([]string, len(writer.IcebergSchemaColumns))
	for i, col := range writer.IcebergSchemaColumns {
		columnSchemas[i] = col.QuotedColumnName() + " " + col.DuckdbType()
	}

	_, err := writer.DuckdbClient.ExecContext(context.Background(), "CREATE TABLE "+tableName+"("+strings.Join(columnSchemas, ",")+")")
	common.PanicIfError(writer.Config, err)

	return tableName
}

func (writer *IcebergWriter) hasOverlappingRowsInParquet(duckdbTableName string, parquetFileS3Path string, uniqueIndexColumnNames []string) bool {
	uniqueIndexConditions := make([]string, len(uniqueIndexColumnNames))
	for i, uniqueIndexColumnName := range uniqueIndexColumnNames {
		uniqueIndexConditions[i] = `"` + uniqueIndexColumnName + `" IN (SELECT "` + uniqueIndexColumnName + `" FROM ` + duckdbTableName + ")"
	}
	countSql := "SELECT COUNT(*) FROM read_parquet('" + parquetFileS3Path + "') WHERE " + strings.Join(uniqueIndexConditions, " AND ")

	var count int
	err := writer.DuckdbClient.QueryRowContext(context.Background(), countSql).Scan(&count)
	common.PanicIfError(writer.Config, err)
	return count > 0
}

func (writer *IcebergWriter) insertToDuckdbTableFromParquet(duckdbTableName string, parquetFileS3Path string, cursorValue CursorValue) {
	sql := "INSERT INTO " + duckdbTableName + " SELECT * FROM read_parquet('" + parquetFileS3Path + "')"
	if cursorValue.OverrideRows {
		// Exclude rows with the cursor value
		sql += ` WHERE "` + cursorValue.ColumnName + `" != '` + cursorValue.StringValue + `'`
		common.LogInfo(writer.Config, "Replacing last existing Parquet file excluding cursor value:", strings.Split(parquetFileS3Path, "/data/")[1])
	} else {
		common.LogInfo(writer.Config, "Replacing last existing Parquet file:", strings.Split(parquetFileS3Path, "/data/")[1])
	}

	_, err := writer.DuckdbClient.ExecContext(context.Background(), sql)
	common.PanicIfError(writer.Config, err)
}

func (writer *IcebergWriter) insertToDuckdbTableFromDuckdbTableThatOverlapWithParquetRows(insertDuckdbTableName string, sourceDuckdbTableName string, parquetFileS3Path string, uniqueIndexColumnNames []string) {
	uniqueIndexConditions := make([]string, len(uniqueIndexColumnNames))
	for i, uniqueIndexColumnName := range uniqueIndexColumnNames {
		uniqueIndexConditions[i] = `"` + uniqueIndexColumnName + `" IN (SELECT "` + uniqueIndexColumnName + `" FROM read_parquet('` + parquetFileS3Path + "'))"
	}
	sql := "INSERT INTO " + insertDuckdbTableName + " SELECT * FROM " + sourceDuckdbTableName + " WHERE " + strings.Join(uniqueIndexConditions, " AND ")
	_, err := writer.DuckdbClient.ExecContext(context.Background(), sql)
	common.PanicIfError(writer.Config, err)
}

func (writer *IcebergWriter) insertToDuckdbTableFromParquetWithoutOverlappingDuckdbTableRows(insertDuckdbTableName string, overlappingDuckdbTableName string, parquetFileS3Path string, uniqueIndexColumnNames []string) {
	uniqueIndexConditions := make([]string, len(uniqueIndexColumnNames))
	for i, uniqueIndexColumnName := range uniqueIndexColumnNames {
		uniqueIndexConditions[i] = `"` + uniqueIndexColumnName + `" NOT IN (SELECT "` + uniqueIndexColumnName + `" FROM ` + overlappingDuckdbTableName + ")"
	}
	sql := "INSERT INTO " + insertDuckdbTableName + " SELECT * FROM read_parquet('" + parquetFileS3Path + "') WHERE " + strings.Join(uniqueIndexConditions, " AND ")
	_, err := writer.DuckdbClient.ExecContext(context.Background(), sql)
	common.PanicIfError(writer.Config, err)
}

func (writer *IcebergWriter) deleteTempDuckdbTable(duckdbTableName string) {
	_, err := writer.DuckdbClient.ExecContext(context.Background(), "DROP TABLE "+duckdbTableName)
	common.PanicIfError(writer.Config, err)
}

func (writer *IcebergWriter) deleteObject(key string) {
	var fileName string
	if strings.Contains(key, "/data/") {
		fileName = strings.Split(key, "/data/")[1]
	} else if strings.Contains(key, "/metadata/") {
		fileName = strings.Split(key, "/metadata/")[1]
	}

	common.LogInfo(writer.Config, "Deleting object:", fileName)
	writer.StorageS3.S3Client.DeleteObject(key)
}

func (writer *IcebergWriter) loadCsvRows(duckdbTableName string, csvReader *csv.Reader, loadedSize int64) (loadedRowCount int, reachedEnd bool) {
	appender, err := writer.DuckdbClient.Appender("", duckdbTableName)
	common.PanicIfError(writer.Config, err)
	defer appender.Close()

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			reachedEnd = true
			break
		}
		common.PanicIfError(writer.Config, err)

		duckdbRowValues := make([]driver.Value, len(writer.IcebergSchemaColumns))
		for i, icebergSchemaColumn := range writer.IcebergSchemaColumns {
			duckdbRowValues[i] = icebergSchemaColumn.DuckdbValueFromCsv(row[i])
			loadedSize += int64(len(row[i]))
		}

		common.LogTrace(writer.Config, "DuckDB appending row values:", duckdbRowValues)
		err = appender.AppendRow(duckdbRowValues...)
		common.PanicIfError(writer.Config, err)

		loadedRowCount++
		if loadedSize >= (MAX_ICEBERG_WRITER_BATCH_SIZE * writer.CompressionFactor) {
			common.LogDebug(writer.Config, "Reached batch size limit")
			break
		}
	}

	common.LogInfo(writer.Config, "Loaded", loadedRowCount, "rows")
	return loadedRowCount, reachedEnd
}

func (writer *IcebergWriter) loadJsonRows(duckdbTableName string, jsonQueueReader *JsonQueueReader, loadedSize int64) (loadedRowCount int, reachedEnd bool) {
	appender, err := writer.DuckdbClient.Appender("", duckdbTableName)
	common.PanicIfError(writer.Config, err)
	defer appender.Close()

	for {
		var rowValues map[string]interface{}
		valueSize, err := jsonQueueReader.Read(&rowValues)
		if err == io.EOF {
			reachedEnd = true
			break
		}
		common.PanicIfError(writer.Config, err)
		loadedSize += int64(valueSize)

		duckdbRowValues := writer.jsonToDuckdbRowValues(rowValues)

		common.LogTrace(writer.Config, "DuckDB appending row values:", rowValues)
		err = appender.AppendRow(duckdbRowValues...)
		common.PanicIfError(writer.Config, err)

		loadedRowCount++
		if loadedSize >= (MAX_ICEBERG_WRITER_BATCH_SIZE * writer.CompressionFactor) {
			common.LogDebug(writer.Config, "Reached batch size limit")
			break
		}
	}

	common.LogInfo(writer.Config, "Loaded", loadedRowCount, "rows")
	return loadedRowCount, reachedEnd
}

func (writer *IcebergWriter) jsonToDuckdbRowValues(rowValues map[string]interface{}) []driver.Value {
	// Detect row column drift
	rowColumnNames := []string{}
	for columnName := range rowValues {
		rowColumnNames = append(rowColumnNames, columnName)
	}
	tableColumnNames := make([]string, len(writer.IcebergSchemaColumns))
	for i, icebergSchemaColumn := range writer.IcebergSchemaColumns {
		tableColumnNames[i] = strings.ToLower(icebergSchemaColumn.ColumnName) // Debezium JSON keys are lowercase
	}
	if len(rowColumnNames) != len(tableColumnNames) {
		common.Panic(writer.Config, "Row column names count doesn't match table column names count: "+strings.Join(rowColumnNames, ", ")+" (row) vs "+strings.Join(tableColumnNames, ", ")+" (table)")
	}
	sort.Strings(rowColumnNames)
	sort.Strings(tableColumnNames)
	for i := range rowColumnNames {
		if rowColumnNames[i] != tableColumnNames[i] {
			common.Panic(writer.Config, "Row column names don't match table column names: "+strings.Join(rowColumnNames, ", ")+" (row) vs "+strings.Join(tableColumnNames, ", ")+" (table)")
		}
	}

	// Convert row values to DuckDB values
	duckdbRowValues := make([]driver.Value, len(writer.IcebergSchemaColumns))
	for i, icebergSchemaColumn := range writer.IcebergSchemaColumns {
		columnName := strings.ToLower(icebergSchemaColumn.ColumnName) // Debezium JSON keys are lowercase
		value := rowValues[columnName]
		duckdbRowValues[i] = icebergSchemaColumn.DuckdbValueFromJson(value)
	}
	return duckdbRowValues
}

func (writer *IcebergWriter) formattedParquetFileSize(parquetFileSize int64) string {
	if parquetFileSize < 1024 {
		return Int64ToString(parquetFileSize) + "B"
	} else if parquetFileSize < 1024*1024 {
		return Int64ToString(parquetFileSize/1024) + "KB"
	} else if parquetFileSize < 1024*1024*1024 {
		return Int64ToString(parquetFileSize/(1024*1024)) + "MB"
	}
	return Int64ToString(parquetFileSize/(1024*1024*1024)) + "GB"
}
