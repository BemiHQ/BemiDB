package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

const (
	ICEBERG_MANIFEST_STATUS_EXISTING = 0
	ICEBERG_MANIFEST_STATUS_ADDED    = 1
	ICEBERG_MANIFEST_STATUS_DELETED  = 2

	ICEBERG_MANIFEST_LIST_OPERATION_REPLACE   = "replace"
	ICEBERG_MANIFEST_LIST_OPERATION_APPEND    = "append"
	ICEBERG_MANIFEST_LIST_OPERATION_OVERWRITE = "overwrite"
	ICEBERG_MANIFEST_LIST_OPERATION_DELETE    = "delete"

	ICEBERG_METADATA_INITIAL_FILE_NAME = "v1.metadata.json"
)

type MetadataJson struct {
	Schemas []struct {
		Fields []struct {
			ID       int         `json:"id"`
			Name     string      `json:"name"`
			Type     interface{} `json:"type"`
			Required bool        `json:"required"`
		} `json:"fields"`
	} `json:"schemas"`
}

type ManifestListsJson struct {
	Snapshots []struct {
		SequenceNumber int    `json:"sequence-number"`
		SnapshotId     int64  `json:"snapshot-id"`
		TimestampMs    int64  `json:"timestamp-ms"`
		Path           string `json:"manifest-list"`
		Summary        struct {
			Operation      string `json:"operation"`
			TotalDataFiles string `json:"total-data-files"`
			TotalFilesSize string `json:"total-files-size"`
			TotalRecords   string `json:"total-records"`
		} `json:"summary"`
	} `json:"snapshots"`
}

type ManifestListSequenceStats struct {
	TotalFilesSize int64
	TotalDataFiles int64
	TotalRecords   int64
}

type StorageUtils struct {
	Config *CommonConfig
}

func NewStorageUtils(config *CommonConfig) *StorageUtils {
	return &StorageUtils{
		Config: config,
	}
}

// Write ---------------------------------------------------------------------------------------------------------------

func (utils *StorageUtils) WriteParquetFile(fileS3Path string, duckdbClient *DuckdbClient, tempDuckdbTableName string, icebergSchemaColumns []*IcebergSchemaColumn) {
	fieldIds := make([]string, len(icebergSchemaColumns))
	for i, col := range icebergSchemaColumns {
		if col.IsList {
			fieldIds[i] = col.QuotedColumnName() + ":{__duckdb_field_id: " + IntToString(col.Position) + ", element: " + IntToString(PARQUET_NESTED_FIELD_ID_PREFIX+col.Position) + "}"
		} else {
			fieldIds[i] = col.QuotedColumnName() + ":" + IntToString(col.Position)
		}
	}

	copyQuery := "COPY " + tempDuckdbTableName + " TO '$fileS3Path' (FORMAT PARQUET, COMPRESSION 'ZSTD', FIELD_IDS {$fieldIds})"
	_, err := duckdbClient.ExecContext(context.Background(), copyQuery, map[string]string{
		"fileS3Path": fileS3Path,
		"fieldIds":   strings.Join(fieldIds, ","),
	})
	PanicIfError(utils.Config, err)
}

func (utils *StorageUtils) ReadParquetStats(fileReader source.ParquetFile, icebergSchemaColumns []*IcebergSchemaColumn) (parquetStats ParquetFileStats) {
	defer fileReader.Close()

	pr, err := reader.NewParquetReader(fileReader, nil, 1)
	PanicIfError(utils.Config, err)
	defer pr.ReadStop()

	parquetStats = ParquetFileStats{
		ColumnSizes:     make(map[int]int64),
		ValueCounts:     make(map[int]int64),
		NullValueCounts: make(map[int]int64),
		LowerBounds:     make(map[int][]byte),
		UpperBounds:     make(map[int][]byte),
		SplitOffsets:    []int64{},
	}

	stringColumnNames := NewSet[string]()
	fieldIdByColumnName := make(map[string]int)
	for _, column := range icebergSchemaColumns {
		columnName := strings.ToLower(column.NormalizedColumnName())

		if column.IsList {
			fieldIdByColumnName[columnName] = PARQUET_NESTED_FIELD_ID_PREFIX + column.Position
		} else {
			fieldIdByColumnName[columnName] = column.Position
		}

		if column.ColumnType == IcebergColumnTypeString {
			stringColumnNames.Add(columnName)
		}
	}

	for _, rowGroup := range pr.Footer.RowGroups {
		if rowGroup.FileOffset != nil {
			parquetStats.SplitOffsets = append(parquetStats.SplitOffsets, *rowGroup.FileOffset)
		}

		for _, columnChunk := range rowGroup.Columns {
			columnMetaData := columnChunk.MetaData
			columnName := strings.ToLower(columnMetaData.PathInSchema[0]) // Parquet column names look like Timemscolumn instead of timeMsColumn
			fieldId, ok := fieldIdByColumnName[columnName]
			if !ok {
				continue
			}
			parquetStats.ColumnSizes[fieldId] += columnMetaData.TotalCompressedSize
			parquetStats.ValueCounts[fieldId] += int64(columnMetaData.NumValues)
			if columnMetaData.Statistics != nil && columnMetaData.Statistics.NullCount != nil {
				parquetStats.NullValueCounts[fieldId] += *columnMetaData.Statistics.NullCount
			} else {
				parquetStats.NullValueCounts[fieldId] += 0
			}

			if columnMetaData.Statistics != nil && fieldId <= PARQUET_NESTED_FIELD_ID_PREFIX { // Not a nested field
				minValue := columnMetaData.Statistics.Min
				maxValue := columnMetaData.Statistics.Max

				// Ignore empty values for non-string columns
				if (stringColumnNames.Contains(columnName) || len(minValue) > 0) && (parquetStats.LowerBounds[fieldId] == nil || bytes.Compare(parquetStats.LowerBounds[fieldId], minValue) > 0) {
					parquetStats.LowerBounds[fieldId] = minValue
				}
				if (stringColumnNames.Contains(columnName) || len(maxValue) > 0) && (parquetStats.UpperBounds[fieldId] == nil || bytes.Compare(parquetStats.UpperBounds[fieldId], maxValue) < 0) {
					parquetStats.UpperBounds[fieldId] = maxValue
				}
			}
		}
	}

	return parquetStats
}
func (utils *StorageUtils) WriteManifestFile(filePath string, parquetFilesSortedAsc []ParquetFile) (int64, error) {
	manifestEntries := make([]map[string]interface{}, len(parquetFilesSortedAsc))

	for i, parquetFile := range parquetFilesSortedAsc {
		snapshotId := time.Now().UnixNano()

		columnSizesArr := []interface{}{}
		for fieldId, size := range parquetFile.Stats.ColumnSizes {
			columnSizesArr = append(columnSizesArr, map[string]interface{}{
				"key":   fieldId,
				"value": size,
			})
		}

		valueCountsArr := []interface{}{}
		for fieldId, count := range parquetFile.Stats.ValueCounts {
			valueCountsArr = append(valueCountsArr, map[string]interface{}{
				"key":   fieldId,
				"value": count,
			})
		}

		nullValueCountsArr := []interface{}{}
		for fieldId, count := range parquetFile.Stats.NullValueCounts {
			nullValueCountsArr = append(nullValueCountsArr, map[string]interface{}{
				"key":   fieldId,
				"value": count,
			})
		}

		lowerBoundsArr := []interface{}{}
		for fieldId, value := range parquetFile.Stats.LowerBounds {
			lowerBoundsArr = append(lowerBoundsArr, map[string]interface{}{
				"key":   fieldId,
				"value": value,
			})
		}

		upperBoundsArr := []interface{}{}
		for fieldId, value := range parquetFile.Stats.UpperBounds {
			upperBoundsArr = append(upperBoundsArr, map[string]interface{}{
				"key":   fieldId,
				"value": value,
			})
		}

		dataFile := map[string]interface{}{
			"content":            0, // 0: DATA, 1: POSITION DELETES, 2: EQUALITY DELETES
			"file_path":          parquetFile.Path,
			"file_format":        "PARQUET",
			"partition":          map[string]interface{}{},
			"record_count":       parquetFile.RecordCount,
			"file_size_in_bytes": parquetFile.Size,
			"column_sizes": map[string]interface{}{
				"array": columnSizesArr,
			},
			"value_counts": map[string]interface{}{
				"array": valueCountsArr,
			},
			"null_value_counts": map[string]interface{}{
				"array": nullValueCountsArr,
			},
			"nan_value_counts": map[string]interface{}{
				"array": []interface{}{},
			},
			"lower_bounds": map[string]interface{}{
				"array": lowerBoundsArr,
			},
			"upper_bounds": map[string]interface{}{
				"array": upperBoundsArr,
			},
			"key_metadata": nil,
			"split_offsets": map[string]interface{}{
				"array": parquetFile.Stats.SplitOffsets,
			},
			"equality_ids": nil,
			"sort_order_id": map[string]interface{}{
				"int": 0,
			},
		}

		manifestEntries[i] = map[string]interface{}{
			"status":               ICEBERG_MANIFEST_STATUS_EXISTING,
			"snapshot_id":          map[string]interface{}{"long": snapshotId},
			"sequence_number":      map[string]interface{}{"long": i + 1},
			"file_sequence_number": map[string]interface{}{"long": i + 1},
			"data_file":            dataFile,
		}
	}

	avroFile, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create manifest file: %v", err)
	}
	defer avroFile.Close()

	codec, err := goavro.NewCodec(MANIFEST_SCHEMA)
	if err != nil {
		return 0, fmt.Errorf("failed to create Avro codec: %v", err)
	}

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      avroFile,
		Codec:  codec,
		Schema: MANIFEST_SCHEMA,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create Avro OCF writer: %v", err)
	}

	err = ocfWriter.Append(manifestEntries)
	if err != nil {
		return 0, fmt.Errorf("failed to write to manifest file: %v", err)
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to get manifest file info: %v", err)
	}
	fileSize := fileInfo.Size()

	return fileSize, nil
}

func (utils *StorageUtils) WriteManifestListFile(filePath string, totalDataFileSize int64, manifestListItemsSortedDesc []ManifestListItem) (ManifestListFile, error) {
	snapshotId := time.Now().UnixNano()
	manifestListEntries := make([]map[string]interface{}, len(manifestListItemsSortedDesc))
	statsBySequenceNumber := make(map[string]ManifestListSequenceStats)

	for i, manifestListItem := range manifestListItemsSortedDesc {
		sequenceNumber := manifestListItem.SequenceNumber
		sequenceStats := statsBySequenceNumber[IntToString(sequenceNumber)]
		manifestFile := manifestListItem.ManifestFile

		manifestListRecord := map[string]interface{}{
			"added_snapshot_id":    snapshotId,
			"manifest_length":      manifestFile.Size,
			"manifest_path":        manifestFile.Path,
			"min_sequence_number":  1,
			"sequence_number":      sequenceNumber,
			"content":              0,
			"deleted_files_count":  0,
			"deleted_rows_count":   0,
			"existing_files_count": manifestFile.TotalDataFileCount,
			"existing_rows_count":  manifestFile.TotalRecordCount,
			"key_metadata":         nil,
			"partition_spec_id":    0,
			"partitions":           map[string]interface{}{"array": []string{}},
		}

		if manifestFile.RecordsDeleted {
			manifestListRecord["added_files_count"] = 0
			manifestListRecord["added_rows_count"] = 0
			manifestListRecord["deleted_files_count"] = 1
			manifestListRecord["deleted_rows_count"] = manifestFile.TotalRecordCount
		} else {
			manifestListRecord["added_files_count"] = 0
			manifestListRecord["added_rows_count"] = 0
			manifestListRecord["deleted_files_count"] = 0
			manifestListRecord["deleted_rows_count"] = 0
			sequenceStats.TotalFilesSize += totalDataFileSize
			sequenceStats.TotalDataFiles += int64(manifestFile.TotalDataFileCount)
			sequenceStats.TotalRecords += manifestFile.TotalRecordCount
		}

		statsBySequenceNumber[IntToString(sequenceNumber)] = sequenceStats
		manifestListEntries[i] = manifestListRecord
	}

	codec, err := goavro.NewCodec(MANIFEST_LIST_SCHEMA)
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to create Avro codec for manifest list: %v", err)
	}

	avroFile, err := os.Create(filePath)
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to create manifest list file: %v", err)
	}
	defer avroFile.Close()

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      avroFile,
		Codec:  codec,
		Schema: MANIFEST_LIST_SCHEMA,
	})
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to create OCF writer for manifest list: %v", err)
	}

	err = ocfWriter.Append(manifestListEntries)
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to write manifest list record: %v", err)
	}

	lastManifestListItem := manifestListItemsSortedDesc[0]
	lastSequenceStats := statsBySequenceNumber[IntToString(lastManifestListItem.SequenceNumber)]

	manifestListFile := ManifestListFile{
		SequenceNumber: lastManifestListItem.SequenceNumber,
		SnapshotId:     snapshotId,
		TimestampMs:    time.Now().UnixNano() / int64(time.Millisecond),
		Operation:      ICEBERG_MANIFEST_LIST_OPERATION_REPLACE,
		TotalFilesSize: lastSequenceStats.TotalFilesSize,
		TotalDataFiles: lastSequenceStats.TotalDataFiles,
		TotalRecords:   lastSequenceStats.TotalRecords,
	}
	return manifestListFile, nil
}

func (utils *StorageUtils) WriteMetadataFile(s3TablePath string, filePath string, icebergSchemaColumns []*IcebergSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (err error) {
	tableUuid := uuid.New().String()
	snapshotId := time.Now().UnixNano()

	lastColumnId := 0
	for _, col := range icebergSchemaColumns {
		if col.IsList {
			columnId := PARQUET_NESTED_FIELD_ID_PREFIX + col.Position
			if columnId > lastColumnId {
				lastColumnId = columnId
			}
		} else if col.Position > lastColumnId {
			lastColumnId = col.Position
		}
	}

	icebergSchemaFields := make([]interface{}, len(icebergSchemaColumns))
	for i, icebergSchemaColumn := range icebergSchemaColumns {
		icebergSchemaFields[i] = icebergSchemaColumn.IcebergSchemaFieldMap()
	}

	snapshots := make([]map[string]interface{}, len(manifestListFilesSortedAsc))
	snapshotLog := make([]map[string]interface{}, len(manifestListFilesSortedAsc))

	var totalDataFiles, totalFilesSize, totalRecords int64

	for i, manifestListFile := range manifestListFilesSortedAsc {
		totalDataFiles += manifestListFile.TotalDataFiles
		totalFilesSize += manifestListFile.TotalFilesSize
		totalRecords += manifestListFile.TotalRecords

		snapshot := map[string]interface{}{
			"schema-id":       0,
			"snapshot-id":     snapshotId,
			"sequence-number": manifestListFile.SequenceNumber,
			"timestamp-ms":    manifestListFile.TimestampMs,
			"manifest-list":   manifestListFile.Path,
			"summary": map[string]interface{}{
				"changed-partition-count": "0",
				"manifests-kept":          "0",
				"manifests-replaced":      "0",
				"manifests-created":       "1",
				"entries-processed":       Int64ToString(totalDataFiles),
				"operation":               manifestListFile.Operation,
				"total-data-files":        Int64ToString(totalDataFiles),
				"total-files-size":        Int64ToString(totalFilesSize),
				"total-records":           Int64ToString(totalRecords),
				"total-delete-files":      "0",
				"total-equality-deletes":  "0",
				"total-position-deletes":  "0",
			},
		}
		if i != 0 {
			snapshot["parent-snapshot-id"] = manifestListFilesSortedAsc[i-1].SnapshotId
		}
		snapshots[i] = snapshot

		snapshotLog[i] = map[string]interface{}{
			"snapshot-id":  snapshotId,
			"timestamp-ms": manifestListFile.TimestampMs,
		}
	}

	lastManifestListFile := manifestListFilesSortedAsc[len(manifestListFilesSortedAsc)-1]
	metadata := map[string]interface{}{
		"format-version":       2,
		"table-uuid":           tableUuid,
		"statistics":           []interface{}{},
		"location":             s3TablePath,
		"last-sequence-number": lastManifestListFile.SequenceNumber,
		"last-updated-ms":      lastManifestListFile.TimestampMs,
		"last-column-id":       lastColumnId,
		"schemas": []interface{}{
			map[string]interface{}{
				"type":                 "struct",
				"schema-id":            0,
				"fields":               icebergSchemaFields,
				"identifier-field-ids": []interface{}{},
			},
		},
		"current-schema-id": 0,
		"partition-specs": []interface{}{
			map[string]interface{}{
				"spec-id": 0,
				"fields":  []interface{}{},
			},
		},
		"default-spec-id":       0,
		"default-sort-order-id": 0,
		"last-partition-id":     999, // Assuming no partitions; set to a placeholder
		"properties":            map[string]string{},
		"current-snapshot-id":   snapshotId,
		"refs": map[string]interface{}{
			"main": map[string]interface{}{
				"snapshot-id": snapshotId,
				"type":        "branch",
			},
		},
		"snapshots":    snapshots,
		"snapshot-log": snapshotLog,
		"metadata-log": []interface{}{},
		"sort-orders": []interface{}{
			map[string]interface{}{
				"order-id": 0,
				"fields":   []interface{}{},
			},
		},
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(metadata)
	if err != nil {
		return fmt.Errorf("failed to write metadata to file: %v", err)
	}

	return nil
}

// Read ---------------------------------------------------------------------------------------------------------------

func (utils *StorageUtils) ParseLastManifestListFile(bucketS3Prefix string, metadataContent []byte) ManifestListFile {
	var manifestListsJson ManifestListsJson
	err := json.Unmarshal(metadataContent, &manifestListsJson)
	PanicIfError(utils.Config, err)

	snapshot := manifestListsJson.Snapshots[len(manifestListsJson.Snapshots)-1]

	return ManifestListFile{
		SequenceNumber: snapshot.SequenceNumber,
		SnapshotId:     snapshot.SnapshotId,
		TimestampMs:    snapshot.TimestampMs,
		Key:            strings.TrimPrefix(snapshot.Path, bucketS3Prefix),
		Path:           snapshot.Path,
		Operation:      snapshot.Summary.Operation,
		TotalFilesSize: StringToInt64(snapshot.Summary.TotalFilesSize),
		TotalDataFiles: StringToInt64(snapshot.Summary.TotalDataFiles),
		TotalRecords:   StringToInt64(snapshot.Summary.TotalRecords),
	}
}

func (utils *StorageUtils) ParseManifestListItems(bucketS3Prefix string, manifestListFileContent []byte) []ManifestListItem {
	ocfReader, err := goavro.NewOCFReader(strings.NewReader(string(manifestListFileContent)))
	PanicIfError(utils.Config, err)

	manifestListItemsSortedDesc := []ManifestListItem{}

	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		PanicIfError(utils.Config, err)

		recordMap := record.(map[string]interface{})

		manifestListItemsSortedDesc = append(manifestListItemsSortedDesc, ManifestListItem{
			ManifestFile: ManifestFile{
				Key:                strings.TrimPrefix(recordMap["manifest_path"].(string), bucketS3Prefix),
				Path:               recordMap["manifest_path"].(string),
				Size:               recordMap["manifest_length"].(int64),
				TotalRecordCount:   recordMap["added_rows_count"].(int64),
				TotalDataFileCount: recordMap["existing_files_count"].(int32),
				RecordsDeleted:     false,
			},
			SequenceNumber: int(recordMap["sequence_number"].(int64)),
		})
	}

	return manifestListItemsSortedDesc
}

func (utils *StorageUtils) ParseParquetFiles(manifestContent []byte) []ParquetFile {
	ocfReader, err := goavro.NewOCFReader(strings.NewReader(string(manifestContent)))
	PanicIfError(utils.Config, err)

	parquetFilesSortedAsc := []ParquetFile{}

	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		PanicIfError(utils.Config, err)

		recordMap := record.(map[string]interface{})
		dataFile := recordMap["data_file"].(map[string]interface{})

		parquetFilesSortedAsc = append(parquetFilesSortedAsc, ParquetFile{
			Path:        dataFile["file_path"].(string),
			Size:        dataFile["file_size_in_bytes"].(int64),
			RecordCount: dataFile["record_count"].(int64),
		})
	}

	return parquetFilesSortedAsc
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	MANIFEST_SCHEMA = `{
		"type" : "record",
		"name" : "manifest_entry",
		"fields" : [ {
			"name" : "status",
			"type" : "int",
			"field-id" : 0
		}, {
			"name" : "snapshot_id",
			"type" : [ "null", "long" ],
			"default" : null,
			"field-id" : 1
		}, {
			"name" : "sequence_number",
			"type" : [ "null", "long" ],
			"default" : null,
			"field-id" : 3
		}, {
			"name" : "file_sequence_number",
			"type" : [ "null", "long" ],
			"default" : null,
			"field-id" : 4
		}, {
			"name" : "data_file",
			"type" : {
			"type" : "record",
			"name" : "r2",
			"fields" : [ {
				"name" : "content",
				"type" : "int",
				"doc" : "File format name: avro, orc, or parquet",
				"field-id" : 134
			}, {
				"name" : "file_path",
				"type" : "string",
				"doc" : "Location URI with FS scheme",
				"field-id" : 100
			}, {
				"name" : "file_format",
				"type" : "string",
				"doc" : "File format name: avro, orc, or parquet",
				"field-id" : 101
			}, {
				"name" : "partition",
				"type" : {
					"type" : "map",
					"values" : [ "null", "string" ],
					"key-id" : 10001,
					"value-id" : 10002
				},
				"doc" : "Partition values tuple, schema based on the partition spec",
				"field-id" : 102
			}, {
				"name" : "record_count",
				"type" : "long",
				"doc" : "Number of records in the file",
				"field-id" : 103
			}, {
				"name" : "file_size_in_bytes",
				"type" : "long",
				"doc" : "Total file size in bytes",
				"field-id" : 104
			}, {
				"name" : "column_sizes",
				"type" : [ "null", {
				"type" : "array",
				"items" : {
					"type" : "record",
					"name" : "k117_v118",
					"fields" : [ {
					"name" : "key",
					"type" : "int",
					"field-id" : 117
					}, {
					"name" : "value",
					"type" : "long",
					"field-id" : 118
					} ]
				},
				"logicalType" : "map"
				} ],
				"doc" : "Map of column id to total size on disk",
				"default" : null,
				"field-id" : 108
			}, {
				"name" : "value_counts",
				"type" : [ "null", {
				"type" : "array",
				"items" : {
					"type" : "record",
					"name" : "k119_v120",
					"fields" : [ {
					"name" : "key",
					"type" : "int",
					"field-id" : 119
					}, {
					"name" : "value",
					"type" : "long",
					"field-id" : 120
					} ]
				},
				"logicalType" : "map"
				} ],
				"doc" : "Map of column id to total count, including null and NaN",
				"default" : null,
				"field-id" : 109
			}, {
				"name" : "null_value_counts",
				"type" : [ "null", {
				"type" : "array",
				"items" : {
					"type" : "record",
					"name" : "k121_v122",
					"fields" : [ {
					"name" : "key",
					"type" : "int",
					"field-id" : 121
					}, {
					"name" : "value",
					"type" : "long",
					"field-id" : 122
					} ]
				},
				"logicalType" : "map"
				} ],
				"doc" : "Map of column id to null value count",
				"default" : null,
				"field-id" : 110
			}, {
				"name" : "nan_value_counts",
				"type" : [ "null", {
				"type" : "array",
				"items" : {
					"type" : "record",
					"name" : "k138_v139",
					"fields" : [ {
					"name" : "key",
					"type" : "int",
					"field-id" : 138
					}, {
					"name" : "value",
					"type" : "long",
					"field-id" : 139
					} ]
				},
				"logicalType" : "map"
				} ],
				"doc" : "Map of column id to number of NaN values in the column",
				"default" : null,
				"field-id" : 137
			}, {
				"name" : "lower_bounds",
				"type" : [ "null", {
				"type" : "array",
				"items" : {
					"type" : "record",
					"name" : "k126_v127",
					"fields" : [ {
					"name" : "key",
					"type" : "int",
					"field-id" : 126
					}, {
					"name" : "value",
					"type" : "bytes",
					"field-id" : 127
					} ]
				},
				"logicalType" : "map"
				} ],
				"doc" : "Map of column id to lower bound",
				"default" : null,
				"field-id" : 125
			}, {
				"name" : "upper_bounds",
				"type" : [ "null", {
				"type" : "array",
				"items" : {
					"type" : "record",
					"name" : "k129_v130",
					"fields" : [ {
					"name" : "key",
					"type" : "int",
					"field-id" : 129
					}, {
					"name" : "value",
					"type" : "bytes",
					"field-id" : 130
					} ]
				},
				"logicalType" : "map"
				} ],
				"doc" : "Map of column id to upper bound",
				"default" : null,
				"field-id" : 128
			}, {
				"name" : "key_metadata",
				"type" : [ "null", "bytes" ],
				"doc" : "Encryption key metadata blob",
				"default" : null,
				"field-id" : 131
			}, {
				"name" : "split_offsets",
				"type" : [ "null", {
				"type" : "array",
				"items" : "long",
				"element-id" : 133
				} ],
				"doc" : "Splittable offsets",
				"default" : null,
				"field-id" : 132
			}, {
				"name" : "equality_ids",
				"type" : [ "null", {
				"type" : "array",
				"items" : "long",
				"element-id" : 136
				} ],
				"doc" : "Field ids used to determine row equality in equality delete files.",
				"default" : null,
				"field-id" : 135
			}, {
				"name" : "sort_order_id",
				"type" : [ "null", "int" ],
				"doc" : "ID representing sort order for this file",
				"default" : null,
				"field-id" : 140
			} ]
			},
			"field-id" : 2
		} ]
	}`
	MANIFEST_LIST_SCHEMA = `{
		"type" : "record",
		"name" : "manifest_file",
		"fields" : [ {
			"name" : "manifest_path",
			"type" : "string",
			"doc" : "Location URI with FS scheme",
			"field-id" : 500
		}, {
			"name" : "manifest_length",
			"type" : "long",
			"field-id" : 501
		}, {
			"name" : "partition_spec_id",
			"type" : "int",
			"field-id" : 502
		}, {
			"name" : "content",
			"type" : "int",
			"field-id" : 517
		}, {
			"name" : "sequence_number",
			"type" : "long",
			"field-id" : 515
		}, {
			"name" : "min_sequence_number",
			"type" : "long",
			"field-id" : 516
		}, {
			"name" : "added_snapshot_id",
			"type" : "long",
			"field-id" : 503
		}, {
			"name" : "added_files_count",
			"type" : "int",
			"field-id" : 504
		}, {
			"name" : "existing_files_count",
			"type" : "int",
			"field-id" : 505
		}, {
			"name" : "deleted_files_count",
			"type" : "int",
			"field-id" : 506
		}, {
			"name" : "added_rows_count",
			"type" : "long",
			"field-id" : 512
		}, {
			"name" : "existing_rows_count",
			"type" : "long",
			"field-id" : 513
		}, {
			"name" : "deleted_rows_count",
			"type" : "long",
			"field-id" : 514
		}, {
			"name" : "partitions",
			"type" : [ "null", {
			"type" : "array",
			"items" : {
				"type" : "record",
				"name" : "r508",
				"fields" : [ {
				"name" : "contains_null",
				"type" : "boolean",
				"field-id" : 509
				}, {
				"name" : "contains_nan",
				"type" : [ "null", "boolean" ],
				"default" : null,
				"field-id" : 518
				}, {
				"name" : "lower_bound",
				"type" : [ "null", "bytes" ],
				"default" : null,
				"field-id" : 510
				}, {
				"name" : "upper_bound",
				"type" : [ "null", "bytes" ],
				"default" : null,
				"field-id" : 511
				} ]
			},
			"element-id" : 508
			} ],
			"default" : null,
			"field-id" : 507
		}, {
			"name" : "key_metadata",
			"type" : [ "null", "bytes" ],
			"default" : null,
			"field-id" : 519
		} ]
	}`
)
