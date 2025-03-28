package main

import (
	"slices"
)

type IcebergWriter struct {
	config  *Config
	storage StorageInterface
}

func NewIcebergWriter(config *Config) *IcebergWriter {
	storage := NewStorage(config)
	return &IcebergWriter{config: config, storage: storage}
}

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

func (icebergWriter *IcebergWriter) Write(schemaTable IcebergSchemaTable, pgSchemaColumns []PgSchemaColumn, maxParquetPayloadThreshold int, loadRows func() [][]string) {
	metadataDirPath, parquetFilesSortedDesc, manifestListItemsSortedDesc := icebergWriter.createParquetAndManifestFileChunks(
		schemaTable,
		pgSchemaColumns,
		maxParquetPayloadThreshold,
		loadRows,
		true, // To create an empty table
	)

	firstParquetFile := parquetFilesSortedDesc[len(parquetFilesSortedDesc)-1]
	manifestListFile, err := icebergWriter.storage.CreateManifestList(metadataDirPath, firstParquetFile.Uuid, manifestListItemsSortedDesc)
	PanicIfError(icebergWriter.config, err)

	_, err = icebergWriter.storage.CreateMetadata(metadataDirPath, pgSchemaColumns, []ManifestListFile{manifestListFile})
	PanicIfError(icebergWriter.config, err)
}

func (icebergWriter *IcebergWriter) WriteIncrementally(schemaTable IcebergSchemaTable, pgSchemaColumns []PgSchemaColumn, dynamicRowCountPerBatch int, maxParquetPayloadThreshold int, loadRows func() [][]string) {
	dataDirPath := icebergWriter.storage.CreateDataDir(schemaTable)

	// Build new parquet file
	metadataDirPath, newParquetFilesSortedDesc, newManifestListItemsSortedDesc := icebergWriter.createParquetAndManifestFileChunks(
		schemaTable,
		pgSchemaColumns,
		maxParquetPayloadThreshold,
		loadRows,
		false, // To avoid creating extra files when there are no records to sync incrementally
	)
	if len(newParquetFilesSortedDesc) == 0 {
		LogDebug(icebergWriter.config, "No records to write incrementally")
		return
	}

	newParquetFilePaths := []string{}
	for _, parquetFile := range newParquetFilesSortedDesc {
		newParquetFilePaths = append(newParquetFilePaths, parquetFile.Path)
	}
	firstNewParquetFile := newParquetFilesSortedDesc[len(newParquetFilesSortedDesc)-1]

	// Read existing metadata
	existingManifestListFilesSortedAsc, err := icebergWriter.storage.ExistingManifestListFiles(metadataDirPath)
	PanicIfError(icebergWriter.config, err)
	existingManifestListItemsSortedAsc := icebergWriter.readExistingManifestListItemsSortedAsc(existingManifestListFilesSortedAsc[len(existingManifestListFilesSortedAsc)-1])

	// Overwrite UPDATEd records by creating new parquet files
	lastSequenceNumber := existingManifestListItemsSortedAsc[len(existingManifestListItemsSortedAsc)-1].SequenceNumber
	finalManifestListItemsSortedAsc := []ManifestListItem{}
	finalManifestListFilesSortedAsc := existingManifestListFilesSortedAsc
	for i, existingManifestListItem := range existingManifestListItemsSortedAsc {
		existingManifestFile := existingManifestListItem.ManifestFile
		existingParquetFilePath, err := icebergWriter.storage.ExistingParquetFilePath(existingManifestFile)
		PanicIfError(icebergWriter.config, err)

		overwrittenParquetFile, err := icebergWriter.storage.CreateOverwrittenParquet(dataDirPath, existingParquetFilePath, newParquetFilePaths, pgSchemaColumns, dynamicRowCountPerBatch)
		PanicIfError(icebergWriter.config, err)

		// Keeping the manifest list item as is if no overlapping records found
		if overwrittenParquetFile.Path == "" {
			LogDebug(icebergWriter.config, "No overlapping records found")
			finalManifestListItemsSortedAsc = append(finalManifestListItemsSortedAsc, existingManifestListItem)
			continue
		}

		// Deleting existing manifest list file if all records are overwritten
		if overwrittenParquetFile.RecordCount == 0 {
			LogDebug(icebergWriter.config, "Deleting", existingManifestFile.RecordCount, "record(s)...")

			deletedRecsManifestFile, err := icebergWriter.storage.CreateDeletedRecordsManifest(metadataDirPath, overwrittenParquetFile.Uuid, existingManifestFile)
			PanicIfError(icebergWriter.config, err)

			overwrittenManifestListItemsSortedAsc := []ManifestListItem{}
			for j, existingItem := range existingManifestListItemsSortedAsc {
				if i != j {
					overwrittenManifestListItemsSortedAsc = append(overwrittenManifestListItemsSortedAsc, existingItem)
				}
			}
			lastSequenceNumber++
			overwrittenManifestListItemsSortedAsc = append(
				overwrittenManifestListItemsSortedAsc,
				ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: deletedRecsManifestFile},
			)
			slices.Reverse(overwrittenManifestListItemsSortedAsc)
			overwrittenManifestListItemsSortedDesc := overwrittenManifestListItemsSortedAsc

			overwrittenManifestList, err := icebergWriter.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, overwrittenManifestListItemsSortedDesc)
			PanicIfError(icebergWriter.config, err)

			finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, overwrittenManifestList)
			continue
		}

		// Overwriting existing manifest list file with a new one
		LogDebug(icebergWriter.config, "Overwritting", existingManifestFile.RecordCount, "record(s) with", overwrittenParquetFile.RecordCount, "record(s)...")

		deletedRecsManifestFile, err := icebergWriter.storage.CreateDeletedRecordsManifest(metadataDirPath, overwrittenParquetFile.Uuid, existingManifestFile)
		PanicIfError(icebergWriter.config, err)

		overwrittenManifestFile, err := icebergWriter.storage.CreateManifest(metadataDirPath, overwrittenParquetFile)
		PanicIfError(icebergWriter.config, err)

		lastSequenceNumber++
		overwrittenManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: overwrittenManifestFile}
		deletedRecsManifestListItem := ManifestListItem{SequenceNumber: lastSequenceNumber, ManifestFile: deletedRecsManifestFile}
		overwrittenManifestList, err := icebergWriter.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, []ManifestListItem{overwrittenManifestListItem, deletedRecsManifestListItem})
		PanicIfError(icebergWriter.config, err)

		finalManifestListItemsSortedAsc = append(finalManifestListItemsSortedAsc, overwrittenManifestListItem)
		finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, overwrittenManifestList)
	}

	// Stitch new parquet file with overwritten parquet files
	lastSequenceNumber++
	for i := range newManifestListItemsSortedDesc {
		newManifestListItemsSortedDesc[i].SequenceNumber = lastSequenceNumber
	}
	slices.Reverse(finalManifestListItemsSortedAsc)
	finalManifestListItemsSortedDesc := append(newManifestListItemsSortedDesc, finalManifestListItemsSortedAsc...)

	newManifestListFile, err := icebergWriter.storage.CreateManifestList(metadataDirPath, firstNewParquetFile.Uuid, finalManifestListItemsSortedDesc)
	PanicIfError(icebergWriter.config, err)

	finalManifestListFilesSortedAsc = append(finalManifestListFilesSortedAsc, newManifestListFile)
	_, err = icebergWriter.storage.CreateMetadata(metadataDirPath, pgSchemaColumns, finalManifestListFilesSortedAsc)
	PanicIfError(icebergWriter.config, err)
}

func (icebergWriter *IcebergWriter) DeleteSchemaTable(schemaTable IcebergSchemaTable) {
	err := icebergWriter.storage.DeleteSchemaTable(schemaTable)
	PanicIfError(icebergWriter.config, err)
}

func (icebergWriter *IcebergWriter) DeleteSchema(schema string) {
	err := icebergWriter.storage.DeleteSchema(schema)
	PanicIfError(icebergWriter.config, err)
}

func (icebergWriter *IcebergWriter) readExistingManifestListItemsSortedAsc(lastExistingManifestListFile ManifestListFile) []ManifestListItem {
	existingManifestListItems, err := icebergWriter.storage.ExistingManifestListItems(lastExistingManifestListFile)
	PanicIfError(icebergWriter.config, err)
	slices.Reverse(existingManifestListItems)
	return existingManifestListItems
}

// Write multiple parquet files up to the maxParquetPayloadThreshold to avoid OOM and create a manifest file for each parquet file
func (icebergWriter *IcebergWriter) createParquetAndManifestFileChunks(
	schemaTable IcebergSchemaTable,
	pgSchemaColumns []PgSchemaColumn,
	maxParquetPayloadThreshold int,
	loadRows func() [][]string,
	allowEmptyParquetFiles bool, // Allowed with full-refresh, not allowed with incremental-refresh
) (string, []ParquetFile, []ManifestListItem) {
	parquetFilesSortedDesc := []ParquetFile{}
	manifestListItemsSortedDesc := []ManifestListItem{}

	dataDirPath := icebergWriter.storage.CreateDataDir(schemaTable)

	loadMoreRows := true
	metadataDirPath := icebergWriter.storage.CreateMetadataDir(schemaTable)

	for loadMoreRows {
		parquetFile, loadedAllRows, err := icebergWriter.storage.CreateParquet(dataDirPath, pgSchemaColumns, loadRows, maxParquetPayloadThreshold)
		PanicIfError(icebergWriter.config, err)

		// Delete Parquet if it is empty && (already written at least one Parquet previously || empty Parquet files are not allowed)
		if parquetFile.RecordCount == 0 && (len(manifestListItemsSortedDesc) > 0 || !allowEmptyParquetFiles) {
			err = icebergWriter.storage.DeleteParquet(parquetFile)
			PanicIfError(icebergWriter.config, err)
			break
		}
		parquetFilesSortedDesc = append([]ParquetFile{parquetFile}, parquetFilesSortedDesc...)

		manifestFile, err := icebergWriter.storage.CreateManifest(metadataDirPath, parquetFile)
		PanicIfError(icebergWriter.config, err)

		manifestListItem := ManifestListItem{SequenceNumber: 1, ManifestFile: manifestFile}
		manifestListItemsSortedDesc = append([]ManifestListItem{manifestListItem}, manifestListItemsSortedDesc...)

		loadMoreRows = !loadedAllRows

		if loadMoreRows {
			LogDebug(icebergWriter.config, "Written", len(manifestListItemsSortedDesc), "Parquet files, continuing...")
		}
	}

	return metadataDirPath, parquetFilesSortedDesc, manifestListItemsSortedDesc
}
