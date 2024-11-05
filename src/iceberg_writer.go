package main

type IcebergWriter struct {
	config  *Config
	storage Storage
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

func (icebergWriter *IcebergWriter) Write(schemaTable SchemaTable, pgSchemaColumns []PgSchemaColumn, loadRows func() [][]string) {
	err := icebergWriter.storage.DeleteSchemaTable(schemaTable)
	PanicIfError(err)

	dataDirPath := icebergWriter.storage.CreateDataDir(schemaTable)

	parquetFile, err := icebergWriter.storage.CreateParquet(dataDirPath, pgSchemaColumns, loadRows)
	PanicIfError(err)

	metadataDirPath := icebergWriter.storage.CreateMetadataDir(schemaTable)

	manifestFile, err := icebergWriter.storage.CreateManifest(metadataDirPath, parquetFile)
	PanicIfError(err)

	manifestListFile, err := icebergWriter.storage.CreateManifestList(metadataDirPath, parquetFile, manifestFile)
	PanicIfError(err)

	metadataFile, err := icebergWriter.storage.CreateMetadata(metadataDirPath, pgSchemaColumns, parquetFile, manifestFile, manifestListFile)
	PanicIfError(err)

	err = icebergWriter.storage.CreateVersionHint(metadataDirPath, metadataFile)
	PanicIfError(err)
}

func (icebergWriter *IcebergWriter) DeleteSchemaTable(schemaTable SchemaTable) {
	err := icebergWriter.storage.DeleteSchemaTable(schemaTable)
	PanicIfError(err)
}
