package main

import (
	"context"
	"testing"
)

var TEST_ICEBERG_WRITER_SCHEMA_TABLE = IcebergSchemaTable{
	Schema: "iceberg_writer_test",
	Table:  "test_table",
}
var TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{ColumnName: "id", DataType: "integer", UdtName: "int4", IsNullable: "NO", NumericPrecision: "32", OrdinalPosition: "1", Namespace: "pg_catalog", PartOfPrimaryKey: true},
	{ColumnName: "name", DataType: "character varying", UdtName: "varchar", IsNullable: "YES", CharacterMaximumLength: "255", OrdinalPosition: "2", Namespace: "pg_catalog"},
}
var TEST_ICEBERG_WRITER_INITIAL_ROWS = [][]string{
	{"1", "John"},
	{"2", PG_NULL_STRING},
}
var TEST_XMIN0 = uint32(0)
var TEST_XMIN1 = uint32(1)

func TestWriteFull(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	t.Run("Processes a full sync with a single Parquet file", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
		icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))

		testManifestListFiles(t, icebergTableWriter.storage,
			ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
		)
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", PG_NULL_STRING},
		})
		testInternalTableMetadata(t, icebergTableWriter.storage,
			InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeFull, MaxXmin: &TEST_XMIN1},
		)
	}))

	t.Run("Processes a full sync with two Parquet files", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
		icebergTableWriter.maxParquetPayloadThreshold = 1

		icebergTableWriter.Write(createTestLoadRowsByOne(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))

		testManifestListFiles(t, icebergTableWriter.storage,
			ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
		)
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", PG_NULL_STRING},
		})
		testInternalTableMetadata(t, icebergTableWriter.storage,
			InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeFull, MaxXmin: &TEST_XMIN1},
		)
	}))

	t.Run("Continues and processes a full in-progress sync", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
		icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
		overrideTestInternalTableMetadata(t, icebergTableWriter.storage, InternalTableMetadata{
			LastSyncedAt:    111,
			LastRefreshMode: RefreshModeFullInProgress,
			MaxXmin:         &TEST_XMIN0,
		})
		icebergTableWriter.continuedRefresh = true

		icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, [][]string{
			{"3", "Jane"},
		}))

		testManifestListFiles(t, icebergTableWriter.storage,
			ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
		)
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", PG_NULL_STRING},
			{"3", "Jane"},
		})
		testInternalTableMetadata(t, icebergTableWriter.storage,
			InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeFull, MaxXmin: &TEST_XMIN1},
		)
	}))
}

func TestWriteIncremental(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	t.Run("Single incremental sync", func(t *testing.T) {
		t.Run("Processes an incremental INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental INSERT with two Parquet files", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true
			icebergTableWriter.maxParquetPayloadThreshold = 1

			icebergTableWriter.Write(createTestLoadRowsByOne(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
				{"4", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
				{"4", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", PG_NULL_STRING},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental UPDATE with two Parquet files", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
			}))
			icebergTableWriter.continuedRefresh = true
			icebergTableWriter.maxParquetPayloadThreshold = 1

			icebergTableWriter.Write(createTestLoadRowsByOne(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 3},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 3, AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental full UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental full UPDATE with three Parquet files", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
			}))
			icebergTableWriter.continuedRefresh = true
			icebergTableWriter.maxParquetPayloadThreshold = 1

			icebergTableWriter.Write(createTestLoadRowsByOne(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 3},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 3, AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 7, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Bob"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental INSERT & UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"3", "Jane"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes an incremental INSERT & full UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 3},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Continues and processes an incremental INSERT in-progress sync with the same rows (same xmins)", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			overrideTestInternalTableMetadata(t, icebergTableWriter.storage, InternalTableMetadata{
				LastSyncedAt:    111,
				LastRefreshMode: RefreshModeIncrementalInProgress,
				MaxXmin:         &TEST_XMIN0,
			})
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Continues and processes an incremental INSERT & UPDATE in-progress sync", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			overrideTestInternalTableMetadata(t, icebergTableWriter.storage, InternalTableMetadata{
				LastSyncedAt:    111,
				LastRefreshMode: RefreshModeIncrementalInProgress,
				MaxXmin:         &TEST_XMIN0,
			})
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))
	})

	t.Run("Two incremental syncs", func(t *testing.T) {
		t.Run("Processes incremental INSERT -> INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
				{"4", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes incremental INSERT -> same-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
			})
			testInternalTableMetadata(t, icebergTableWriter.storage,
				InternalTableMetadata{LastSyncedAt: 123, LastRefreshMode: RefreshModeIncremental, MaxXmin: &TEST_XMIN1},
			)
		}))

		t.Run("Processes incremental INSERT -> same-record UPDATE & INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
				{"4", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT & UPDATE -> same-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 4, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Jane"},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Alice"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial-record UPDATE & INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Alice"},
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Alice"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
				{"4", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial & inserted-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Alice"},
				{"2", PG_NULL_STRING},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> full UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 3},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Alice"},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT & UPDATE -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Bob"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
		}))

		t.Run("Processes incremental full UPDATE -> INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
		}))

		t.Run("Processes incremental full UPDATE -> UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 4, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Alice"},
				{"2", "Jane"},
			})
		}))

		t.Run("Processes incremental UPDATE -> full UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Jane"},
				{"2", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Jane"},
				{"2", "Alice"},
			})
		}))

		t.Run("Processes incremental UPDATE -> same-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Alice"},
			})
		}))

		t.Run("Processes incremental UPDATE -> same-record UPDATE & INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Alice"},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental UPDATE -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Alice"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)

			testRecords(t, duckdb, [][]string{
				{"1", "Alice"},
				{"2", "Jane"},
			})
		}))

		t.Run("Processes incremental UPDATE -> initial-record UPDATE & INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)

			testRecords(t, duckdb, [][]string{
				{"1", "Alice"},
				{"2", "Jane"},
				{"3", "Bob"},
			})
		}))
	})

	t.Run("Three incremental syncs", func(t *testing.T) {
		t.Run("Processes incremental INSERT -> INSERT -> last-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
				{"4", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> INSERT -> previous-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Bob"},
				{"4", "Alice"},
			})
		}))

		t.Run("Processes incremental INSERT -> INSERT -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Bob"},
				{"3", "Jane"},
				{"4", "Alice"},
			})
		}))

		t.Run("Processes incremental INSERT -> same-record UPDATE -> INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Alice"},
				{"4", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Alice"},
				{"3", "Jane"},
				{"4", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> same-record UPDATE -> same-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> same-record UPDATE -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Bob"},
				{"3", "Alice"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Bob"},
				{"2", "Alice"},
				{"3", "Jane"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> inserted-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Alice"},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> updated-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Bob"},
				{"3", "Jane"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT -> INSERT", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Jane"},
				{"3", "Alice"},
				{"4", "Bob"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT -> inserted-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Jane"},
				{"3", "Bob"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT -> updated-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Bob"},
				{"3", "Alice"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT -> initial-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "Bob"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "Bob"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT -> full UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"1", "John Doe"},
				{"2", "Bob"},
				{"3", "Alice Smith"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 7, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 8, Operation: "append", AddedDataFiles: 1, AddedRecords: 3},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Bob"},
				{"3", "Alice Smith"},
			})
		}))

		t.Run("Processes incremental UPDATE -> INSERT -> updated and inserted-record UPDATE", withTestIcebergTableWriter(config, func(t *testing.T, icebergTableWriter *IcebergWriterTable) {
			icebergTableWriter.Write(createTestLoadRows(RefreshModeFull, TEST_ICEBERG_WRITER_INITIAL_ROWS))
			icebergTableWriter.continuedRefresh = true

			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Jane"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"3", "Alice"},
			}))
			icebergTableWriter.Write(createTestLoadRows(RefreshModeIncremental, [][]string{
				{"2", "Bob"},
				{"3", "Alice Smith"},
			}))

			testManifestListFiles(t, icebergTableWriter.storage,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 4, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 5, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 6, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 1},
				ManifestListFile{SequenceNumber: 7, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", "Bob"},
				{"3", "Alice Smith"},
			})
		}))
	})
}

// ---------------------------------------------------------------------------------------------------------------------

func withTestIcebergTableWriter(config *Config, testFunc func(t *testing.T, icebergTableWriter *IcebergWriterTable)) func(*testing.T) {
	return func(t *testing.T) {
		icebergTableWriter := NewIcebergWriterTable(config, TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 777, MAX_PARQUET_PAYLOAD_THRESHOLD, false)

		testFunc(t, icebergTableWriter)

		icebergTableWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
	}
}

func createTestLoadRows(refreshMode RefreshMode, testRows [][]string) func() ([][]string, InternalTableMetadata) {
	internalTableMetadata := InternalTableMetadata{
		LastSyncedAt:    123,
		LastRefreshMode: refreshMode,
		MaxXmin:         &TEST_XMIN1,
	}

	loadedAllRows := false
	return func() ([][]string, InternalTableMetadata) {
		if loadedAllRows {
			return [][]string{}, internalTableMetadata
		}
		loadedAllRows = true
		return testRows, internalTableMetadata
	}
}

func createTestLoadRowsByOne(refreshMode RefreshMode, testRows [][]string) func() ([][]string, InternalTableMetadata) {
	var inProgressRefreshMode RefreshMode
	if refreshMode == RefreshModeFull {
		inProgressRefreshMode = RefreshModeFullInProgress
	} else {
		inProgressRefreshMode = RefreshModeIncrementalInProgress
	}

	internalTableMetadata := InternalTableMetadata{
		LastSyncedAt:    123,
		LastRefreshMode: inProgressRefreshMode,
		MaxXmin:         &TEST_XMIN1,
	}

	loadedRowIndex := -1
	return func() ([][]string, InternalTableMetadata) {
		if loadedRowIndex == len(testRows)-1 {
			return [][]string{}, internalTableMetadata
		}

		row := testRows[loadedRowIndex+1]

		loadedRowIndex++
		if loadedRowIndex == len(testRows)-1 {
			internalTableMetadata.LastRefreshMode = refreshMode
		}

		return [][]string{row}, internalTableMetadata
	}
}

func overrideTestInternalTableMetadata(t *testing.T, storage StorageInterface, internalTableMetadata InternalTableMetadata) {
	metadataDirPath := storage.CreateMetadataDir(TEST_ICEBERG_WRITER_SCHEMA_TABLE)
	err := storage.WriteInternalTableMetadata(metadataDirPath, internalTableMetadata)
	if err != nil {
		t.Fatalf("Failed to write internal table metadata: %v", err)
	}
}

func testManifestListFiles(t *testing.T, storage StorageInterface, expectedManifestListFiles ...ManifestListFile) {
	metadataDirPath := storage.CreateMetadataDir(TEST_ICEBERG_WRITER_SCHEMA_TABLE)
	manifestListFiles, err := storage.ExistingManifestListFiles(metadataDirPath)
	if err != nil {
		t.Fatalf("Error reading manifest list files: %v", err)
	}

	if len(manifestListFiles) != len(expectedManifestListFiles) {
		t.Fatalf("Expected %d manifest list files, got %d", len(expectedManifestListFiles), len(manifestListFiles))
	}

	for i, expectedManifestListFile := range expectedManifestListFiles {
		actualManifestListFile := manifestListFiles[i]
		if actualManifestListFile.SequenceNumber != expectedManifestListFile.SequenceNumber {
			t.Fatalf("Expected sequence number %d, got %d", expectedManifestListFile.SequenceNumber, actualManifestListFile.SequenceNumber)
		}
		if actualManifestListFile.Operation != expectedManifestListFile.Operation {
			t.Fatalf("Expected operation '%s', got '%s' (sequence number %d)", expectedManifestListFile.Operation, actualManifestListFile.Operation, actualManifestListFile.SequenceNumber)
		}
		if actualManifestListFile.AddedDataFiles != expectedManifestListFile.AddedDataFiles {
			t.Fatalf("Expected %d added data files, got %d (sequence number %d)", expectedManifestListFile.AddedDataFiles, actualManifestListFile.AddedDataFiles, actualManifestListFile.SequenceNumber)
		}
		if actualManifestListFile.AddedRecords != expectedManifestListFile.AddedRecords {
			t.Fatalf("Expected %d added records, got %d (sequence number %d)", expectedManifestListFile.AddedRecords, actualManifestListFile.AddedRecords, actualManifestListFile.SequenceNumber)
		}
		if actualManifestListFile.DeletedDataFiles != expectedManifestListFile.DeletedDataFiles {
			t.Fatalf("Expected %d deleted data files, got %d (sequence number %d)", expectedManifestListFile.DeletedDataFiles, actualManifestListFile.DeletedDataFiles, actualManifestListFile.SequenceNumber)
		}
		if actualManifestListFile.DeletedRecords != expectedManifestListFile.DeletedRecords {
			t.Fatalf("Expected %d deleted records, got %d (sequence number %d)", expectedManifestListFile.DeletedRecords, actualManifestListFile.DeletedRecords, actualManifestListFile.SequenceNumber)
		}
	}
}

func testRecords(t *testing.T, duckdb *Duckdb, expectedRecords [][]string) {
	icebergReader := NewIcebergReader(duckdb.config)
	metadataFilePath := icebergReader.MetadataFilePath(TEST_ICEBERG_WRITER_SCHEMA_TABLE)

	rows, err := duckdb.QueryContext(context.Background(), "SELECT id::text, COALESCE(name, '"+PG_NULL_STRING+"') FROM iceberg_scan('"+metadataFilePath+"', skip_schema_inference = true) ORDER BY id")
	if err != nil {
		t.Fatalf("Error querying DuckDB: %v", err)
	}
	defer rows.Close()

	actualRecords := [][]string{}
	for rows.Next() {
		row := make([]string, len(expectedRecords[0]))
		err := rows.Scan(&row[0], &row[1])
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		actualRecords = append(actualRecords, row)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Expected %d records, got %d", len(expectedRecords), len(actualRecords))
	}

	for i, expectedRecord := range expectedRecords {
		actualRecord := actualRecords[i]
		for j, expectedValue := range expectedRecord {
			if actualRecord[j] != expectedValue {
				t.Fatalf("Expected value '%s' at %d, got '%s'", expectedValue, i, actualRecord[j])
			}
		}
	}
}

func testInternalTableMetadata(t *testing.T, storage StorageInterface, expectedInternalTableMetadata InternalTableMetadata) {
	actualInternalTableMetadata, err := storage.InternalTableMetadata(PgSchemaTable{
		Schema: TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema,
		Table:  TEST_ICEBERG_WRITER_SCHEMA_TABLE.Table,
	})
	if err != nil {
		t.Fatalf("Error reading internal table metadata: %v", err)
	}

	if actualInternalTableMetadata.LastSyncedAt != expectedInternalTableMetadata.LastSyncedAt {
		t.Fatalf("Expected LastSyncedAt %d, got %d", expectedInternalTableMetadata.LastSyncedAt, actualInternalTableMetadata.LastSyncedAt)
	}
	if actualInternalTableMetadata.LastRefreshMode != expectedInternalTableMetadata.LastRefreshMode {
		t.Fatalf("Expected LastRefreshMode %s, got %s", expectedInternalTableMetadata.LastRefreshMode, actualInternalTableMetadata.LastRefreshMode)
	}
	if *actualInternalTableMetadata.MaxXmin != *expectedInternalTableMetadata.MaxXmin {
		t.Fatalf("Expected MaxXmin %v, got %v", expectedInternalTableMetadata.MaxXmin, actualInternalTableMetadata.MaxXmin)
	}
}
