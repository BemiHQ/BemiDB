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

func TestWrite(t *testing.T) {
	config := loadTestConfig()
	icebergWriter := NewIcebergWriter(config)
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	t.Cleanup(func() {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
	})

	t.Run("Processes a full sync with a single Parquet file", func(t *testing.T) {
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

		testManifestListFiles(t, icebergWriter,
			ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
		)
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", PG_NULL_STRING},
		})
	})

	t.Run("Processes a full sync with two Parquet files", func(t *testing.T) {
		loadedRowIndex := -1
		icebergWriter.Write(
			TEST_ICEBERG_WRITER_SCHEMA_TABLE,
			TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS,
			1,
			func() [][]string {
				if loadedRowIndex == len(TEST_ICEBERG_WRITER_INITIAL_ROWS)-1 {
					return [][]string{}
				}

				row := TEST_ICEBERG_WRITER_INITIAL_ROWS[loadedRowIndex+1]
				loadedRowIndex++
				return [][]string{row}
			},
		)

		testManifestListFiles(t, icebergWriter,
			ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 2, AddedRecords: 2},
		)
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", PG_NULL_STRING},
		})
	})
}

func TestWriteIncrementally(t *testing.T) {
	config := loadTestConfig()
	icebergWriter := NewIcebergWriter(config)
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	t.Cleanup(func() {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
	})

	t.Run("Single incremental sync", func(t *testing.T) {
		t.Run("Processes an incremental INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))

			testManifestListFiles(t, icebergWriter,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
			})
		})

		t.Run("Processes an incremental UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
			}))

			testManifestListFiles(t, icebergWriter,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 1},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", PG_NULL_STRING},
			})
		})

		t.Run("Processes an incremental full UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))

			testManifestListFiles(t, icebergWriter,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			})
		})

		t.Run("Processes an incremental INSERT & UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"3", "Jane"},
			}))

			testManifestListFiles(t, icebergWriter,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "overwrite", DeletedDataFiles: 1, DeletedRecords: 2, AddedDataFiles: 1, AddedRecords: 1},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", PG_NULL_STRING},
				{"3", "Jane"},
			})
		})

		t.Run("Processes an incremental INSERT & full UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
				ManifestListFile{SequenceNumber: 1, Operation: "append", AddedDataFiles: 1, AddedRecords: 2},
				ManifestListFile{SequenceNumber: 2, Operation: "delete", DeletedDataFiles: 1, DeletedRecords: 2},
				ManifestListFile{SequenceNumber: 3, Operation: "append", AddedDataFiles: 1, AddedRecords: 3},
			)
			testRecords(t, duckdb, [][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
				{"3", "Alice"},
			})
		})
	})

	t.Run("Two incremental syncs", func(t *testing.T) {
		t.Run("Processes incremental INSERT -> INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> same-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> same-record UPDATE & INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT & UPDATE -> same-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial-record UPDATE & INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Alice"},
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial & inserted-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> full UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"2", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT & UPDATE -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental full UPDATE -> INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental full UPDATE -> UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> full UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Jane"},
				{"2", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> same-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> same-record UPDATE & INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Alice"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> initial-record UPDATE & INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Alice"},
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})
	})

	t.Run("Three incremental syncs", func(t *testing.T) {
		t.Run("Processes incremental INSERT -> INSERT -> last-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> INSERT -> previous-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> INSERT -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> same-record UPDATE -> INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> same-record UPDATE -> same-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> same-record UPDATE -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> inserted-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental INSERT -> initial-record UPDATE -> updated-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT -> INSERT", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"4", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT -> inserted-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT -> updated-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT -> initial-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "Bob"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT -> full UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"1", "John Doe"},
				{"2", "Bob"},
				{"3", "Alice Smith"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})

		t.Run("Processes incremental UPDATE -> INSERT -> updated and inserted-record UPDATE", func(t *testing.T) {
			icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, MAX_WRITE_PARQUET_PAYLOAD_SIZE, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Jane"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"3", "Alice"},
			}))
			icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
				{"2", "Bob"},
				{"3", "Alice Smith"},
			}))

			testManifestListFiles(t, icebergWriter,
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
		})
	})
}

// ---------------------------------------------------------------------------------------------------------------------

func createTestLoadRows(testRows [][]string) func() [][]string {
	loadedRows := false
	return func() [][]string {
		if loadedRows {
			return [][]string{}
		}
		loadedRows = true
		return testRows
	}
}

func testManifestListFiles(t *testing.T, icebergWriter *IcebergWriter, expectedManifestListFiles ...ManifestListFile) {
	metadataDirPath := icebergWriter.storage.CreateMetadataDir(TEST_ICEBERG_WRITER_SCHEMA_TABLE)
	manifestListFiles, err := icebergWriter.storage.ExistingManifestListFiles(metadataDirPath)
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
