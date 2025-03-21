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

func TestWriteIncrementally(t *testing.T) {
	config := loadTestConfig()
	icebergWriter := NewIcebergWriter(config)
	duckdb := NewDuckdb(config)
	defer duckdb.Close()

	t.Run("Processes an incremental INSERT", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental INSERT -> INSERT", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental INSERT & UPDATE simultaneously", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental INSERT -> same-record UPDATE", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental INSERT -> another-record UPDATE", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes an incremental UPDATE", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental UPDATE -> INSERT", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental UPDATE -> same-record UPDATE", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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

	t.Run("Processes incremental UPDATE -> another-record UPDATE", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

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
				t.Fatalf("Expected value '%s' at (%d, %d), got '%s'", expectedValue, i, j, actualRecord[j])
			}
		}
	}
}
