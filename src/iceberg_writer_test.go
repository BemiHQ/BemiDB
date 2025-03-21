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

		manifestListFiles := readManifestListFiles(t, icebergWriter)
		if len(manifestListFiles) != 2 {
			t.Fatalf("Expected 2 manifest list files, got %d", len(manifestListFiles))
		}
		testManifestListFile(t, manifestListFiles[0], ManifestListFile{
			SequenceNumber: 1,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[1], ManifestListFile{
			SequenceNumber: 2,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
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

		manifestListFiles := readManifestListFiles(t, icebergWriter)
		if len(manifestListFiles) != 3 {
			t.Fatalf("Expected 3 manifest list files, got %d", len(manifestListFiles))
		}
		testManifestListFile(t, manifestListFiles[0], ManifestListFile{
			SequenceNumber: 1,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[1], ManifestListFile{
			SequenceNumber:   2,
			Operation:        "overwrite",
			DeletedDataFiles: 1,
			DeletedRecords:   2,
			AddedDataFiles:   1,
			AddedRecords:     1,
		})
		testManifestListFile(t, manifestListFiles[2], ManifestListFile{
			SequenceNumber: 3,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
		testRecords(t, duckdb, [][]string{
			{"1", "John Doe"},
			{"2", PG_NULL_STRING},
		})
	})

	t.Run("Processes an incremental INSERT and UPDATE simultaneously", func(t *testing.T) {
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_INITIAL_ROWS))

		icebergWriter.WriteIncrementally(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, 10, createTestLoadRows([][]string{
			{"1", "John Doe"},
			{"3", "Jane"},
		}))

		manifestListFiles := readManifestListFiles(t, icebergWriter)
		if len(manifestListFiles) != 3 {
			t.Fatalf("Expected 3 manifest list files, got %d", len(manifestListFiles))
		}
		testManifestListFile(t, manifestListFiles[0], ManifestListFile{
			SequenceNumber: 1,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[1], ManifestListFile{
			SequenceNumber:   2,
			Operation:        "overwrite",
			DeletedDataFiles: 1,
			DeletedRecords:   2,
			AddedDataFiles:   1,
			AddedRecords:     1,
		})
		testManifestListFile(t, manifestListFiles[2], ManifestListFile{
			SequenceNumber: 3,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
		testRecords(t, duckdb, [][]string{
			{"1", "John Doe"},
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

		manifestListFiles := readManifestListFiles(t, icebergWriter)
		if len(manifestListFiles) != 3 {
			t.Fatalf("Expected 3 manifest list files, got %d", len(manifestListFiles))
		}
		testManifestListFile(t, manifestListFiles[0], ManifestListFile{
			SequenceNumber: 1,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[1], ManifestListFile{
			SequenceNumber: 2,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
		testManifestListFile(t, manifestListFiles[2], ManifestListFile{
			SequenceNumber: 3,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", PG_NULL_STRING},
			{"3", "Jane"},
			{"4", "Alice"},
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

		manifestListFiles := readManifestListFiles(t, icebergWriter)
		if len(manifestListFiles) != 5 {
			t.Fatalf("Expected 5 manifest list files, got %d", len(manifestListFiles))
		}
		testManifestListFile(t, manifestListFiles[0], ManifestListFile{
			SequenceNumber: 1,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[1], ManifestListFile{
			SequenceNumber:   2,
			Operation:        "overwrite",
			DeletedDataFiles: 1,
			DeletedRecords:   2,
			AddedDataFiles:   1,
			AddedRecords:     1,
		})
		testManifestListFile(t, manifestListFiles[2], ManifestListFile{
			SequenceNumber: 3,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
		testManifestListFile(t, manifestListFiles[3], ManifestListFile{
			SequenceNumber:   4,
			Operation:        "delete",
			DeletedDataFiles: 1,
			DeletedRecords:   1,
		})
		testManifestListFile(t, manifestListFiles[4], ManifestListFile{
			SequenceNumber: 5,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
		testRecords(t, duckdb, [][]string{
			{"1", "John"},
			{"2", "Alice"},
		})
	})
}

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

func readManifestListFiles(t *testing.T, icebergWriter *IcebergWriter) []ManifestListFile {
	metadataDirPath := icebergWriter.storage.CreateMetadataDir(TEST_ICEBERG_WRITER_SCHEMA_TABLE)
	manifestListFiles, err := icebergWriter.storage.ExistingManifestListFiles(metadataDirPath)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	return manifestListFiles
}

func testManifestListFile(t *testing.T, actualManifestListFile ManifestListFile, expectedManifestListFile ManifestListFile) {
	if actualManifestListFile.SequenceNumber != expectedManifestListFile.SequenceNumber {
		t.Fatalf("Expected sequence number %d, got %d", expectedManifestListFile.SequenceNumber, actualManifestListFile.SequenceNumber)
	}
	if actualManifestListFile.Operation != expectedManifestListFile.Operation {
		t.Fatalf("Expected operation '%s', got '%s'", expectedManifestListFile.Operation, actualManifestListFile.Operation)
	}
	if actualManifestListFile.AddedDataFiles != expectedManifestListFile.AddedDataFiles {
		t.Fatalf("Expected %d added data files, got %d", expectedManifestListFile.AddedDataFiles, actualManifestListFile.AddedDataFiles)
	}
	if actualManifestListFile.AddedRecords != expectedManifestListFile.AddedRecords {
		t.Fatalf("Expected %d added records, got %d", expectedManifestListFile.AddedRecords, actualManifestListFile.AddedRecords)
	}
	if actualManifestListFile.DeletedDataFiles != expectedManifestListFile.DeletedDataFiles {
		t.Fatalf("Expected %d deleted data files, got %d", expectedManifestListFile.DeletedDataFiles, actualManifestListFile.DeletedDataFiles)
	}
	if actualManifestListFile.DeletedRecords != expectedManifestListFile.DeletedRecords {
		t.Fatalf("Expected %d deleted records, got %d", expectedManifestListFile.DeletedRecords, actualManifestListFile.DeletedRecords)
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
