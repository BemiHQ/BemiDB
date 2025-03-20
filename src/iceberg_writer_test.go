package main

import (
	"testing"
)

var TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{ColumnName: "id", DataType: "integer", UdtName: "int4", IsNullable: "NO", NumericPrecision: "32", OrdinalPosition: "1", Namespace: "pg_catalog", PartOfPrimaryKey: true},
	{ColumnName: "name", DataType: "character varying", UdtName: "varchar", IsNullable: "YES", CharacterMaximumLength: "255", OrdinalPosition: "2", Namespace: "pg_catalog"},
}
var TEST_ICEBERG_WRITER_SCHEMA_TABLE = IcebergSchemaTable{
	Schema: "iceberg_writer_test",
	Table:  "test_table",
}
var TEST_ICEBERG_WRITER_ROWS = [][]string{
	{"1", "John"},
	{"2", PG_NULL_STRING},
}

func TestWriteIncrementally(t *testing.T) {
	t.Run("Processes an incremental INSERT", func(t *testing.T) {
		config := loadTestConfig()
		icebergWriter := NewIcebergWriter(config)
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_ROWS))

		icebergWriter.WriteIncrementally(
			TEST_ICEBERG_WRITER_SCHEMA_TABLE,
			TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS,
			10,
			createTestLoadRows([][]string{{"3", "Jane"}}),
		)

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
	})

	t.Run("Processes an incremental UPDATE", func(t *testing.T) {
		config := loadTestConfig()
		icebergWriter := NewIcebergWriter(config)
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_ROWS))

		icebergWriter.WriteIncrementally(
			TEST_ICEBERG_WRITER_SCHEMA_TABLE,
			TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS,
			10,
			createTestLoadRows([][]string{{"1", "John Doe"}}),
		)

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
			AddedDataFiles:   1,
			AddedRecords:     1,
			DeletedDataFiles: 1,
			DeletedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[2], ManifestListFile{
			SequenceNumber: 3,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   1,
		})
	})

	t.Run("Processes an incremental INSERT and UPDATE simultaneously", func(t *testing.T) {
		config := loadTestConfig()
		icebergWriter := NewIcebergWriter(config)
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_ROWS))

		icebergWriter.WriteIncrementally(
			TEST_ICEBERG_WRITER_SCHEMA_TABLE,
			TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS,
			10,
			createTestLoadRows([][]string{{"1", "John Doe"}, {"3", "Jane"}}),
		)

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
			AddedDataFiles:   1,
			AddedRecords:     1,
			DeletedDataFiles: 1,
			DeletedRecords:   2,
		})
		testManifestListFile(t, manifestListFiles[2], ManifestListFile{
			SequenceNumber: 3,
			Operation:      "append",
			AddedDataFiles: 1,
			AddedRecords:   2,
		})
	})

	t.Run("Processes incremental INSERT->INSERT", func(t *testing.T) {
		config := loadTestConfig()
		icebergWriter := NewIcebergWriter(config)
		icebergWriter.storage.DeleteSchema(TEST_ICEBERG_WRITER_SCHEMA_TABLE.Schema)
		icebergWriter.Write(TEST_ICEBERG_WRITER_SCHEMA_TABLE, TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS, createTestLoadRows(TEST_ICEBERG_WRITER_ROWS))

		icebergWriter.WriteIncrementally(
			TEST_ICEBERG_WRITER_SCHEMA_TABLE,
			TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS,
			10,
			createTestLoadRows([][]string{{"3", "Jane"}}),
		)
		icebergWriter.WriteIncrementally(
			TEST_ICEBERG_WRITER_SCHEMA_TABLE,
			TEST_ICEBERG_WRITER_PG_SCHEMA_COLUMNS,
			10,
			createTestLoadRows([][]string{{"4", "Alice"}}),
		)

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
