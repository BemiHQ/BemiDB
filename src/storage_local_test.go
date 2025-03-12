package main

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestCreateParquet(t *testing.T) {
	t.Run("Creates a parquet file", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		pgSchemaColumns := []PgSchemaColumn{
			{ColumnName: "id", DataType: "integer", UdtName: "int4", IsNullable: "NO", NumericPrecision: "32", OrdinalPosition: "1", Namespace: "pg_catalog"},
			{ColumnName: "name", DataType: "character varying", UdtName: "varchar", IsNullable: "YES", CharacterMaximumLength: "255", OrdinalPosition: "2", Namespace: "pg_catalog"},
		}
		rows := [][]string{{"1", "John"}, {"2", PG_NULL_STRING}}
		loadedRows := false
		loadRows := func() [][]string {
			if loadedRows {
				return [][]string{}
			}
			loadedRows = true
			return rows
		}

		parquetFile, err := storage.CreateParquet(tempDir, pgSchemaColumns, loadRows)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if parquetFile.Uuid == "" {
			t.Errorf("Expected a non-empty UUID, got %v", parquetFile.Uuid)
		}
		if parquetFile.Path == "" {
			t.Errorf("Expected a non-empty path, got %v", parquetFile.Path)
		}
		if parquetFile.Size == 0 {
			t.Errorf("Expected a non-zero size, got %v", parquetFile.Size)
		}
		if parquetFile.RecordCount != 2 {
			t.Errorf("Expected a non-zero record count, got %v", parquetFile.RecordCount)
		}
		if len(parquetFile.Stats.ColumnSizes) != 2 {
			t.Errorf("Expected 2 column sizes, got %v", len(parquetFile.Stats.ColumnSizes))
		}
		if parquetFile.Stats.ColumnSizes[1] == 0 {
			t.Errorf("Expected a non-zero column size, got %v", parquetFile.Stats.ColumnSizes[1])
		}
		if parquetFile.Stats.ColumnSizes[2] == 0 {
			t.Errorf("Expected a non-zero value count, got %v", parquetFile.Stats.ColumnSizes[2])
		}
		if parquetFile.Stats.ValueCounts[1] != 2 {
			t.Errorf("Expected a value count of 2, got %v", parquetFile.Stats.ValueCounts[1])
		}
		if parquetFile.Stats.ValueCounts[2] != 2 {
			t.Errorf("Expected a value count of 1, got %v", parquetFile.Stats.ValueCounts[2])
		}
		if parquetFile.Stats.NullValueCounts[1] != 0 {
			t.Errorf("Expected a null value count of 0, got %v", parquetFile.Stats.NullValueCounts[1])
		}
		if parquetFile.Stats.NullValueCounts[2] != 1 {
			t.Errorf("Expected a null value count of 1, got %v", parquetFile.Stats.NullValueCounts[2])
		}
		if binary.LittleEndian.Uint32(parquetFile.Stats.LowerBounds[1]) != 1 {
			t.Errorf("Expected a lower bound of 1, got %v", binary.LittleEndian.Uint32(parquetFile.Stats.LowerBounds[1]))
		}
		if string(parquetFile.Stats.LowerBounds[2]) != "John" {
			t.Errorf("Expected a lower bound of John, got %v", parquetFile.Stats.LowerBounds[2])
		}
		if binary.LittleEndian.Uint32(parquetFile.Stats.UpperBounds[1]) != 2 {
			t.Errorf("Expected an upper bound of 2, got %v", binary.LittleEndian.Uint32(parquetFile.Stats.UpperBounds[1]))
		}
		if string(parquetFile.Stats.UpperBounds[2]) != "John" {
			t.Errorf("Expected an upper bound of John, got %v", parquetFile.Stats.UpperBounds[2])
		}
		if len(parquetFile.Stats.SplitOffsets) != 0 {
			t.Errorf("Expected 0 split offsets, got %v", len(parquetFile.Stats.SplitOffsets))
		}
	})
}
