package main

import (
	"encoding/binary"
	"os"
	"testing"
)

var TEST_STORAGE_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{ColumnName: "id", DataType: "integer", UdtName: "int4", IsNullable: "NO", NumericPrecision: "32", OrdinalPosition: "1", Namespace: "pg_catalog"},
	{ColumnName: "name", DataType: "character varying", UdtName: "varchar", IsNullable: "YES", CharacterMaximumLength: "255", OrdinalPosition: "2", Namespace: "pg_catalog"},
}
var TEST_STORAGE_ROWS = [][]string{
	{"1", "John"},
	{"2", PG_NULL_STRING},
}

func TestCreateParquet(t *testing.T) {
	t.Run("Creates a parquet file", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		loadedRows := false
		loadRows := func() [][]string {
			if loadedRows {
				return [][]string{}
			}
			loadedRows = true
			return TEST_STORAGE_ROWS
		}

		parquetFile, err := storage.CreateParquet(tempDir, TEST_STORAGE_PG_SCHEMA_COLUMNS, loadRows)

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

func TestCreateManifest(t *testing.T) {
	t.Run("Creates a manifest file", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		parquetFile := createTestParquetFile(storage, tempDir)

		manifestFile, err := storage.CreateManifest(tempDir, parquetFile)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if manifestFile.SnapshotId == 0 {
			t.Errorf("Expected a non-zero snapshot ID, got %v", manifestFile.SnapshotId)
		}
		if manifestFile.Path == "" {
			t.Errorf("Expected a non-empty path, got %v", manifestFile.Path)
		}
		if manifestFile.Size == 0 {
			t.Errorf("Expected a non-zero size, got %v", manifestFile.Size)
		}
		if manifestFile.RecordCount != parquetFile.RecordCount {
			t.Errorf("Expected a record count of %v, got %v", parquetFile.RecordCount, manifestFile.RecordCount)
		}
		if manifestFile.DataFileSize != parquetFile.Size {
			t.Errorf("Expected a data file size of %v, got %v", parquetFile.Size, manifestFile.DataFileSize)
		}
	})
}

func TestCreateManifestList(t *testing.T) {
	t.Run("Creates a manifest list file", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		parquetFile := createTestParquetFile(storage, tempDir)
		manifestFile, err := storage.CreateManifest(tempDir, parquetFile)
		PanicIfError(err, config)
		manifestListItem := ManifestListItem{SequenceNumber: 1, ManifestFile: manifestFile}

		manifestListFile, err := storage.CreateManifestList(tempDir, parquetFile.Uuid, []ManifestListItem{manifestListItem})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if manifestListFile.SnapshotId != manifestFile.SnapshotId {
			t.Errorf("Expected a snapshot ID of %v, got %v", manifestFile.SnapshotId, manifestListFile.SnapshotId)
		}
		if manifestListFile.TimestampMs == 0 {
			t.Errorf("Expected a non-zero timestamp, got %v", manifestListFile.TimestampMs)
		}
		if manifestListFile.Path == "" {
			t.Errorf("Expected a non-empty path, got %v", manifestListFile.Path)
		}
		if manifestListFile.Operation != "append" {
			t.Errorf("Expected an operation of append, got %v", manifestListFile.Operation)
		}
		if manifestListFile.AddedFilesSize != parquetFile.Size {
			t.Errorf("Expected an added files size of %v, got %v", parquetFile.Size, manifestListFile.AddedFilesSize)
		}
		if manifestListFile.AddedDataFiles != 1 {
			t.Errorf("Expected an added data files count of 1, got %v", manifestListFile.AddedDataFiles)
		}
		if manifestListFile.AddedRecords != parquetFile.RecordCount {
			t.Errorf("Expected an added records count of %v, got %v", parquetFile.RecordCount, manifestListFile.AddedRecords)
		}
	})
}

func TestCreateMetadata(t *testing.T) {
	t.Run("Creates a metadata file", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		parquetFile := createTestParquetFile(storage, tempDir)
		manifestFile, err := storage.CreateManifest(tempDir, parquetFile)
		PanicIfError(err, config)
		manifestListItem := ManifestListItem{SequenceNumber: 1, ManifestFile: manifestFile}
		manifestListFile, err := storage.CreateManifestList(tempDir, parquetFile.Uuid, []ManifestListItem{manifestListItem})
		PanicIfError(err, config)

		metadataFile, err := storage.CreateMetadata(tempDir, TEST_STORAGE_PG_SCHEMA_COLUMNS, []ManifestListFile{manifestListFile})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if metadataFile.Version != 1 {
			t.Errorf("Expected a version of 1, got %v", metadataFile.Version)
		}
		if metadataFile.Path == "" {
			t.Errorf("Expected a non-empty path, got %v", metadataFile.Path)
		}
	})
}

func TestExistingManifestListFiles(t *testing.T) {
	t.Run("Returns existing manifest list files", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		parquetFile := createTestParquetFile(storage, tempDir)
		manifestFile, err := storage.CreateManifest(tempDir, parquetFile)
		PanicIfError(err, config)
		manifestListItem := ManifestListItem{SequenceNumber: 1, ManifestFile: manifestFile}
		manifestListFile, err := storage.CreateManifestList(tempDir, parquetFile.Uuid, []ManifestListItem{manifestListItem})
		PanicIfError(err, config)
		_, err = storage.CreateMetadata(tempDir, TEST_STORAGE_PG_SCHEMA_COLUMNS, []ManifestListFile{manifestListFile})
		PanicIfError(err, config)

		existingManifestListFiles, err := storage.ExistingManifestListFiles(tempDir)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if len(existingManifestListFiles) != 1 {
			t.Errorf("Expected 1 existing manifest list file, got %v", len(existingManifestListFiles))
		}
		if existingManifestListFiles[0].SnapshotId != manifestListFile.SnapshotId {
			t.Errorf("Expected a snapshot ID of %v, got %v", manifestListFile.SnapshotId, existingManifestListFiles[0].SnapshotId)
		}
		if existingManifestListFiles[0].TimestampMs != manifestListFile.TimestampMs {
			t.Errorf("Expected a timestamp of %v, got %v", manifestListFile.TimestampMs, existingManifestListFiles[0].TimestampMs)
		}
		if existingManifestListFiles[0].Path != manifestListFile.Path {
			t.Errorf("Expected a path of %v, got %v", manifestListFile.Path, existingManifestListFiles[0].Path)
		}
		if existingManifestListFiles[0].Operation != manifestListFile.Operation {
			t.Errorf("Expected an operation of %v, got %v", manifestListFile.Operation, existingManifestListFiles[0].Operation)
		}
		if existingManifestListFiles[0].AddedFilesSize != manifestListFile.AddedFilesSize {
			t.Errorf("Expected an added files size of %v, got %v", manifestListFile.AddedFilesSize, existingManifestListFiles[0].AddedFilesSize)
		}
		if existingManifestListFiles[0].AddedDataFiles != manifestListFile.AddedDataFiles {
			t.Errorf("Expected an added data files count of %v, got %v", manifestListFile.AddedDataFiles, existingManifestListFiles[0].AddedDataFiles)
		}
		if existingManifestListFiles[0].AddedRecords != manifestListFile.AddedRecords {
			t.Errorf("Expected an added records count of %v, got %v", manifestListFile.AddedRecords, existingManifestListFiles[0].AddedRecords)
		}
	})
}

func TestExistingManifestFiles(t *testing.T) {
	t.Run("Returns existing manifest files", func(t *testing.T) {
		tempDir := os.TempDir()
		config := loadTestConfig()
		storage := NewLocalStorage(config)
		parquetFile := createTestParquetFile(storage, tempDir)
		manifestFile, err := storage.CreateManifest(tempDir, parquetFile)
		PanicIfError(err, config)
		manifestListItem := ManifestListItem{SequenceNumber: 1, ManifestFile: manifestFile}
		manifestListFile, err := storage.CreateManifestList(tempDir, parquetFile.Uuid, []ManifestListItem{manifestListItem})
		PanicIfError(err, config)

		existingManifestListItems, err := storage.ExistingManifestListItems(manifestListFile)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if len(existingManifestListItems) != 1 {
			t.Errorf("Expected 1 existing manifest file, got %v", len(existingManifestListItems))
		}
		if existingManifestListItems[0].SequenceNumber != 1 {
			t.Errorf("Expected a sequence number of 1, got %v", existingManifestListItems[0].SequenceNumber)
		}
		if existingManifestListItems[0].ManifestFile.SnapshotId != manifestFile.SnapshotId {
			t.Errorf("Expected a snapshot ID of %v, got %v", manifestFile.SnapshotId, existingManifestListItems[0].ManifestFile.SnapshotId)
		}
		if existingManifestListItems[0].ManifestFile.Path != manifestFile.Path {
			t.Errorf("Expected a path of %v, got %v", manifestFile.Path, existingManifestListItems[0].ManifestFile.Path)
		}
		if existingManifestListItems[0].ManifestFile.Size != manifestFile.Size {
			t.Errorf("Expected a size of %v, got %v", manifestFile.Size, existingManifestListItems[0].ManifestFile.Size)
		}
		if existingManifestListItems[0].ManifestFile.RecordCount != manifestFile.RecordCount {
			t.Errorf("Expected a record count of %v, got %v", manifestFile.RecordCount, existingManifestListItems[0].ManifestFile.RecordCount)
		}
	})
}

func createTestParquetFile(storage *StorageLocal, dir string) ParquetFile {
	loadedRows := false
	loadRows := func() [][]string {
		if loadedRows {
			return [][]string{}
		}
		loadedRows = true
		return TEST_STORAGE_ROWS
	}

	parquetFile, err := storage.CreateParquet(dir, TEST_STORAGE_PG_SCHEMA_COLUMNS, loadRows)
	if err != nil {
		panic(err)
	}

	return parquetFile
}
