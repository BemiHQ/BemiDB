package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
)

type StorageLocal struct {
	config       *Config
	storageUtils *StorageUtils
}

func NewLocalStorage(config *Config) *StorageLocal {
	return &StorageLocal{config: config, storageUtils: &StorageUtils{config: config}}
}

// Read ----------------------------------------------------------------------------------------------------------------

func (storage *StorageLocal) IcebergMetadataFilePath(icebergSchemaTable IcebergSchemaTable) string {
	tablePath := storage.tablePath(icebergSchemaTable, true)
	return filepath.Join(tablePath, "metadata", ICEBERG_METADATA_FILE_NAME)
}

func (storage *StorageLocal) IcebergSchemas() (icebergSchemas []string, err error) {
	schemasPath := storage.absoluteIcebergPath()
	icebergSchemas, err = storage.nestedDirectories(schemasPath)
	if err != nil {
		return nil, err
	}

	return icebergSchemas, nil
}

func (storage *StorageLocal) IcebergSchemaTables() (Set[IcebergSchemaTable], error) {
	icebergSchemaTables := make(Set[IcebergSchemaTable])
	schemasPath := storage.absoluteIcebergPath()
	icebergSchemas, err := storage.IcebergSchemas()
	if err != nil {
		return nil, err
	}

	for _, icebergSchema := range icebergSchemas {
		tablesPath := filepath.Join(schemasPath, icebergSchema)
		tables, err := storage.nestedDirectories(tablesPath)
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			icebergSchemaTables.Add(IcebergSchemaTable{Schema: icebergSchema, Table: table})
		}
	}

	return icebergSchemaTables, nil
}

func (storage *StorageLocal) IcebergTableFields(icebergSchemaTable IcebergSchemaTable) ([]IcebergTableField, error) {
	metadataPath := storage.IcebergMetadataFilePath(icebergSchemaTable)
	metadataContent, err := storage.readFileContent(metadataPath)
	if err != nil {
		return nil, err
	}

	return storage.storageUtils.ParseIcebergTableFields(metadataContent)
}

func (storage *StorageLocal) ExistingManifestListFiles(metadataDirPath string) ([]ManifestListFile, error) {
	metadataPath := filepath.Join(metadataDirPath, ICEBERG_METADATA_FILE_NAME)
	metadataContent, err := storage.readFileContent(metadataPath)
	if err != nil {
		return nil, err
	}

	return storage.storageUtils.ParseManifestListFiles(storage.fileSystemPrefix(), metadataContent)
}

func (storage *StorageLocal) ExistingManifestListItems(manifestListFile ManifestListFile) ([]ManifestListItem, error) {
	manifestListContent, err := storage.readFileContent(manifestListFile.Path)
	if err != nil {
		return nil, err
	}

	return storage.storageUtils.ParseManifestFiles(storage.fileSystemPrefix(), manifestListContent)
}

func (storage *StorageLocal) ExistingParquetFilePath(manifestFile ManifestFile) (string, error) {
	manifestContent, err := storage.readFileContent(manifestFile.Path)
	if err != nil {
		return "", err
	}

	return storage.storageUtils.ParseParquetFilePath(storage.fileSystemPrefix(), manifestContent)
}

// Write ---------------------------------------------------------------------------------------------------------------

func (storage *StorageLocal) DeleteSchema(schema string) error {
	schemaPath := storage.absoluteIcebergPath(schema)

	_, err := os.Stat(schemaPath)
	if !os.IsNotExist(err) {
		err := os.RemoveAll(schemaPath)
		return err
	}

	return nil
}

func (storage *StorageLocal) DeleteSchemaTable(schemaTable IcebergSchemaTable) error {
	tablePath := storage.tablePath(schemaTable)

	_, err := os.Stat(tablePath)
	if !os.IsNotExist(err) {
		err := os.RemoveAll(tablePath)
		return err
	}

	return nil
}

func (storage *StorageLocal) CreateDataDir(schemaTable IcebergSchemaTable) string {
	tablePath := storage.tablePath(schemaTable)
	dataPath := filepath.Join(tablePath, "data")
	err := os.MkdirAll(dataPath, os.ModePerm)
	PanicIfError(err, storage.config)
	return dataPath
}

func (storage *StorageLocal) CreateMetadataDir(schemaTable IcebergSchemaTable) string {
	tablePath := storage.tablePath(schemaTable)
	metadataPath := filepath.Join(tablePath, "metadata")
	err := os.MkdirAll(metadataPath, os.ModePerm)
	PanicIfError(err, storage.config)
	return metadataPath
}

func (storage *StorageLocal) CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, loadRows func() [][]string) (parquetFile ParquetFile, err error) {
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	filePath := filepath.Join(dataDirPath, fileName)

	fileWriter, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for writing: %v", err)
	}

	recordCount, err := storage.storageUtils.WriteParquetFile(fileWriter, pgSchemaColumns, loadRows)
	if err != nil {
		return ParquetFile{}, err
	}
	LogDebug(storage.config, "Parquet file with", recordCount, "record(s) created at:", filePath)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to get Parquet file info: %v", err)
	}
	fileSize := fileInfo.Size()

	fileReader, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for reading: %v", err)
	}
	parquetStats, err := storage.storageUtils.ReadParquetStats(fileReader)
	if err != nil {
		return ParquetFile{}, err
	}

	return ParquetFile{
		Uuid:        uuid,
		Path:        filePath,
		Size:        fileSize,
		RecordCount: recordCount,
		Stats:       parquetStats,
	}, nil
}

func (storage *StorageLocal) CreateOverwrittenParquet(dataDirPath string, existingParquetFilePath string, newParquetFilePath string, pgSchemaColumns []PgSchemaColumn, rowCountPerBatch int) (overwrittenParquetFile ParquetFile, err error) {
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	filePath := filepath.Join(dataDirPath, fileName)

	fileWriter, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for writing: %v", err)
	}

	recordCount, err := storage.storageUtils.WriteOverwrittenParquetFile(fileWriter, existingParquetFilePath, newParquetFilePath, pgSchemaColumns, rowCountPerBatch)
	if err != nil {
		return ParquetFile{}, err
	}
	LogDebug(storage.config, "Parquet file with", recordCount, "record(s) created at:", filePath)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to get Parquet file info: %v", err)
	}
	fileSize := fileInfo.Size()

	fileReader, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for reading: %v", err)
	}
	parquetStats, err := storage.storageUtils.ReadParquetStats(fileReader)
	if err != nil {
		return ParquetFile{}, err
	}

	return ParquetFile{
		Uuid:        uuid,
		Path:        filePath,
		Size:        fileSize,
		RecordCount: recordCount,
		Stats:       parquetStats,
	}, nil
}

func (storage *StorageLocal) DeleteParquet(parquetFile ParquetFile) error {
	err := os.Remove(parquetFile.Path)
	return err
}

func (storage *StorageLocal) CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m0.avro", parquetFile.Uuid)
	filePath := filepath.Join(metadataDirPath, fileName)

	manifestFile, err = storage.storageUtils.WriteManifestFile(storage.fileSystemPrefix(), filePath, parquetFile)
	if err != nil {
		return ManifestFile{}, err
	}
	LogDebug(storage.config, "Manifest file created at:", filePath)

	return manifestFile, nil
}

func (storage *StorageLocal) CreateDeletedRecordsManifest(metadataDirPath string, uuid string, existingManifestFile ManifestFile) (deletedRecsManifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m1.avro", uuid)
	filePath := filepath.Join(metadataDirPath, fileName)

	existingManifestContent, err := storage.readFileContent(existingManifestFile.Path)
	if err != nil {
		return ManifestFile{}, err
	}

	deletedRecsManifestFile, err = storage.storageUtils.WriteDeletedRecordsManifestFile(storage.fileSystemPrefix(), filePath, existingManifestContent)
	if err != nil {
		return ManifestFile{}, err
	}
	LogDebug(storage.config, "Manifest file created at:", filePath)

	return deletedRecsManifestFile, nil
}

func (storage *StorageLocal) CreateManifestList(metadataDirPath string, parquetFileUuid string, manifestListItemsSortedDesc []ManifestListItem) (manifestListFile ManifestListFile, err error) {
	fileName := fmt.Sprintf("snap-%d-0-%s.avro", manifestListItemsSortedDesc[0].ManifestFile.SnapshotId, parquetFileUuid)
	filePath := filepath.Join(metadataDirPath, fileName)

	manifestListFile, err = storage.storageUtils.WriteManifestListFile(storage.fileSystemPrefix(), filePath, manifestListItemsSortedDesc)
	if err != nil {
		return ManifestListFile{}, err
	}
	LogDebug(storage.config, "Manifest list file created at:", filePath)

	return manifestListFile, nil
}

func (storage *StorageLocal) CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (metadataFile MetadataFile, err error) {
	filePath := filepath.Join(metadataDirPath, ICEBERG_METADATA_FILE_NAME)

	err = storage.storageUtils.WriteMetadataFile(storage.fileSystemPrefix(), filePath, pgSchemaColumns, manifestListFilesSortedAsc)
	if err != nil {
		return MetadataFile{}, err
	}
	LogDebug(storage.config, "Metadata file created at:", filePath)

	return MetadataFile{Version: 1, Path: filePath}, nil
}

// Read (internal) -----------------------------------------------------------------------------------------------------

func (storage *StorageLocal) InternalTableMetadata(pgSchemaTable PgSchemaTable) (InternalTableMetadata, error) {
	internalMetadataPath := storage.internalTableMetadataFilePath(pgSchemaTable)
	internalMetadataContent, err := storage.readFileContent(internalMetadataPath)
	if err != nil {
		return InternalTableMetadata{}, err
	}

	return storage.storageUtils.ParseInternalTableMetadata(internalMetadataContent)
}

// Write (internal) ----------------------------------------------------------------------------------------------------

func (storage *StorageLocal) WriteInternalTableMetadata(pgSchemaTable PgSchemaTable, internalTableMetadata InternalTableMetadata) error {
	filePath := storage.internalTableMetadataFilePath(pgSchemaTable)

	err := storage.storageUtils.WriteInternalTableMetadataFile(filePath, internalTableMetadata)
	if err != nil {
		return err
	}
	LogDebug(storage.config, "Internal table metadata file created at:", filePath)

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (storage *StorageLocal) readFileContent(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return io.ReadAll(file)
}

func (storage *StorageLocal) internalTableMetadataFilePath(pgSchemaTable PgSchemaTable) string {
	return filepath.Join(storage.tablePath(pgSchemaTable.ToIcebergSchemaTable()), "metadata", INTERNAL_METADATA_FILE_NAME)
}

func (storage *StorageLocal) tablePath(schemaTable IcebergSchemaTable, readWithoutSchemaPrefix ...bool) string {
	if len(readWithoutSchemaPrefix) > 0 && readWithoutSchemaPrefix[0] {
		return storage.absoluteIcebergPath(schemaTable.Schema, schemaTable.Table)
	}
	return storage.absoluteIcebergPath(storage.config.Pg.SchemaPrefix+schemaTable.Schema, schemaTable.Table)
}

func (storage *StorageLocal) absoluteIcebergPath(relativePaths ...string) string {
	if filepath.IsAbs(storage.config.StoragePath) {
		return filepath.Join(storage.config.StoragePath, filepath.Join(relativePaths...))
	}

	execPath, err := os.Getwd()
	PanicIfError(err, storage.config)
	return filepath.Join(execPath, storage.config.StoragePath, filepath.Join(relativePaths...))
}

func (storage *StorageLocal) fileSystemPrefix() string {
	return "" // DuckDB doesn't support file:// prefixes
}

func (storage *StorageLocal) nestedDirectories(path string) (dirs []string, err error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %s.\nPlease make sure to run 'bemidb sync' first", path)
	}

	for _, file := range files {
		if file.IsDir() {
			dirs = append(dirs, file.Name())
		}
	}

	return dirs, nil
}
