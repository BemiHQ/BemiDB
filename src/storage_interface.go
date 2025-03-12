package main

import (
	"fmt"
)

var STORAGE_TYPES = []string{STORAGE_TYPE_LOCAL, STORAGE_TYPE_S3}

type ParquetFileStats struct {
	ColumnSizes     map[int]int64
	ValueCounts     map[int]int64
	NullValueCounts map[int]int64
	LowerBounds     map[int][]byte
	UpperBounds     map[int][]byte
	SplitOffsets    []int64
}

type ParquetFile struct {
	Uuid        string
	Path        string
	Size        int64
	RecordCount int64
	Stats       ParquetFileStats
}

type ManifestFile struct {
	SnapshotId       int64
	ParentSnapshotId int64
	SequenceNumber   int64
	TimestampMs      int64
	Path             string
	Size             int64
	ManifestSummary  ManifestSummary
}

type ManifestSummary struct {
	Operation            string
	AddedFilesSize       int64
	AddedDataFiles       int64
	AddedRecords         int64
	TotalDataFiles       int64
	TotalDeleteFiles     int64
	TotalRecords         int64
	TotalFilesSize       int64
	TotalPositionDeletes int64
	TotalEqualityDeletes int64
}

type ManifestListFile struct {
	Path string
}

type MetadataFile struct {
	Version int64
	Path    string
}

type InternalTableMetadata struct {
	LastSyncedAt int64   `json:"last-synced-at"`
	XminMax      *uint32 `json:"xmin-max"`
	XminMin      *uint32 `json:"xmin-min"`
}

func (internalTableMetadata InternalTableMetadata) XminMaxString() string {
	if internalTableMetadata.XminMax == nil {
		return "null"
	}
	return fmt.Sprint(*internalTableMetadata.XminMax)
}

func (internalTableMetadata InternalTableMetadata) XminMinString() string {
	if internalTableMetadata.XminMin == nil {
		return "null"
	}
	return fmt.Sprint(*internalTableMetadata.XminMin)
}

func (internalTableMetadata InternalTableMetadata) String() string {
	return fmt.Sprintf("LastSyncedAt: %d, XminMax: %s, XminMin: %s", internalTableMetadata.LastSyncedAt, internalTableMetadata.XminMaxString(), internalTableMetadata.XminMinString())
}

type StorageInterface interface {
	// Read
	IcebergSchemas() (icebergSchemas []string, err error)
	IcebergSchemaTables() (icebersSchemaTables Set[IcebergSchemaTable], err error)
	IcebergMetadataFilePath(icebergSchemaTable IcebergSchemaTable) (path string)
	IcebergTableFields(icebergSchemaTable IcebergSchemaTable) (icebergTableFields []IcebergTableField, err error)

	// Write
	DeleteSchema(schema string) (err error)
	DeleteSchemaTable(schemaTable IcebergSchemaTable) (err error)
	CreateDataDir(schemaTable IcebergSchemaTable) (dataDirPath string)
	CreateMetadataDir(schemaTable IcebergSchemaTable) (metadataDirPath string)
	CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, loadRows func() [][]string) (parquetFile ParquetFile, err error)
	DeleteParquet(parquetFile ParquetFile) (err error)
	CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error)
	CreateManifestList(metadataDirPath string, parquetFile ParquetFile, manifestFilesSortedDesc []ManifestFile) (manifestListFile ManifestListFile, err error)
	CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, manifestFilesSortedDesc []ManifestFile, manifestListFile ManifestListFile) (metadataFile MetadataFile, err error)
	CreateVersionHint(metadataDirPath string, metadataFile MetadataFile) (err error)

	// Read (internal)
	InternalTableMetadata(pgSchemaTable PgSchemaTable) (internalTableMetadata InternalTableMetadata, err error)
	// Write (internal)
	WriteInternalTableMetadata(pgSchemaTable PgSchemaTable, internalTableMetadata InternalTableMetadata) (err error)
}

func NewStorage(config *Config) StorageInterface {
	switch config.StorageType {
	case STORAGE_TYPE_LOCAL:
		return NewLocalStorage(config)
	case STORAGE_TYPE_S3:
		return NewS3Storage(config)
	}

	return nil
}
