package main

import (
	"fmt"
)

type RefreshMode string

const (
	RefreshModeFull                  RefreshMode = "FULL"
	RefreshModeFullInProgress        RefreshMode = "FULL_IN_PROGRESS"
	RefreshModeIncremental           RefreshMode = "INCREMENTAL"
	RefreshModeIncrementalInProgress RefreshMode = "INCREMENTAL_IN_PROGRESS"
)

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
	RecordsDeleted bool
	SnapshotId     int64
	Path           string
	Size           int64
	RecordCount    int64
	DataFileSize   int64
}

type ManifestListItem struct {
	SequenceNumber int
	ManifestFile   ManifestFile
}

type ManifestListFile struct {
	SequenceNumber   int
	SnapshotId       int64
	TimestampMs      int64
	Path             string
	Operation        string
	AddedFilesSize   int64
	AddedDataFiles   int64
	AddedRecords     int64
	RemovedFilesSize int64
	DeletedDataFiles int64
	DeletedRecords   int64
}

type MetadataFile struct {
	Version int64
	Path    string
}

type InternalTableMetadata struct {
	LastRefreshMode RefreshMode `json:"last-refresh-mode"`
	LastSyncedAt    int64       `json:"last-synced-at"`
	LastTxid        int64       `json:"last-txid"`
	MaxXmin         *uint32     `json:"max-xmin"`
}

func (internalTableMetadata InternalTableMetadata) IsInProgress() bool {
	return internalTableMetadata.LastRefreshMode == RefreshModeIncrementalInProgress || internalTableMetadata.LastRefreshMode == RefreshModeFullInProgress
}

func (internalTableMetadata InternalTableMetadata) MaxXminString() string {
	if internalTableMetadata.MaxXmin == nil {
		panic("MaxXmin is unexpectedly null. " + internalTableMetadata.String())
	}
	return Uint32ToString(*internalTableMetadata.MaxXmin)
}

func (internalTableMetadata InternalTableMetadata) LastWrappedAroundTxidString() string {
	return Int64ToString(PgWraparoundTxid(internalTableMetadata.LastTxid))
}

func (internalTableMetadata InternalTableMetadata) String() string {
	return fmt.Sprintf(
		"LastRefreshMode: %s, LastSyncedAt: %d, MaxXmin: %d",
		internalTableMetadata.LastRefreshMode,
		internalTableMetadata.LastSyncedAt,
		*internalTableMetadata.MaxXmin,
	)
}

type StorageInterface interface {
	// Read
	IcebergSchemas() (icebergSchemas []string, err error)
	IcebergSchemaTables() (icebersSchemaTables Set[IcebergSchemaTable], err error)
	IcebergMetadataFilePath(icebergSchemaTable IcebergSchemaTable) (path string)
	IcebergTableFields(icebergSchemaTable IcebergSchemaTable) (icebergTableFields []IcebergTableField, err error)
	ExistingManifestListFiles(metadataDirPath string) (manifestListFilesSortedAsc []ManifestListFile, err error)
	ExistingManifestListItems(manifestListFile ManifestListFile) (manifestListItemsSortedDesc []ManifestListItem, err error)
	ExistingParquetFilePath(manifestFile ManifestFile) (parquetFilePath string, err error)

	// Write
	DeleteSchema(schema string) (err error)
	DeleteSchemaTable(schemaTable IcebergSchemaTable) (err error)
	CreateDataDir(schemaTable IcebergSchemaTable) (dataDirPath string)
	CreateMetadataDir(schemaTable IcebergSchemaTable) (metadataDirPath string)
	CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, maxPayloadThreshold int, loadRows func() ([][]string, InternalTableMetadata)) (parquetFile ParquetFile, internalTableMetadata InternalTableMetadata, err error)
	CreateOverwrittenParquet(dataDirPath string, existingParquetFilePath string, newParquetFilePath string, pgSchemaColumns []PgSchemaColumn, dynamicRowCountPerBatch int) (overwrittenParquetFile ParquetFile, err error)
	DeleteParquet(parquetFile ParquetFile) (err error)
	CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error)
	CreateDeletedRecordsManifest(metadataDirPath string, uuid string, existingManifestFile ManifestFile) (deletedRecsManifestFile ManifestFile, err error)
	CreateManifestList(metadataDirPath string, parquetFileUuid string, manifestListItemsSortedDesc []ManifestListItem) (manifestListFile ManifestListFile, err error)
	CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (metadataFile MetadataFile, err error)

	// Read (internal)
	InternalTableMetadata(pgSchemaTable PgSchemaTable) (internalTableMetadata InternalTableMetadata, err error)
	// Write (internal)
	WriteInternalTableMetadata(metadataDirPath string, internalTableMetadata InternalTableMetadata) (err error)
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
