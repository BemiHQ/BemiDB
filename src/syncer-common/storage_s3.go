package syncerCommon

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb/v2"
	"github.com/xitongsys/parquet-go-source/s3v2"
)

type StorageS3 struct {
	S3Client     *common.S3Client
	Config       *common.CommonConfig
	StorageUtils *StorageUtils
	DuckdbClient *common.DuckdbClient
}

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
	Key         string
	Path        string // With s3://bucket/ prefix
	Size        int64
	RecordCount int64
	Stats       ParquetFileStats
}

type ManifestFile struct {
	RecordsDeleted bool
	SnapshotId     int64
	Key            string
	Path           string // With s3://bucket/ prefix
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
	Key              string
	Path             string // With s3://bucket/ prefix
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
	Key     string
}

func NewStorageS3(Config *common.CommonConfig) *StorageS3 {
	return &StorageS3{
		S3Client:     common.NewS3Client(Config),
		Config:       Config,
		StorageUtils: NewStorageUtils(Config),
		DuckdbClient: common.NewDuckdbClient(Config),
	}
}

// Write ---------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) CreateParquet(s3DataPath string, icebergSchemaColumns []*IcebergSchemaColumn, loadRows func(appender *duckdb.Appender) (rowCount int, reachedEnd bool)) (parquetFile ParquetFile, reachedEnd bool) {
	ctx := context.Background()
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	fileS3Path := s3DataPath + "/" + fileName
	fileKey := storage.S3Client.ObjectKey(fileS3Path)

	rowCount, reachedEnd := storage.StorageUtils.WriteParquetFile(storage.DuckdbClient, fileS3Path, icebergSchemaColumns, loadRows)
	common.LogDebug(storage.Config, "Parquet file with", rowCount, "record(s) created at:", fileKey)

	headObjectOutput := storage.S3Client.HeadObject(fileKey)
	fileSize := *headObjectOutput.ContentLength

	fileReader, err := s3v2.NewS3FileReaderWithClient(ctx, storage.S3Client.S3, storage.Config.Aws.S3Bucket, fileKey)
	common.PanicIfError(storage.Config, err)
	parquetStats := storage.StorageUtils.ReadParquetStats(fileReader, icebergSchemaColumns)

	return ParquetFile{
		Uuid:        uuid,
		Key:         fileKey,
		Path:        s3DataPath + "/" + fileName,
		Size:        fileSize,
		RecordCount: int64(rowCount),
		Stats:       parquetStats,
	}, reachedEnd
}

func (storage *StorageS3) DeleteParquet(parquetFile ParquetFile) {
	storage.S3Client.DeleteObject(parquetFile.Key)
}

func (storage *StorageS3) CreateManifest(s3MetadataPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m0.avro", parquetFile.Uuid)
	fileKey := storage.S3Client.ObjectKey(s3MetadataPath) + "/" + fileName

	err = storage.uploadTemporaryFile("manifest", fileKey, func(tempFile *os.File) error {
		manifestFile, err = storage.StorageUtils.WriteManifestFile(tempFile.Name(), parquetFile)
		return err
	})
	if err != nil {
		return manifestFile, err
	}
	common.LogDebug(storage.Config, "Manifest file created at:", fileKey)

	manifestFile.Key = fileKey
	manifestFile.Path = s3MetadataPath + "/" + fileName
	return manifestFile, nil
}

func (storage *StorageS3) CreateManifestList(s3MetadataPath string, parquetFileUuid string, manifestListItemsSortedDesc []ManifestListItem) (manifestListFile ManifestListFile, err error) {
	fileName := fmt.Sprintf("snap-%d-0-%s.avro", manifestListItemsSortedDesc[0].ManifestFile.SnapshotId, parquetFileUuid)
	fileKey := storage.S3Client.ObjectKey(s3MetadataPath) + "/" + fileName

	err = storage.uploadTemporaryFile("manifest-list", fileKey, func(tempFile *os.File) error {
		manifestListFile, err = storage.StorageUtils.WriteManifestListFile(tempFile.Name(), manifestListItemsSortedDesc)
		return err
	})
	if err != nil {
		return manifestListFile, err
	}
	common.LogDebug(storage.Config, "Manifest list file created at:", fileKey)

	manifestListFile.Key = fileKey
	manifestListFile.Path = s3MetadataPath + "/" + fileName
	return manifestListFile, nil
}

func (storage *StorageS3) CreateMetadata(s3MetadataPath string, icebergSchemaColumns []*IcebergSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (metadataFile MetadataFile, err error) {
	fileKey := storage.S3Client.ObjectKey(s3MetadataPath) + "/" + ICEBERG_METADATA_INITIAL_FILE_NAME

	err = storage.uploadTemporaryFile("metadata", fileKey, func(tempFile *os.File) error {
		s3TablePath := strings.TrimSuffix(s3MetadataPath, "/metadata")
		return storage.StorageUtils.WriteMetadataFile(s3TablePath, tempFile.Name(), icebergSchemaColumns, manifestListFilesSortedAsc)
	})
	if err != nil {
		return metadataFile, err
	}
	common.LogDebug(storage.Config, "Metadata file created at:", fileKey)

	return MetadataFile{Version: 1, Key: fileKey}, nil
}

func (storage *StorageS3) DeleteTableFiles(s3TablePath string) (err error) {
	tableKey := storage.S3Client.ObjectKey(s3TablePath)
	return storage.deleteNestedObjects(tableKey)
}

// ---------------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) uploadTemporaryFile(tempFilePattern string, uploadFilePath string, writeTempFileFunc func(*os.File) error) error {
	tempFile, err := os.CreateTemp("", tempFilePattern)
	if err != nil {
		return err
	}
	defer func() {
		os.Remove(tempFile.Name())
	}()

	err = writeTempFileFunc(tempFile)
	if err != nil {
		return err
	}

	storage.S3Client.UploadObject(uploadFilePath, tempFile)

	err = tempFile.Close()
	if err != nil {
		return err
	}

	return nil
}

func (storage *StorageS3) deleteNestedObjects(prefixKey string) (err error) {
	listResponse := storage.S3Client.ListObjects(prefixKey)

	var fileKeys []*string
	for _, obj := range listResponse.Contents {
		common.LogDebug(storage.Config, "Object to delete:", *obj.Key)
		fileKeys = append(fileKeys, obj.Key)
	}

	if len(fileKeys) > 0 {
		storage.S3Client.DeleteObjects(fileKeys)
		common.LogDebug(storage.Config, "Deleted", len(fileKeys), "object(s).")
	} else {
		common.LogDebug(storage.Config, "No objects to delete.")
	}

	return nil
}
