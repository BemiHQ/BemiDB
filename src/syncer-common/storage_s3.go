package syncerCommon

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/s3v2"
)

const (
	UUID_LENGTH = 36
)

type StorageS3 struct {
	S3Client     *common.S3Client
	Config       *common.CommonConfig
	StorageUtils *StorageUtils
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
	Key         string
	Path        string // With s3://bucket/ prefix
	Size        int64
	RecordCount int64
	Stats       ParquetFileStats
}

type ManifestFile struct {
	Key                string
	Path               string // With s3://bucket/ prefix
	Size               int64
	TotalRecordCount   int64
	TotalDataFileCount int32
	RecordsDeleted     bool
}

type ManifestListItem struct {
	SequenceNumber int
	ManifestFile   ManifestFile
}

type ManifestListFile struct {
	SequenceNumber int
	SnapshotId     int64
	TimestampMs    int64
	Key            string
	Path           string // With s3://bucket/ prefix
	Operation      string
	TotalFilesSize int64
	TotalDataFiles int64
	TotalRecords   int64
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
	}
}

// Write ---------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) CreateParquet(dataS3Path string, duckdbClient *common.DuckdbClient, tempDuckdbTableName string, icebergSchemaColumns []*IcebergSchemaColumn, rowCount int64) ParquetFile {
	ctx := context.Background()
	fileName := storage.generateTimestampString() + "_" + uuid.New().String() + ".parquet"
	fileS3Path := dataS3Path + "/" + fileName
	fileS3Key := storage.S3Client.ObjectKey(fileS3Path)

	storage.StorageUtils.WriteParquetFile(fileS3Path, duckdbClient, tempDuckdbTableName, icebergSchemaColumns)

	headObjectOutput := storage.S3Client.HeadObject(fileS3Key)
	fileSize := *headObjectOutput.ContentLength

	fileReader, err := s3v2.NewS3FileReaderWithClient(ctx, storage.S3Client.S3, storage.Config.Aws.S3Bucket, fileS3Key)
	common.PanicIfError(storage.Config, err)
	parquetStats := storage.StorageUtils.ReadParquetStats(fileReader, icebergSchemaColumns)

	return ParquetFile{
		Key:         fileS3Key,
		Path:        dataS3Path + "/" + fileName,
		Size:        fileSize,
		RecordCount: rowCount,
		Stats:       parquetStats,
	}
}

func (storage *StorageS3) CreateManifest(metadataS3Path string, parquetFilesSortedAsc []ParquetFile) (manifestFile ManifestFile) {
	fileName := storage.generateTimestampString() + "_" + uuid.New().String() + "-m0.avro"
	fileS3Key := storage.S3Client.ObjectKey(metadataS3Path) + "/" + fileName

	var fileSize int64
	storage.uploadTemporaryFile("manifest", fileS3Key, func(tempFile *os.File) {
		var err error
		fileSize, err = storage.StorageUtils.WriteManifestFile(tempFile.Name(), parquetFilesSortedAsc)
		common.PanicIfError(storage.Config, err)
	})
	common.LogDebug(storage.Config, "Manifest file created at:", fileS3Key)

	var totalRecordCount int64
	for _, parquetFile := range parquetFilesSortedAsc {
		totalRecordCount += parquetFile.RecordCount
	}

	return ManifestFile{
		Key:                fileS3Key,
		Path:               metadataS3Path + "/" + fileName,
		Size:               fileSize,
		TotalDataFileCount: int32(len(parquetFilesSortedAsc)),
		TotalRecordCount:   totalRecordCount,
		RecordsDeleted:     false,
	}
}

func (storage *StorageS3) CreateManifestList(metadataS3Path string, totalDataFileSize int64, manifestListItemsSortedDesc []ManifestListItem) (manifestListFile ManifestListFile) {
	fileName := "snap-" + storage.generateTimestampString() + "_" + uuid.New().String() + ".avro"
	fileS3Key := storage.S3Client.ObjectKey(metadataS3Path) + "/" + fileName

	storage.uploadTemporaryFile("manifest-list", fileS3Key, func(tempFile *os.File) {
		var err error
		manifestListFile, err = storage.StorageUtils.WriteManifestListFile(tempFile.Name(), totalDataFileSize, manifestListItemsSortedDesc)
		common.PanicIfError(storage.Config, err)
	})

	common.LogDebug(storage.Config, "Manifest list file created at:", fileS3Key)
	manifestListFile.Key = fileS3Key
	manifestListFile.Path = metadataS3Path + "/" + fileName
	return manifestListFile
}

func (storage *StorageS3) CreateMetadata(metadataS3Path string, icebergSchemaColumns []*IcebergSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (metadataFile MetadataFile) {
	fileS3Key := storage.S3Client.ObjectKey(metadataS3Path) + "/" + ICEBERG_METADATA_INITIAL_FILE_NAME

	storage.uploadTemporaryFile("metadata", fileS3Key, func(tempFile *os.File) {
		tableS3Path := strings.TrimSuffix(metadataS3Path, "/metadata")
		err := storage.StorageUtils.WriteMetadataFile(tableS3Path, tempFile.Name(), icebergSchemaColumns, manifestListFilesSortedAsc)
		common.PanicIfError(storage.Config, err)
	})

	common.LogDebug(storage.Config, "Metadata file created at:", fileS3Key)
	return MetadataFile{Version: 1, Key: fileS3Key}
}

func (storage *StorageS3) DeleteTableFiles(tableS3Path string) {
	tableS3Key := storage.S3Client.ObjectKey(tableS3Path)
	storage.deleteNestedObjects(tableS3Key)
}

// Read ----------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) LastManifestListFile(metadataS3Path string) ManifestListFile {
	metadataContent := storage.readObjectContent(metadataS3Path)
	return storage.StorageUtils.ParseLastManifestListFile(storage.S3Client.BucketS3Prefix(), metadataContent)
}

func (storage *StorageS3) ManifestListItems(manifestListFile ManifestListFile) []ManifestListItem {
	manifestListContent := storage.readObjectContent(manifestListFile.Path)
	return storage.StorageUtils.ParseManifestListItems(storage.S3Client.BucketS3Prefix(), manifestListContent)
}

func (storage *StorageS3) ParquetFiles(manifestFile ManifestFile, icebergSchemaColumns []*IcebergSchemaColumn) []ParquetFile {
	ctx := context.Background()
	manifestContent := storage.readObjectContent(manifestFile.Path)
	parquetFilesSortedAsc := storage.StorageUtils.ParseParquetFiles(manifestContent)

	for i, parquetFile := range parquetFilesSortedAsc {
		fileS3Key := storage.S3Client.ObjectKey(parquetFile.Path)

		fileReader, err := s3v2.NewS3FileReaderWithClient(ctx, storage.S3Client.S3, storage.Config.Aws.S3Bucket, fileS3Key)
		common.PanicIfError(storage.Config, err)
		parquetStats := storage.StorageUtils.ReadParquetStats(fileReader, icebergSchemaColumns)

		parquetFilesSortedAsc[i].Key = fileS3Key
		parquetFilesSortedAsc[i].Stats = parquetStats
	}

	return parquetFilesSortedAsc
}

// ---------------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) generateTimestampString() string {
	now := time.Now()
	return fmt.Sprintf("%s_%06d", now.Format("20060102_150405"), now.Nanosecond()/1000)
}

func (storage *StorageS3) uploadTemporaryFile(tempFilePattern string, uploadFileS3Key string, writeTempFileFunc func(*os.File)) {
	tempFile, err := os.CreateTemp("", tempFilePattern)
	common.PanicIfError(storage.Config, err)

	defer func() {
		os.Remove(tempFile.Name())
	}()

	writeTempFileFunc(tempFile)

	storage.S3Client.UploadObject(uploadFileS3Key, tempFile)

	err = tempFile.Close()
	common.PanicIfError(storage.Config, err)
}

func (storage *StorageS3) readObjectContent(fileS3Path string) []byte {
	fileS3Key := storage.S3Client.ObjectKey(fileS3Path)
	getObjectResponse := storage.S3Client.GetObject(fileS3Key)

	fileContent, err := io.ReadAll(getObjectResponse.Body)
	common.PanicIfError(storage.Config, err)

	return fileContent
}

func (storage *StorageS3) deleteNestedObjects(prefixS3Key string) {
	listResponse := storage.S3Client.ListObjects(prefixS3Key)

	var fileS3Keys []*string
	for _, obj := range listResponse.Contents {
		common.LogDebug(storage.Config, "Object to delete:", *obj.Key)
		fileS3Keys = append(fileS3Keys, obj.Key)
	}

	if len(fileS3Keys) > 0 {
		storage.S3Client.DeleteObjects(fileS3Keys)
		common.LogDebug(storage.Config, "Deleted", len(fileS3Keys), "object(s).")
	} else {
		common.LogDebug(storage.Config, "No objects to delete.")
	}
}
