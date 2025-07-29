package common

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	goDuckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/xitongsys/parquet-go-source/s3v2"
)

type StorageS3 struct {
	S3Client     *s3.Client
	Config       *BaseConfig
	StorageUtils *StorageUtils
	Duckdb       *Duckdb
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

func NewStorageS3(Config *BaseConfig) *StorageS3 {
	var awsConfigOptions = []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(Config.Aws.Region),
	}

	if Config.LogLevel == LOG_LEVEL_TRACE {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithClientLogMode(aws.LogRequest))
	}

	if IsLocalHost(Config.Aws.S3Endpoint) {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithBaseEndpoint("http://"+Config.Aws.S3Endpoint))
	} else {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithBaseEndpoint("https://"+Config.Aws.S3Endpoint))
	}

	awsCredentials := credentials.NewStaticCredentialsProvider(
		Config.Aws.AccessKeyId,
		Config.Aws.SecretAccessKey,
		"",
	)
	awsConfigOptions = append(awsConfigOptions, awsConfig.WithCredentialsProvider(awsCredentials))

	loadedAwsConfig, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfigOptions...)
	PanicIfError(Config, err)

	S3Client := s3.NewFromConfig(loadedAwsConfig, func(o *s3.Options) {
		if Config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
			o.UsePathStyle = true
		}
	})

	return &StorageS3{
		S3Client:     S3Client,
		Config:       Config,
		StorageUtils: NewStorageUtils(Config),
		Duckdb:       NewDuckdb(Config),
	}
}

// Write ---------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) CreateParquet(s3DataPath string, icebergSchemaColumns []*IcebergSchemaColumn, loadRows func(appender *goDuckdb.Appender) (rowCount int, reachedEnd bool)) (parquetFile ParquetFile, reachedEnd bool) {
	ctx := context.Background()
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	fileS3Path := s3DataPath + "/" + fileName
	fileKey := storage.s3Key(fileS3Path)

	rowCount, reachedEnd := storage.StorageUtils.WriteParquetFile(storage.Duckdb, fileS3Path, icebergSchemaColumns, loadRows)
	LogDebug(storage.Config, "Parquet file with", rowCount, "record(s) created at:", fileKey)

	headObjectResponse, err := storage.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(storage.Config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
	})
	PanicIfError(storage.Config, err)
	fileSize := *headObjectResponse.ContentLength

	fileReader, err := s3v2.NewS3FileReaderWithClient(ctx, storage.S3Client, storage.Config.Aws.S3Bucket, fileKey)
	PanicIfError(storage.Config, err)
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
	ctx := context.Background()
	_, err := storage.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(storage.Config.Aws.S3Bucket),
		Key:    aws.String(parquetFile.Key),
	})
	PanicIfError(storage.Config, err)
}

func (storage *StorageS3) CreateManifest(s3MetadataPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m0.avro", parquetFile.Uuid)
	fileKey := storage.s3Key(s3MetadataPath) + "/" + fileName

	err = storage.uploadTemporaryFile("manifest", fileKey, func(tempFile *os.File) error {
		manifestFile, err = storage.StorageUtils.WriteManifestFile(tempFile.Name(), parquetFile)
		return err
	})
	if err != nil {
		return manifestFile, err
	}
	LogDebug(storage.Config, "Manifest file created at:", fileKey)

	manifestFile.Key = fileKey
	manifestFile.Path = s3MetadataPath + "/" + fileName
	return manifestFile, nil
}

func (storage *StorageS3) CreateManifestList(s3MetadataPath string, parquetFileUuid string, manifestListItemsSortedDesc []ManifestListItem) (manifestListFile ManifestListFile, err error) {
	fileName := fmt.Sprintf("snap-%d-0-%s.avro", manifestListItemsSortedDesc[0].ManifestFile.SnapshotId, parquetFileUuid)
	fileKey := storage.s3Key(s3MetadataPath) + "/" + fileName

	err = storage.uploadTemporaryFile("manifest-list", fileKey, func(tempFile *os.File) error {
		manifestListFile, err = storage.StorageUtils.WriteManifestListFile(tempFile.Name(), manifestListItemsSortedDesc)
		return err
	})
	if err != nil {
		return manifestListFile, err
	}
	LogDebug(storage.Config, "Manifest list file created at:", fileKey)

	manifestListFile.Key = fileKey
	manifestListFile.Path = s3MetadataPath + "/" + fileName
	return manifestListFile, nil
}

func (storage *StorageS3) CreateMetadata(s3MetadataPath string, icebergSchemaColumns []*IcebergSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (metadataFile MetadataFile, err error) {
	fileKey := storage.s3Key(s3MetadataPath) + "/" + ICEBERG_METADATA_INITIAL_FILE_NAME

	err = storage.uploadTemporaryFile("metadata", fileKey, func(tempFile *os.File) error {
		s3TablePath := strings.TrimSuffix(s3MetadataPath, "/metadata")
		return storage.StorageUtils.WriteMetadataFile(s3TablePath, tempFile.Name(), icebergSchemaColumns, manifestListFilesSortedAsc)
	})
	if err != nil {
		return metadataFile, err
	}
	LogDebug(storage.Config, "Metadata file created at:", fileKey)

	return MetadataFile{Version: 1, Key: fileKey}, nil
}

func (storage *StorageS3) DeleteTableFiles(s3TablePath string) (err error) {
	tableKey := storage.s3Key(s3TablePath)
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

	uploader := manager.NewUploader(storage.S3Client)
	_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(storage.Config.Aws.S3Bucket),
		Key:    aws.String(uploadFilePath),
		Body:   tempFile,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	err = tempFile.Close()
	if err != nil {
		return err
	}

	return nil
}

// s3://bucket/some/path -> some/path
func (storage *StorageS3) s3Key(s3Path string) string {
	return strings.Split(s3Path, "s3://"+storage.Config.Aws.S3Bucket+"/")[1]
}

func (storage *StorageS3) deleteNestedObjects(prefixKey string) (err error) {
	ctx := context.Background()

	listResponse, err := storage.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(storage.Config.Aws.S3Bucket),
		Prefix: aws.String(prefixKey),
	})
	if err != nil {
		return fmt.Errorf("failed to list objects: %v", err)
	}

	var objectsToDelete []types.ObjectIdentifier
	for _, obj := range listResponse.Contents {
		LogDebug(storage.Config, "Object to delete:", *obj.Key)
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: obj.Key})
	}

	if len(objectsToDelete) > 0 {
		_, err = storage.S3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(storage.Config.Aws.S3Bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %v", err)
		}
		LogDebug(storage.Config, "Deleted", len(objectsToDelete), "object(s).")
	} else {
		LogDebug(storage.Config, "No objects to delete.")
	}

	return nil
}
