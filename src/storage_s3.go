package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/s3v2"
)

type StorageS3 struct {
	s3Client    *s3.Client
	config      *Config
	storageBase *StorageBase
}

func NewS3Storage(config *Config) *StorageS3 {
	awsCredentials := credentials.NewStaticCredentialsProvider(
		config.Aws.AccessKeyId,
		config.Aws.SecretAccessKey,
		"",
	)

	var logMode aws.ClientLogMode
	// if config.LogLevel == LOG_LEVEL_DEBUG {
	// 	logMode = aws.LogRequest | aws.LogResponse
	// }

	loadedAwsConfig, err := awsConfig.LoadDefaultConfig(
		context.Background(),
		awsConfig.WithRegion(config.Aws.Region),
		awsConfig.WithCredentialsProvider(awsCredentials),
		awsConfig.WithClientLogMode(logMode),
	)
	PanicIfError(err, config)

	return &StorageS3{
		s3Client:    s3.NewFromConfig(loadedAwsConfig),
		config:      config,
		storageBase: &StorageBase{config: config},
	}
}

// Read ----------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) IcebergMetadataFilePath(icebergSchemaTable IcebergSchemaTable) string {
	return storage.fullBucketPath() + storage.tablePrefix(icebergSchemaTable, true) + "metadata/v1.metadata.json"
}

func (storage *StorageS3) IcebergSchemas() (icebergSchemas []string, err error) {
	schemasPrefix := storage.config.StoragePath + "/"
	icebergSchemas, err = storage.nestedDirectoryPrefixes(schemasPrefix)
	if err != nil {
		return nil, err
	}

	for i, schema := range icebergSchemas {
		schemaParts := strings.Split(schema, "/")
		icebergSchemas[i] = schemaParts[len(schemaParts)-2]
	}

	return icebergSchemas, nil
}

func (storage *StorageS3) IcebergSchemaTables() (Set[IcebergSchemaTable], error) {
	icebergSchemaTables := make(Set[IcebergSchemaTable])
	icebergSchemas, err := storage.IcebergSchemas()
	if err != nil {
		return nil, err
	}

	for _, icebergSchema := range icebergSchemas {
		tables, err := storage.nestedDirectoryPrefixes(storage.config.StoragePath + "/" + icebergSchema + "/")
		if err != nil {
			return nil, err
		}

		for _, tablePrefix := range tables {
			tableParts := strings.Split(tablePrefix, "/")
			table := tableParts[len(tableParts)-2]

			icebergSchemaTables.Add(IcebergSchemaTable{Schema: icebergSchema, Table: table})
		}
	}

	return icebergSchemaTables, nil
}

func (storage *StorageS3) IcebergTableFields(icebergSchemaTable IcebergSchemaTable) ([]IcebergTableField, error) {
	metadataPath := storage.tablePrefix(icebergSchemaTable, true) + "metadata/v1.metadata.json"

	ctx := context.Background()
	getObjectResponse, err := storage.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(metadataPath),
	})
	if err != nil {
		return nil, err
	}

	metadataContent, err := io.ReadAll(getObjectResponse.Body)
	if err != nil {
		return nil, err
	}

	return storage.storageBase.ParseIcebergTableFields(metadataContent)
}

// Write ---------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) DeleteSchema(schema string) (err error) {
	return storage.deleteNestedObjects(storage.config.StoragePath + "/" + schema + "/")
}

func (storage *StorageS3) DeleteSchemaTable(schemaTable IcebergSchemaTable) (err error) {
	tablePrefix := storage.tablePrefix(schemaTable)
	return storage.deleteNestedObjects(tablePrefix)
}

func (storage *StorageS3) CreateDataDir(schemaTable IcebergSchemaTable) (dataDirPath string) {
	tablePrefix := storage.tablePrefix(schemaTable)
	return tablePrefix + "data"
}

func (storage *StorageS3) CreateMetadataDir(schemaTable IcebergSchemaTable) (metadataDirPath string) {
	tablePrefix := storage.tablePrefix(schemaTable)
	return tablePrefix + "metadata"
}

func (storage *StorageS3) CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, loadRows func() [][]string) (parquetFile ParquetFile, err error) {
	ctx := context.Background()
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	fileKey := dataDirPath + "/" + fileName

	fileWriter, err := s3v2.NewS3FileWriterWithClient(ctx, storage.s3Client, storage.config.Aws.S3Bucket, fileKey, nil)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for writing: %v", err)
	}

	recordCount, err := storage.storageBase.WriteParquetFile(fileWriter, pgSchemaColumns, loadRows)
	if err != nil {
		return ParquetFile{}, err
	}
	LogDebug(storage.config, "Parquet file with", recordCount, "record(s) created at:", fileKey)

	headObjectResponse, err := storage.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
	})
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to get Parquet file info: %v", err)
	}
	fileSize := *headObjectResponse.ContentLength

	fileReader, err := s3v2.NewS3FileReaderWithClient(ctx, storage.s3Client, storage.config.Aws.S3Bucket, fileKey)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for reading: %v", err)
	}
	parquetStats, err := storage.storageBase.ReadParquetStats(fileReader)
	if err != nil {
		return ParquetFile{}, err
	}

	return ParquetFile{
		Uuid:        uuid,
		Path:        fileKey,
		Size:        fileSize,
		RecordCount: recordCount,
		Stats:       parquetStats,
	}, nil
}

func (storage *StorageS3) CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m0.avro", parquetFile.Uuid)
	filePath := metadataDirPath + "/" + fileName

	tempFile, err := CreateTemporaryFile("manifest")
	if err != nil {
		return ManifestFile{}, err
	}
	defer DeleteTemporaryFile(tempFile)

	manifestFile, err = storage.storageBase.WriteManifestFile(storage.fullBucketPath(), tempFile.Name(), parquetFile)
	if err != nil {
		return ManifestFile{}, err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return ManifestFile{}, err
	}
	LogDebug(storage.config, "Manifest file created at:", filePath)

	manifestFile.Path = filePath
	return manifestFile, nil
}

func (storage *StorageS3) CreateManifestList(metadataDirPath string, parquetFile ParquetFile, manifestFile ManifestFile) (manifestListFile ManifestListFile, err error) {
	fileName := fmt.Sprintf("snap-%d-0-%s.avro", manifestFile.SnapshotId, parquetFile.Uuid)
	filePath := metadataDirPath + "/" + fileName

	tempFile, err := CreateTemporaryFile("manifest")
	if err != nil {
		return ManifestListFile{}, err
	}
	defer DeleteTemporaryFile(tempFile)

	err = storage.storageBase.WriteManifestListFile(storage.fullBucketPath(), tempFile.Name(), parquetFile, manifestFile)
	if err != nil {
		return ManifestListFile{}, err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return ManifestListFile{}, err
	}
	LogDebug(storage.config, "Manifest list file created at:", filePath)

	return ManifestListFile{Path: filePath}, nil
}

func (storage *StorageS3) CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, parquetFile ParquetFile, manifestFile ManifestFile, manifestListFile ManifestListFile) (metadataFile MetadataFile, err error) {
	version := int64(1)
	fileName := fmt.Sprintf("v%d.metadata.json", version)
	filePath := metadataDirPath + "/" + fileName

	tempFile, err := CreateTemporaryFile("manifest")
	if err != nil {
		return MetadataFile{}, err
	}
	defer DeleteTemporaryFile(tempFile)

	err = storage.storageBase.WriteMetadataFile(storage.fullBucketPath(), tempFile.Name(), pgSchemaColumns, parquetFile, manifestFile, manifestListFile)
	if err != nil {
		return MetadataFile{}, err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return MetadataFile{}, err
	}
	LogDebug(storage.config, "Metadata file created at:", filePath)

	return MetadataFile{Version: version, Path: filePath}, nil
}

func (storage *StorageS3) CreateVersionHint(metadataDirPath string, metadataFile MetadataFile) (err error) {
	filePath := metadataDirPath + "/" + VERSION_HINT_FILE_NAME

	tempFile, err := CreateTemporaryFile("manifest")
	if err != nil {
		return err
	}
	defer DeleteTemporaryFile(tempFile)

	err = storage.storageBase.WriteVersionHintFile(tempFile.Name(), metadataFile)
	if err != nil {
		return err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return err
	}
	LogDebug(storage.config, "Version hint file created at:", filePath)

	return nil
}

func (storage *StorageS3) uploadFile(filePath string, file *os.File) (err error) {
	uploader := manager.NewUploader(storage.s3Client)

	_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(filePath),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	return nil
}

func (storage *StorageS3) tablePrefix(schemaTable IcebergSchemaTable, isIcebergSchemaTable ...bool) string {
	if len(isIcebergSchemaTable) > 0 && isIcebergSchemaTable[0] {
		return storage.config.StoragePath + "/" + schemaTable.Schema + "/" + schemaTable.Table + "/"
	}

	return storage.config.StoragePath + "/" + storage.config.Pg.SchemaPrefix + schemaTable.Schema + "/" + schemaTable.Table + "/"
}

func (storage *StorageS3) fullBucketPath() string {
	return "s3://" + storage.config.Aws.S3Bucket + "/"
}

func (storage *StorageS3) nestedDirectoryPrefixes(prefix string) (dirs []string, err error) {
	ctx := context.Background()
	listResponse, err := storage.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(storage.config.Aws.S3Bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %v", err)
	}

	for _, prefix := range listResponse.CommonPrefixes {
		dirs = append(dirs, *prefix.Prefix)
	}

	return dirs, nil
}

func (storage *StorageS3) deleteNestedObjects(prefix string) (err error) {
	ctx := context.Background()

	listResponse, err := storage.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return fmt.Errorf("failed to list objects: %v", err)
	}

	var objectsToDelete []types.ObjectIdentifier
	for _, obj := range listResponse.Contents {
		LogDebug(storage.config, "Object to delete:", *obj.Key)
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: obj.Key})
	}

	if len(objectsToDelete) > 0 {
		_, err = storage.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(storage.config.Aws.S3Bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %v", err)
		}
		LogDebug(storage.config, "Deleted", len(objectsToDelete), "object(s).")
	} else {
		LogDebug(storage.config, "No objects to delete.")
	}

	return nil
}
