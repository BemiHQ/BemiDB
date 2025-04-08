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
	s3Client     *s3.Client
	config       *Config
	storageUtils *StorageUtils
}

func NewS3Storage(config *Config) *StorageS3 {
	var awsConfigOptions = []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(config.Aws.Region),
	}

	if config.LogLevel == LOG_LEVEL_TRACE {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithClientLogMode(aws.LogRequest))
	}

	if config.Aws.AccessKeyId != "" && config.Aws.SecretAccessKey != "" {
		awsCredentials := credentials.NewStaticCredentialsProvider(
			config.Aws.AccessKeyId,
			config.Aws.SecretAccessKey,
			"",
		)
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithCredentialsProvider(awsCredentials))
	}

	loadedAwsConfig, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfigOptions...)
	PanicIfError(config, err)

	return &StorageS3{
		s3Client:     s3.NewFromConfig(loadedAwsConfig),
		config:       config,
		storageUtils: &StorageUtils{config: config},
	}
}

// Read ----------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) IcebergMetadataFilePath(icebergSchemaTable IcebergSchemaTable) string {
	return storage.fullBucketPath() + storage.tablePrefix(icebergSchemaTable, true) + "metadata/" + ICEBERG_METADATA_FILE_NAME
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
	metadataPath := storage.tablePrefix(icebergSchemaTable, true) + "metadata/" + ICEBERG_METADATA_FILE_NAME
	metadataContent, err := storage.readFileContent(metadataPath)
	if err != nil {
		return nil, err
	}

	return storage.storageUtils.ParseIcebergTableFields(metadataContent)
}

func (storage *StorageS3) ExistingManifestListFiles(metadataDirPath string) ([]ManifestListFile, error) {
	metadataPath := metadataDirPath + "/" + ICEBERG_METADATA_FILE_NAME
	metadataContent, err := storage.readFileContent(metadataPath)
	if err != nil {
		return nil, err
	}

	return storage.storageUtils.ParseManifestListFiles(storage.fullBucketPath(), metadataContent)
}

func (storage *StorageS3) ExistingManifestListItems(manifestListFile ManifestListFile) ([]ManifestListItem, error) {
	manifestListContent, err := storage.readFileContent(manifestListFile.Path)
	if err != nil {
		return nil, err
	}

	return storage.storageUtils.ParseManifestFiles(storage.fullBucketPath(), manifestListContent)
}

func (storage *StorageS3) ExistingParquetFilePath(manifestFile ManifestFile) (string, error) {
	manifestListContent, err := storage.readFileContent(manifestFile.Path)
	if err != nil {
		return "", err
	}

	return storage.storageUtils.ParseParquetFilePath(storage.fullBucketPath(), manifestListContent)
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

func (storage *StorageS3) CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, maxPayloadThreshold int, loadRows func() ([][]string, InternalTableMetadata)) (parquetFile ParquetFile, internalTableMetadata InternalTableMetadata, err error) {
	ctx := context.Background()
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	fileKey := dataDirPath + "/" + fileName

	fileWriter, err := s3v2.NewS3FileWriterWithClient(ctx, storage.s3Client, storage.config.Aws.S3Bucket, fileKey, nil)
	if err != nil {
		return parquetFile, internalTableMetadata, fmt.Errorf("failed to open Parquet file for writing: %v", err)
	}

	var recordCount int64
	recordCount, internalTableMetadata, err = storage.storageUtils.WriteParquetFile(fileWriter, pgSchemaColumns, maxPayloadThreshold, loadRows)
	if err != nil {
		return parquetFile, internalTableMetadata, err
	}
	LogDebug(storage.config, "Parquet file with", recordCount, "record(s) created at:", fileKey)

	headObjectResponse, err := storage.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
	})
	if err != nil {
		return parquetFile, internalTableMetadata, fmt.Errorf("failed to get Parquet file info: %v", err)
	}
	fileSize := *headObjectResponse.ContentLength

	fileReader, err := s3v2.NewS3FileReaderWithClient(ctx, storage.s3Client, storage.config.Aws.S3Bucket, fileKey)
	if err != nil {
		return parquetFile, internalTableMetadata, fmt.Errorf("failed to open Parquet file for reading: %v", err)
	}
	parquetStats, err := storage.storageUtils.ReadParquetStats(fileReader)
	if err != nil {
		return parquetFile, internalTableMetadata, err
	}

	return ParquetFile{
		Uuid:        uuid,
		Path:        fileKey,
		Size:        fileSize,
		RecordCount: recordCount,
		Stats:       parquetStats,
	}, internalTableMetadata, nil
}

func (storage *StorageS3) CreateOverwrittenParquet(dataDirPath string, existingParquetFilePath string, newParquetFilePath string, pgSchemaColumns []PgSchemaColumn, dynamicRowCountPerBatch int) (overwrittenParquetFile ParquetFile, err error) {
	ctx := context.Background()
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	fileKey := dataDirPath + "/" + fileName

	fileWriter, err := s3v2.NewS3FileWriterWithClient(ctx, storage.s3Client, storage.config.Aws.S3Bucket, fileKey, nil)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("failed to open Parquet file for writing: %v", err)
	}

	duckdb, err := storage.storageUtils.NewDuckDBIfHasOverlappingRows(storage.fullBucketPath(), existingParquetFilePath, newParquetFilePath, pgSchemaColumns)
	if err != nil {
		return ParquetFile{}, err
	}
	if duckdb == nil {
		fileWriter.Close()
		storage.DeleteParquet(ParquetFile{Path: fileKey})
		return ParquetFile{}, nil
	}
	defer duckdb.Close()

	recordCount, err := storage.storageUtils.WriteOverwrittenParquetFile(duckdb, fileWriter, pgSchemaColumns, dynamicRowCountPerBatch)
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
	parquetStats, err := storage.storageUtils.ReadParquetStats(fileReader)
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

func (storage *StorageS3) DeleteParquet(parquetFile ParquetFile) (err error) {
	ctx := context.Background()
	_, err = storage.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(parquetFile.Path),
	})
	return err
}

func (storage *StorageS3) CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m0.avro", parquetFile.Uuid)
	filePath := metadataDirPath + "/" + fileName

	tempFile, err := storage.createTemporaryFile("manifest")
	if err != nil {
		return ManifestFile{}, err
	}
	defer storage.deleteTemporaryFile(tempFile)

	manifestFile, err = storage.storageUtils.WriteManifestFile(storage.fullBucketPath(), tempFile.Name(), parquetFile)
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

func (storage *StorageS3) CreateDeletedRecordsManifest(metadataDirPath string, uuid string, existingManifestFile ManifestFile) (deletedRecsManifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m1.avro", uuid)
	filePath := metadataDirPath + "/" + fileName

	existingManifestContent, err := storage.readFileContent(existingManifestFile.Path)
	if err != nil {
		return ManifestFile{}, err
	}

	tempFile, err := storage.createTemporaryFile("manifest")
	if err != nil {
		return ManifestFile{}, err
	}
	defer storage.deleteTemporaryFile(tempFile)

	deletedRecsManifestFile, err = storage.storageUtils.WriteDeletedRecordsManifestFile(storage.fullBucketPath(), tempFile.Name(), existingManifestContent)
	if err != nil {
		return ManifestFile{}, err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return ManifestFile{}, err
	}
	LogDebug(storage.config, "Manifest file created at:", filePath)

	return deletedRecsManifestFile, nil
}

func (storage *StorageS3) CreateManifestList(metadataDirPath string, parquetFileUuid string, manifestListItemsSortedDesc []ManifestListItem) (manifestListFile ManifestListFile, err error) {
	fileName := fmt.Sprintf("snap-%d-0-%s.avro", manifestListItemsSortedDesc[0].ManifestFile.SnapshotId, parquetFileUuid)
	filePath := metadataDirPath + "/" + fileName

	tempFile, err := storage.createTemporaryFile("manifest")
	if err != nil {
		return ManifestListFile{}, err
	}
	defer storage.deleteTemporaryFile(tempFile)

	manifestListFile, err = storage.storageUtils.WriteManifestListFile(storage.fullBucketPath(), tempFile.Name(), manifestListItemsSortedDesc)
	if err != nil {
		return ManifestListFile{}, err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return ManifestListFile{}, err
	}
	LogDebug(storage.config, "Manifest list file created at:", filePath)

	manifestListFile.Path = filePath
	return manifestListFile, nil
}

func (storage *StorageS3) CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, manifestListFilesSortedAsc []ManifestListFile) (metadataFile MetadataFile, err error) {
	filePath := metadataDirPath + "/" + ICEBERG_METADATA_FILE_NAME

	tempFile, err := storage.createTemporaryFile("manifest")
	if err != nil {
		return MetadataFile{}, err
	}
	defer storage.deleteTemporaryFile(tempFile)

	err = storage.storageUtils.WriteMetadataFile(storage.fullBucketPath(), tempFile.Name(), pgSchemaColumns, manifestListFilesSortedAsc)
	if err != nil {
		return MetadataFile{}, err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return MetadataFile{}, err
	}
	LogDebug(storage.config, "Metadata file created at:", filePath)

	return MetadataFile{Version: 1, Path: filePath}, nil
}

// Read (internal) -----------------------------------------------------------------------------------------------------

func (storage *StorageS3) InternalTableMetadata(pgSchemaTable PgSchemaTable) (InternalTableMetadata, error) {
	internalMetadataPath := storage.internalTableMetadataFilePath(pgSchemaTable)
	internalMetadataContent, err := storage.readFileContent(internalMetadataPath)
	if err != nil {
		return InternalTableMetadata{}, err
	}

	return storage.storageUtils.ParseInternalTableMetadata(internalMetadataContent)
}

// Write (internal) ----------------------------------------------------------------------------------------------------

func (storage *StorageS3) WriteInternalTableMetadata(metadataDirPath string, internalTableMetadata InternalTableMetadata) error {
	filePath := metadataDirPath + "/" + INTERNAL_METADATA_FILE_NAME

	tempFile, err := storage.createTemporaryFile("internal-metadata")
	if err != nil {
		return err
	}
	defer storage.deleteTemporaryFile(tempFile)

	err = storage.storageUtils.WriteInternalTableMetadataFile(tempFile.Name(), internalTableMetadata)
	if err != nil {
		return err
	}

	err = storage.uploadFile(filePath, tempFile)
	if err != nil {
		return err
	}
	LogDebug(storage.config, "Internal metadata file created at:", filePath)

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (storage *StorageS3) readFileContent(filePath string) ([]byte, error) {
	ctx := context.Background()
	getObjectResponse, err := storage.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(filePath),
	})
	if err != nil {
		return nil, err
	}

	fileContent, err := io.ReadAll(getObjectResponse.Body)
	if err != nil {
		return nil, err
	}

	return fileContent, nil
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

func (storage *StorageS3) internalTableMetadataFilePath(pgSchemaTable PgSchemaTable) string {
	return storage.tablePrefix(pgSchemaTable.ToIcebergSchemaTable()) + "metadata/" + INTERNAL_METADATA_FILE_NAME
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

func (storage *StorageS3) createTemporaryFile(prefix string) (file *os.File, err error) {
	tempFile, err := os.CreateTemp("", prefix)
	PanicIfError(storage.config, err)

	return tempFile, nil
}

func (storage *StorageS3) deleteTemporaryFile(file *os.File) {
	os.Remove(file.Name())
}
