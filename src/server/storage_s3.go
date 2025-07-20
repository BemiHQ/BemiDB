package main

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type MetadataJson struct {
	Schemas []struct {
		Fields []struct {
			ID       int         `json:"id"`
			Name     string      `json:"name"`
			Type     interface{} `json:"type"`
			Required bool        `json:"required"`
		} `json:"fields"`
	} `json:"schemas"`
}

type StorageS3 struct {
	s3Client *s3.Client
	config   *Config
}

func NewS3Storage(config *Config) *StorageS3 {
	var awsConfigOptions = []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(config.Aws.Region),
	}

	if config.LogLevel == LOG_LEVEL_TRACE {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithClientLogMode(aws.LogRequest))
	}

	if IsLocalHost(config.Aws.S3Endpoint) {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithBaseEndpoint("http://"+config.Aws.S3Endpoint))
	} else {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithBaseEndpoint("https://"+config.Aws.S3Endpoint))
	}

	awsCredentials := credentials.NewStaticCredentialsProvider(
		config.Aws.AccessKeyId,
		config.Aws.SecretAccessKey,
		"",
	)
	awsConfigOptions = append(awsConfigOptions, awsConfig.WithCredentialsProvider(awsCredentials))

	loadedAwsConfig, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfigOptions...)
	PanicIfError(config, err)

	s3Client := s3.NewFromConfig(loadedAwsConfig, func(o *s3.Options) {
		if config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
			o.UsePathStyle = true
		}
	})

	return &StorageS3{
		s3Client: s3Client,
		config:   config,
	}
}

func (storage *StorageS3) IcebergTableFields(metadataPath string) ([]IcebergTableField, error) {
	metadataKey := strings.TrimPrefix(metadataPath, "s3://"+storage.config.Aws.S3Bucket+"/")
	metadataContent, err := storage.readFileContent(metadataKey)
	if err != nil {
		return nil, err
	}

	return storage.parseIcebergTableFields(metadataContent)
}

func (storage *StorageS3) readFileContent(fileKey string) ([]byte, error) {
	ctx := context.Background()
	getObjectResponse, err := storage.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(storage.config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
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

func (storage *StorageS3) parseIcebergTableFields(metadataContent []byte) ([]IcebergTableField, error) {
	var metadataJson MetadataJson
	err := json.Unmarshal(metadataContent, &metadataJson)
	if err != nil {
		return nil, err
	}

	var icebergTableFields []IcebergTableField
	schema := metadataJson.Schemas[len(metadataJson.Schemas)-1] // Get the last schema
	if schema.Fields != nil {
		for _, field := range schema.Fields {
			icebergTableField := IcebergTableField{
				Name: field.Name,
			}

			if reflect.TypeOf(field.Type).Kind() == reflect.String {
				icebergTableField.Type = field.Type.(string)
				icebergTableField.Required = field.Required
			} else {
				listType := field.Type.(map[string]interface{})
				icebergTableField.Type = listType["element"].(string)
				icebergTableField.Required = listType["element-required"].(bool)
				icebergTableField.IsList = true
			}

			icebergTableFields = append(icebergTableFields, icebergTableField)
		}
	}

	return icebergTableFields, nil
}
