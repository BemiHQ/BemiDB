package main

import (
	"encoding/json"
	"io"
	"reflect"

	"github.com/BemiHQ/BemiDB/src/common"
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
	S3Client *common.S3Client
	Config   *Config
}

func NewS3Storage(config *Config) *StorageS3 {
	s3Client := common.NewS3Client(config.CommonConfig)

	return &StorageS3{
		Config:   config,
		S3Client: s3Client,
	}
}

func (storage *StorageS3) IcebergTableFields(metadataPath string) ([]IcebergTableField, error) {
	metadataKey := storage.S3Client.ObjectKey(metadataPath)
	metadataContent, err := storage.readFileContent(metadataKey)
	if err != nil {
		return nil, err
	}

	return storage.parseIcebergTableFields(metadataContent)
}

func (storage *StorageS3) readFileContent(fileKey string) ([]byte, error) {
	getObjectResponse := storage.S3Client.GetObject(fileKey)

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
