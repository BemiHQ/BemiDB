package main

import (
	"io"
)

type IcebergReader struct {
	config  *Config
	storage StorageInterface
}

func NewIcebergReader(config *Config) *IcebergReader {
	storage := NewStorage(config)
	return &IcebergReader{config: config, storage: storage}
}

func (reader *IcebergReader) Schemas() (icebergSchemas []string, err error) {
	LogDebug(reader.config, "Reading Iceberg schemas...")
	return reader.storage.IcebergSchemas()
}

func (reader *IcebergReader) SchemaTables() (icebergSchemaTables Set[IcebergSchemaTable], err error) {
	LogDebug(reader.config, "Reading Iceberg tables...")
	return reader.storage.IcebergSchemaTables()
}

func (reader *IcebergReader) TableFields(icebergSchemaTable IcebergSchemaTable) (icebergTableFields []IcebergTableField, err error) {
	LogDebug(reader.config, "Reading Iceberg table "+icebergSchemaTable.String()+" fields...")
	return reader.storage.IcebergTableFields(icebergSchemaTable)
}

func (reader *IcebergReader) MetadataFilePath(icebergSchemaTable IcebergSchemaTable) string {
	return reader.storage.IcebergMetadataFilePath(icebergSchemaTable)
}

func (reader *IcebergReader) InternalStartSqlFile() io.ReadCloser {
	return reader.storage.InternalStartSqlFile()
}
