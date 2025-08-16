package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type IcebergReader struct {
	Config  *Config
	Storage *StorageS3
	Catalog *IcebergCatalog
}

func NewIcebergReader(config *Config, catalog *IcebergCatalog) *IcebergReader {
	return &IcebergReader{
		Config:  config,
		Catalog: catalog,
		Storage: NewS3Storage(config),
	}
}

func (reader *IcebergReader) Schemas() (icebergSchemas []string, err error) {
	return reader.Catalog.Schemas()
}

func (reader *IcebergReader) SchemaTables() (icebergSchemaTables common.Set[IcebergSchemaTable], err error) {
	return reader.Catalog.SchemaTables()
}

func (reader *IcebergReader) TableFields(icebergSchemaTable IcebergSchemaTable) (icebergTableFields []IcebergTableField, err error) {
	metadataPath := reader.MetadataFilePath(icebergSchemaTable)
	common.LogDebug(reader.Config.CommonConfig, "Reading Iceberg table "+icebergSchemaTable.String()+" fields from "+metadataPath+" ...")
	return reader.Storage.IcebergTableFields(metadataPath)
}

func (reader *IcebergReader) MetadataFilePath(icebergSchemaTable IcebergSchemaTable) string {
	return reader.Catalog.MetadataFilePath(icebergSchemaTable)
}
