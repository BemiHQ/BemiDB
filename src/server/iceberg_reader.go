package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type IcebergReader struct {
	Config         *Config
	StorageS3      *common.StorageS3
	IcebergCatalog *common.IcebergCatalog
}

func NewIcebergReader(config *Config, storageS3 *common.StorageS3, icebergCatalog *common.IcebergCatalog) *IcebergReader {
	return &IcebergReader{
		Config:         config,
		StorageS3:      storageS3,
		IcebergCatalog: icebergCatalog,
	}
}

func (reader *IcebergReader) Schemas() (icebergSchemas []string, err error) {
	return reader.IcebergCatalog.Schemas()
}

func (reader *IcebergReader) SchemaTables() (icebergSchemaTables common.Set[common.IcebergSchemaTable], err error) {
	return reader.IcebergCatalog.SchemaTables()
}

func (reader *IcebergReader) MaterializedViews() (icebergSchemaTables []common.IcebergMaterializedView, err error) {
	return reader.IcebergCatalog.MaterializedViews()
}

func (reader *IcebergReader) MaterializedView(icebergSchemaTable common.IcebergSchemaTable) (icebergMaterializedView common.IcebergMaterializedView, err error) {
	return reader.IcebergCatalog.MaterializedView(icebergSchemaTable)
}

func (reader *IcebergReader) TableFields(icebergSchemaTable common.IcebergSchemaTable) (icebergTableFields []common.IcebergTableField, err error) {
	metadataPath := reader.MetadataFileS3Path(icebergSchemaTable)
	common.LogDebug(reader.Config.CommonConfig, "Reading Iceberg table "+icebergSchemaTable.String()+" fields from "+metadataPath+" ...")
	return reader.StorageS3.IcebergTableFields(metadataPath)
}

func (reader *IcebergReader) MetadataFileS3Path(icebergSchemaTable common.IcebergSchemaTable) string {
	return reader.IcebergCatalog.MetadataFileS3Path(icebergSchemaTable)
}
