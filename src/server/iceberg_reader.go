package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type IcebergReader struct {
	Config         *Config
	IcebergCatalog *common.IcebergCatalog
}

func NewIcebergReader(config *Config, icebergCatalog *common.IcebergCatalog) *IcebergReader {
	return &IcebergReader{
		Config:         config,
		IcebergCatalog: icebergCatalog,
	}
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

func (reader *IcebergReader) TableColumns(icebergSchemaTable common.IcebergSchemaTable) (catalogTableColumns []common.CatalogTableColumn, err error) {
	return reader.IcebergCatalog.TableColumns(icebergSchemaTable)
}

func (reader *IcebergReader) MetadataFileS3Path(icebergSchemaTable common.IcebergSchemaTable) string {
	return reader.IcebergCatalog.MetadataFileS3Path(icebergSchemaTable)
}
