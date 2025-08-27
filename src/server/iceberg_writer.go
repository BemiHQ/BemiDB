package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type IcebergWriter struct {
	Config             *Config
	StorageS3          *common.StorageS3
	ServerDuckdbClient *common.DuckdbClient
	IcebergCatalog     *common.IcebergCatalog
}

func NewIcebergWriter(config *Config, storageS3 *common.StorageS3, serverDuckdbClient *common.DuckdbClient, icebergCatalog *common.IcebergCatalog) *IcebergWriter {
	return &IcebergWriter{
		Config:             config,
		StorageS3:          storageS3,
		ServerDuckdbClient: serverDuckdbClient,
		IcebergCatalog:     icebergCatalog,
	}
}

func (writer *IcebergWriter) CreateMaterializedView(icebergSchemaTable common.IcebergSchemaTable, remappedDefinitionQuery string, ifNotExists bool) error {
	return writer.IcebergCatalog.CreateMaterializedView(icebergSchemaTable, remappedDefinitionQuery, ifNotExists)
}

func (writer *IcebergWriter) RenameMaterializedView(icebergSchemaTable common.IcebergSchemaTable, newName string, missingOk bool) error {
	err := writer.IcebergCatalog.RenameMaterializedView(icebergSchemaTable, newName, missingOk)
	if err != nil {
		return err
	}

	icebergTable := common.NewIcebergTable(writer.Config.CommonConfig, writer.StorageS3, writer.ServerDuckdbClient, icebergSchemaTable)
	icebergTable.Rename(newName)
	return nil
}

func (writer *IcebergWriter) RefreshMaterializedView(icebergSchemaTable common.IcebergSchemaTable, remappedDefinitionQuery string) error {
	// Delete -syncing table
	syncingIcebergSchemaTable := common.IcebergSchemaTable{Schema: icebergSchemaTable.Schema, Table: icebergSchemaTable.Table + common.TEMP_TABLE_SUFFIX_SYNCING}
	syncingIcebergTable := common.NewIcebergTable(writer.Config.CommonConfig, writer.StorageS3, writer.ServerDuckdbClient, syncingIcebergSchemaTable)
	syncingIcebergTable.DropIfExists()

	// Insert and create -syncing table
	icebergTableWriter := common.NewIcebergTableWriter(
		writer.Config.CommonConfig,
		writer.StorageS3,
		writer.ServerDuckdbClient,
		syncingIcebergTable,
		[]*common.IcebergSchemaColumn{},
		1,
	)
	err := icebergTableWriter.InsertFromQuery(remappedDefinitionQuery)
	if err != nil {
		return err
	}

	// Delete -deleting table
	deletingIcebergSchemaTable := common.IcebergSchemaTable{Schema: icebergSchemaTable.Schema, Table: icebergSchemaTable.Table + common.TEMP_TABLE_SUFFIX_DELETING}
	deletingIcebergTable := common.NewIcebergTable(writer.Config.CommonConfig, writer.StorageS3, writer.ServerDuckdbClient, deletingIcebergSchemaTable)
	deletingIcebergTable.DropIfExists()

	// Rename table to -deleting
	icebergTable := common.NewIcebergTable(writer.Config.CommonConfig, writer.StorageS3, writer.ServerDuckdbClient, icebergSchemaTable)
	icebergTable.Rename(deletingIcebergSchemaTable.Table)

	// Rename -syncing to table
	syncingIcebergTable.Rename(icebergSchemaTable.Table)

	// Delete -deleting table
	deletingIcebergTable.DropIfExists()

	return nil
}

func (writer *IcebergWriter) DropMaterializedView(icebergSchemaTable common.IcebergSchemaTable, missingOk bool) error {
	err := writer.IcebergCatalog.DropMaterializedView(icebergSchemaTable, missingOk)
	if err != nil {
		return err
	}

	icebergTable := common.NewIcebergTable(writer.Config.CommonConfig, writer.StorageS3, writer.ServerDuckdbClient, icebergSchemaTable)
	icebergTable.DropIfExists()

	return nil
}
