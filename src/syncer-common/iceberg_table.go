package syncerCommon

import (
	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/google/uuid"
)

type IcebergTable struct {
	Config               *common.CommonConfig
	SchemaName           string
	TableName            string
	IcebergCatalog       *IcebergCatalog
	StorageS3            *StorageS3
	GeneratedS3TablePath string
}

func NewIcebergTable(config *common.CommonConfig, storageS3 *StorageS3, schemaName string, tableName string) *IcebergTable {
	return &IcebergTable{
		Config:         config,
		SchemaName:     schemaName,
		TableName:      tableName,
		IcebergCatalog: NewIcebergCatalog(config, schemaName),
		StorageS3:      storageS3,
	}
}

func (table *IcebergTable) Create() {
	common.LogInfo(table.Config, "Creating Iceberg table:", table.TableName)
	table.IcebergCatalog.CreateTable(table.TableName, table.GeneratedS3TablePath+"/metadata/"+ICEBERG_METADATA_INITIAL_FILE_NAME)
}

func (table *IcebergTable) DeleteIfExists() {
	s3TablePath := table.IcebergCatalog.S3TablePath(table.TableName)
	if s3TablePath == "" {
		return
	}

	common.LogInfo(table.Config, "Deleting Iceberg table:", table.TableName)
	table.StorageS3.DeleteTableFiles(s3TablePath)
	table.IcebergCatalog.DeleteTable(table.TableName)
}

func (table *IcebergTable) Rename(newName string) {
	common.LogInfo(table.Config, "Renaming Iceberg table from", table.TableName, "to", newName)
	table.IcebergCatalog.RenameTable(table.TableName, newName)
	table.TableName = newName
}

func (table *IcebergTable) GenerateS3TablePath() {
	table.GeneratedS3TablePath = "s3://" + table.Config.Aws.S3Bucket + "/iceberg/" + table.SchemaName + "/" + table.TableName + "-" + uuid.New().String()
}
