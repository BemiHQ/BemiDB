package common

import (
	"github.com/google/uuid"
)

type IcebergTable struct {
	Config               *BaseConfig
	Name                 string
	IcebergCatalog       *IcebergCatalog
	StorageS3            *StorageS3
	GeneratedS3TablePath string
}

func NewIcebergTable(config *BaseConfig, storageS3 *StorageS3, name string) *IcebergTable {
	return &IcebergTable{
		Config:         config,
		Name:           name,
		IcebergCatalog: NewIcebergCatalog(config),
		StorageS3:      storageS3,
	}
}

func (table *IcebergTable) Create() {
	LogInfo(table.Config, "Creating Iceberg table:", table.Name)
	table.IcebergCatalog.CreateTable(table.Name, table.GeneratedS3TablePath+"/metadata/"+ICEBERG_METADATA_INITIAL_FILE_NAME)
}

func (table *IcebergTable) DeleteIfExists() {
	s3TablePath := table.IcebergCatalog.S3TablePath(table.Name)
	if s3TablePath == "" {
		return
	}

	LogInfo(table.Config, "Deleting Iceberg table:", table.Name)
	table.StorageS3.DeleteTableFiles(s3TablePath)
	table.IcebergCatalog.DeleteTable(table.Name)
}

func (table *IcebergTable) Rename(newName string) {
	LogInfo(table.Config, "Renaming Iceberg table from", table.Name, "to", newName)
	table.IcebergCatalog.RenameTable(table.Name, newName)
	table.Name = newName
}

func (table *IcebergTable) GenerateS3TablePath() {
	table.GeneratedS3TablePath = "s3://" + table.Config.Aws.S3Bucket + "/iceberg/" + table.Config.DestinationSchemaName + "/" + table.Name + "-" + uuid.New().String()
}
