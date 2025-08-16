package syncerCommon

import (
	"context"

	"github.com/google/uuid"

	"github.com/BemiHQ/BemiDB/src/common"
)

type IcebergTable struct {
	Config         *common.CommonConfig
	SchemaName     string
	TableName      string
	IcebergCatalog *IcebergCatalog
	StorageS3      *StorageS3
	DuckdbClient   *common.DuckdbClient
}

type CursorValue struct {
	ColumnName   string
	StringValue  string
	OverrideRows bool // Override rows that have the same value as this cursor value (if new rows are added with the same value, they will be included in the next sync)
}

func NewIcebergTable(config *common.CommonConfig, storageS3 *StorageS3, duckdbClient *common.DuckdbClient, schemaName string, tableName string) *IcebergTable {
	return &IcebergTable{
		Config:         config,
		SchemaName:     schemaName,
		TableName:      tableName,
		IcebergCatalog: NewIcebergCatalog(config, schemaName),
		StorageS3:      storageS3,
		DuckdbClient:   duckdbClient,
	}
}

func (table *IcebergTable) String() string {
	return table.SchemaName + "." + table.TableName
}

func (table *IcebergTable) MetadataFileS3Path() string {
	return table.IcebergCatalog.MetadataFileS3Path(table.TableName)
}

func (table *IcebergTable) Create(tableS3Path string) {
	common.LogInfo(table.Config, "Creating Iceberg table:", table.TableName)
	table.IcebergCatalog.CreateTable(table.TableName, tableS3Path+"/metadata/"+ICEBERG_METADATA_INITIAL_FILE_NAME)
}

func (table *IcebergTable) DeleteIfExists() {
	tableS3Path := table.IcebergCatalog.TableS3Path(table.TableName)
	if tableS3Path == "" {
		return
	}

	common.LogInfo(table.Config, "Deleting Iceberg table:", table.TableName)
	table.StorageS3.DeleteTableFiles(tableS3Path)
	table.IcebergCatalog.DeleteTable(table.TableName)
}

func (table *IcebergTable) Rename(newName string) {
	common.LogInfo(table.Config, "Renaming Iceberg table from", table.TableName, "to", newName)
	table.IcebergCatalog.RenameTable(table.TableName, newName)
	table.TableName = newName
}

func (table *IcebergTable) GenerateTableS3Path() string {
	return "s3://" + table.Config.Aws.S3Bucket + "/iceberg/" + table.SchemaName + "/" + table.TableName + "-" + uuid.New().String()
}

func (table *IcebergTable) LastCursorValue(columnName string) CursorValue {
	if columnName == "" {
		common.Panic(table.Config, "Couldn't find cursor column for table "+table.TableName)
	}

	metadataFileS3Path := table.IcebergCatalog.MetadataFileS3Path(table.TableName)
	if metadataFileS3Path == "" {
		common.LogDebug(table.Config, "No S3 table path found for Iceberg table:", table.TableName)
		return CursorValue{ColumnName: columnName}
	}

	row := table.DuckdbClient.QueryRowContext(
		context.Background(),
		`SELECT CAST(max("`+columnName+`") AS VARCHAR) FROM iceberg_scan('`+metadataFileS3Path+`')`,
	)
	if row == nil {
		return CursorValue{ColumnName: columnName}
	}

	var lastCursorValue string
	err := row.Scan(&lastCursorValue)
	common.PanicIfError(table.Config, err)

	return CursorValue{ColumnName: columnName, StringValue: lastCursorValue}
}
