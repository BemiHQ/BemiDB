package syncerCommon

import (
	"context"
	"database/sql"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"

	CATALOG_NAME = "postgres"
)

type IcebergCatalog struct {
	Config     *common.CommonConfig
	SchemaName string
}

func NewIcebergCatalog(config *common.CommonConfig, schemaName string) *IcebergCatalog {
	return &IcebergCatalog{
		Config:     config,
		SchemaName: schemaName,
	}
}

func (catalog *IcebergCatalog) TableNames() common.Set[string] {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	rows, err := pgClient.Query(
		context.Background(),
		"SELECT table_name FROM iceberg_tables WHERE table_namespace=$1 AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_SYNCING+"' AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_DELETING+"'",
		catalog.SchemaName,
	)
	common.PanicIfError(catalog.Config, err)
	defer rows.Close()

	tableNames := make(common.Set[string])
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		common.PanicIfError(catalog.Config, err)
		tableNames.Add(table)
	}
	return tableNames
}

func (catalog *IcebergCatalog) MetadataFileS3Path(icebergTableName string) string {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	row := pgClient.QueryRow(
		context.Background(),
		"SELECT metadata_location FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2",
		catalog.SchemaName,
		icebergTableName,
	)

	var nullPath sql.NullString
	err := row.Scan(&nullPath)
	if !nullPath.Valid || nullPath.String == "" {
		return ""
	}
	common.PanicIfError(catalog.Config, err)

	return nullPath.String
}

func (catalog *IcebergCatalog) TableS3Path(icebergTableName string) string {
	metadataFileS3Path := catalog.MetadataFileS3Path(icebergTableName)
	if metadataFileS3Path == "" {
		return ""
	}

	return strings.Split(metadataFileS3Path, "/metadata/")[0]
}

func (catalog *IcebergCatalog) RenameTable(oldIcebergTableName string, newIcebergTableName string) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	_, err := pgClient.Exec(
		context.Background(),
		"UPDATE iceberg_tables SET table_name=$1 WHERE table_namespace=$2 AND table_name=$3",
		newIcebergTableName,
		catalog.SchemaName,
		oldIcebergTableName,
	)
	common.PanicIfError(catalog.Config, err)
}

func (catalog *IcebergCatalog) DeleteTable(icebergTableName string) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	_, err := pgClient.Exec(
		context.Background(),
		"DELETE FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2",
		catalog.SchemaName,
		icebergTableName,
	)
	common.PanicIfError(catalog.Config, err)
}

func (catalog *IcebergCatalog) CreateTable(icebergTableName string, metadataLocation string) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	_, err := pgClient.Exec(
		context.Background(),
		"INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) VALUES ($1, $2, $3, $4)",
		CATALOG_NAME,
		catalog.SchemaName,
		icebergTableName,
		metadataLocation,
	)
	common.PanicIfError(catalog.Config, err)
}

func (catalog *IcebergCatalog) newPostgresClient() *common.PostgresClient {
	return common.NewPostgresClient(catalog.Config, catalog.Config.CatalogDatabaseUrl)
}
