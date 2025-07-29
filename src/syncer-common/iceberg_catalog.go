package common

import (
	"context"
	"database/sql"
	"strings"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"

	CATALOG_NAME = "postgres"
)

type IcebergCatalog struct {
	Config *BaseConfig
}

func NewIcebergCatalog(config *BaseConfig) *IcebergCatalog {
	return &IcebergCatalog{Config: config}
}

func (catalog *IcebergCatalog) TableNames() Set[string] {
	pgClient := catalog.newPgClient()
	defer pgClient.Close()

	ctx := context.Background()
	rows, err := pgClient.Query(ctx,
		"SELECT table_name FROM iceberg_tables WHERE table_namespace=$1 AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_SYNCING+"' AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_DELETING+"'",
		catalog.Config.DestinationSchemaName,
	)
	PanicIfError(catalog.Config, err)
	defer rows.Close()

	tableNames := make(Set[string])
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		PanicIfError(catalog.Config, err)
		tableNames.Add(table)
	}
	return tableNames
}

func (catalog *IcebergCatalog) S3TablePath(icebergTableName string) string {
	pgClient := catalog.newPgClient()
	defer pgClient.Close()

	ctx := context.Background()
	row := pgClient.QueryRow(ctx,
		"SELECT metadata_location FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2",
		catalog.Config.DestinationSchemaName,
		icebergTableName,
	)

	var nullPath sql.NullString
	err := row.Scan(&nullPath)
	if !nullPath.Valid || nullPath.String == "" {
		return ""
	}
	PanicIfError(catalog.Config, err)

	return strings.Split(nullPath.String, "/metadata/")[0]
}

func (catalog *IcebergCatalog) RenameTable(oldIcebergTableName string, newIcebergTableName string) {
	pgClient := catalog.newPgClient()
	defer pgClient.Close()

	ctx := context.Background()
	_, err := pgClient.Exec(ctx,
		"UPDATE iceberg_tables SET table_name=$1 WHERE table_namespace=$2 AND table_name=$3",
		newIcebergTableName,
		catalog.Config.DestinationSchemaName,
		oldIcebergTableName,
	)
	PanicIfError(nil, err)
}

func (catalog *IcebergCatalog) DeleteTable(icebergTableName string) {
	pgClient := catalog.newPgClient()
	defer pgClient.Close()

	ctx := context.Background()
	_, err := pgClient.Exec(ctx,
		"DELETE FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2",
		catalog.Config.DestinationSchemaName,
		icebergTableName,
	)
	PanicIfError(nil, err)
}

func (catalog *IcebergCatalog) CreateTable(icebergTableName string, metadataLocation string) {
	pgClient := catalog.newPgClient()
	defer pgClient.Close()

	ctx := context.Background()
	_, err := pgClient.Exec(ctx,
		"INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) VALUES ($1, $2, $3, $4)",
		CATALOG_NAME,
		catalog.Config.DestinationSchemaName,
		icebergTableName,
		metadataLocation,
	)
	PanicIfError(nil, err)
}

func (catalog *IcebergCatalog) newPgClient() *Postgres {
	return NewPostgres(catalog.Config, catalog.Config.CatalogDatabaseUrl)
}
