package main

import (
	"context"
	"fmt"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"
)

type IcebergSchemaTable struct {
	Schema string
	Table  string
}

func (schemaTable IcebergSchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, schemaTable.Schema, schemaTable.Table)
}

type IcebergCatalog struct {
	Config *Config
}

func NewIcebergCatalog(config *Config) *IcebergCatalog {
	return &IcebergCatalog{Config: config}
}

func (catalog *IcebergCatalog) Schemas() ([]string, error) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	ctx := context.Background()
	rows, err := pgClient.Query(ctx, "SELECT namespace FROM iceberg_namespace_properties")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		err := rows.Scan(&schema)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func (catalog *IcebergCatalog) SchemaTables() (common.Set[IcebergSchemaTable], error) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	ctx := context.Background()
	rows, err := pgClient.Query(ctx, "SELECT table_namespace, table_name FROM iceberg_tables WHERE table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_SYNCING+"' AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_DELETING+"'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaTables := make(common.Set[IcebergSchemaTable])
	for rows.Next() {
		var schema, table string
		err := rows.Scan(&schema, &table)
		if err != nil {
			return nil, err
		}
		schemaTables.Add(IcebergSchemaTable{Schema: schema, Table: table})
	}
	return schemaTables, nil
}

func (catalog *IcebergCatalog) MetadataFilePath(t IcebergSchemaTable) string {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	ctx := context.Background()
	row := pgClient.QueryRow(ctx, "SELECT metadata_location FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2", t.Schema, t.Table)

	var path string
	err := row.Scan(&path)
	common.PanicIfError(catalog.Config.CommonConfig, err)

	return path
}

func (catalog *IcebergCatalog) newPostgresClient() *common.PostgresClient {
	return common.NewPostgresClient(catalog.Config.CommonConfig, catalog.Config.CommonConfig.CatalogDatabaseUrl)
}
