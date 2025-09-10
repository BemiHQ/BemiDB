package common

import (
	"context"
	"fmt"
	"strings"
)

const (
	TEMP_TABLE_SUFFIX_SYNCING  = "-bemidb-syncing"
	TEMP_TABLE_SUFFIX_DELETING = "-bemidb-deleting"

	CATALOG_NAME_PLACEHOLDER = "postgres" // Trino legacy, to delete
)

type IcebergSchemaTable struct {
	Schema string
	Table  string
}

func (schemaTable IcebergSchemaTable) ToArg() string {
	return schemaTable.Schema + "." + schemaTable.Table
}

func (schemaTable IcebergSchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, schemaTable.Schema, schemaTable.Table)
}

type IcebergMaterializedView struct {
	Schema     string
	Table      string
	Definition string
}

func (view IcebergMaterializedView) ToIcebergSchemaTable() IcebergSchemaTable {
	return IcebergSchemaTable{
		Schema: view.Schema,
		Table:  view.Table,
	}
}

type IcebergCatalog struct {
	Config *CommonConfig
}

func NewIcebergCatalog(config *CommonConfig) *IcebergCatalog {
	return &IcebergCatalog{
		Config: config,
	}
}

// Read ----------------------------------------------------------------------------------------------------------------

func (catalog *IcebergCatalog) SchemaTables() (Set[IcebergSchemaTable], error) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	rows, err := pgClient.Query(
		context.Background(),
		"SELECT table_namespace, table_name FROM iceberg_tables WHERE table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_SYNCING+"' AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_DELETING+"'",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaTables := make(Set[IcebergSchemaTable])
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

func (catalog *IcebergCatalog) MaterializedViews() ([]IcebergMaterializedView, error) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	rows, err := pgClient.Query(
		context.Background(),
		"SELECT schema_name, table_name, definition FROM iceberg_materialized_views WHERE table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_SYNCING+"' AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_DELETING+"'",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	materializedViews := []IcebergMaterializedView{}
	for rows.Next() {
		var schema, table, definition string
		err := rows.Scan(&schema, &table, &definition)
		if err != nil {
			return nil, err
		}
		materializedViews = append(materializedViews, IcebergMaterializedView{
			Schema:     schema,
			Table:      table,
			Definition: definition,
		})
	}
	return materializedViews, nil
}

func (catalog *IcebergCatalog) MaterializedView(icebergSchemaTable IcebergSchemaTable) (IcebergMaterializedView, error) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	var schema, table, definition string
	err := pgClient.QueryRow(
		context.Background(),
		"SELECT schema_name, table_name, definition FROM iceberg_materialized_views WHERE schema_name=$1 AND table_name=$2",
		icebergSchemaTable.Schema, icebergSchemaTable.Table,
	).Scan(&schema, &table, &definition)

	if err != nil {
		if err.Error() == "no rows in result set" {
			return IcebergMaterializedView{}, fmt.Errorf("relation %s does not exist", icebergSchemaTable.String())
		}
		return IcebergMaterializedView{}, err
	}

	return IcebergMaterializedView{
		Schema:     schema,
		Table:      table,
		Definition: definition,
	}, nil
}

func (catalog *IcebergCatalog) MetadataFileS3Path(icebergSchemaTable IcebergSchemaTable) string {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	var path string
	err := pgClient.QueryRow(
		context.Background(),
		"SELECT metadata_location FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2",
		icebergSchemaTable.Schema, icebergSchemaTable.Table,
	).Scan(&path)
	if err != nil && err.Error() == "no rows in result set" {
		return ""
	}
	PanicIfError(catalog.Config, err)

	return path
}

func (catalog *IcebergCatalog) SchemaTableNames(schemaName string) Set[string] {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	rows, err := pgClient.Query(
		context.Background(),
		"SELECT table_name FROM iceberg_tables WHERE table_namespace=$1 AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_SYNCING+"' AND table_name NOT LIKE '%"+TEMP_TABLE_SUFFIX_DELETING+"'",
		schemaName,
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

func (catalog *IcebergCatalog) TableS3Path(icebergTableName IcebergSchemaTable) string {
	metadataFileS3Path := catalog.MetadataFileS3Path(icebergTableName)
	if metadataFileS3Path == "" {
		return ""
	}

	return strings.Split(metadataFileS3Path, "/metadata/")[0]
}

// Write ---------------------------------------------------------------------------------------------------------------

func (catalog *IcebergCatalog) CreateTable(icebergSchemaTable IcebergSchemaTable, metadataLocation string) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	_, err := pgClient.Exec(
		context.Background(),
		"INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) VALUES ($1, $2, $3, $4)",
		CATALOG_NAME_PLACEHOLDER,
		icebergSchemaTable.Schema,
		icebergSchemaTable.Table,
		metadataLocation,
	)
	PanicIfError(catalog.Config, err)
}

func (catalog *IcebergCatalog) RenameTable(oldIcebergSchemaTable IcebergSchemaTable, newIcebergTableName string) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	_, err := pgClient.Exec(
		context.Background(),
		"UPDATE iceberg_tables SET table_name=$1 WHERE table_namespace=$2 AND table_name=$3",
		newIcebergTableName,
		oldIcebergSchemaTable.Schema,
		oldIcebergSchemaTable.Table,
	)
	PanicIfError(catalog.Config, err)
}

func (catalog *IcebergCatalog) DropTable(icebergSchemaTable IcebergSchemaTable) {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	_, err := pgClient.Exec(
		context.Background(),
		"DELETE FROM iceberg_tables WHERE table_namespace=$1 AND table_name=$2",
		icebergSchemaTable.Schema,
		icebergSchemaTable.Table,
	)
	PanicIfError(catalog.Config, err)
}

func (catalog *IcebergCatalog) CreateMaterializedView(icebergSchemaTable IcebergSchemaTable, definition string, ifNotExists bool) error {
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	exists, err := catalog.doesMaterializedViewExist(pgClient, icebergSchemaTable)
	if err != nil {
		return err
	}
	if exists {
		if ifNotExists {
			return nil
		} else {
			return fmt.Errorf("materialized view %s already exists", icebergSchemaTable.String())
		}
	}

	ctx := context.Background()
	_, err = pgClient.Exec(
		ctx,
		"INSERT INTO iceberg_materialized_views (schema_name, table_name, definition) VALUES ($1, $2, $3)",
		icebergSchemaTable.Schema, icebergSchemaTable.Table, definition,
	)

	return err
}

func (catalog *IcebergCatalog) RenameMaterializedView(icebergSchemaTable IcebergSchemaTable, newName string, missingOk bool) error {
	ctx := context.Background()
	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	exists, err := catalog.doesMaterializedViewExist(pgClient, icebergSchemaTable)
	if err != nil {
		return err
	}
	if !exists {
		if missingOk {
			return nil
		} else {
			return fmt.Errorf("materialized view %s does not exist", icebergSchemaTable.String())
		}
	}

	_, err = pgClient.Exec(
		ctx,
		"UPDATE iceberg_materialized_views SET table_name=$1 WHERE schema_name=$2 AND table_name=$3",
		newName, icebergSchemaTable.Schema, icebergSchemaTable.Table,
	)

	return err
}

func (catalog *IcebergCatalog) DropMaterializedView(icebergSchemaTable IcebergSchemaTable, missingOk bool) error {
	ctx := context.Background()

	pgClient := catalog.newPostgresClient()
	defer pgClient.Close()

	exists, err := catalog.doesMaterializedViewExist(pgClient, icebergSchemaTable)
	if err != nil {
		return err
	}
	if !exists {
		if missingOk {
			return nil
		} else {
			return fmt.Errorf("materialized view %s does not exist", icebergSchemaTable.String())
		}
	}

	_, err = pgClient.Exec(
		ctx,
		"DELETE FROM iceberg_materialized_views WHERE schema_name=$1 AND table_name=$2",
		icebergSchemaTable.Schema, icebergSchemaTable.Table,
	)

	return err
}

func (catalog *IcebergCatalog) doesMaterializedViewExist(pgClient *PostgresClient, icebergSchemaTable IcebergSchemaTable) (bool, error) {
	var exists bool
	err := pgClient.QueryRow(
		context.Background(),
		"SELECT TRUE FROM iceberg_materialized_views WHERE schema_name=$1 AND table_name=$2",
		icebergSchemaTable.Schema, icebergSchemaTable.Table,
	).Scan(&exists)

	if err != nil {
		if err.Error() == "no rows in result set" {
			return false, nil
		} else {
			return false, fmt.Errorf("error checking materialized view existence: %w", err)
		}
	}
	return exists, nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (catalog *IcebergCatalog) newPostgresClient() *PostgresClient {
	return NewPostgresClient(catalog.Config, catalog.Config.CatalogDatabaseUrl)
}
