CREATE TABLE IF NOT EXISTS iceberg_tables (
  catalog_name VARCHAR(255) NOT NULL,
  table_namespace VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  metadata_location VARCHAR(1000),
  previous_metadata_location VARCHAR(1000),
  PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_materialized_views (
  schema_name VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  definition TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_materialized_views ON iceberg_materialized_views (schema_name, table_name);
