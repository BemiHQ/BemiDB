CREATE TABLE IF NOT EXISTS iceberg_tables (
  table_namespace VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  metadata_location VARCHAR(1000),
  columns JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tables ON iceberg_tables (table_namespace, table_name);

CREATE TABLE IF NOT EXISTS iceberg_materialized_views (
  schema_name VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  definition TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_materialized_views ON iceberg_materialized_views (schema_name, table_name);
