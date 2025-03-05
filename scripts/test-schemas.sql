-- Usage: psql postgres://127.0.0.1:5432/dbname -P pager=off -v ON_ERROR_STOP=on -f ./scripts/test-schemas.sql

DROP SCHEMA IF EXISTS test_schema CASCADE;

CREATE SCHEMA test_schema;

CREATE TABLE test_schema.simple_table (
  id SERIAL PRIMARY KEY
);

INSERT INTO test_schema.simple_table DEFAULT VALUES;

SELECT
  table_schema,
  table_name,
  column_name,
  data_type,
  udt_name,
  is_nullable,
  character_maximum_length,
  numeric_precision,
  numeric_scale,
  datetime_precision
FROM information_schema.columns
WHERE table_schema = 'test_schema'
ORDER BY table_schema, table_name, ordinal_position;
