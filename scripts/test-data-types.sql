-- Usage: psql postgres://127.0.0.1:5432/dbname -P pager=off -v ON_ERROR_STOP=on -f ./scripts/test-data-types.sql

DROP TABLE IF EXISTS test_table;
DROP TYPE IF EXISTS address;

CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TYPE address AS (
  city VARCHAR(50)
);

CREATE TABLE test_table (
  id SERIAL PRIMARY KEY,
  bit_column BIT,
  bool_column BOOLEAN,
  bpchar_column BPCHAR(10),
  varchar_column VARCHAR(255),
  text_column TEXT,
  int2_column INT2,
  int4_column INT4,
  int8_column INT8,
  hugeint_column NUMERIC(20, 0),
  xid_column XID,
  xid8_column XID8,
  float4_column FLOAT4,
  float8_column FLOAT8,
  numeric_column NUMERIC(40, 2),
  numeric_column_without_precision NUMERIC,
  date_column DATE,
  time_column TIME,
  timeMsColumn TIME(3),
  timetz_column TIMETZ,
  timetz_ms_column TIMETZ(3),
  timestamp_column TIMESTAMP,
  timestamp_ms_column TIMESTAMP(3),
  timestamptz_column TIMESTAMPTZ,
  timestamptz_ms_column TIMESTAMPTZ(3),
  uuid_column UUID,
  bytea_column BYTEA,
  interval_column INTERVAL,
  point_column POINT,
  inet_column INET,
  json_column JSON,
  jsonb_column JSONB,
  tsvector_column TSVECTOR,
  xml_column XML,
  pg_snapshot_column PG_SNAPSHOT,
  array_text_column TEXT[],
  array_int_column INT[],
  array_jsonb_column JSONB[],
  array_ltree_column LTREE[],
  user_defined_column address
);

INSERT INTO test_table (
  bit_column,
  bool_column,
  bpchar_column,
  varchar_column,
  text_column,
  int2_column,
  int4_column,
  int8_column,
  hugeint_column,
  xid_column,
  xid8_column,
  float4_column,
  float8_column,
  numeric_column,
  numeric_column_without_precision,
  date_column,
  time_column,
  timeMsColumn,
  timetz_column,
  timetz_ms_column,
  timestamp_column,
  timestamp_ms_column,
  timestamptz_column,
  timestamptz_ms_column,
  uuid_column,
  bytea_column,
  interval_column,
  point_column,
  inet_column,
  json_column,
  jsonb_column,
  tsvector_column,
  xml_column,
  pg_snapshot_column,
  array_text_column,
  array_int_column,
  array_jsonb_column,
  array_ltree_column,
  user_defined_column
) VALUES (
  B'1',                                     -- bit_column
  TRUE,                                     -- bool_column
  'bpchar',                                 -- bpchar_column
  'varchar',                                -- varchar_column
  'text',                                   -- text_column
  32767::INT2,                              -- int2_column
  2147483647::INT4,                         -- int4_column
  9223372036854775807::INT8,                -- int8_column
  10000000000000000000,                     -- hugeint_column
  '4294967295'::XID,                        -- xid_column
  '18446744073709551615'::XID8,             -- xid8_column
  3.14::FLOAT4,                             -- float4_column
  3.141592653589793::FLOAT8,                -- float8_column
  12345.67::NUMERIC(10, 2),                 -- numeric_column
  12345.67,                                 -- numeric_column_without_precision
  '2024-01-01',                             -- date_column
  '12:00:00.123456',                        -- time_column
  '12:00:00.123',                           -- timeMsColumn
  '12:00:00.123456-05',                     -- timetz_column
  '12:00:00.123-05',                        -- timetz_ms_column
  '2024-01-01 12:00:00.123456',             -- timestamp_column
  '2024-01-01 12:00:00.123',                -- timestamp_ms_column
  '2024-01-01 12:00:00.123456-05',          -- timestamptz_column
  '2024-01-01 12:00:00.123-05',             -- timestamptz_ms_column
  '58a7c845-af77-44b2-8664-7ca613d92f04',   -- uuid_column
  decode('48656c6c6f', 'hex'),              -- bytea_column
  '1 mon 2 days 01:00:01.000001'::INTERVAL, -- interval_column
  '(1, 2)'::POINT,                          -- point_column
  '192.168.0.1',                            -- inet_column
  '{"key": "value"}'::JSON,                 -- json_column
  '{"key": "value"}'::JSONB,                -- jsonb_column
  to_tsvector('Sample text for tsvector'),  -- tsvector_column
  '<root><child>text</child></root>',       -- xml_column
  pg_current_snapshot(),                    -- pg_snapshot_column
  '{"one", "two", "three"}',                -- array_text_column
  '{1, 2, 3}',                              -- array_int_column
  '{"{\"key\": \"value1\"}", "{\"key\": \"value2\"}"}'::JSONB[], -- array_jsonb_column
  '{"a.b", "c.d"}'::LTREE[],                -- array_ltree_column
  ROW('Toronto')                            -- user_defined_column
), (
  NULL,                                     -- bit_column
  FALSE,                                    -- bool_column
  '',                                       -- bpchar_column
  NULL,                                     -- varchar_column
  '',                                       -- text_column
  -32767::INT2,                             -- int2_column
  NULL,                                     -- int4_column
  -9223372036854775807::INT8,               -- int8_column
  NULL,                                     -- hugeint_column
  NULL,                                     -- xid_column
  NULL,                                     -- xid8_column
  'NaN',                                    -- float4_column
  -3.141592653589793::FLOAT8,               -- float8_column
  -12345.00::NUMERIC(10, 2),                -- numeric_column
  NULL,                                     -- numeric_column_without_precision
  NULL,                                     -- date_column
  '12:00:00.123',                           -- time_column
  NULL,                                     -- timeMsColumn
  '12:00:00.12300+05',                      -- timetz_column
  '12:00:00.1+05',                          -- timetz_ms_column
  '2024-01-01 12:00:00',                    -- timestamp_column
  NULL,                                     -- timestamp_ms_column
  '2024-01-01 12:00:00.000123+05',          -- timestamptz_column
  '2024-01-01 12:00:00.12+05',              -- timestamptz_ms_column
  NULL,                                     -- uuid_column
  NULL,                                     -- bytea_column
  NULL,                                     -- interval_column
  NULL,                                     -- point_column
  NULL,                                     -- inet_column
  NULL,                                     -- json_column
  '{}'::JSONB,                              -- jsonb_column
  NULL,                                     -- tsvector_column
  NULL,                                     -- xml_column
  NULL,                                     -- pg_snapshot_column
  NULL,                                     -- array_text_column
  '{}',                                     -- array_int_column
  NULL,                                     -- array_jsonb_column
  NULL,                                     -- array_ltree_column
  NULL                                      -- user_defined_column
);

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
WHERE table_schema = 'public'
ORDER BY table_schema, table_name, ordinal_position;
