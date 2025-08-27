{{ config(materialized='table') }}

SELECT id, varchar_column
FROM postgres.test_table
WHERE id < 10
