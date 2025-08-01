package main

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"
)

const (
	DUCKDB_SCHEMA_MAIN = "main"
)

var DUCKDB_INIT_BOOT_QUERIES = []string{
	// Set up Iceberg
	"INSTALL iceberg",
	"LOAD iceberg",

	// Set up schemas
	"SELECT oid FROM pg_catalog.pg_namespace",
	"CREATE SCHEMA public",

	// Configure DuckDB
	"SET scalar_subquery_error_on_multiple_rows=false",
	"SET timezone='UTC'",
	"SET memory_limit='2GB'",
}

type Duckdb struct {
	db                                    *sql.DB
	config                                *Config
	stopImplicitAwsCredentialsRefreshChan chan struct{}
}

func NewDuckdb(config *Config, withPgCompatibility bool) *Duckdb {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	PanicIfError(config, err)

	duckdb := &Duckdb{
		db:                                    db,
		config:                                config,
		stopImplicitAwsCredentialsRefreshChan: make(chan struct{}),
	}

	bootQueries := []string{}
	if withPgCompatibility {
		bootQueries = slices.Concat(
			// Set up DuckDB
			DUCKDB_INIT_BOOT_QUERIES,

			// Create pg-compatible functions
			CreatePgCatalogMacroQueries(config),
			CreateInformationSchemaMacroQueries(config),

			// Create pg-compatible tables and views
			CreatePgCatalogTableQueries(config),
			CreateInformationSchemaTableQueries(config),

			// Use the public schema
			[]string{"USE public"},
		)
	}

	for _, query := range bootQueries {
		_, err := duckdb.ExecContext(ctx, query, nil)
		PanicIfError(config, err)
	}

	duckdb.setExplicitAwsCredentials(ctx)

	if IsLocalHost(config.Aws.S3Endpoint) {
		_, err = duckdb.ExecContext(ctx, "SET s3_use_ssl=false", nil)
		PanicIfError(config, err)
	}

	if config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
		// Use endpoint/bucket/key (path, deprecated on AWS) instead of bucket.endpoint/key (vhost)
		_, err = duckdb.ExecContext(ctx, "SET s3_url_style='path'", nil)
		PanicIfError(config, err)
	}

	if config.LogLevel == LOG_LEVEL_TRACE {
		_, err = duckdb.ExecContext(ctx, "PRAGMA enable_logging('HTTP')", nil)
		PanicIfError(config, err)
		_, err = duckdb.ExecContext(ctx, "SET logging_storage = 'stdout'", nil)
		PanicIfError(config, err)
	}

	return duckdb
}

func (duckdb *Duckdb) ExecContext(ctx context.Context, query string, args map[string]string) (sql.Result, error) {
	LogDebug(duckdb.config, "Querying DuckDB:", query)
	return duckdb.db.ExecContext(ctx, replaceNamedStringArgs(query, args))
}

func (duckdb *Duckdb) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	LogDebug(duckdb.config, "Querying DuckDB:", query)
	return duckdb.db.QueryContext(ctx, query)
}

func (duckdb *Duckdb) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	LogDebug(duckdb.config, "Preparing DuckDB statement:", query)
	return duckdb.db.PrepareContext(ctx, query)
}

func (duckdb *Duckdb) Close() {
	close(duckdb.stopImplicitAwsCredentialsRefreshChan)
	duckdb.db.Close()
}

func (duckdb *Duckdb) ExecTransactionContext(ctx context.Context, queries []string) error {
	tx, err := duckdb.db.Begin()
	LogDebug(duckdb.config, "Querying DuckDB: BEGIN")
	if err != nil {
		return err
	}

	for _, query := range queries {
		LogDebug(duckdb.config, "Querying DuckDB:", query)
		_, err := tx.ExecContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	LogDebug(duckdb.config, "Querying DuckDB: COMMIT")
	return tx.Commit()
}

func (duckdb *Duckdb) setExplicitAwsCredentials(ctx context.Context) {
	config := duckdb.config
	query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
	_, err := duckdb.ExecContext(ctx, query, map[string]string{
		"accessKeyId":     config.Aws.AccessKeyId,
		"secretAccessKey": config.Aws.SecretAccessKey,
		"region":          config.Aws.Region,
		"endpoint":        config.Aws.S3Endpoint,
		"s3Bucket":        "s3://" + config.Aws.S3Bucket,
	})
	PanicIfError(config, err)
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	for key, value := range args {
		query = strings.ReplaceAll(query, "$"+key, value)
	}
	return query
}
