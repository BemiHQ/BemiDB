package main

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var DUCKDB_SCHEMA_MAIN = "main"

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
}

type Duckdb struct {
	refreshQuit chan struct{}
	db          *sql.DB
	config      *Config
}

func NewDuckdb(config *Config) *Duckdb {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	PanicIfError(err, config)

	duckdb := &Duckdb{
		db:          db,
		config:      config,
		refreshQuit: make(chan struct{}),
	}

	bootQueries := slices.Concat(
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

	for _, query := range bootQueries {
		_, err := duckdb.ExecContext(ctx, query, nil)
		PanicIfError(err, config)
	}

	switch config.StorageType {
	case STORAGE_TYPE_S3:
		duckdb.setAwsCredentials(ctx)
		ticker := time.NewTicker(10 * time.Minute)
		time.Tick(10 * time.Minute)
		go func() {
			for {
				select {
				case <-ticker.C:
					duckdb.setAwsCredentials(ctx)
				case <-duckdb.refreshQuit:
					ticker.Stop()
					return
				}
			}
		}()

		if config.LogLevel == LOG_LEVEL_TRACE {
			_, err = duckdb.ExecContext(ctx, "SET enable_http_logging=true", nil)
			PanicIfError(err, config)
		}
	}

	return duckdb
}

func (duckdb *Duckdb) setAwsCredentials(ctx context.Context) {
	config := duckdb.config
	switch config.Aws.CredentialsType {
	case AWS_CREDENTIALS_TYPE_STATIC:
		query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
		_, err := duckdb.ExecContext(ctx, query, map[string]string{
			"accessKeyId":     config.Aws.AccessKeyId,
			"secretAccessKey": config.Aws.SecretAccessKey,
			"region":          config.Aws.Region,
			"endpoint":        config.Aws.S3Endpoint,
			"s3Bucket":        "s3://" + config.Aws.S3Bucket,
		})
		PanicIfError(err, config)
	case AWS_CREDENTIALS_TYPE_DEFAULT:
		query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
		_, err := duckdb.ExecContext(ctx, query, map[string]string{
			"region":   config.Aws.Region,
			"endpoint": config.Aws.S3Endpoint,
			"s3Bucket": "s3://" + config.Aws.S3Bucket,
		})
		PanicIfError(err, config)
	}
}

func (duckdb *Duckdb) ExecContext(ctx context.Context, query string, args map[string]string) (sql.Result, error) {
	LogDebug(duckdb.config, "Querying DuckDB:", query, args)
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
	close(duckdb.refreshQuit)
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

func (duckdb *Duckdb) ExecInitFile() {
	initFileQueries := readInitFile(duckdb.config)
	if initFileQueries == nil {
		return
	}

	ctx := context.Background()
	for _, query := range initFileQueries {
		_, err := duckdb.ExecContext(ctx, query, nil)
		PanicIfError(err, duckdb.config)
	}
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	re := regexp.MustCompile(`['";]`) // Escape single quotes, double quotes, and semicolons from args

	for key, value := range args {
		query = strings.ReplaceAll(query, "$"+key, re.ReplaceAllString(value, ""))
	}
	return query
}

func readInitFile(config *Config) []string {
	_, err := os.Stat(config.InitSqlFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			LogDebug(config, "DuckDB: No init file found at", config.InitSqlFilepath)
			return nil
		}
		PanicIfError(err, config)
	}

	LogInfo(config, "DuckDB: Reading init file", config.InitSqlFilepath)
	file, err := os.Open(config.InitSqlFilepath)
	PanicIfError(err, config)
	defer file.Close()

	lines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	PanicIfError(scanner.Err(), config)
	return lines
}
