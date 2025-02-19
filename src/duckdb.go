package main

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"regexp"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

var DEFAULT_BOOT_QUERIES = []string{
	// Set up Iceberg
	"INSTALL iceberg",
	"LOAD iceberg",

	// Set up schemas
	"SELECT oid FROM pg_catalog.pg_namespace",
	"CREATE SCHEMA public",
	"USE public",

	// Configure DuckDB
	"SET scalar_subquery_error_on_multiple_rows=false",

	// Create macros
	"CREATE MACRO version() AS 'PostgreSQL " + PG_VERSION + ", compiled by BemiDB'",
	"CREATE MACRO set_config(setting_name, new_value, is_local) AS new_value",
	"CREATE MACRO current_setting(setting_name) AS '', (setting_name, missing_ok) AS ''",
}

type Duckdb struct {
	db     *sql.DB
	config *Config
}

func NewDuckdb(config *Config) *Duckdb {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	PanicIfError(err)

	duckdb := &Duckdb{
		db:     db,
		config: config,
	}

	bootQueries := readDuckdbInitFile(config)
	if bootQueries == nil {
		bootQueries = DEFAULT_BOOT_QUERIES
	}
	for _, query := range bootQueries {
		_, err := duckdb.ExecContext(ctx, query, nil)
		PanicIfError(err)
	}

	switch config.StorageType {
	case STORAGE_TYPE_S3:
		query := "CREATE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
		_, err = duckdb.ExecContext(ctx, query, map[string]string{
			"accessKeyId":     config.Aws.AccessKeyId,
			"secretAccessKey": config.Aws.SecretAccessKey,
			"region":          config.Aws.Region,
			"endpoint":        config.Aws.S3Endpoint,
			"s3Bucket":        "s3://" + config.Aws.S3Bucket,
		})
		PanicIfError(err)

		if config.LogLevel == LOG_LEVEL_TRACE {
			_, err = duckdb.ExecContext(ctx, "SET enable_http_logging=true", nil)
			PanicIfError(err)
		}
	}

	return duckdb
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
	duckdb.db.Close()
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	re := regexp.MustCompile(`['";]`) // Escape single quotes, double quotes, and semicolons from args

	for key, value := range args {
		query = strings.ReplaceAll(query, "$"+key, re.ReplaceAllString(value, ""))
	}
	return query
}

func readDuckdbInitFile(config *Config) []string {
	_, err := os.Stat(config.InitSqlFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			LogDebug(config, "DuckDB: No init file found at", config.InitSqlFilepath)
			return nil
		}
		PanicIfError(err)
	}

	LogInfo(config, "DuckDB: Reading init file", config.InitSqlFilepath)
	file, err := os.Open(config.InitSqlFilepath)
	PanicIfError(err)
	defer file.Close()

	lines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	PanicIfError(scanner.Err())
	return lines
}
