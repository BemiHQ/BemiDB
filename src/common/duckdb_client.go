package common

import (
	"context"
	"database/sql"
	"strings"

	"github.com/marcboeker/go-duckdb/v2"
)

var SYNCER_DUCKDB_BOOT_QUERIES = []string{
	"SET memory_limit='2GB'",
	"SET threads=2",
}

type DuckdbClient struct {
	Config    *CommonConfig
	Db        *sql.DB
	Connector *duckdb.Connector
}

func NewDuckdbClient(config *CommonConfig, bootQueries ...[]string) *DuckdbClient {
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	PanicIfError(config, err)
	db := sql.OpenDB(connector)
	PanicIfError(config, err)

	client := &DuckdbClient{
		Config:    config,
		Db:        db,
		Connector: connector,
	}

	queries := []string{
		"SET timezone='UTC'",
	}
	if bootQueries != nil {
		queries = append(queries, bootQueries[0]...)
	}
	for _, query := range queries {
		_, err := client.ExecContext(ctx, query)
		PanicIfError(config, err)
	}

	client.setExplicitAwsCredentials(ctx)

	if IsLocalHost(config.Aws.S3Endpoint) {
		_, err = client.ExecContext(ctx, "SET s3_use_ssl=false")
		PanicIfError(config, err)
	}

	if config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
		// Use endpoint/bucket/key (path, deprecated on AWS) instead of bucket.endpoint/key (vhost)
		_, err = client.ExecContext(ctx, "SET s3_url_style='path'")
		PanicIfError(config, err)
	}

	if config.LogLevel == LOG_LEVEL_TRACE {
		_, err = client.ExecContext(ctx, "PRAGMA enable_logging('HTTP')")
		PanicIfError(config, err)
		_, err = client.ExecContext(ctx, "SET logging_storage = 'stdout'")
		PanicIfError(config, err)
	}

	return client
}

func (client *DuckdbClient) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	LogDebug(client.Config, "Querying DuckDBClient:", query)
	return client.Db.QueryContext(ctx, query)
}

func (client *DuckdbClient) QueryRowContext(ctx context.Context, query string, args ...map[string]string) *sql.Row {
	LogDebug(client.Config, "Querying DuckDBClient:", query)
	if len(args) == 0 {
		return client.Db.QueryRowContext(ctx, query)
	}
	return client.Db.QueryRowContext(ctx, replaceNamedStringArgs(query, args[0]))
}

func (client *DuckdbClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	LogDebug(client.Config, "Preparing DuckDBClient statement:", query)
	return client.Db.PrepareContext(ctx, query)
}

func (client *DuckdbClient) ExecContext(ctx context.Context, query string, args ...map[string]string) (sql.Result, error) {
	LogDebug(client.Config, "Querying DuckDBClient:", query)
	if len(args) == 0 {
		return client.Db.ExecContext(ctx, query)
	}

	return client.Db.ExecContext(ctx, replaceNamedStringArgs(query, args[0]))
}

func (client *DuckdbClient) ExecTransactionContext(ctx context.Context, queries []string, args ...[]map[string]string) error {
	tx, err := client.Db.Begin()
	LogDebug(client.Config, "Querying DuckDBClient: BEGIN")
	if err != nil {
		return err
	}

	for i, query := range queries {
		LogDebug(client.Config, "Querying DuckDBClient:", query)
		var err error
		if len(args) == 0 {
			_, err = tx.ExecContext(ctx, query)
		} else {
			_, err = tx.ExecContext(ctx, replaceNamedStringArgs(query, args[0][i]))
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	LogDebug(client.Config, "Querying DuckDBClient: COMMIT")
	return tx.Commit()
}

func (client *DuckdbClient) Appender(schema string, table string) (*duckdb.Appender, error) {
	conn, err := client.Connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	return duckdb.NewAppenderFromConn(conn, schema, table)
}

func (client *DuckdbClient) Close() {
	client.Db.Close()
}

func (client *DuckdbClient) setExplicitAwsCredentials(ctx context.Context) {
	config := client.Config
	query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
	_, err := client.ExecContext(ctx, query, map[string]string{
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
		query = strings.ReplaceAll(
			query,
			"$"+key,
			strings.ReplaceAll(value, "'", "''"),
		)
	}
	return query
}
