package common

import (
	"context"
	"database/sql"
	"strings"

	goDuckdb "github.com/marcboeker/go-duckdb/v2"
)

var DUCKDB_INIT_BOOT_QUERIES = []string{
	"INSTALL iceberg",
	"LOAD iceberg",
	"SET timezone='UTC'",
	"SET memory_limit='2GB'",
}

type Duckdb struct {
	Db        *sql.DB
	Connector *goDuckdb.Connector
	Config    *BaseConfig
}

func NewDuckdb(config *BaseConfig) *Duckdb {
	ctx := context.Background()
	connector, err := goDuckdb.NewConnector("", nil)
	PanicIfError(config, err)
	db := sql.OpenDB(connector)
	PanicIfError(config, err)

	duckdb := &Duckdb{
		Db:        db,
		Connector: connector,
		Config:    config,
	}

	for _, query := range DUCKDB_INIT_BOOT_QUERIES {
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
	LogDebug(duckdb.Config, "Querying DuckDB:", query, args)
	return duckdb.Db.ExecContext(ctx, replaceNamedStringArgs(query, args))
}

func (duckdb *Duckdb) Appender(schema string, table string) (*goDuckdb.Appender, error) {
	conn, err := duckdb.Connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	return goDuckdb.NewAppenderFromConn(conn, schema, table)
}

func (duckdb *Duckdb) Close() {
	duckdb.Db.Close()
}

func (duckdb *Duckdb) setExplicitAwsCredentials(ctx context.Context) {
	Config := duckdb.Config
	query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
	_, err := duckdb.ExecContext(ctx, query, map[string]string{
		"accessKeyId":     Config.Aws.AccessKeyId,
		"secretAccessKey": Config.Aws.SecretAccessKey,
		"region":          Config.Aws.Region,
		"endpoint":        Config.Aws.S3Endpoint,
		"s3Bucket":        "s3://" + Config.Aws.S3Bucket,
	})
	PanicIfError(Config, err)
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	for key, value := range args {
		query = strings.ReplaceAll(query, "$"+key, value)
	}
	return query
}
