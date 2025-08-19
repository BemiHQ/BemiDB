package common

import (
	"context"
	"database/sql"

	_ "github.com/trinodb/trino-go-client/trino"
)

const (
	TRINO_MAX_DECIMAL_PRECISION = 38
	TRINO_MAX_QUERY_LENGTH      = 1_000_000
)

type Trino struct {
	Config *BaseConfig
	Db     *sql.DB
}

func NewTrino(config *BaseConfig) *Trino {
	db, err := sql.Open("trino", config.Trino.DatabaseUrl)
	PanicIfError(config, err)

	return &Trino{
		Config: config,
		Db:     db,
	}
}

func (trino *Trino) Schema() string {
	return trino.Config.Trino.CatalogName + "." + trino.Config.DestinationSchemaName
}

func (trino *Trino) Close() {
	if err := trino.Db.Close(); err != nil {
		PanicIfError(trino.Config, err)
	}
}

func (trino *Trino) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	LogDebug(trino.Config, "Trino query:", query)
	result, err := trino.Db.ExecContext(ctx, query, args...)
	if err != nil {
		LogError(trino.Config, "Trino query failed:", query)
		return nil, err
	}

	return result, nil
}

func (trino *Trino) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	LogDebug(trino.Config, "Trino query:", query)
	return trino.Db.QueryContext(ctx, query, args...)
}

func (trino *Trino) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	LogDebug(trino.Config, "Trino query:", query)
	return trino.Db.QueryRowContext(ctx, query, args...)
}

func (trino *Trino) CompactTable(quotedTrinoTablePath string) {
	// ctx := context.Background()

	// _, err := trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE optimize")
	// PanicIfError(trino.Config, err)
	//
	// _, err = trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE remove_orphan_files(retention_threshold => '0m')")
	// PanicIfError(trino.Config, err)

	// _, err := trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE optimize_manifests")
	// PanicIfError(trino.Config, err)
	//
	// _, err = trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE expire_snapshots(retention_threshold => '0m')")
	// PanicIfError(trino.Config, err)
}

func (trino *Trino) CreateSchemaIfNotExists() {
	_, err := trino.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS "+trino.Schema())
	PanicIfError(trino.Config, err)
}

func (trino *Trino) CreateTableIfNotExists(tableName string, tableSchema string) {
	ctx := context.Background()

	createTableSql := "CREATE TABLE IF NOT EXISTS " + trino.Schema() + `."` + tableName + `"` + tableSchema
	_, err := trino.ExecContext(ctx, createTableSql)
	PanicIfError(trino.Config, err)
}
