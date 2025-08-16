package syncerCommon

import (
	"context"
	"database/sql"

	"github.com/BemiHQ/BemiDB/src/common"
	_ "github.com/trinodb/trino-go-client/trino"
)

const (
	TRINO_MAX_DECIMAL_PRECISION  = 38
	TRINO_FALLBACK_DECIMAL_SCALE = 6
	TRINO_MAX_QUERY_LENGTH       = 1_000_000
)

type TrinoConfig struct {
	DatabaseUrl string
	CatalogName string
}

type Trino struct {
	Config      *common.CommonConfig
	TrinoConfig *TrinoConfig
	Db          *sql.DB
	SchemaName  string
}

func NewTrino(config *common.CommonConfig, trinoConfig *TrinoConfig, schemaName string) *Trino {
	db, err := sql.Open("trino", trinoConfig.DatabaseUrl)
	common.PanicIfError(config, err)

	return &Trino{
		Config:      config,
		TrinoConfig: trinoConfig,
		Db:          db,
		SchemaName:  schemaName,
	}
}

func (trino *Trino) Schema() string {
	return trino.TrinoConfig.CatalogName + "." + trino.SchemaName
}

func (trino *Trino) Close() {
	if err := trino.Db.Close(); err != nil {
		common.PanicIfError(trino.Config, err)
	}
}

func (trino *Trino) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	common.LogDebug(trino.Config, "Trino query:", query)
	result, err := trino.Db.ExecContext(ctx, query, args...)
	if err != nil {
		common.LogError(trino.Config, "Trino query failed:", query)
		return nil, err
	}

	return result, nil
}

func (trino *Trino) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	common.LogDebug(trino.Config, "Trino query:", query)
	return trino.Db.QueryContext(ctx, query, args...)
}

func (trino *Trino) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	common.LogDebug(trino.Config, "Trino query:", query)
	return trino.Db.QueryRowContext(ctx, query, args...)
}

func (trino *Trino) CompactTable(quotedTrinoTablePath string) {
	ctx := context.Background()

	_, err := trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE optimize")
	common.PanicIfError(trino.Config, err)

	_, err = trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE remove_orphan_files(retention_threshold => '0m')")
	common.PanicIfError(trino.Config, err)

	_, err = trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE optimize_manifests")
	common.PanicIfError(trino.Config, err)

	_, err = trino.ExecContext(ctx, "ALTER TABLE "+quotedTrinoTablePath+" EXECUTE expire_snapshots(retention_threshold => '0m')")
	common.PanicIfError(trino.Config, err)
}

func (trino *Trino) CreateSchemaIfNotExists() {
	_, err := trino.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS "+trino.Schema())
	common.PanicIfError(trino.Config, err)
}

func (trino *Trino) CreateTableIfNotExists(tableName string, tableStructure string) {
	ctx := context.Background()

	createTableSql := "CREATE TABLE IF NOT EXISTS " + trino.Schema() + `."` + tableName + `"` + tableStructure
	_, err := trino.ExecContext(ctx, createTableSql)
	common.PanicIfError(trino.Config, err)
}
