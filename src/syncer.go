package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	MAX_IN_MEMORY_BUFFER_SIZE = 128 * 1024 * 1024 // 128 MB (expands to ~160 MB memory usage)
	MAX_PG_ROWS_BATCH_SIZE    = 1 * 1024 * 1024   // 1 MB
	PING_PG_INTERVAL_SECONDS  = 24

	MAX_PARQUET_PAYLOAD_THRESHOLD = 4 * 1024 * 1024 * 1024 // 4 GB (compressed to ~512 MB Parquet)
)

type Syncer struct {
	config        *Config
	icebergWriter *IcebergWriter
	icebergReader *IcebergReader
	syncerTable   *SyncerTable
}

func NewSyncer(config *Config) *Syncer {
	if config.Pg.DatabaseUrl == "" {
		PrintErrorAndExit(config, "Missing PostgreSQL database URL.\n\n"+
			"See https://github.com/BemiHQ/BemiDB#sync-command-options for more information.",
		)
	}

	icebergWriter := NewIcebergWriter(config)
	icebergReader := NewIcebergReader(config)
	return &Syncer{
		config:        config,
		icebergWriter: icebergWriter,
		icebergReader: icebergReader,
		syncerTable:   NewSyncerTable(config),
	}
}

func (syncer *Syncer) SyncFromPostgres() {
	ctx := context.Background()
	syncer.sendAnonymousAnalytics("sync-start")

	databaseUrl := syncer.urlEncodePassword(syncer.config.Pg.DatabaseUrl)
	icebergSchemaTables, icebergSchemaTablesErr := syncer.icebergReader.SchemaTables()

	structureConn := syncer.newConnection(ctx, databaseUrl)
	defer structureConn.Close(ctx)

	copyConn := syncer.newConnection(ctx, databaseUrl)
	defer copyConn.Close(ctx)

	syncedPgSchemaTables := []PgSchemaTable{}

	for _, schema := range syncer.listPgSchemas(structureConn) {
		for _, pgSchemaTable := range syncer.listPgSchemaTables(structureConn, schema) {
			if syncer.shouldSyncTable(pgSchemaTable) {
				var internalTableMetadata InternalTableMetadata
				syncedPreviously := icebergSchemaTablesErr == nil && icebergSchemaTables.Contains(pgSchemaTable.ToIcebergSchemaTable())
				if syncedPreviously {
					internalTableMetadata = syncer.readInternalTableMetadata(pgSchemaTable)
				}

				incrementalRefresh := syncer.config.Pg.IncrementallyRefreshedTables != nil && HasExactOrWildcardMatch(syncer.config.Pg.IncrementallyRefreshedTables, pgSchemaTable.ToConfigArg())

				syncer.syncerTable.SyncPgTable(pgSchemaTable, structureConn, copyConn, internalTableMetadata, incrementalRefresh)
				LogInfo(syncer.config, "Finished writing to Iceberg\n")

				syncedPgSchemaTables = append(syncedPgSchemaTables, pgSchemaTable)
			}
		}
	}

	syncer.WriteInternalStartSqlFile(syncedPgSchemaTables)

	if !syncer.config.Pg.PreserveUnsynced {
		syncer.deleteOldIcebergSchemaTables(syncedPgSchemaTables)
	}

	syncer.sendAnonymousAnalytics("sync-finish")
}

func (syncer *Syncer) WriteInternalStartSqlFile(pgSchemaTables []PgSchemaTable) {
	childTablesByParentTable := make(map[string][]string)
	for _, pgSchemaTable := range pgSchemaTables {
		if pgSchemaTable.ParentPartitionedTable != "" {
			parent := pgSchemaTable.ParentPartitionedTableString()
			childTablesByParentTable[parent] = append(childTablesByParentTable[parent], pgSchemaTable.String())
		}
	}

	queryRemapper := NewQueryRemapper(syncer.config, syncer.icebergReader, nil)
	queries := []string{}

	for parent, children := range childTablesByParentTable {
		// CREATE OR REPLACE TABLE test_table AS
		//   SELECT * FROM iceberg_scan('/iceberg/public/test_table_q1/metadata/v1.metadata.json', skip_schema_inference = true)
		//   UNION ALL
		//   SELECT * FROM iceberg_scan('/iceberg/public/test_table_q2/metadata/v1.metadata.json', skip_schema_inference = true)

		subqueries := []string{}
		for _, child := range children {
			originalSubquery := fmt.Sprintf("SELECT * FROM %s", child)
			queryStatements, _, err := queryRemapper.ParseAndRemapQuery(originalSubquery)
			PanicIfError(syncer.config, err)
			subqueries = append(subqueries, queryStatements[0])
		}
		queries = append(queries, fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s", parent, strings.Join(subqueries, " UNION ALL ")))
	}

	syncer.icebergWriter.WriteInternalStartSqlFile(queries)
}

// Example:
// - From postgres://username:pas$:wor^d@host:port/database
// - To postgres://username:pas%24%3Awor%5Ed@host:port/database
func (syncer *Syncer) urlEncodePassword(databaseUrl string) string {
	// No credentials
	if !strings.Contains(databaseUrl, "@") {
		return databaseUrl
	}

	password := strings.TrimPrefix(databaseUrl, "postgresql://")
	password = strings.TrimPrefix(password, "postgres://")
	passwordEndIndex := strings.LastIndex(password, "@")
	password = password[:passwordEndIndex]

	// Credentials without password
	if !strings.Contains(password, ":") {
		return databaseUrl
	}

	_, password, _ = strings.Cut(password, ":")
	decodedPassword, err := url.QueryUnescape(password)
	if err != nil {
		return databaseUrl
	}

	// Password is already encoded
	if decodedPassword != password {
		return databaseUrl
	}

	return strings.Replace(databaseUrl, ":"+password+"@", ":"+url.QueryEscape(password)+"@", 1)
}

func (syncer *Syncer) shouldSyncTable(pgSchemaTable PgSchemaTable) bool {
	if syncer.config.Pg.ExcludeTables != nil && HasExactOrWildcardMatch(syncer.config.Pg.ExcludeTables, pgSchemaTable.ToConfigArg()) {
		return false
	}

	if syncer.config.Pg.IncludeTables != nil {
		return HasExactOrWildcardMatch(syncer.config.Pg.IncludeTables, pgSchemaTable.ToConfigArg())
	}

	return true
}

func (syncer *Syncer) listPgSchemas(conn *pgx.Conn) []string {
	var schemas []string

	schemasRows, err := conn.Query(
		context.Background(),
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'pg_toast', 'information_schema')",
	)
	PanicIfError(syncer.config, err)
	defer schemasRows.Close()

	for schemasRows.Next() {
		var schema string
		err = schemasRows.Scan(&schema)
		PanicIfError(syncer.config, err)
		schemas = append(schemas, schema)
	}

	return schemas
}

func (syncer *Syncer) listPgSchemaTables(conn *pgx.Conn, schema string) []PgSchemaTable {
	var pgSchemaTables []PgSchemaTable

	tablesRows, err := conn.Query(
		context.Background(),
		`
		SELECT pg_class.relname AS table, COALESCE(parent.relname, '') AS parent_partitioned_table
		FROM pg_class
		JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
		LEFT JOIN pg_inherits ON pg_inherits.inhrelid = pg_class.oid
		LEFT JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
		WHERE pg_namespace.nspname = $1 AND pg_class.relkind = 'r';
		`,
		schema,
	)
	PanicIfError(syncer.config, err)
	defer tablesRows.Close()

	for tablesRows.Next() {
		pgSchemaTable := PgSchemaTable{Schema: schema}
		err = tablesRows.Scan(&pgSchemaTable.Table, &pgSchemaTable.ParentPartitionedTable)
		PanicIfError(syncer.config, err)
		pgSchemaTables = append(pgSchemaTables, pgSchemaTable)
	}

	return pgSchemaTables
}

func (syncer *Syncer) newConnection(ctx context.Context, databaseUrl string) *pgx.Conn {
	conn, err := pgx.Connect(ctx, databaseUrl)
	PanicIfError(syncer.config, err)

	_, err = conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
	PanicIfError(syncer.config, err)

	return conn
}

func (syncer *Syncer) readInternalTableMetadata(pgSchemaTable PgSchemaTable) InternalTableMetadata {
	internalTableMetadata, err := syncer.icebergReader.InternalTableMetadata(pgSchemaTable)
	PanicIfError(syncer.config, err)
	return internalTableMetadata
}

func (syncer *Syncer) deleteOldIcebergSchemaTables(pgSchemaTables []PgSchemaTable) {
	var prefixedPgSchemaTables []PgSchemaTable
	for _, pgSchemaTable := range pgSchemaTables {
		prefixedPgSchemaTables = append(
			prefixedPgSchemaTables,
			PgSchemaTable{Schema: syncer.config.Pg.SchemaPrefix + pgSchemaTable.Schema, Table: pgSchemaTable.Table},
		)
	}

	icebergSchemas, err := syncer.icebergReader.Schemas()
	PanicIfError(syncer.config, err)

	for _, icebergSchema := range icebergSchemas {
		found := false
		for _, pgSchemaTable := range prefixedPgSchemaTables {
			if icebergSchema == pgSchemaTable.Schema {
				found = true
				break
			}
		}

		if !found {
			LogInfo(syncer.config, "Deleting", icebergSchema, "...")
			err := syncer.icebergWriter.DeleteSchema(icebergSchema)
			PanicIfError(syncer.config, err)
		}
	}

	icebergSchemaTables, err := syncer.icebergReader.SchemaTables()
	PanicIfError(syncer.config, err)

	for _, icebergSchemaTable := range icebergSchemaTables.Values() {
		found := false
		for _, pgSchemaTable := range prefixedPgSchemaTables {
			if icebergSchemaTable.String() == pgSchemaTable.String() {
				found = true
				break
			}
		}

		if !found {
			LogInfo(syncer.config, "Deleting", icebergSchemaTable.String(), "...")
			err := syncer.icebergWriter.DeleteSchemaTable(icebergSchemaTable)
			PanicIfError(syncer.config, err)
		}
	}
}

type AnonymousAnalyticsData struct {
	Command string `json:"command"`
	OsName  string `json:"osName"`
	Version string `json:"version"`
	PgHost  string `json:"pgHost"`
}

func (syncer *Syncer) sendAnonymousAnalytics(command string) {
	if syncer.config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousAnalyticsData{
		Command: command,
		OsName:  runtime.GOOS + "-" + runtime.GOARCH,
		Version: VERSION,
		PgHost:  ParseDatabaseHost(syncer.config.Pg.DatabaseUrl),
	}
	if data.PgHost == "" || IsLocalHost(data.PgHost) {
		return
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("https://api.bemidb.com/api/analytics", "application/json", bytes.NewBuffer(jsonData))
}
