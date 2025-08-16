package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"slices"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	COMMAND_START   = "start"
	COMMAND_VERSION = "version"

	DUCKDB_SCHEMA_MAIN = "main"
)

func main() {
	config := LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	if config.CommonConfig.LogLevel == common.LOG_LEVEL_TRACE {
		go enableProfiling()
	}

	tcpListener := NewTcpListener(config)
	common.LogInfo(config.CommonConfig, "BemiDB: Listening on", tcpListener.Addr())

	duckdbClient := common.NewDuckdbClient(config.CommonConfig, duckdbBootQueris(config))
	common.LogInfo(config.CommonConfig, "DuckDB: Connected")
	defer duckdbClient.Close()

	catalog := NewIcebergCatalog(config)

	icebergReader := NewIcebergReader(config, catalog)
	queryHandler := NewQueryHandler(config, duckdbClient, icebergReader)

	for {
		conn := AcceptConnection(config, tcpListener)
		common.LogInfo(config.CommonConfig, "BemiDB: Accepted connection from", conn.RemoteAddr())
		server := NewPostgresServer(config, &conn)

		go func() {
			server.Run(queryHandler)
			defer server.Close()
			common.LogInfo(config.CommonConfig, "BemiDB: Closed connection from", conn.RemoteAddr())
		}()
	}
}

func duckdbBootQueris(config *Config) []string {
	return slices.Concat(
		[]string{
			// Set up Iceberg
			"INSTALL iceberg",
			"LOAD iceberg",

			// Set up schemas
			"SELECT oid FROM pg_catalog.pg_namespace",
			"CREATE SCHEMA public",

			// Configure DuckDB
			"SET scalar_subquery_error_on_multiple_rows=false",
		},

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

func enableProfiling() {
	func() { log.Println(http.ListenAndServe(":6060", nil)) }()
}
