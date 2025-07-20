package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

const (
	COMMAND_START   = "start"
	COMMAND_VERSION = "version"
)

func main() {
	config := LoadConfig()
	defer handlePanic(config)

	if config.LogLevel == LOG_LEVEL_TRACE {
		go enableProfiling()
	}

	tcpListener := NewTcpListener(config)
	LogInfo(config, "BemiDB: Listening on", tcpListener.Addr())

	duckdb := NewDuckdb(config, true)
	LogInfo(config, "DuckDB: Connected")
	defer duckdb.Close()

	catalog := NewIcebergCatalog(config)

	icebergReader := NewIcebergReader(config, catalog)
	queryHandler := NewQueryHandler(config, duckdb, icebergReader)

	for {
		conn := AcceptConnection(config, tcpListener)
		LogInfo(config, "BemiDB: Accepted connection from", conn.RemoteAddr())
		server := NewPostgresServer(config, &conn)

		go func() {
			server.Run(queryHandler)
			defer server.Close()
			LogInfo(config, "BemiDB: Closed connection from", conn.RemoteAddr())
		}()
	}
}

func enableProfiling() {
	func() { log.Println(http.ListenAndServe(":6060", nil)) }()
}

func handlePanic(config *Config) {
	func() {
		if r := recover(); r != nil {
			err, _ := r.(error)
			HandleUnexpectedError(config, err)
		}
	}()
}
