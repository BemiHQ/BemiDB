package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

const (
	COMMAND_START   = "start"
	COMMAND_SYNC    = "sync"
	COMMAND_VERSION = "version"
)

func main() {
	config := LoadConfig()
	defer handlePanic(config)

	if config.LogLevel == LOG_LEVEL_TRACE {
		go enableProfiling()
	}

	command := flag.Arg(0)
	if len(flag.Args()) == 0 {
		command = COMMAND_START
	}

	switch command {
	case COMMAND_START:
		start(config)
	case COMMAND_SYNC:
		if config.Pg.SyncInterval != "" {
			duration, err := time.ParseDuration(config.Pg.SyncInterval)
			if err != nil {
				PrintErrorAndExit(config, "Invalid interval format: "+config.Pg.SyncInterval+".\n\n"+
					"Supported formats: 1h, 20m, 30s.\n"+
					"See https://github.com/BemiHQ/BemiDB#sync-command-options for more information.",
				)

			}
			LogInfo(config, "Starting sync loop with interval:", config.Pg.SyncInterval)
			for {
				syncFromPg(config)
				LogInfo(config, "Sleeping for", config.Pg.SyncInterval)
				time.Sleep(duration)
			}
		} else {
			syncFromPg(config)
		}
	case COMMAND_VERSION:
		fmt.Println("BemiDB version:", VERSION)
	default:
		PrintErrorAndExit(config, "Unknown command: "+command+".\n\n"+
			"Supported commands: "+COMMAND_START+", "+COMMAND_SYNC+", "+COMMAND_VERSION+".\n"+
			"See https://github.com/BemiHQ/BemiDB#quickstart for more information.",
		)
	}
}

func start(config *Config) {
	tcpListener := NewTcpListener(config)
	LogInfo(config, "BemiDB: Listening on", tcpListener.Addr())

	duckdb := NewDuckdb(config, true)
	LogInfo(config, "DuckDB: Connected")
	defer duckdb.Close()

	icebergReader := NewIcebergReader(config)
	queryHandler := NewQueryHandler(config, duckdb, icebergReader)

	for {
		conn := AcceptConnection(config, tcpListener)
		LogInfo(config, "BemiDB: Accepted connection from", conn.RemoteAddr())
		postgres := NewPostgres(config, &conn)

		go func() {
			postgres.Run(queryHandler)
			defer postgres.Close()
			LogInfo(config, "BemiDB: Closed connection from", conn.RemoteAddr())
		}()
	}
}

func syncFromPg(config *Config) {
	syncer := NewSyncer(config)
	syncer.SyncFromPostgres()
	LogInfo(config, "Sync from PostgreSQL completed successfully.")
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
