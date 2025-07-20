package main

import (
	"flag"
	"os"
	"slices"
	"strings"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

type SyncMode string

const (
	SyncModeFullRefresh SyncMode = "FULL_REFRESH"
	SyncModeCDC         SyncMode = "CDC"
	SyncModeIncremental SyncMode = "INCREMENTAL"

	ENV_NATS_URL                   = "NATS_URL"
	ENV_NATS_STREAM                = "NATS_JETSTREAM_STREAM"
	ENV_NATS_SUBJECT               = "NATS_JETSTREAM_SUBJECT"
	ENV_NATS_CONSUMER_NAME         = "NATS_JETSTREAM_CONSUMER_NAME"
	ENV_NATS_FETCH_TIMEOUT_SECONDS = "NATS_FETCH_TIMEOUT_SECONDS"

	ENV_DATABASE_URL          = "SOURCE_POSTGRES_DATABASE_URL"
	ENV_SYNC_MODE             = "SOURCE_POSTGRES_SYNC_MODE"
	ENV_INCLUDE_SCHEMAS       = "SOURCE_POSTGRES_INCLUDE_SCHEMAS"
	ENV_INCLUDE_TABLES        = "SOURCE_POSTGRES_INCLUDE_TABLES"
	ENV_EXCLUDE_TABLES        = "SOURCE_POSTGRES_EXCLUDE_TABLES"
	ENV_CURSOR_COLUMNS        = "SOURCE_POSTGRES_CURSOR_COLUMNS"
	ENV_IGNORE_UPDATE_COLUMNS = "SOURCE_POSTGRES_IGNORE_UPDATE_COLUMNS"

	DEFAULT_NATS_FETCH_TIMEOUT_SECONDS = 30
)

type NatsConfig struct {
	Url                 string
	Stream              string
	Subject             string
	ConsumerName        string
	FetchTimeoutSeconds int
}

type Config struct {
	BaseConfig                  *common.BaseConfig
	SyncMode                    SyncMode
	DatabaseUrl                 string
	IncludeSchemas              common.Set[string]
	IncludeTables               common.Set[string]
	ExcludeTables               common.Set[string]
	CursorColumnNameByTableName map[string]string  // Incremental sync
	IgnoreUpdateColumns         common.Set[string] // CDC sync
	Nats                        NatsConfig         // CDC sync
}

type configParseValues struct {
	IncludeSchemas      string
	IncludeTables       string
	ExcludeTables       string
	IgnoreUpdateColumns string
	CursorColumns       string
}

var _config Config
var _configParseValues configParseValues

func init() {
	registerFlags()
}

func registerFlags() {
	_config.BaseConfig = &common.BaseConfig{}

	flag.StringVar(&_config.BaseConfig.LogLevel, "log-level", os.Getenv(common.ENV_LOG_LEVEL), `Log level: "ERROR", "WARN", "INFO", "DEBUG", "TRACE". Default: "`+common.DEFAULT_LOG_LEVEL+`"`)
	flag.StringVar(&_config.BaseConfig.DestinationSchemaName, "destination-schema-name", os.Getenv(common.ENV_DESTINATION_SCHEMA_NAME), "Destination schema name to store the synced data")
	flag.StringVar(&_config.BaseConfig.Trino.DatabaseUrl, "trino-database-url", os.Getenv(common.ENV_TRINO_DATABASE_URL), "Trino database URL to sync to")
	flag.StringVar(&_config.BaseConfig.Trino.CatalogName, "trino-catalog-name", os.Getenv(common.ENV_TRINO_CATALOG_NAME), "Trino catalog name")
	flag.StringVar(&_config.BaseConfig.Aws.Region, "aws-region", os.Getenv(common.ENV_AWS_REGION), "AWS region")
	flag.StringVar(&_config.BaseConfig.Aws.S3Endpoint, "aws-s3-endpoint", os.Getenv(common.ENV_AWS_S3_ENDPOINT), "AWS S3 endpoint. Default: \""+common.DEFAULT_AWS_S3_ENDPOINT+`"`)
	flag.StringVar(&_config.BaseConfig.Aws.S3Bucket, "aws-s3-bucket", os.Getenv(common.ENV_AWS_S3_BUCKET), "AWS S3 bucket name")
	flag.StringVar(&_config.BaseConfig.Aws.AccessKeyId, "aws-access-key-id", os.Getenv(common.ENV_AWS_ACCESS_KEY_ID), "AWS access key ID")
	flag.StringVar(&_config.BaseConfig.Aws.SecretAccessKey, "aws-secret-access-key", os.Getenv(common.ENV_AWS_SECRET_ACCESS_KEY), "AWS secret access key")
	flag.BoolVar(&_config.BaseConfig.DisableAnonymousAnalytics, "disable-anonymous-analytics", os.Getenv(common.ENV_DISABLE_ANONYMOUS_ANALYTICS) == "true", "Disable anonymous analytics collection")

	flag.StringVar(&_config.DatabaseUrl, "database-url", os.Getenv(ENV_DATABASE_URL), "PostgreSQL database URL")
	flag.StringVar((*string)(&_config.SyncMode), "sync-mode", os.Getenv(ENV_SYNC_MODE), `Sync mode: "FULL_REFRESH", "CDC", or "INCREMENTAL"`)
	flag.StringVar(&_configParseValues.IncludeSchemas, "include-schemas", os.Getenv(ENV_INCLUDE_SCHEMAS), "Comma-separated list of schemas to include in the sync. Default: all schemas included")
	flag.StringVar(&_configParseValues.IncludeTables, "include-tables", os.Getenv(ENV_INCLUDE_TABLES), "Comma-separated list of tables to include in the sync. Default: all tables included")
	flag.StringVar(&_configParseValues.ExcludeTables, "exclude-tables", os.Getenv(ENV_EXCLUDE_TABLES), "Comma-separated list of tables to exclude from the sync. Default: no tables excluded")
	flag.StringVar(&_configParseValues.CursorColumns, "cursor-columns", os.Getenv(ENV_CURSOR_COLUMNS), "Cursor columns to use for incremental sync. Format: schema.table=column,schema2.table2=column2. Default: no cursor columns specified")
	flag.StringVar(&_configParseValues.IgnoreUpdateColumns, "ignore-update-columns", os.Getenv(ENV_IGNORE_UPDATE_COLUMNS), "Comma-separated list of columns to ignore for updates in CDC mode. Default: no columns ignored")
	flag.StringVar(&_config.Nats.Url, "nats-url", os.Getenv(ENV_NATS_URL), "NATS URL")
	flag.StringVar(&_config.Nats.Stream, "nats-stream", os.Getenv(ENV_NATS_STREAM), "NATS stream to read from")
	flag.StringVar(&_config.Nats.Subject, "nats-subject", os.Getenv(ENV_NATS_SUBJECT), "NATS subject to read from")
	flag.StringVar(&_config.Nats.ConsumerName, "nats-consumer-name", os.Getenv(ENV_NATS_CONSUMER_NAME), "NATS consumer name for the JetStream consumer")
	flag.IntVar(&_config.Nats.FetchTimeoutSeconds, "nats-fetch-timeout-seconds", DEFAULT_NATS_FETCH_TIMEOUT_SECONDS, "NATS fetch timeout in seconds")
	fetchTimeoutSeconds := os.Getenv(ENV_NATS_FETCH_TIMEOUT_SECONDS)
	if fetchTimeoutSeconds != "" {
		_config.Nats.FetchTimeoutSeconds = common.StringToInt(fetchTimeoutSeconds)
	}
}

func parseFlags() {
	flag.Parse()

	if _config.BaseConfig.LogLevel == "" {
		_config.BaseConfig.LogLevel = common.DEFAULT_LOG_LEVEL
	} else if !slices.Contains(common.LOG_LEVELS, _config.BaseConfig.LogLevel) {
		panic("Invalid log level " + _config.BaseConfig.LogLevel + ". Must be one of " + strings.Join(common.LOG_LEVELS, ", "))
	}
	if _config.BaseConfig.DestinationSchemaName == "" {
		panic("Destination schema name is required")
	}
	if _config.BaseConfig.Trino.DatabaseUrl == "" {
		panic("Trino database URL is required")
	}
	if _config.BaseConfig.Trino.CatalogName == "" {
		panic("Trino catalog name is required")
	}

	if _configParseValues.IncludeSchemas != "" {
		_config.IncludeSchemas = common.NewSet[string]().AddAll(strings.Split(_configParseValues.IncludeSchemas, ","))
	}
	if _configParseValues.IncludeTables != "" && _configParseValues.ExcludeTables != "" {
		panic("Cannot specify both include-tables and exclude-tables. Please use one or the other.")
	}
	if _configParseValues.IncludeTables != "" {
		_config.IncludeTables = common.NewSet[string]().AddAll(strings.Split(_configParseValues.IncludeTables, ","))
	}
	if _configParseValues.ExcludeTables != "" {
		_config.ExcludeTables = common.NewSet[string]().AddAll(strings.Split(_configParseValues.ExcludeTables, ","))
	}
	if _configParseValues.IgnoreUpdateColumns != "" {
		_config.IgnoreUpdateColumns = common.NewSet[string]().AddAll(strings.Split(_configParseValues.IgnoreUpdateColumns, ","))
	}
	if _configParseValues.CursorColumns != "" {
		_config.CursorColumnNameByTableName = make(map[string]string)
		cursorColumns := strings.Split(_configParseValues.CursorColumns, ",")
		for _, cursorColumn := range cursorColumns {
			parts := strings.Split(cursorColumn, "=")
			if len(parts) != 2 {
				panic("Invalid cursor column format. Expected schema.table=column, got: " + cursorColumn)
			}
			_config.CursorColumnNameByTableName[parts[0]] = parts[1]
		}
	}

	if _config.SyncMode == "" {
		panic("Sync mode is required")
	} else if _config.SyncMode != SyncModeFullRefresh && _config.SyncMode != SyncModeCDC && _config.SyncMode != SyncModeIncremental {
		panic("Invalid sync mode " + string(_config.SyncMode) + ". Must be one of FULL_REFRESH, CDC, or INCREMENTAL")
	}

	switch _config.SyncMode {
	case SyncModeCDC:
		if _config.Nats.Url == "" {
			panic("NATS URL is required")
		}
		if _config.Nats.Stream == "" {
			panic("NATS stream is required")
		}
		if _config.Nats.Subject == "" {
			panic("NATS subject is required")
		}
		if _config.Nats.FetchTimeoutSeconds <= 0 {
			panic("NATS fetch timeout must be greater than 0")
		}
	}

	if _config.DatabaseUrl == "" {
		panic("Source PostgreSQL database URL is required")
	}
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}
