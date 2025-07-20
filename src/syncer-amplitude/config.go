package main

import (
	"flag"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	ENV_API_KEY    = "SOURCE_AMPLITUDE_API_KEY"
	ENV_SECRET_KEY = "SOURCE_AMPLITUDE_SECRET_KEY"
	ENV_START_DATE = "SOURCE_AMPLITUDE_START_DATE"

	DEFAULT_START_DATE = "2025-01-01"
)

type Config struct {
	BaseConfig *common.BaseConfig
	ApiKey     string
	SecretKey  string
	StartDate  time.Time
}

type configParseValues struct {
	StartDate string
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

	flag.StringVar(&_config.ApiKey, "api-key", os.Getenv(ENV_API_KEY), "Amplitude API Key")
	flag.StringVar(&_config.SecretKey, "secret-key", os.Getenv(ENV_SECRET_KEY), "Amplitude Secret Key")
	flag.StringVar(&_configParseValues.StartDate, "start-date", os.Getenv(ENV_START_DATE), "Amplitude start date in YYYY-MM-DD format")
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
	if _config.ApiKey == "" {
		panic("Amplitude API key is required")
	}
	if _config.SecretKey == "" {
		panic("Amplitude Secret key is required")
	}

	if _configParseValues.StartDate == "" {
		_configParseValues.StartDate = DEFAULT_START_DATE
	}
	parsedStartDate, err := time.Parse("2006-01-02", _configParseValues.StartDate)
	if err != nil {
		panic("Invalid start date format. Expected YYYY-MM-DD, got: " + _configParseValues.StartDate)
	}
	_config.StartDate = parsedStartDate
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}
