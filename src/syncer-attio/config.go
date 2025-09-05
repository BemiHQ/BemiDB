package main

import (
	"flag"
	"os"
	"slices"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	ENV_DESTINATION_SCHEMA_NAME = "DESTINATION_SCHEMA_NAME"
	ENV_API_ACCESS_TOKEN        = "SOURCE_ATTIO_API_ACCESS_TOKEN"
)

type Config struct {
	CommonConfig          *common.CommonConfig
	DestinationSchemaName string
	ApiAccessToken        string
}

var _config Config

func init() {
	registerFlags()
}

func registerFlags() {
	_config.CommonConfig = &common.CommonConfig{}

	flag.StringVar(&_config.CommonConfig.LogLevel, "log-level", os.Getenv(common.ENV_LOG_LEVEL), `Log level: "ERROR", "WARN", "INFO", "DEBUG", "TRACE". Default: "`+common.DEFAULT_LOG_LEVEL+`"`)
	flag.StringVar(&_config.CommonConfig.CatalogDatabaseUrl, "catalog-database-url", os.Getenv(common.ENV_CATALOG_DATABASE_URL), "Catalog database URL")
	flag.StringVar(&_config.CommonConfig.Aws.Region, "aws-region", os.Getenv(common.ENV_AWS_REGION), "AWS region")
	flag.StringVar(&_config.CommonConfig.Aws.S3Endpoint, "aws-s3-endpoint", os.Getenv(common.ENV_AWS_S3_ENDPOINT), "AWS S3 endpoint. Default: \""+common.DEFAULT_AWS_S3_ENDPOINT+`"`)
	flag.StringVar(&_config.CommonConfig.Aws.S3Bucket, "aws-s3-bucket", os.Getenv(common.ENV_AWS_S3_BUCKET), "AWS S3 bucket name")
	flag.StringVar(&_config.CommonConfig.Aws.AccessKeyId, "aws-access-key-id", os.Getenv(common.ENV_AWS_ACCESS_KEY_ID), "AWS access key ID")
	flag.StringVar(&_config.CommonConfig.Aws.SecretAccessKey, "aws-secret-access-key", os.Getenv(common.ENV_AWS_SECRET_ACCESS_KEY), "AWS secret access key")
	flag.BoolVar(&_config.CommonConfig.DisableAnonymousAnalytics, "disable-anonymous-analytics", os.Getenv(common.ENV_DISABLE_ANONYMOUS_ANALYTICS) == "true", "Disable anonymous analytics collection")

	flag.StringVar(&_config.DestinationSchemaName, "destination-schema-name", os.Getenv(ENV_DESTINATION_SCHEMA_NAME), "Destination schema name to store the synced data")
	flag.StringVar(&_config.ApiAccessToken, "api-access-token", os.Getenv(ENV_API_ACCESS_TOKEN), "Attio API Key")
}

func parseFlags() {
	flag.Parse()

	if _config.CommonConfig.LogLevel == "" {
		_config.CommonConfig.LogLevel = common.DEFAULT_LOG_LEVEL
	} else if !slices.Contains(common.LOG_LEVELS, _config.CommonConfig.LogLevel) {
		panic("Invalid log level " + _config.CommonConfig.LogLevel + ". Must be one of " + strings.Join(common.LOG_LEVELS, ", "))
	}
	if _config.CommonConfig.CatalogDatabaseUrl == "" {
		panic("Catalog database URL is required")
	}
	if _config.CommonConfig.Aws.Region == "" {
		panic("AWS region is required")
	}
	if _config.CommonConfig.Aws.S3Endpoint == "" {
		_config.CommonConfig.Aws.S3Endpoint = common.DEFAULT_AWS_S3_ENDPOINT
	}
	if _config.CommonConfig.Aws.S3Bucket == "" {
		panic("AWS S3 bucket name is required")
	}
	if _config.CommonConfig.Aws.AccessKeyId != "" && _config.CommonConfig.Aws.SecretAccessKey == "" {
		panic("AWS secret access key is required")
	}
	if _config.CommonConfig.Aws.AccessKeyId == "" && _config.CommonConfig.Aws.SecretAccessKey != "" {
		panic("AWS access key ID is required")
	}

	if _config.DestinationSchemaName == "" {
		panic("Destination schema name is required")
	}
	if _config.ApiAccessToken == "" {
		panic("Attio API key is required")
	}
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}
