package main

import (
	"flag"
	"os"
	"slices"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	VERSION = "1.0.0-beta.2"

	ENV_PORT     = "BEMIDB_PORT"
	ENV_DATABASE = "BEMIDB_DATABASE"
	ENV_USER     = "BEMIDB_USER"
	ENV_PASSWORD = "BEMIDB_PASSWORD"
	ENV_HOST     = "BEMIDB_HOST"

	DEFAULT_LOG_LEVEL       = "INFO"
	DEFAULT_HOST            = "0.0.0.0"
	DEFAULT_PORT            = "54321"
	DEFAULT_DATABASE        = "bemidb"
	DEFAULT_AWS_S3_ENDPOINT = "s3.amazonaws.com"
)

type Config struct {
	CommonConfig      *common.CommonConfig
	Host              string
	Port              string
	Database          string
	User              string
	EncryptedPassword string
}

type configParseValues struct {
	password string
}

var _config Config
var _configParseValues configParseValues

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

	flag.StringVar(&_config.Host, "host", os.Getenv(ENV_HOST), "Host for BemiDB to listen on")
	flag.StringVar(&_config.Port, "port", os.Getenv(ENV_PORT), "Port for BemiDB to listen on")
	flag.StringVar(&_config.Database, "database", os.Getenv(ENV_DATABASE), "Database name")
	flag.StringVar(&_config.User, "user", os.Getenv(ENV_USER), "Database user")
	flag.StringVar(&_configParseValues.password, "password", os.Getenv(ENV_PASSWORD), "Database password")
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

	if _config.Host == "" {
		_config.Host = DEFAULT_HOST
	}
	if _config.Port == "" {
		_config.Port = DEFAULT_PORT
	}
	if _config.Database == "" {
		_config.Database = DEFAULT_DATABASE
	}
	if _configParseValues.password != "" {
		_config.EncryptedPassword = StringToScramSha256(_configParseValues.password)
	}

	_configParseValues = configParseValues{}
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}
