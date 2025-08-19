package main

import (
	"flag"
	"os"
	"slices"
	"strings"
)

const (
	VERSION = "trino"

	ENV_PORT     = "BEMIDB_PORT"
	ENV_DATABASE = "BEMIDB_DATABASE"
	ENV_USER     = "BEMIDB_USER"
	ENV_PASSWORD = "BEMIDB_PASSWORD"
	ENV_HOST     = "BEMIDB_HOST"

	ENV_LOG_LEVEL                   = "BEMIDB_LOG_LEVEL"
	ENV_DISABLE_ANONYMOUS_ANALYTICS = "BEMIDB_DISABLE_ANONYMOUS_ANALYTICS"

	ENV_CATALOG_DATABASE_URL = "CATALOG_DATABASE_URL"

	ENV_AWS_REGION            = "AWS_REGION"
	ENV_AWS_S3_ENDPOINT       = "AWS_S3_ENDPOINT"
	ENV_AWS_S3_BUCKET         = "AWS_S3_BUCKET"
	ENV_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

	DEFAULT_LOG_LEVEL       = "INFO"
	DEFAULT_HOST            = "0.0.0.0"
	DEFAULT_PORT            = "54321"
	DEFAULT_DATABASE        = "bemidb"
	DEFAULT_AWS_S3_ENDPOINT = "s3.amazonaws.com"
)

type AwsConfig struct {
	Region          string
	S3Endpoint      string // optional
	S3Bucket        string
	AccessKeyId     string
	SecretAccessKey string
}

type Config struct {
	Host                      string
	Port                      string
	Database                  string
	User                      string
	EncryptedPassword         string
	LogLevel                  string
	CatalogDatabaseUrl        string
	Aws                       AwsConfig
	DisableAnonymousAnalytics bool
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
	flag.StringVar(&_config.Host, "host", os.Getenv(ENV_HOST), "Host for BemiDB to listen on")
	flag.StringVar(&_config.Port, "port", os.Getenv(ENV_PORT), "Port for BemiDB to listen on")
	flag.StringVar(&_config.Database, "database", os.Getenv(ENV_DATABASE), "Database name")
	flag.StringVar(&_config.User, "user", os.Getenv(ENV_USER), "Database user")
	flag.StringVar(&_configParseValues.password, "password", os.Getenv(ENV_PASSWORD), "Database password")
	flag.StringVar(&_config.LogLevel, "log-level", os.Getenv(ENV_LOG_LEVEL), "Log level: \"ERROR\", \"WARN\", \"INFO\", \"DEBUG\", \"TRACE\". Default: \""+DEFAULT_LOG_LEVEL+`"`)
	flag.StringVar(&_config.CatalogDatabaseUrl, "catalog-database-url", os.Getenv(ENV_CATALOG_DATABASE_URL), "Catalog database URL")
	flag.StringVar(&_config.Aws.Region, "aws-region", os.Getenv(ENV_AWS_REGION), "AWS region")
	flag.StringVar(&_config.Aws.S3Endpoint, "aws-s3-endpoint", os.Getenv(ENV_AWS_S3_ENDPOINT), "AWS S3 endpoint. Default: \""+DEFAULT_AWS_S3_ENDPOINT+`"`)
	flag.StringVar(&_config.Aws.S3Bucket, "aws-s3-bucket", os.Getenv(ENV_AWS_S3_BUCKET), "AWS S3 bucket name")
	flag.StringVar(&_config.Aws.AccessKeyId, "aws-access-key-id", os.Getenv(ENV_AWS_ACCESS_KEY_ID), "AWS access key ID")
	flag.StringVar(&_config.Aws.SecretAccessKey, "aws-secret-access-key", os.Getenv(ENV_AWS_SECRET_ACCESS_KEY), "AWS secret access key")
	flag.BoolVar(&_config.DisableAnonymousAnalytics, "disable-anonymous-analytics", os.Getenv(ENV_DISABLE_ANONYMOUS_ANALYTICS) == "true", "Disable anonymous analytics collection")
}

func parseFlags() {
	flag.Parse()

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

	if _config.LogLevel == "" {
		_config.LogLevel = DEFAULT_LOG_LEVEL
	} else if !slices.Contains(LOG_LEVELS, _config.LogLevel) {
		panic("Invalid log level " + _config.LogLevel + ". Must be one of " + strings.Join(LOG_LEVELS, ", "))
	}
	if _config.CatalogDatabaseUrl == "" {
		panic("Catalog database URL is required")
	}

	if _config.Aws.Region == "" {
		panic("AWS region is required")
	}
	if _config.Aws.S3Endpoint == "" {
		_config.Aws.S3Endpoint = DEFAULT_AWS_S3_ENDPOINT
	}
	if _config.Aws.S3Bucket == "" {
		panic("AWS S3 bucket name is required")
	}
	if _config.Aws.AccessKeyId != "" && _config.Aws.SecretAccessKey == "" {
		panic("AWS secret access key is required")
	}
	if _config.Aws.AccessKeyId == "" && _config.Aws.SecretAccessKey != "" {
		panic("AWS access key ID is required")
	}

	_configParseValues = configParseValues{}
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}
