package common

const (
	VERSION = "trino"

	ENV_LOG_LEVEL                   = "BEMIDB_LOG_LEVEL"
	ENV_DISABLE_ANONYMOUS_ANALYTICS = "BEMIDB_DISABLE_ANONYMOUS_ANALYTICS"

	ENV_DESTINATION_SCHEMA_NAME = "DESTINATION_SCHEMA_NAME"

	ENV_TRINO_DATABASE_URL = "TRINO_DATABASE_URL"
	ENV_TRINO_CATALOG_NAME = "TRINO_CATALOG_NAME"

	ENV_AWS_REGION            = "AWS_REGION"
	ENV_AWS_S3_ENDPOINT       = "AWS_S3_ENDPOINT"
	ENV_AWS_S3_BUCKET         = "AWS_S3_BUCKET"
	ENV_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

	DEFAULT_LOG_LEVEL       = "INFO"
	DEFAULT_AWS_S3_ENDPOINT = "s3.amazonaws.com"
)

type TrinoConfig struct {
	DatabaseUrl string
	CatalogName string
}

type AwsConfig struct {
	Region          string
	S3Endpoint      string // optional
	S3Bucket        string
	AccessKeyId     string
	SecretAccessKey string
}

type BaseConfig struct {
	LogLevel                  string
	Trino                     TrinoConfig
	Aws                       AwsConfig
	DestinationSchemaName     string
	DisableAnonymousAnalytics bool
}
