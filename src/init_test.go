package main

import (
	"flag"
	"os"
)

var PUBLIC_TEST_TABLE_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{
		ColumnName:       "id",
		DataType:         "integer",
		UdtName:          "int4",
		IsNullable:       "NO",
		NumericPrecision: "32",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:             "bit_column",
		DataType:               "bit",
		UdtName:                "bit",
		CharacterMaximumLength: "1",
		Namespace:              "pg_catalog",
	},
	{
		ColumnName: "bool_column",
		DataType:   "boolean",
		UdtName:    "bool",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName:             "bpchar_column",
		DataType:               "character",
		UdtName:                "bpchar",
		CharacterMaximumLength: "10",
		Namespace:              "pg_catalog",
	},
	{
		ColumnName:             "varchar_column",
		DataType:               "character varying",
		UdtName:                "varchar",
		CharacterMaximumLength: "255",
		Namespace:              "pg_catalog",
	},
	{
		ColumnName: "text_column",
		DataType:   "text",
		UdtName:    "text",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName:       "int2_column",
		DataType:         "smallint",
		UdtName:          "int2",
		NumericPrecision: "16",
		NumericScale:     "0",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "int4_column",
		DataType:         "integer",
		UdtName:          "int4",
		NumericPrecision: "32",
		NumericScale:     "0",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "int8_column",
		DataType:         "bigint",
		UdtName:          "int8",
		NumericPrecision: "64",
		NumericScale:     "0",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "hugeint_column",
		DataType:         "numeric",
		UdtName:          "numeric",
		NumericPrecision: "20", // Will be capped to 38
		NumericScale:     "0",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName: "xid_column",
		DataType:   "xid",
		UdtName:    "xid",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "xid8_column",
		DataType:   "xid8",
		UdtName:    "xid8",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName:       "float4_column",
		DataType:         "real",
		UdtName:          "float4",
		NumericPrecision: "24",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "float8_column",
		DataType:         "double precision",
		UdtName:          "float8",
		NumericPrecision: "53",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "numeric_column",
		DataType:         "numeric",
		UdtName:          "numeric",
		NumericPrecision: "40", // Will be capped to 38
		NumericScale:     "2",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "numeric_column_without_precision",
		DataType:         "numeric",
		UdtName:          "numeric",
		NumericPrecision: "0", // Will be changed to 19
		NumericScale:     "0", // Will be changed to 19
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:        "date_column",
		DataType:          "date",
		UdtName:           "date",
		DatetimePrecision: "0",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "time_column",
		DataType:          "time without time zone",
		UdtName:           "time",
		DatetimePrecision: "6",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timeMsColumn",
		DataType:          "time without time zone",
		UdtName:           "time",
		DatetimePrecision: "3",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timetz_column",
		DataType:          "time with time zone",
		UdtName:           "timetz",
		DatetimePrecision: "6",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timetz_ms_column",
		DataType:          "time with time zone",
		UdtName:           "timetz",
		DatetimePrecision: "3",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timestamp_column",
		DataType:          "timestamp without time zone",
		UdtName:           "timestamp",
		DatetimePrecision: "6",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timestamp_ms_column",
		DataType:          "timestamp without time zone",
		UdtName:           "timestamp",
		DatetimePrecision: "3",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timestamptz_column",
		DataType:          "timestamp with time zone",
		UdtName:           "timestamptz",
		DatetimePrecision: "6",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timestamptz_ms_column",
		DataType:          "timestamp with time zone",
		UdtName:           "timestamptz",
		DatetimePrecision: "3",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName:        "timestamptz_column_timezone_mins",
		DataType:          "timestamp with time zone",
		UdtName:           "timestamptz",
		DatetimePrecision: "6",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName: "uuid_column",
		DataType:   "uuid",
		UdtName:    "uuid",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "bytea_column",
		DataType:   "bytea",
		UdtName:    "bytea",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName:        "interval_column",
		DataType:          "interval",
		UdtName:           "interval",
		DatetimePrecision: "6",
		Namespace:         "pg_catalog",
	},
	{
		ColumnName: "tsvector_column",
		DataType:   "tsvector",
		UdtName:    "tsvector",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "xml_column",
		DataType:   "xml",
		UdtName:    "xml",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "pg_snapshot_column",
		DataType:   "pg_snapshot",
		UdtName:    "pg_snapshot",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "point_column",
		DataType:   "point",
		UdtName:    "point",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "inet_column",
		DataType:   "inet",
		UdtName:    "inet",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "json_column",
		DataType:   "json",
		UdtName:    "json",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "jsonb_column",
		DataType:   "jsonb",
		UdtName:    "jsonb",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "array_text_column",
		DataType:   "ARRAY",
		UdtName:    "_text",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "array_int_column",
		DataType:   "ARRAY",
		UdtName:    "_int4",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "array_jsonb_column",
		DataType:   "ARRAY",
		UdtName:    "_jsonb",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "array_ltree_column",
		DataType:   "ARRAY",
		UdtName:    "_ltree",
		Namespace:  "public",
	},
	{
		ColumnName: "user_defined_column",
		DataType:   "USER-DEFINED",
		UdtName:    "address",
		Namespace:  "public",
	},
}

var PUBLIC_TEST_TABLE_LOADED_ROWS = [][]string{
	{
		"1",                                    // id
		"1",                                    // bit_column
		"true",                                 // bool_column
		"bpchar",                               // bpchar_column
		"varchar",                              // varchar_column
		"text",                                 // text_column
		"32767",                                // int2_column
		"2147483647",                           // int4_column
		"9223372036854775807",                  // int8_column
		"10000000000000000000",                 // hugeint_column
		"4294967295",                           // xid_column
		"18446744073709551615",                 // xid8_column
		"3.14",                                 // float4_column
		"3.141592653589793",                    // float8_column
		"12345.67",                             // numeric_column
		"12345.67",                             // numeric_column_without_precision
		"2021-01-01",                           // date_column
		"12:00:00.123456",                      // time_column
		"12:00:00.123",                         // timeMsColumn
		"12:00:00.123456-05",                   // timetz_column
		"12:00:00.123-05",                      // timetz_ms_column
		"2024-01-01 12:00:00.123456",           // timestamp_column
		"2024-01-01 12:00:00.123",              // timestamp_ms_column
		"2024-01-01 12:00:00.123456-05",        // timestamptz_column
		"2024-01-01 12:00:00.123-05",           // timestamptz_ms_column
		"2024-01-01 12:00:00.123456-05:30",     // timestamptz_column_timezone_mins
		"58a7c845-af77-44b2-8664-7ca613d92f04", // uuid_column
		"\\x1234",                              // bytea_column
		"1 mon 2 days 01:00:01.000001",         // interval_column
		"'sampl':1 'text':2 'tsvector':4",      // tsvector_column
		"<root><child>text</child></root>",     // xml_column
		"2784:2784:",                           // pg_snapshot_column
		"(37.347301483154,45.002101898193)",    // point_column
		"192.168.0.1",                          // inet_column
		"{\"key\": \"value\"}",                 // json_column
		"{\"key\": \"value\"}",                 // jsonb_column
		"{one,two,three}",                      // array_text_column
		"{1,2,3}",                              // array_int_column
		`{"{\"key\": \"value1\"}","{\"key\": \"value2\"}"}`, // array_jsonb_column
		"{\"a.b\",\"c.d\"}", // array_ltree_column
		"(Toronto)",         // user_defined_column
	},
	{
		"2",                                // id
		PG_NULL_STRING,                     // bit_column
		"false",                            // bool_column
		"",                                 // bpchar_column
		PG_NULL_STRING,                     // varchar_column
		"",                                 // text_column
		"-32767",                           // int2_column
		PG_NULL_STRING,                     // int4_column
		"-9223372036854775807",             // int8_column
		PG_NULL_STRING,                     // hugeint_column
		PG_NULL_STRING,                     // xid_column
		PG_NULL_STRING,                     // xid8_column
		"NaN",                              // float4_column
		"-3.141592653589793",               // float8_column
		"-12345.00",                        // numeric_column
		PG_NULL_STRING,                     // numeric_column_without_precision
		"20025-11-12",                      // date_column
		"12:00:00.123",                     // time_column
		PG_NULL_STRING,                     // timeMsColumn
		"12:00:00.12300+05",                // timetz_column
		"12:00:00.1+05",                    // timetz_ms_column
		"2024-01-01 12:00:00",              // timestamp_column
		PG_NULL_STRING,                     // timestamp_ms_column
		"2024-01-01 12:00:00.000123+05",    // timestamptz_column
		"2024-01-01 12:00:00.12+05",        // timestamptz_ms_column
		"2024-01-01 12:00:00.000123+05:30", // timestamptz_column_timezone_mins
		PG_NULL_STRING,                     // uuid_column
		PG_NULL_STRING,                     // bytea_column
		PG_NULL_STRING,                     // interval_column
		PG_NULL_STRING,                     // tsvector_column
		PG_NULL_STRING,                     // xml_column
		PG_NULL_STRING,                     // pg_snapshot_column
		PG_NULL_STRING,                     // point_column
		PG_NULL_STRING,                     // inet_column
		PG_NULL_STRING,                     // json_column
		"{}",                               // jsonb_column
		PG_NULL_STRING,                     // array_text_column
		"{}",                               // array_int_column
		PG_NULL_STRING,                     // array_jsonb_column
		PG_NULL_STRING,                     // array_ltree_column
		PG_NULL_STRING,                     // user_defined_column
	},
}

var TEST_SCHEMA_SIMPLE_TABLE_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{
		ColumnName:             "id",
		DataType:               "integer",
		UdtName:                "int4",
		IsNullable:             "NO",
		OrdinalPosition:        "1",
		CharacterMaximumLength: "0",
		NumericPrecision:       "32",
		NumericScale:           "0",
		DatetimePrecision:      "0",
		Namespace:              "pg_catalog",
	},
}

var TEST_SCHEMA_SIMPLE_TABLE_LOADED_ROWS = [][]string{{}}

func init() {
	config := loadTestConfig()
	icebergWriter := NewIcebergWriter(config)

	for i := range PUBLIC_TEST_TABLE_PG_SCHEMA_COLUMNS {
		PUBLIC_TEST_TABLE_PG_SCHEMA_COLUMNS[i].OrdinalPosition = IntToString(i + 1)
		if PUBLIC_TEST_TABLE_PG_SCHEMA_COLUMNS[i].IsNullable == "" {
			PUBLIC_TEST_TABLE_PG_SCHEMA_COLUMNS[i].IsNullable = "YES"
		}
	}

	i := 0
	icebergWriter.Write(
		IcebergSchemaTable{Schema: "public", Table: "test_table"},
		PUBLIC_TEST_TABLE_PG_SCHEMA_COLUMNS,
		MAX_WRITE_PARQUET_PAYLOAD_SIZE,
		func() [][]string {
			if i > 0 {
				return [][]string{}
			}

			i++
			return PUBLIC_TEST_TABLE_LOADED_ROWS
		},
	)
	icebergWriter.Write(
		IcebergSchemaTable{Schema: "test_schema", Table: "simple_table"},
		TEST_SCHEMA_SIMPLE_TABLE_PG_SCHEMA_COLUMNS,
		MAX_WRITE_PARQUET_PAYLOAD_SIZE,
		func() [][]string {
			if i > 0 {
				return [][]string{}
			}

			i++
			return TEST_SCHEMA_SIMPLE_TABLE_LOADED_ROWS
		},
	)
}

func loadTestConfig() *Config {
	setTestArgs([]string{})

	config := LoadConfig(true)
	config.User = "bemidb"
	config.EncryptedPassword = "bemidb-encrypted"
	config.StorageType = STORAGE_TYPE_LOCAL
	config.StoragePath = "../iceberg-test"
	config.LogLevel = "ERROR"
	config.DisableAnonymousAnalytics = true

	return config
}

func setTestArgs(args []string) {
	// Reset state
	_config = Config{Version: VERSION}
	_configParseValues = configParseValues{}

	os.Args = append([]string{"cmd"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	registerFlags()
}
