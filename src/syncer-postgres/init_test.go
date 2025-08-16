package main

import (
	"encoding/csv"
	"flag"
	"os"

	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

var PG_SCHEMA_COLUMNS_TEST_TABLE = []PgSchemaColumn{
	{
		ColumnName:          "id",
		DataType:            "integer",
		UdtName:             "int4",
		IsNullable:          "NO",
		NumericPrecision:    "32",
		Namespace:           "pg_catalog",
		IsPartOfUniqueIndex: true,
	},
	{
		ColumnName: "bit_column",
		DataType:   "bit",
		UdtName:    "bit",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "bool_column",
		DataType:   "boolean",
		UdtName:    "bool",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "bpchar_column",
		DataType:   "character",
		UdtName:    "bpchar",
		Namespace:  "pg_catalog",
	},
	{
		ColumnName: "varchar_column",
		DataType:   "character varying",
		UdtName:    "varchar",
		Namespace:  "pg_catalog",
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
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "int4_column",
		DataType:         "integer",
		UdtName:          "int4",
		NumericPrecision: "32",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "int8_column",
		DataType:         "bigint",
		UdtName:          "int8",
		NumericPrecision: "64",
		Namespace:        "pg_catalog",
	},
	{
		ColumnName:       "hugeint_column",
		DataType:         "numeric",
		UdtName:          "numeric",
		NumericPrecision: "20", // Will be capped to 38
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
var PG_SCHEMA_COLUMNS_PARTITIONED_TABLE = []PgSchemaColumn{
	{
		ColumnName:          "timestamp_column",
		DataType:            "timestamp without time zone",
		UdtName:             "timestamp",
		IsNullable:          "NO",
		OrdinalPosition:     "1",
		NumericPrecision:    "0",
		NumericScale:        "0",
		DatetimePrecision:   "6",
		Namespace:           "pg_catalog",
		IsPartOfUniqueIndex: true,
	},
}
var PG_SCHEMA_COLUMNS_EMPTY_TABLE = []PgSchemaColumn{
	{
		ColumnName:          "id",
		DataType:            "integer",
		UdtName:             "int4",
		IsNullable:          "NO",
		OrdinalPosition:     "1",
		NumericPrecision:    "32",
		NumericScale:        "0",
		DatetimePrecision:   "0",
		Namespace:           "pg_catalog",
		IsPartOfUniqueIndex: true,
	},
}

var CSV_ROWS_TEST_TABLE = [][]string{
	{
		"1",                                    // id
		"1",                                    // bit_column
		"t",                                    // bool_column
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
		"2024-01-01",                           // date_column
		"12:00:00.123456",                      // time_column
		"12:00:00.123",                         // timeMsColumn
		"12:00:00.123456-05",                   // timetz_column
		"12:00:00.123-05",                      // timetz_ms_column
		"2024-01-01 12:00:00.123456",           // timestamp_column
		"2024-01-01 12:00:00.123",              // timestamp_ms_column
		"2024-01-01 12:00:00.123456-05",        // timestamptz_column
		"2024-01-01 12:00:00.123-05",           // timestamptz_ms_column
		"58a7c845-af77-44b2-8664-7ca613d92f04", // uuid_column
		"\\x48656c6c6f",                        // bytea_column
		"1 mon 2 days 01:00:01.000001",         // interval_column
		"'sampl':1 'text':2 'tsvector':4",      // tsvector_column
		"<root><child>text</child></root>",     // xml_column
		"1896:1896:",                           // pg_snapshot_column
		"(37.347301483154,45.002101898193)",    // point_column
		"192.168.0.1",                          // inet_column
		"{\"key\": \"value\"}",                 // json_column
		"{\"key\": \"value\", \"nestedKey\": { \"key\": \"value\" }}", // jsonb_column
		"{one,two,three}", // array_text_column
		"{1,2,3}",         // array_int_column
		`{"{\"key\": \"value1\"}","{\"key\": \"value2\"}"}`, // array_jsonb_column
		"{\"a.b\",\"c.d\"}", // array_ltree_column
		"(Toronto)",         // user_defined_column
	},
	{
		"2",                                // id
		syncerCommon.BEMIDB_NULL_STRING,    // bit_column
		"f",                                // bool_column
		"",                                 // bpchar_column
		syncerCommon.BEMIDB_NULL_STRING,    // varchar_column
		"",                                 // text_column
		"-32767",                           // int2_column
		syncerCommon.BEMIDB_NULL_STRING,    // int4_column
		"-9223372036854775807",             // int8_column
		syncerCommon.BEMIDB_NULL_STRING,    // hugeint_column
		syncerCommon.BEMIDB_NULL_STRING,    // xid_column
		syncerCommon.BEMIDB_NULL_STRING,    // xid8_column
		"NaN",                              // float4_column
		"-3.141592653589793",               // float8_column
		"-12345.00",                        // numeric_column
		syncerCommon.BEMIDB_NULL_STRING,    // numeric_column_without_precision
		"20025-11-12",                      // date_column
		"12:00:00.123",                     // time_column
		syncerCommon.BEMIDB_NULL_STRING,    // timeMsColumn
		"12:00:00.12300+05",                // timetz_column
		"12:00:00.1+05",                    // timetz_ms_column
		"2024-01-01 12:00:00",              // timestamp_column
		syncerCommon.BEMIDB_NULL_STRING,    // timestamp_ms_column
		"2024-01-01 12:00:00.000123+05:30", // timestamptz_column
		"2024-01-01 12:00:00.12+05",        // timestamptz_ms_column
		syncerCommon.BEMIDB_NULL_STRING,    // uuid_column
		syncerCommon.BEMIDB_NULL_STRING,    // bytea_column
		syncerCommon.BEMIDB_NULL_STRING,    // interval_column
		syncerCommon.BEMIDB_NULL_STRING,    // tsvector_column
		syncerCommon.BEMIDB_NULL_STRING,    // xml_column
		syncerCommon.BEMIDB_NULL_STRING,    // pg_snapshot_column
		syncerCommon.BEMIDB_NULL_STRING,    // point_column
		syncerCommon.BEMIDB_NULL_STRING,    // inet_column
		syncerCommon.BEMIDB_NULL_STRING,    // json_column
		"{}",                               // jsonb_column
		syncerCommon.BEMIDB_NULL_STRING,    // array_text_column
		"{}",                               // array_int_column
		syncerCommon.BEMIDB_NULL_STRING,    // array_jsonb_column
		syncerCommon.BEMIDB_NULL_STRING,    // array_ltree_column
		syncerCommon.BEMIDB_NULL_STRING,    // user_defined_column
	},
}
var CSV_ROWS_PARTITIONED_TABLE1 = [][]string{{"2024-01-01 01:02:03.123456"}}
var CSV_ROWS_PARTITIONED_TABLE2 = [][]string{{"2024-02-12 11:12:13"}}
var CSV_ROWS_PARTITIONED_TABLE3 = [][]string{{"2024-03-30 23:59:59"}}

var JSON_ROWS_TEST_TABLE = []string{
	`{
		"before":null,
		"after":{
			"id":1,
			"bit_column":true,
			"bool_column":true,
			"bpchar_column":"bpchar    ",
			"varchar_column":"varchar",
			"text_column":"text",
			"int2_column":32767,
			"int4_column":2147483647,
			"int8_column":9223372036854775807,
			"hugeint_column":1.0E19,
			"xid_column":4294967295,
			"xid8_column":1.8446744073709552E19,
			"float4_column":3.14,
			"float8_column":3.141592653589793,
			"numeric_column":12345.67,
			"numeric_column_without_precision":12345.67,
			"date_column":19723,
			"time_column":43200123456,
			"timemscolumn":43200123,
			"timetz_column":"17:00:00.123456Z",
			"timetz_ms_column":"17:00:00.123Z",
			"timestamp_column":1704110400123456,
			"timestamp_ms_column":1704110400123,
			"timestamptz_column":"2024-01-01T17:00:00.123456Z",
			"timestamptz_ms_column":"2024-01-01T17:00:00.123Z",
			"uuid_column":"58a7c845-af77-44b2-8664-7ca613d92f04",
			"bytea_column":"48656c6c6f",
			"interval_column":2806201000001,
			"point_column":{
				"x":37.347301483154,
				"y":45.002101898193,
				"wkb":"AQEAAADW//9fdKxCQM3//99EgEZA",
				"srid":null
			},
			"inet_column":"192.168.0.1",
			"json_column":"{\"key\": \"value\"}",
			"jsonb_column":"{\"key\": \"value\", \"nestedKey\": { \"key\": \"value\" }}",
			"tsvector_column":"'sampl':1 'text':2 'tsvector':4",
			"xml_column":"<root><child>text</child></root>",
			"pg_snapshot_column":"1896:1896:",
			"array_text_column":[
				"one",
				"two",
				"three"
			],
			"array_int_column":[
				1,
				2,
				3
			],
			"array_jsonb_column":[
				"{\"key\": \"value1\"}",
				"{\"key\": \"value2\"}"
			],
			"array_ltree_column":[
				"a.b",
				"c.d"
			],
			"user_defined_column":"28546f726f6e746f29"
		},
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"first_in_data_collection",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"public",
			"table":"test_table",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"c",
		"ts_ms":1750942485897,
		"ts_us":1750942485897437,
		"ts_ns":1750942485897437000
	}`,
	`{
		"before":null,
		"after":{
			"id":2,
			"bit_column":null,
			"bool_column":false,
			"bpchar_column":"          ",
			"varchar_column":null,
			"text_column":"",
			"int2_column":-32767,
			"int4_column":null,
			"int8_column":-9223372036854775807,
			"hugeint_column":null,
			"xid_column":null,
			"xid8_column":null,
			"float4_column":"NaN",
			"float8_column":-3.141592653589793,
			"numeric_column":-12345.0,
			"numeric_column_without_precision":null,
			"date_column":6594769,
			"time_column":43200123000,
			"timemscolumn":null,
			"timetz_column":"07:00:00.123Z",
			"timetz_ms_column":"07:00:00.1Z",
			"timestamp_column":1704110400000000,
			"timestamp_ms_column":null,
			"timestamptz_column":"2024-01-01T06:30:00.000123Z",
			"timestamptz_ms_column":"2024-01-01T07:00:00.120Z",
			"uuid_column":null,
			"bytea_column":null,
			"interval_column":null,
			"point_column":null,
			"inet_column":null,
			"json_column":null,
			"jsonb_column":"{}",
			"tsvector_column":null,
			"xml_column":null,
			"pg_snapshot_column":null,
			"array_text_column":null,
			"array_int_column":[

			],
			"array_jsonb_column":null,
			"array_ltree_column":null,
			"user_defined_column":null
		},
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"last",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"public",
			"table":"test_table",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"c",
		"ts_ms":1750942485898,
		"ts_us":1750942485898829,
		"ts_ns":1750942485898829000
	}`,
}
var JSON_ROWS_PARTITIONED_TABLE1 = []string{
	`{
		"before":null,
		"after":{
			"timestamp_column":1704070923123456
		},
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"last_in_data_collection",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"public",
			"table":"partitioned_table1",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"c",
		"ts_ms":1750942485881,
		"ts_us":1750942485881498,
		"ts_ns":1750942485881498000
	}`,
}
var JSON_ROWS_PARTITIONED_TABLE2 = []string{
	`{
		"before":null,
		"after":{
			"timestamp_column":1707736333000000
		},
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"last_in_data_collection",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"public",
			"table":"partitioned_table2",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"c",
		"ts_ms":1750942485882,
		"ts_us":1750942485882849,
		"ts_ns":1750942485882849000
	}`,
}
var JSON_ROWS_PARTITIONED_TABLE3 = []string{
	`{
		"before":null,
		"after":{
			"timestamp_column":1711843199000000
		},
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"last_in_data_collection",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"public",
			"table":"partitioned_table3",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"c",
		"ts_ms":1750942485884,
		"ts_us":1750942485884078,
		"ts_ns":1750942485884078000
	}`,
}
var JSON_ROWS_EMPTY_TABLE = []string{
	`{
		"before":null,
		"after":{
			"id":1
		},
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"last_in_data_collection",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"test",
			"table":"empty_table",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"c",
		"ts_ms":1750942485884,
		"ts_us":1750942485884078,
		"ts_ns":1750942485884078000
	}`,
	`{
		"before":{
			"id":1
		},
		"after":null,
		"source":{
			"version":"3.1.3.Final",
			"connector":"postgresql",
			"name":"prefix",
			"ts_ms":1750942485790,
			"snapshot":"last_in_data_collection",
			"db":"tpch",
			"sequence":"[null,\"98162912\"]",
			"ts_us":1750942485790370,
			"ts_ns":1750942485790370000,
			"schema":"test",
			"table":"empty_table",
			"txId":2352,
			"lsn":98162912,
			"xmin":null
		},
		"transaction":null,
		"op":"d",
		"ts_ms":1750942485884,
		"ts_us":1750942485884078,
		"ts_ns":1750942485884078000
	}`,
}

func init() {
	config := loadTestConfig()

	storageS3 := syncerCommon.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig)
	utils := NewSyncerUtils(config, storageS3, duckdbClient)

	// Prepare PgSchemaColumns
	for i := range PG_SCHEMA_COLUMNS_TEST_TABLE {
		PG_SCHEMA_COLUMNS_TEST_TABLE[i].OrdinalPosition = common.IntToString(i + 1)
		if PG_SCHEMA_COLUMNS_TEST_TABLE[i].IsNullable == "" {
			PG_SCHEMA_COLUMNS_TEST_TABLE[i].IsNullable = "YES"
		}
		if PG_SCHEMA_COLUMNS_TEST_TABLE[i].NumericPrecision == "" {
			PG_SCHEMA_COLUMNS_TEST_TABLE[i].NumericPrecision = "0"
		}
		if PG_SCHEMA_COLUMNS_TEST_TABLE[i].NumericScale == "" {
			PG_SCHEMA_COLUMNS_TEST_TABLE[i].NumericScale = "0"
		}
		if PG_SCHEMA_COLUMNS_TEST_TABLE[i].DatetimePrecision == "" {
			PG_SCHEMA_COLUMNS_TEST_TABLE[i].DatetimePrecision = "0"
		}
		PG_SCHEMA_COLUMNS_TEST_TABLE[i].Config = config
	}
	for i := range PG_SCHEMA_COLUMNS_PARTITIONED_TABLE {
		PG_SCHEMA_COLUMNS_PARTITIONED_TABLE[i].Config = config
	}
	for i := range PG_SCHEMA_COLUMNS_EMPTY_TABLE {
		PG_SCHEMA_COLUMNS_EMPTY_TABLE[i].Config = config
	}

	switch config.SyncMode {
	case SyncModeFullRefresh:
		syncer := NewSyncerFullRefresh(config, utils, storageS3, duckdbClient)
		createTestTableViaFullRefresh(syncer, PgSchemaTable{Schema: "public", Table: "test_table"}, PG_SCHEMA_COLUMNS_TEST_TABLE, CSV_ROWS_TEST_TABLE)
		createTestTableViaFullRefresh(syncer, PgSchemaTable{Schema: "public", Table: "partitioned_table1"}, PG_SCHEMA_COLUMNS_PARTITIONED_TABLE, CSV_ROWS_PARTITIONED_TABLE1)
		createTestTableViaFullRefresh(syncer, PgSchemaTable{Schema: "public", Table: "partitioned_table2"}, PG_SCHEMA_COLUMNS_PARTITIONED_TABLE, CSV_ROWS_PARTITIONED_TABLE2)
		createTestTableViaFullRefresh(syncer, PgSchemaTable{Schema: "public", Table: "partitioned_table3"}, PG_SCHEMA_COLUMNS_PARTITIONED_TABLE, CSV_ROWS_PARTITIONED_TABLE3)
		createTestTableViaFullRefresh(syncer, PgSchemaTable{Schema: "test", Table: "empty_table"}, PG_SCHEMA_COLUMNS_EMPTY_TABLE, [][]string{})
	case SyncModeCDC:
		panic("CDC is not supported")
	}
}

func loadTestConfig() *Config {
	setTestArgs([]string{})

	_config.DatabaseUrl = "postgres://test:test@localhost:5432/dummy"

	return LoadConfig()
}

func setTestArgs(args []string) {
	os.Args = append([]string{"cmd"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	registerFlags()
}

func createTestTableViaFullRefresh(syncer *SyncerFullRefresh, pgSchemaTable PgSchemaTable, pgSchemaColumns []PgSchemaColumn, rows [][]string) {
	cappedBuffer := syncerCommon.NewCappedBuffer(syncer.Config.CommonConfig, MAX_IN_MEMORY_BUFFER_SIZE)
	writer := csv.NewWriter(cappedBuffer)

	headerRow := []string{}
	for _, pgSchemaColumn := range pgSchemaColumns {
		headerRow = append(headerRow, pgSchemaColumn.ColumnName)
	}
	err := writer.Write(headerRow)
	common.PanicIfError(syncer.Config.CommonConfig, err)

	for _, row := range rows {
		err := writer.Write(row)
		common.PanicIfError(syncer.Config.CommonConfig, err)
	}

	writer.Flush()
	cappedBuffer.Close()

	syncer.writeToIceberg(pgSchemaTable, pgSchemaColumns, cappedBuffer)
}
