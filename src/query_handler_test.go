package main

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestHandleQuery(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("PG functions", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT VERSION()": {
				"description": {"version"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"PostgreSQL 17.0, compiled by BemiDB"},
			},
			"SELECT pg_catalog.pg_get_userbyid(p.proowner) AS owner, 'Foo' AS foo FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace LIMIT 1": {
				"description": {"owner", "foo"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb", "Foo"},
			},
			"SELECT QUOTE_IDENT('fooBar') AS quote_ident": {
				"description": {"quote_ident"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"\"fooBar\""},
			},
			"SELECT setting from pg_show_all_settings() WHERE name = 'default_null_order'": {
				"description": {"setting"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"nulls_last"},
			},
			"SELECT setting from pg_catalog.pg_show_all_settings() WHERE name = 'default_null_order'": {
				"description": {"setting"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"nulls_last"},
			},
			"SELECT pg_catalog.pg_get_partkeydef(c.oid) AS pg_get_partkeydef FROM pg_catalog.pg_class c LIMIT 1": {
				"description": {"pg_get_partkeydef"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT pg_tablespace_location(t.oid) loc FROM pg_catalog.pg_tablespace": {
				"description": {"loc"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT pg_catalog.pg_get_expr(adbin, drelid) AS def_value FROM pg_catalog.pg_attrdef": {
				"description": {"def_value"},
			},
			"SELECT pg_catalog.pg_get_expr(adbin, drelid, TRUE) AS def_value FROM pg_catalog.pg_attrdef": {
				"description": {"def_value"},
			},
			"SELECT pg_catalog.pg_get_viewdef(NULL, TRUE) AS viewdef": {
				"description": {"viewdef"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT set_config('bytea_output', 'hex', false) AS set_config": {
				"description": {"set_config"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"hex"},
			},
			"SELECT pg_catalog.pg_encoding_to_char(6) AS pg_encoding_to_char": {
				"description": {"pg_encoding_to_char"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"UTF8"},
			},
			"SELECT pg_backend_pid() AS pg_backend_pid": {
				"description": {"pg_backend_pid"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"0"},
			},
			"SELECT * from pg_is_in_recovery()": {
				"description": {"pg_is_in_recovery"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT row_to_json(t) AS row_to_json FROM (SELECT usename, passwd FROM pg_shadow WHERE usename='bemidb') t": {
				"description": {"row_to_json"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {`{"usename":"bemidb","passwd":"bemidb-encrypted"}`},
			},
			"SELECT current_setting('default_tablespace') AS current_setting": {
				"description": {"current_setting"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT main.array_to_string('[1, 2, 3]', '') as str": {
				"description": {"str"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"123"},
			},
			"SELECT pg_catalog.array_to_string('[1, 2, 3]', '') as str": {
				"description": {"str"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"123"},
			},
			"SELECT array_to_string('[1, 2, 3]', '') as str": {
				"description": {"str"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"123"},
			},
			"SELECT * FROM pg_catalog.generate_series(1, 1)": {
				"description": {"generate_series"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT pg_catalog.aclexplode(db.datacl) AS d FROM pg_catalog.pg_database db": {
				"description": {"d"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT TRIM (BOTH '\"' FROM pg_catalog.pg_get_indexdef(1, 1, false)) AS trim": {
				"description": {"trim"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT (d).grantee AS grantee, (d).grantor AS grantor, (d).is_grantable AS is_grantable, (d).privilege_type AS privilege_type FROM (SELECT pg_catalog.aclexplode(db.datacl) AS d FROM pg_catalog.pg_database db WHERE db.oid = 16388::OID) a": {
				"description": {"grantee", "grantor", "is_grantable", "privilege_type"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"", "", "", ""},
			},
			"SELECT format('Hello %s, %s, %1$s', 'World', 'Earth') AS str": {
				"description": {"str"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"Hello World, Earth, World"},
			},
			"SELECT format('%s', \"test_table\".\"varchar_column\") AS str FROM test_table LIMIT 1": {
				"description": {"str"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"varchar"},
			},
			"SELECT jsonb_extract_path_text(json_column, 'key') AS jsonb_extract_path_text FROM test_table LIMIT 1": {
				"description": {"jsonb_extract_path_text"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"value"},
			},
			"SELECT jsonb_extract_path_text(json_column, VARIADIC ARRAY['key']) AS jsonb_extract_path_text FROM test_table LIMIT 1": {
				"description": {"jsonb_extract_path_text"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"value"},
			},
			"SELECT encode(sha256('foo'), 'hex'::text) AS encode": {
				"description": {"encode"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"},
			},
			"SELECT json_build_object('min', 1, 'max', 2) AS json_build_object": {
				"description": {"json_build_object"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"{\"min\":1,\"max\":2}"},
			},
			"SELECT pg_catalog.pg_get_statisticsobjdef_columns(1) AS pg_get_statisticsobjdef_columns": {
				"description": {"pg_get_statisticsobjdef_columns"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
			"SELECT pg_catalog.pg_relation_is_publishable('1') AS pg_relation_is_publishable": {
				"description": {"pg_relation_is_publishable"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
		})
	})

	t.Run("PG system tables", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT oid, typname AS typename FROM pg_type WHERE typname='geometry' OR typname='geography'": {
				"description": {"oid", "typename"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public' LIMIT 1) ORDER BY relname DESC LIMIT 1": {
				"description": {"relname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"test_table"},
			},
			"SELECT oid FROM pg_catalog.pg_extension": {
				"description": {"oid"},
				"types":       {Uint32ToString(pgtype.OIDOID)},
				"values":      {"13823"},
			},
			"SELECT slot_name FROM pg_replication_slots": {
				"description": {"slot_name"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT oid, datname, datdba FROM pg_catalog.pg_database where oid = 16388": {
				"description": {"oid", "datname", "datdba"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int8OID)},
				"values":      {"16388", "bemidb", "10"},
			},
			"SELECT COALESCE(NULL, (SELECT datname FROM pg_database WHERE datname = 'bemidb')) AS datname": {
				"description": {"datname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
			"SELECT * FROM pg_catalog.pg_stat_gssapi": {
				"description": {"pid", "gss_authenticated", "principal", "encrypted", "credentials_delegated"},
				"types":       {Uint32ToString(pgtype.Int4OID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
			"SELECT * FROM pg_catalog.pg_user": {
				"description": {"usename", "usesysid", "usecreatedb", "usesuper", "userepl", "usebypassrls", "passwd", "valuntil", "useconfig"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb", "10", "t", "t", "t", "t", "", "", ""},
			},
			"SELECT datid FROM pg_catalog.pg_stat_activity": {
				"description": {"datid"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {},
			},
			"SELECT schemaname, matviewname AS objectname FROM pg_catalog.pg_matviews": {
				"description": {"schemaname", "objectname"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT * FROM pg_catalog.pg_views": {
				"description": {"schemaname", "viewname", "viewowner", "definition"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
			},
			"SELECT oid FROM pg_collation": {
				"description": {"oid"},
				"types":       {Uint32ToString(pgtype.OIDOID)},
				"values":      {"100"},
			},
			"SELECT * FROM pg_opclass": {
				"description": {"oid", "opcmethod", "opcname", "opcnamespace", "opcowner", "opcfamily", "opcintype", "opcdefault", "opckeytype"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.Int8OID)},
			},
			"SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY relname DESC LIMIT 1": {
				"description": {"schemaname", "relname", "n_live_tup"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int8OID)},
				"values":      {"public", "test_table", "1"},
			},
			"SELECT DISTINCT(nspname) FROM pg_catalog.pg_namespace WHERE nspname != 'information_schema' AND nspname != 'pg_catalog' ORDER BY nspname DESC LIMIT 1": {
				"description": {"nspname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"test"},
			},
			"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname == 'main'": {
				"description": {"nspname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT n.nspname FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid ORDER BY n.nspname DESC LIMIT 1": {
				"description": {"nspname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"test"},
			},
			"SELECT rel.oid FROM pg_class rel LEFT JOIN pg_extension ON rel.oid = pg_extension.oid ORDER BY rel.oid LIMIT 1;": {
				"description": {"oid"},
				"types":       {Uint32ToString(pgtype.OIDOID)},
				"values":      {"1262"},
			},
			"SELECT pg_total_relation_size(relid) AS total_size FROM pg_catalog.pg_statio_user_tables WHERE schemaname = 'public'": {
				"description": {"total_size"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
			},
			"SELECT pg_total_relation_size(relid) AS total_size FROM pg_catalog.pg_statio_user_tables WHERE schemaname = 'public' UNION SELECT NULL AS total_size FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'": {
				"description": {"total_size"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
			},
			"SELECT * FROM pg_catalog.pg_shdescription": {
				"description": {"objoid", "classoid", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_roles": {
				"description": {"oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb", "rolcanlogin", "rolreplication", "rolconnlimit", "rolpassword", "rolvaliduntil", "rolbypassrls", "rolconfig"},
				"types": {
					Uint32ToString(pgtype.OIDOID),
					Uint32ToString(pgtype.TextOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.Int4OID),
					Uint32ToString(pgtype.TextOID),
					Uint32ToString(pgtype.TimestampOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.TextArrayOID),
				},
				"values": {"10", "bemidb", "t", "t", "t", "t", "t", "f", "-1", "", "", "f", ""},
			},
			"SELECT * FROM pg_catalog.pg_inherits": {
				"description": {"inhrelid", "inhparent", "inhseqno", "inhdetachpending"},
			},
			"SELECT * FROM pg_auth_members": {
				"description": {"oid", "roleid", "member", "grantor", "admin_option", "inherit_option", "set_option"},
			},
			"SELECT ARRAY(select pg_get_indexdef(indexrelid, attnum, true) FROM pg_attribute WHERE attrelid = indexrelid ORDER BY attnum) AS expressions FROM pg_index": {
				"description": {"expressions"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {},
			},
			"SELECT indnullsnotdistinct FROM pg_index": {
				"description": {"indnullsnotdistinct"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
			},
			"SELECT n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname OPERATOR(pg_catalog.~) '^(test_table)$' COLLATE pg_catalog.default AND n.nspname OPERATOR(pg_catalog.~) '^(public)$' COLLATE pg_catalog.default": {
				"description": {"nspname", "relname"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"public", "test_table"},
			},
			"SELECT * FROM pg_catalog.pg_policy": {
				"description": {"oid", "polname", "polrelid", "polcmd", "polpermissive", "polroles", "polqual", "polwithcheck"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_statistic_ext": {
				"description": {"oid", "stxrelid", "stxname", "stxnamespace", "stxowner", "stxstattarget", "stxkeys", "stxkind", "stxexprs"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int4OID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_publication": {
				"description": {"oid", "pubname", "pubowner", "puballtables", "pubinsert", "pubupdate", "pubdelete", "pubtruncate", "pubviaroot"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID)},
			},
			"SELECT * FROM pg_catalog.pg_publication_rel": {
				"description": {"oid", "prpubid", "prrelid", "prqual", "prattrs"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_publication_namespace": {
				"description": {"oid", "pnpubid", "pnnspid"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.Int8OID)},
			},
			"SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid UNION SELECT pubname, pg_get_expr(pr.prqual, c.oid), (CASE WHEN pr.prattrs IS NOT NULL THEN (SELECT string_agg(attname, ', ') FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s, pg_catalog.pg_attribute WHERE attrelid = pr.prrelid AND attnum = prattrs[s]) ELSE NULL END) FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid UNION SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p ORDER BY 1": {
				"description": {"pubname", "NULL", "NULL"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM user": {
				"description": {"user"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
		})
	})

	t.Run("Information schema", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT * FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name DESC LIMIT 1": {
				"description": {"table_catalog", "table_schema", "table_name", "table_type", "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action", "TABLE_COMMENT"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"memory", "public", "test_table", "BASE TABLE", "", "", "", "", "", "YES", "NO", "", ""},
			},
			"SELECT column_name, udt_schema, udt_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'test_table' ORDER BY ordinal_position LIMIT 1": {
				"description": {"column_name", "udt_schema", "udt_name"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"id", "pg_catalog", "int4"},
			},
		})
	})

	t.Run("SHOW", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SHOW search_path": {
				"description": {"search_path"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {`"$user", public`},
			},
			"SHOW timezone": {
				"description": {"timezone"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"UTC"},
			},
		})
	})

	t.Run("Iceberg tables", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT COUNT(*) AS count FROM public.test_table": {
				"description": {"count"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"2"},
			},
			"SELECT COUNT(*) AS count FROM test_table": {
				"description": {"count"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"2"},
			},
			"SELECT COUNT(DISTINCT public.test_table.id) AS count FROM public.test_table": {
				"description": {"count"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"2"},
			},
			"SELECT x.id FROM public.test_table x WHERE x.id = 1": {
				"description": {"id"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT public.test_table.id FROM public.test_table WHERE public.test_table.id = 1": {
				"description": {"id"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT test.empty_table.id FROM test.empty_table": {
				"description": {"id"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
			},
			"SELECT test_table.id FROM public.test_table WHERE id = 1": {
				"description": {"id"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT COUNT(*) FROM partitioned_table": {
				"description": {"count"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"3"},
			},
		})
	})

	t.Run("BemiDB functions", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT bemidb_last_synced_at('public.test_table')": {
				"description": {"bemidb_last_synced_at"},
				"types":       {Uint32ToString(pgtype.TimestamptzOID)},
				"values":      {"2025-04-16 14:28:33+00:00"},
			},
			"SELECT bemidb_last_synced_at('table_does_not_exist')": {
				"description": {"bemidb_last_synced_at"},
				"types":       {Uint32ToString(pgtype.TimestamptzOID)},
				"values":      {""},
			},
		})
	})

	t.Run("Column types", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT bit_column FROM public.test_table WHERE bit_column IS NOT NULL": {
				"description": {"bit_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"1"},
			},
			"SELECT bit_column FROM public.test_table WHERE bit_column IS NULL": {
				"description": {"bit_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT bool_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"bool_column"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT bool_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"bool_column"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT bpchar_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"bpchar_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bpchar"},
			},
			"SELECT bpchar_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"bpchar_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT varchar_column FROM public.test_table WHERE varchar_column IS NOT NULL": {
				"description": {"varchar_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"varchar"},
			},
			"SELECT varchar_column FROM public.test_table WHERE varchar_column IS NULL": {
				"description": {"varchar_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT text_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"text_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT text_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"text_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT int2_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"int2_column"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"32767"},
			},
			"SELECT int2_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"int2_column"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"-32767"},
			},
			"SELECT int4_column FROM public.test_table WHERE int4_column IS NOT NULL": {
				"description": {"int4_column"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"2147483647"},
			},
			"SELECT int4_column FROM public.test_table WHERE int4_column IS NULL": {
				"description": {"int4_column"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
			"SELECT int8_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"int8_column"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"9223372036854775807"},
			},
			"SELECT int8_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"int8_column"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"-9223372036854775807"},
			},
			"SELECT hugeint_column FROM public.test_table WHERE hugeint_column IS NOT NULL": {
				"description": {"hugeint_column"},
				"types":       {Uint32ToString(pgtype.NumericOID)},
				"values":      {"1e+19"},
			},
			"SELECT hugeint_column FROM public.test_table WHERE hugeint_column IS NULL": {
				"description": {"hugeint_column"},
				"types":       {Uint32ToString(pgtype.NumericOID)},
				"values":      {""},
			},
			"SELECT xid_column FROM public.test_table WHERE xid_column IS NOT NULL": {
				"description": {"xid_column"},
				"types":       {Uint32ToString(pgtype.XIDOID)},
				"values":      {"4294967295"},
			},
			"SELECT xid_column FROM public.test_table WHERE xid_column IS NULL": {
				"description": {"xid_column"},
				"types":       {Uint32ToString(pgtype.XIDOID)},
				"values":      {""},
			},
			"SELECT xid8_column FROM public.test_table WHERE xid8_column IS NOT NULL": {
				"description": {"xid8_column"},
				"types":       {Uint32ToString(pgtype.XID8OID)},
				"values":      {"18446744073709551615"},
			},
			"SELECT xid8_column FROM public.test_table WHERE xid8_column IS NULL": {
				"description": {"xid8_column"},
				"types":       {Uint32ToString(pgtype.XID8OID)},
				"values":      {""},
			},
			"SELECT float4_column FROM public.test_table WHERE float4_column = 3.14": {
				"description": {"float4_column"},
				"types":       {Uint32ToString(pgtype.Float4OID)},
				"values":      {"3.14"},
			},
			"SELECT float4_column FROM public.test_table WHERE float4_column != 3.14": {
				"description": {"float4_column"},
				"types":       {Uint32ToString(pgtype.Float4OID)},
				"values":      {"NaN"},
			},
			"SELECT float8_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"float8_column"},
				"types":       {Uint32ToString(pgtype.Float8OID)},
				"values":      {"3.141592653589793"},
			},
			"SELECT float8_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"float8_column"},
				"types":       {Uint32ToString(pgtype.Float8OID)},
				"values":      {"-3.141592653589793"},
			},
			"SELECT numeric_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"numeric_column"},
				"types":       {Uint32ToString(pgtype.NumericOID)},
				"values":      {"12345.67"},
			},
			"SELECT numeric_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"numeric_column"},
				"types":       {Uint32ToString(pgtype.NumericOID)},
				"values":      {"-12345"},
			},
			"SELECT numeric_column_without_precision FROM public.test_table WHERE numeric_column_without_precision IS NOT NULL": {
				"description": {"numeric_column_without_precision"},
				"types":       {Uint32ToString(pgtype.NumericOID)},
				"values":      {"12345.67"},
			},
			"SELECT numeric_column_without_precision FROM public.test_table WHERE numeric_column_without_precision IS NULL": {
				"description": {"numeric_column_without_precision"},
				"types":       {Uint32ToString(pgtype.NumericOID)},
				"values":      {""},
			},
			"SELECT date_column FROM public.test_table LIMIT 1": {
				"description": {"date_column"},
				"types":       {Uint32ToString(pgtype.DateOID)},
				"values":      {"2021-01-01"},
			},
			"SELECT date_column FROM public.test_table LIMIT 1 OFFSET 1": {
				"description": {"date_column"},
				"types":       {Uint32ToString(pgtype.DateOID)},
				"values":      {"20025-11-12"},
			},
			"SELECT time_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"time_column"},
				"types":       {Uint32ToString(pgtype.TimeOID)},
				"values":      {"12:00:00.123456"},
			},
			"SELECT time_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"time_column"},
				"types":       {Uint32ToString(pgtype.TimeOID)},
				"values":      {"12:00:00.123"},
			},
			"SELECT timeMsColumn FROM public.test_table WHERE timeMsColumn IS NOT NULL": {
				"description": {"timeMsColumn"},
				"types":       {Uint32ToString(pgtype.TimeOID)},
				"values":      {"12:00:00.123"},
			},
			"SELECT timeMsColumn FROM public.test_table WHERE timeMsColumn IS NULL": {
				"description": {"timeMsColumn"},
				"types":       {Uint32ToString(pgtype.TimeOID)},
				"values":      {""},
			},
			"SELECT timetz_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"timetz_column"},
				"types":       {Uint32ToString(pgtype.TimeOID)},
				"values":      {"17:00:00.123456"},
			},
			"SELECT timetz_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"timetz_column"},
				"types":       {Uint32ToString(pgtype.TimeOID)},
				"values":      {"07:00:00.123"},
			},
			"SELECT timestamp_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"timestamp_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 12:00:00.123456"},
			},
			"SELECT timestamp_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"timestamp_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 12:00:00"},
			},
			"SELECT timestamp_ms_column FROM public.test_table WHERE timestamp_ms_column IS NOT NULL": {
				"description": {"timestamp_ms_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 12:00:00.123"},
			},
			"SELECT timestamp_ms_column FROM public.test_table WHERE timestamp_ms_column IS NULL": {
				"description": {"timestamp_ms_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {""},
			},
			"SELECT timestamptz_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"timestamptz_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 17:00:00.123456"},
			},
			"SELECT timestamptz_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"timestamptz_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 07:00:00.000123"},
			},
			"SELECT timestamptz_ms_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"timestamptz_ms_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 17:00:00.123"},
			},
			"SELECT timestamptz_ms_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"timestamptz_ms_column"},
				"types":       {Uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 07:00:00.12"},
			},
			"SELECT uuid_column FROM public.test_table WHERE uuid_column = '58a7c845-af77-44b2-8664-7ca613d92f04'": {
				"description": {"uuid_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"58a7c845-af77-44b2-8664-7ca613d92f04"},
			},
			"SELECT uuid_column FROM public.test_table WHERE uuid_column IS NULL": {
				"description": {"uuid_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT bytea_column FROM public.test_table WHERE bytea_column IS NOT NULL": {
				"description": {"bytea_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"\\x1234"},
			},
			"SELECT bytea_column FROM public.test_table WHERE bytea_column IS NULL": {
				"description": {"bytea_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT interval_column FROM public.test_table WHERE interval_column IS NOT NULL": {
				"description": {"interval_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"1 mon 2 days 01:00:01.000001"},
			},
			"SELECT interval_column FROM public.test_table WHERE interval_column IS NULL": {
				"description": {"interval_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT json_column FROM public.test_table WHERE json_column IS NOT NULL": {
				"description": {"json_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"{\"key\": \"value\"}"},
			},
			"SELECT json_column FROM public.test_table WHERE json_column IS NULL": {
				"description": {"json_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT jsonb_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"jsonb_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"{\"key\": \"value\"}"},
			},
			"SELECT jsonb_column->'key' AS key FROM public.test_table WHERE jsonb_column->>'key' = 'value'": {
				"description": {"key"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"\"value\""},
			},
			"SELECT jsonb_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"jsonb_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"{}"},
			},
			"SELECT tsvector_column FROM public.test_table WHERE tsvector_column IS NOT NULL": {
				"description": {"tsvector_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"'sampl':1 'text':2 'tsvector':4"},
			},
			"SELECT tsvector_column FROM public.test_table WHERE tsvector_column IS NULL": {
				"description": {"tsvector_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT xml_column FROM public.test_table WHERE xml_column IS NOT NULL": {
				"description": {"xml_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"<root><child>text</child></root>"},
			},
			"SELECT xml_column FROM public.test_table WHERE xml_column IS NULL": {
				"description": {"xml_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT pg_snapshot_column FROM public.test_table WHERE pg_snapshot_column IS NOT NULL": {
				"description": {"pg_snapshot_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"2784:2784:"},
			},
			"SELECT pg_snapshot_column FROM public.test_table WHERE pg_snapshot_column IS NULL": {
				"description": {"pg_snapshot_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT point_column FROM public.test_table WHERE point_column IS NOT NULL": {
				"description": {"point_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"(37.347301483154,45.002101898193)"},
			},
			"SELECT point_column FROM public.test_table WHERE point_column IS NULL": {
				"description": {"point_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT inet_column FROM public.test_table WHERE inet_column IS NOT NULL": {
				"description": {"inet_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"192.168.0.1"},
			},
			"SELECT inet_column FROM public.test_table WHERE inet_column IS NULL": {
				"description": {"inet_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT array_text_column FROM public.test_table WHERE array_text_column IS NOT NULL": {
				"description": {"array_text_column"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {"{one,two,three}"},
			},
			"SELECT array_text_column FROM public.test_table WHERE array_text_column IS NULL": {
				"description": {"array_text_column"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {""},
			},
			"SELECT array_int_column FROM public.test_table WHERE bool_column = TRUE": {
				"description": {"array_int_column"},
				"types":       {Uint32ToString(pgtype.Int4ArrayOID)},
				"values":      {"{1,2,3}"},
			},
			"SELECT array_int_column FROM public.test_table WHERE bool_column = FALSE": {
				"description": {"array_int_column"},
				"types":       {Uint32ToString(pgtype.Int4ArrayOID)},
				"values":      {""},
			},
			"SELECT array_jsonb_column FROM public.test_table WHERE array_jsonb_column IS NOT NULL": {
				"description": {"array_jsonb_column"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {`{"{""key"": ""value1""}","{""key"": ""value2""}"}`},
			},
			"SELECT array_jsonb_column FROM public.test_table WHERE array_jsonb_column IS NULL": {
				"description": {"array_jsonb_column"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {""},
			},
			"SELECT array_ltree_column FROM public.test_table WHERE array_ltree_column IS NOT NULL": {
				"description": {"array_ltree_column"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {"{a.b,c.d}"},
			},
			"SELECT array_ltree_column FROM public.test_table WHERE array_ltree_column IS NULL": {
				"description": {"array_ltree_column"},
				"types":       {Uint32ToString(pgtype.TextArrayOID)},
				"values":      {""},
			},
			"SELECT user_defined_column FROM public.test_table WHERE user_defined_column IS NOT NULL": {
				"description": {"user_defined_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"(Toronto)"},
			},
			"SELECT user_defined_column FROM public.test_table WHERE user_defined_column IS NULL": {
				"description": {"user_defined_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT relforcerowsecurity FROM pg_catalog.pg_class LIMIT 1": {
				"description": {"relforcerowsecurity"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
		})
	})

	t.Run("Type casts", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT '\"public\".\"test_table\"'::regclass::oid > 0 AS oid": {
				"description": {"oid"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT FORMAT('%I.%I', 'public', 'test_table')::regclass::oid > 0 AS oid": { // NOTE: ::regclass::oid on non-constants is not fully supported yet
				"description": {"oid"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {""},
			},
			"SELECT attrelid > 0 AS attrelid FROM pg_attribute WHERE attrelid = '\"public\".\"test_table\"'::regclass LIMIT 1": {
				"description": {"attrelid"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT COUNT(*) AS count FROM pg_attribute WHERE attrelid = '\"public\".\"test_table\"'::regclass": {
				"description": {"count"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"41"},
			},
			"SELECT objoid, classoid, objsubid, description FROM pg_description WHERE classoid = 'pg_class'::regclass": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int4OID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT d.objoid, d.classoid, c.relname, d.description FROM pg_description d JOIN pg_class c ON d.classoid = 'pg_class'::regclass": {
				"description": {"objoid", "classoid", "relname", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid, classoid, objsubid, description FROM (SELECT * FROM pg_description WHERE classoid = 'pg_class'::regclass) d": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int4OID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid, classoid, objsubid, description FROM pg_description WHERE (classoid = 'pg_class'::regclass AND objsubid = 0) OR classoid = 'pg_type'::regclass": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int4OID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid, classoid, objsubid, description FROM pg_description WHERE classoid IN ('pg_class'::regclass, 'pg_type'::regclass)": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.Int4OID), Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid FROM pg_description WHERE classoid = CASE WHEN true THEN 'pg_class'::regclass ELSE 'pg_type'::regclass END": {
				"description": {"objoid"},
				"types":       {Uint32ToString(pgtype.OIDOID)},
				"values":      {},
			},
			"SELECT word FROM (VALUES ('abort', 'U', 't', 'unreserved', 'can be bare label')) t(word, catcode, barelabel, catdesc, baredesc) WHERE word <> ALL('{a,abs,absolute,action}'::text[])": {
				"description": {"word"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"abort"},
			},
			"SELECT NULL::text AS word": {
				"description": {"word"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT t.x FROM (VALUES (1::int2, 'pg_type'::regclass)) t(x, y)": {
				"description": {"x"},
				"types":       {Uint32ToString(pgtype.Int2OID)},
				"values":      {"1"},
			},
			"SELECT 'pg_catalog.array_in'::regproc AS regproc": {
				"description": {"regproc"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"array_in"},
			},
			"SELECT uuid_column FROM test_table WHERE uuid_column IN ('58a7c845-af77-44b2-8664-7ca613d92f04'::uuid)": {
				"description": {"uuid_column"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"58a7c845-af77-44b2-8664-7ca613d92f04"},
			},
			"SELECT '1 week'::INTERVAL AS interval": {
				"description": {"interval"},
				"types":       {Uint32ToString(pgtype.IntervalOID)},
				"values":      {"0 months 7 days 0 microseconds"},
			},
			"SELECT date_trunc('month', '2025-02-24 15:58:23-05'::timestamptz + '-1 month'::interval) AS date": {
				"description": {"date"},
				"types":       {Uint32ToString(pgtype.TimestamptzOID)},
				"values":      {"2025-01-01 00:00:00+00:00"},
			},
			"SELECT 'foo'::pg_catalog.text AS text": {
				"description": {"text"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"foo"},
			},
			"SELECT 1::pg_catalog.regtype::pg_catalog.text AS text": {
				"description": {"text"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"1"},
			},
			"SELECT 1 AS value ORDER BY 1::pg_catalog.regclass::pg_catalog.text": {
				"description": {"value"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS text FROM pg_catalog.pg_statistic_ext": {
				"description": {"text"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
		})
	})

	t.Run("FROM function()", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT * FROM pg_catalog.pg_get_keywords() LIMIT 1": {
				"description": {"word", "catcode", "barelabel", "catdesc", "baredesc"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"abort", "U", "t", "unreserved", "can be bare label"},
			},
			"SELECT pg_get_keywords.word FROM pg_catalog.pg_get_keywords() LIMIT 1": {
				"description": {"word"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"abort"},
			},
			"SELECT * FROM generate_series(1, 2) AS series(index) LIMIT 1": {
				"description": {"index"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT * FROM generate_series(1, array_upper(current_schemas(FALSE), 1)) AS series(index) LIMIT 1": {
				"description": {"index"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT (information_schema._pg_expandarray(ARRAY[10])).n": {
				"description": {"n"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT (information_schema._pg_expandarray(ARRAY[10])).x AS value": {
				"description": {"value"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"10"},
			},
		})
	})

	t.Run("JOIN", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT s.usename, r.rolconfig FROM pg_catalog.pg_shadow s LEFT JOIN pg_catalog.pg_roles r ON s.usename = r.rolname": {
				"description": {"usename", "rolconfig"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb", ""},
			},
			"SELECT a.oid, pd.description FROM pg_catalog.pg_roles a LEFT JOIN pg_catalog.pg_shdescription pd ON a.oid = pd.objoid": {
				"description": {"oid", "description"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"10", ""},
			},
			"SELECT (SELECT 1 FROM (SELECT 1 AS inner_val) JOIN (SELECT NULL) ON inner_val = indclass[1]) AS test FROM pg_index": {
				"description": {"test"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
			},
		})
	})

	t.Run("CASE", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT CASE WHEN true THEN 'yes' ELSE 'no' END AS case": {
				"description": {"case"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"yes"},
			},
			"SELECT CASE WHEN false THEN 'yes' ELSE 'no' END AS case": {
				"description": {"case"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"no"},
			},
			"SELECT CASE WHEN true THEN 'one' WHEN false THEN 'two' ELSE 'three' END AS case": {
				"description": {"case"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"one"},
			},
			"SELECT CASE WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname = 'bdr') > 0 THEN 'pgd' WHEN (SELECT count(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END AS type": {
				"description": {"type"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT roles.oid AS id, roles.rolname AS name, roles.rolsuper AS is_superuser, CASE WHEN roles.rolsuper THEN true ELSE false END AS can_create_role FROM pg_catalog.pg_roles roles WHERE rolname = current_user": {
				"description": {"id", "name", "is_superuser", "can_create_role"},
				"types":       {Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
			"SELECT roles.oid AS id, roles.rolname AS name, roles.rolsuper AS is_superuser, CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END AS can_create_role FROM pg_catalog.pg_roles roles WHERE rolname = current_user": {
				"description": {"id", "name", "is_superuser", "can_create_role"},
				"types":       {Uint32ToString(pgtype.Int8OID), Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
			"SELECT CASE WHEN TRUE THEN pg_catalog.pg_is_in_recovery() END AS CASE": {
				"description": {"case"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT CASE WHEN FALSE THEN true ELSE pg_catalog.pg_is_in_recovery() END AS CASE": {
				"description": {"case"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END AS db_support FROM pg_catalog.pg_namespace nsp WHERE nsp.oid = 1268::OID;": {
				"description": {"db_support"},
				"types":       {Uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT CASE WHEN FORMAT('%s', test_table.varchar_column) = 'varchar' THEN 1 ELSE 2 END AS test_case FROM test_table LIMIT 1": {
				"description": {"test_case"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
		})
	})

	t.Run("WHERE pg_function()", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT gss_authenticated, encrypted FROM (SELECT false, false, false, false, false WHERE false) t(pid, gss_authenticated, principal, encrypted, credentials_delegated) WHERE pid = pg_backend_pid()": {
				"description": {"gss_authenticated", "encrypted"},
				"types":       {Uint32ToString(pgtype.BoolOID), Uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
		})
	})

	t.Run("WHERE with nested SELECT", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT int2_column FROM test_table WHERE int2_column > 0 AND int2_column = (SELECT int2_column FROM test_table WHERE int2_column = 32767)": {
				"description": {"int2_column"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"32767"},
			},
		})
	})

	t.Run("WHERE ANY(column reference)", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT id FROM test_table WHERE id = ANY(id)": { // NOTE: ... = ANY() on non-constants is not fully supported yet
				"description": {"id"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
			},
		})
	})

	t.Run("WITH", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"WITH RECURSIVE simple_cte AS (SELECT oid, rolname FROM pg_roles WHERE rolname = 'postgres' UNION ALL SELECT oid, rolname FROM pg_roles) SELECT * FROM simple_cte": {
				"description": {"oid", "rolname"},
				"types":       {Uint32ToString(pgtype.OIDOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"10", "bemidb"},
			},
		})
	})

	t.Run("ORDER BY", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT ARRAY(SELECT 1 FROM pg_enum ORDER BY enumsortorder) AS array": {
				"description": {"array"},
				"types":       {Uint32ToString(pgtype.Int4ArrayOID)},
				"values":      {"{}"},
			},
			"SELECT test_table.id FROM public.test_table ORDER BY public.test_table.id DESC LIMIT 1": {
				"description": {"id"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"2"},
			},
		})
	})

	t.Run("GROUP BY", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT MAX(id) AS max FROM public.test_table GROUP BY public.test_table.id LIMIT 1": {
				"description": {"max"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
		})
	})

	t.Run("FROM table alias", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT pg_shadow.usename FROM pg_shadow": {
				"description": {"usename"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
			"SELECT pg_roles.rolname FROM pg_roles": {
				"description": {"rolname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
			"SELECT pg_extension.extname FROM pg_extension": {
				"description": {"extname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"plpgsql"},
			},
			"SELECT pg_database.datname FROM pg_database": {
				"description": {"datname"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
			"SELECT pg_inherits.inhrelid FROM pg_inherits": {
				"description": {"inhrelid"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {},
			},
			"SELECT pg_shdescription.objoid FROM pg_shdescription": {
				"description": {"objoid"},
				"types":       {Uint32ToString(pgtype.OIDOID)},
				"values":      {},
			},
			"SELECT pg_statio_user_tables.relid FROM pg_statio_user_tables": {
				"description": {"relid"},
				"types":       {Uint32ToString(pgtype.Int8OID)},
				"values":      {},
			},
			"SELECT pg_replication_slots.slot_name FROM pg_replication_slots": {
				"description": {"slot_name"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT pg_stat_gssapi.pid FROM pg_stat_gssapi": {
				"description": {"pid"},
				"types":       {Uint32ToString(pgtype.Int4OID)},
				"values":      {},
			},
			"SELECT pg_auth_members.oid FROM pg_auth_members": {
				"description": {"oid"},
				"types":       {Uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
		})
	})

	t.Run("Sublink", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT x.usename, (SELECT passwd FROM pg_shadow WHERE usename = x.usename) as password FROM pg_shadow x WHERE x.usename = 'bemidb'": {
				"description": {"usename", "password"},
				"types":       {Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb", "bemidb-encrypted"},
			},
		})
	})

	t.Run("Type comparisons", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT db.oid AS did, db.datname AS name, ta.spcname AS spcname, db.datallowconn, db.datistemplate AS is_template, pg_catalog.has_database_privilege(db.oid, 'CREATE') AS cancreate, datdba AS owner, descr.description FROM pg_catalog.pg_database db LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace = ta.oid LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (db.oid = descr.objoid AND descr.classoid = 'pg_database'::regclass) WHERE db.oid > 1145::OID OR db.datname IN ('postgres', 'edb') ORDER BY datname": {
				"description": {"did", "name", "spcname", "datallowconn", "is_template", "cancreate", "owner", "description"},
				"types": {
					Uint32ToString(pgtype.OIDOID),
					Uint32ToString(pgtype.TextOID),
					Uint32ToString(pgtype.TextOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.BoolOID),
					Uint32ToString(pgtype.Int8OID),
					Uint32ToString(pgtype.TextOID),
				},
				"values": {"16388", "bemidb", "", "t", "f", "t", "10", ""},
			},
		})
	})

	t.Run("Returns an error if a table does not exist", func(t *testing.T) {
		_, err := queryHandler.HandleSimpleQuery("SELECT * FROM non_existent_table")

		if err == nil {
			t.Errorf("Expected an error, got nil")
		}

		expectedErrorMessage := strings.Join([]string{
			"Catalog Error: Table with name non_existent_table does not exist!",
			"Did you mean \"test_table\"?",
			"LINE 1: SELECT * FROM non_existent_table",
			"                      ^",
		}, "\n")
		if err.Error() != expectedErrorMessage {
			t.Errorf("Expected the error to be '"+expectedErrorMessage+"', got %v", err.Error())
		}
	})

	t.Run("Returns a result without a row description for SET queries", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "SET")
	})

	t.Run("Allows setting and querying timezone", func(t *testing.T) {
		queryHandler.HandleSimpleQuery("SET timezone = 'UTC'")

		messages, err := queryHandler.HandleSimpleQuery("show timezone")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testRowDescription(t, messages[0], []string{"timezone"}, []string{Uint32ToString(pgtype.TextOID)})
		testDataRowValues(t, messages[1], []string{"UTC"})
		testCommandCompleteTag(t, messages[2], "SHOW")
	})

	t.Run("Handles an empty query", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("-- ping")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.EmptyQueryResponse{},
		})
	})

	t.Run("Handles a DISCARD ALL query", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("DISCARD ALL")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "DISCARD ALL")
	})

	t.Run("Handles a BEGIN query", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("BEGIN")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "BEGIN")
	})
}

func TestHandleParseQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("Handles PARSE extended query step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		message := &pgproto3.Parse{Query: query}

		messages, preparedStatement, err := queryHandler.HandleParseQuery(message)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.ParseComplete{},
		})

		remappedQuery := "SELECT usename, passwd FROM main.pg_shadow WHERE usename = $1"
		if preparedStatement.Query != remappedQuery {
			t.Errorf("Expected the prepared statement query to be %v, got %v", remappedQuery, preparedStatement.Query)
		}
		if preparedStatement.Statement == nil {
			t.Errorf("Expected the prepared statement to have a statement")
		}
	})

	t.Run("Handles PARSE extended query step if query is empty", func(t *testing.T) {
		message := &pgproto3.Parse{Query: ""}

		messages, preparedStatement, err := queryHandler.HandleParseQuery(message)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.ParseComplete{},
		})

		if preparedStatement.Query != "" {
			t.Errorf("Expected the prepared statement query to be empty, got %v", preparedStatement.Query)
		}
		if preparedStatement.Statement != nil {
			t.Errorf("Expected the prepared statement not to have a statement, got %v", preparedStatement.Statement)
		}
	})
}

func TestHandleBindQuery(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("Handles BIND extended query step with text format parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{[]byte("bemidb")},
			ParameterFormatCodes: []int16{0}, // Text format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != "bemidb" {
			t.Errorf("Expected the prepared statement variable to be 'bemidb', got %v", preparedStatement.Variables[0])
		}
	})

	t.Run("Handles BIND extended query step with binary format 4-byte parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace = $1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		paramValue := int32(2200)
		paramBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(paramBytes, uint32(paramValue))

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{paramBytes},
			ParameterFormatCodes: []int16{1}, // Binary format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != paramValue {
			t.Errorf("Expected the prepared statement variable to be %v, got %v", paramValue, preparedStatement.Variables[0])
		}
	})

	t.Run("Handles BIND extended query step with binary format 8-byte parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace = $1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		paramValue := int64(2200)
		paramBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(paramBytes, uint64(paramValue))

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{paramBytes},
			ParameterFormatCodes: []int16{1}, // Binary format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != paramValue {
			t.Errorf("Expected the prepared statement variable to be %v, got %v", paramValue, preparedStatement.Variables[0])
		}
	})

	t.Run("Handles BIND extended query step with binary format 16-byte (uuid) parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT uuid_column FROM public.test_table WHERE uuid_column = $1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		uuidParam := "58a7c845-af77-44b2-8664-7ca613d92f04"
		paramBytes, _ := uuid.Must(uuid.Parse(uuidParam)).MarshalBinary()

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{paramBytes},
			ParameterFormatCodes: []int16{1}, // Binary format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != uuidParam {
			t.Errorf("Expected the prepared statement variable to be %v, got %v", uuidParam, preparedStatement.Variables[0])
		}
	})
}

func TestHandleDescribeQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("Handles DESCRIBE extended query step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		parseMessage := &pgproto3.Parse{Query: query}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{Parameters: [][]byte{[]byte("bemidb")}}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		message := &pgproto3.Describe{ObjectType: 'P'}

		messages, preparedStatement, err := queryHandler.HandleDescribeQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.RowDescription{},
		})
		testRowDescription(t, messages[0], []string{"usename", "passwd"}, []string{Uint32ToString(pgtype.TextOID), Uint32ToString(pgtype.TextOID)})
		if preparedStatement.Rows == nil {
			t.Errorf("Expected the prepared statement to have rows")
		}
	})

	t.Run("Handles DESCRIBE extended query step if query is empty", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: ""}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		message := &pgproto3.Describe{ObjectType: 'P'}

		messages, _, err := queryHandler.HandleDescribeQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.NoData{},
		})
	})

	t.Run("Handles DESCRIBE (Statement) extended query step if there was no BIND step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		parseMessage := &pgproto3.Parse{Query: query, ParameterOIDs: []uint32{pgtype.TextOID}}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		message := &pgproto3.Describe{ObjectType: 'S'}

		messages, _, err := queryHandler.HandleDescribeQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.NoData{},
		})
	})
}

func TestHandleExecuteQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("Handles EXECUTE extended query step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		parseMessage := &pgproto3.Parse{Query: query}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{Parameters: [][]byte{[]byte("bemidb")}}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		describeMessage := &pgproto3.Describe{ObjectType: 'P'}
		_, preparedStatement, _ = queryHandler.HandleDescribeQuery(describeMessage, preparedStatement)
		message := &pgproto3.Execute{}

		messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testDataRowValues(t, messages[0], []string{"bemidb", "bemidb-encrypted"})
	})

	t.Run("Handles EXECUTE extended query step if query is empty", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: ""}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		describeMessage := &pgproto3.Describe{ObjectType: 'P'}
		_, preparedStatement, _ = queryHandler.HandleDescribeQuery(describeMessage, preparedStatement)
		message := &pgproto3.Execute{}

		messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.EmptyQueryResponse{},
		})
	})
}

func TestHandleMultipleQueries(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("Handles multiple SET statements", func(t *testing.T) {
		query := `SET client_encoding TO 'UTF8';
SET client_min_messages TO 'warning';
SET standard_conforming_strings = on;`

		messages, err := queryHandler.HandleSimpleQuery(query)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
			&pgproto3.CommandComplete{},
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "SET")
		testCommandCompleteTag(t, messages[1], "SET")
		testCommandCompleteTag(t, messages[2], "SET")
	})

	t.Run("Handles mixed SET and SELECT statements", func(t *testing.T) {
		query := `SET client_encoding TO 'UTF8';
SELECT passwd FROM pg_shadow WHERE usename='bemidb';`

		messages, err := queryHandler.HandleSimpleQuery(query)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "SET")
		testDataRowValues(t, messages[2], []string{"bemidb-encrypted"})
		testCommandCompleteTag(t, messages[3], "SELECT 1")
	})

	t.Run("Handles multiple SELECT statements", func(t *testing.T) {
		query := `SELECT 1;
SELECT passwd FROM pg_shadow WHERE usename='bemidb';`

		messages, err := queryHandler.HandleSimpleQuery(query)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testDataRowValues(t, messages[1], []string{"1"})
		testCommandCompleteTag(t, messages[2], "SELECT 1")
		testDataRowValues(t, messages[4], []string{"bemidb-encrypted"})
		testCommandCompleteTag(t, messages[5], "SELECT 1")
	})

	t.Run("Handles error in any of multiple statements", func(t *testing.T) {
		query := `SET client_encoding TO 'UTF8';
SELECT * FROM non_existent_table;
SET standard_conforming_strings = on;`

		_, err := queryHandler.HandleSimpleQuery(query)

		if err == nil {
			t.Error("Expected an error for non-existent table, got nil")
			return
		}

		if !strings.Contains(err.Error(), "non_existent_table") {
			t.Errorf("Expected error message to contain 'non_existent_table', got: %s", err.Error())
		}
	})
}

func initQueryHandler() *QueryHandler {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	icebergReader := NewIcebergReader(config)
	duckdb.ExecFile(icebergReader.InternalStartSqlFile())
	return NewQueryHandler(config, duckdb, icebergReader)
}

func createTestTables(t *testing.T) {
	config := loadTestConfig()

	createTestTable(config, IcebergSchemaTable{Schema: "public", Table: "test_table"}, PUBLIC_SCHEMA_TEST_TABLE_PG_SCHEMA_COLUMNS, PUBLIC_SCHEMA_TEST_TABLE_LOADED_ROWS)
	createTestTable(config, IcebergSchemaTable{Schema: "public", Table: "partitioned_table1"}, PUBLIC_SCHEMA_PARTITIONED_TABLE_PG_SCHEMA_COLUMNS, PUBLIC_SCHEMA_PARTITIONED_TABLE1_LOADED_ROWS)
	createTestTable(config, IcebergSchemaTable{Schema: "public", Table: "partitioned_table2"}, PUBLIC_SCHEMA_PARTITIONED_TABLE_PG_SCHEMA_COLUMNS, PUBLIC_SCHEMA_PARTITIONED_TABLE2_LOADED_ROWS)
	createTestTable(config, IcebergSchemaTable{Schema: "public", Table: "partitioned_table3"}, PUBLIC_SCHEMA_PARTITIONED_TABLE_PG_SCHEMA_COLUMNS, PUBLIC_SCHEMA_PARTITIONED_TABLE3_LOADED_ROWS)
	createTestTable(config, IcebergSchemaTable{Schema: "test", Table: "empty_table"}, TEST_SCHEMA_EMPTY_TABLE_PG_SCHEMA_COLUMNS, TEST_SCHEMA_EMPTY_TABLE_LOADED_ROWS)

	syncer := NewSyncer(config)
	syncer.WriteInternalStartSqlFile([]PgSchemaTable{
		{Schema: "public", Table: "test_table"},
		{Schema: "public", Table: "partitioned_table1", ParentPartitionedTable: "partitioned_table"},
		{Schema: "public", Table: "partitioned_table2", ParentPartitionedTable: "partitioned_table"},
		{Schema: "public", Table: "partitioned_table3", ParentPartitionedTable: "partitioned_table"},
		{Schema: "test", Table: "empty_table"},
	})

	t.Cleanup(func() {
		syncer.icebergWriter.DeleteSchema("public")
		syncer.icebergWriter.DeleteSchema("test")
		syncer.icebergWriter.WriteInternalStartSqlFile([]string{})
	})
}

func createTestTable(config *Config, icebergSchemaTable IcebergSchemaTable, pgSchemaColumns []PgSchemaColumn, rows [][]string) StorageInterface {
	xmin := uint32(0)
	internalTableMetadata := InternalTableMetadata{LastSyncedAt: 1744813713, LastRefreshMode: RefreshModeFull, MaxXmin: &xmin}
	icebergTableWriter := NewIcebergWriterTable(config, icebergSchemaTable, pgSchemaColumns, 777, MAX_PARQUET_PAYLOAD_THRESHOLD, false)
	i := 0
	icebergTableWriter.Write(
		func() ([][]string, InternalTableMetadata) {
			if i > 0 {
				return [][]string{}, internalTableMetadata
			}

			i++
			return rows, internalTableMetadata
		},
	)

	return icebergTableWriter.storage
}

func testNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func testMessageTypes(t *testing.T, messages []pgproto3.Message, expectedTypes []pgproto3.Message) {
	if len(messages) != len(expectedTypes) {
		t.Errorf("Expected %v messages, got %v", len(expectedTypes), len(messages))
	}

	for i, expectedType := range expectedTypes {
		if reflect.TypeOf(messages[i]) != reflect.TypeOf(expectedType) {
			t.Errorf("Expected the %v message to be a %v", i, expectedType)
		}
	}
}

func testRowDescription(t *testing.T, rowDescriptionMessage pgproto3.Message, expectedColumnNames []string, expectedColumnTypes []string) {
	rowDescription := rowDescriptionMessage.(*pgproto3.RowDescription)

	if len(rowDescription.Fields) != len(expectedColumnNames) {
		t.Errorf("Expected %v row description fields, got %v", len(expectedColumnNames), len(rowDescription.Fields))
	}

	for i, expectedColumnName := range expectedColumnNames {
		if string(rowDescription.Fields[i].Name) != expectedColumnName {
			t.Errorf("Expected the %v row description field to be %v, got %v", i, expectedColumnName, string(rowDescription.Fields[i].Name))
		}
	}

	for i, expectedColumnType := range expectedColumnTypes {
		if Uint32ToString(rowDescription.Fields[i].DataTypeOID) != expectedColumnType {
			t.Errorf("Expected the %v row description field data type to be %v, got %v", i, expectedColumnType, Uint32ToString(rowDescription.Fields[i].DataTypeOID))
		}
	}
}

func testDataRowValues(t *testing.T, dataRowMessage pgproto3.Message, expectedValues []string) {
	dataRow := dataRowMessage.(*pgproto3.DataRow)

	if len(dataRow.Values) != len(expectedValues) {
		t.Errorf("Expected %v data row values, got %v", len(expectedValues), len(dataRow.Values))
	}

	for i, expectedValue := range expectedValues {
		if string(dataRow.Values[i]) != expectedValue {
			t.Errorf("Expected the %v data row value to be %v, got %v", i, expectedValue, string(dataRow.Values[i]))
		}
	}
}

func testCommandCompleteTag(t *testing.T, message pgproto3.Message, expectedTag string) {
	commandComplete := message.(*pgproto3.CommandComplete)
	if string(commandComplete.CommandTag) != expectedTag {
		t.Errorf("Expected the command tag to be %v, got %v", expectedTag, string(commandComplete.CommandTag))
	}
}

func testResponseByQuery(t *testing.T, queryHandler *QueryHandler, responseByQuery map[string]map[string][]string) {
	for query, responses := range responseByQuery {
		t.Run(query, func(t *testing.T) {
			messages, err := queryHandler.HandleSimpleQuery(query)

			testNoError(t, err)
			testRowDescription(t, messages[0], responses["description"], responses["types"])

			if len(responses["values"]) > 0 {
				testMessageTypes(t, messages, []pgproto3.Message{
					&pgproto3.RowDescription{},
					&pgproto3.DataRow{},
					&pgproto3.CommandComplete{},
				})
				testDataRowValues(t, messages[1], responses["values"])
			} else {
				testMessageTypes(t, messages, []pgproto3.Message{
					&pgproto3.RowDescription{},
					&pgproto3.CommandComplete{},
				})
			}
		})
	}
}
