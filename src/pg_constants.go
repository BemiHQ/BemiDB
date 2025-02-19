package main

const (
	PG_SCHEMA_INFORMATION_SCHEMA = "information_schema"
	PG_SCHEMA_PG_CATALOG         = "pg_catalog"
	PG_SCHEMA_PUBLIC             = "public"

	PG_FUNCTION_ARRAY_TO_STRING      = "array_to_string"
	PG_FUNCTION_ARRAY_UPPER          = "array_upper"
	PG_FUNCTION_PG_EXPANDARRAY       = "_pg_expandarray"
	PG_FUNCTION_PG_GET_EXPR          = "pg_get_expr"
	PG_FUNCTION_PG_GET_KEYWORDS      = "pg_get_keywords"
	PG_FUNCTION_PG_IS_IN_RECOVERY    = "pg_is_in_recovery"
	PG_FUNCTION_PG_SHOW_ALL_SETTINGS = "pg_show_all_settings"
	PG_FUNCTION_QUOTE_INDENT         = "quote_ident"
	PG_FUNCTION_ROW_TO_JSON          = "row_to_json"
	PG_FUNCTION_ACLEXPLODE           = "aclexplode"
	PG_FUNCTION_PG_GET_VIEWDEF       = "pg_get_viewdef"
	PG_FUNCTION_FORMAT               = "format"

	PG_TABLE_PG_ATTRIBUTE          = "pg_attribute"
	PG_TABLE_PG_AUTH_MEMBERS       = "pg_auth_members"
	PG_TABLE_PG_CLASS              = "pg_class"
	PG_TABLE_PG_COLLATION          = "pg_collation"
	PG_TABLE_PG_DATABASE           = "pg_database"
	PG_TABLE_PG_EXTENSION          = "pg_extension"
	PG_TABLE_PG_INDEX              = "pg_index"
	PG_TABLE_PG_INHERITS           = "pg_inherits"
	PG_TABLE_PG_MATVIEWS           = "pg_matviews"
	PG_TABLE_PG_NAMESPACE          = "pg_namespace"
	PG_TABLE_PG_OPCLASS            = "pg_opclass"
	PG_TABLE_PG_REPLICATION_SLOTS  = "pg_replication_slots"
	PG_TABLE_PG_ROLES              = "pg_roles"
	PG_TABLE_PG_SHADOW             = "pg_shadow"
	PG_TABLE_PG_SHDESCRIPTION      = "pg_shdescription"
	PG_TABLE_PG_STATIO_USER_TABLES = "pg_statio_user_tables"
	PG_TABLE_PG_STAT_ACTIVITY      = "pg_stat_activity"
	PG_TABLE_PG_STAT_GSSAPI        = "pg_stat_gssapi"
	PG_TABLE_PG_STAT_USER_TABLES   = "pg_stat_user_tables"
	PG_TABLE_PG_USER               = "pg_user"
	PG_TABLE_PG_VIEWS              = "pg_views"
	PG_TABLE_TABLES                = "tables"

	PG_VAR_SEARCH_PATH = "search_path"
)

type ColumnDefinition struct {
	Name string
	Type string
}

type TableDefinition struct {
	Columns []ColumnDefinition
	Values  []string
}

type ConstantDefinition struct {
	Value string
	Type  string
}

var PG_INHERITS_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"inhrelid", "oid"},
		{"inhparent", "oid"},
		{"inhseqno", "int4"},
		{"inhdetachpending", "bool"},
	},
}

var PG_SHDESCRIPTION_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"objoid", "oid"},
		{"classoid", "oid"},
		{"description", "text"},
	},
}

var PG_STATIO_USER_TABLES_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"relid", "oid"},
		{"schemaname", "text"},
		{"relname", "text"},
		{"heap_blks_read", "int8"},
		{"heap_blks_hit", "int8"},
		{"idx_blks_read", "int8"},
		{"idx_blks_hit", "int8"},
		{"toast_blks_read", "int8"},
		{"toast_blks_hit", "int8"},
		{"tidx_blks_read", "int8"},
		{"tidx_blks_hit", "int8"},
	},
}

var PG_REPLICATION_SLOTS_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"slot_name", "text"},
		{"plugin", "text"},
		{"slot_type", "text"},
		{"datoid", "oid"},
		{"database", "text"},
		{"temporary", "bool"},
		{"active", "bool"},
		{"active_pid", "int4"},
		{"xmin", "int8"},
		{"catalog_xmin", "int8"},
		{"restart_lsn", "text"},
		{"confirmed_flush_lsn", "text"},
		{"wal_status", "text"},
		{"safe_wal_size", "int8"},
		{"two_phase", "bool"},
		{"conflicting", "bool"},
	},
}

var PG_SHADOW_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"usename", "text"},
		{"usesysid", "oid"},
		{"usecreatedb", "bool"},
		{"usesuper", "bool"},
		{"userepl", "bool"},
		{"usebypassrls", "bool"},
		{"passwd", "text"},
		{"valuntil", "timestamp"},
		{"useconfig", "text[]"},
	},
	Values: []string{
		"bemidb",
		"10",
		"FALSE",
		"FALSE",
		"TRUE",
		"FALSE",
		"",
		"NULL",
		"NULL",
	},
}

var PG_ROLES_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"oid", "oid"},
		{"rolname", "text"},
		{"rolsuper", "bool"},
		{"rolinherit", "bool"},
		{"rolcreaterole", "bool"},
		{"rolcreatedb", "bool"},
		{"rolcanlogin", "bool"},
		{"rolreplication", "bool"},
		{"rolconnlimit", "int4"},
		{"rolpassword", "text"},
		{"rolvaliduntil", "timestamp"},
		{"rolbypassrls", "bool"},
		{"rolconfig", "text[]"},
	},
	Values: []string{
		"10",
		"",
		"true",
		"true",
		"true",
		"true",
		"true",
		"false",
		"-1",
		"NULL",
		"NULL",
		"false",
		"NULL",
	},
}

var PG_USER_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"usename", "text"},
		{"usesysid", "oid"},
		{"usecreatedb", "bool"},
		{"usesuper", "bool"},
		{"userepl", "bool"},
		{"usebypassrls", "bool"},
		{"passwd", "text"},
		{"valuntil", "timestamp"},
		{"useconfig", "text[]"},
	},
	Values: []string{
		"",
		"10",
		"t",
		"t",
		"t",
		"t",
		"",
		"NULL",
		"NULL",
	},
}

var PG_DATABASE_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"oid", "oid"},
		{"datname", "text"},
		{"datdba", "oid"},
		{"encoding", "int4"},
		{"datlocprovider", "text"},
		{"datistemplate", "bool"},
		{"datallowconn", "bool"},
		{"datconnlimit", "int4"},
		{"datfrozenxid", "int8"},
		{"datminmxid", "int4"},
		{"dattablespace", "oid"},
		{"datcollate", "text"},
		{"datctype", "text"},
		{"datlocale", "text"},
		{"daticurules", "text"},
		{"datcollversion", "text"},
		{"datacl", "text[]"},
	},
	Values: []string{
		"16388",
		"",
		"10",
		"6",
		"c",
		"FALSE",
		"TRUE",
		"-1",
		"722",
		"1",
		"1663",
		"en_US.UTF-8",
		"en_US.UTF-8",
		"NULL",
		"NULL",
		"NULL",
		"NULL",
	},
}

var PG_EXTENSION_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"oid", "oid"},
		{"extname", "text"},
		{"extowner", "oid"},
		{"extnamespace", "oid"},
		{"extrelocatable", "bool"},
		{"extversion", "text"},
		{"extconfig", "text[]"},
		{"extcondition", "text[]"},
	},
	Values: []string{
		"13823",
		"plpgsql",
		"10",
		"11",
		"false",
		"1.0",
		"NULL",
		"NULL",
	},
}

var PG_COLLATION_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"oid", "oid"},
		{"collname", "text"},
		{"collnamespace", "oid"},
		{"collowner", "oid"},
		{"collprovider", "text"},
		{"collisdeterministic", "bool"},
		{"collencoding", "int4"},
		{"collcollate", "text"},
		{"collctype", "text"},
		{"colliculocale", "text"},
		{"collicurules", "text"},
		{"collversion", "text"},
	},
	Values: []string{
		"100",
		"default",
		"11",
		"10",
		"d",
		"TRUE",
		"-1",
		"NULL",
		"NULL",
		"NULL",
		"NULL",
		"NULL",
	},
}

var PG_STAT_USER_TABLES_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"relid", "oid"},
		{"schemaname", "text"},
		{"relname", "text"},
		{"seq_scan", "int8"},
		{"last_seq_scan", "timestamp"},
		{"seq_tup_read", "int8"},
		{"idx_scan", "int8"},
		{"last_idx_scan", "timestamp"},
		{"idx_tup_fetch", "int8"},
		{"n_tup_ins", "int8"},
		{"n_tup_upd", "int8"},
		{"n_tup_del", "int8"},
		{"n_tup_hot_upd", "int8"},
		{"n_tup_newpage_upd", "int8"},
		{"n_live_tup", "int8"},
		{"n_dead_tup", "int8"},
		{"n_mod_since_analyze", "int8"},
		{"n_ins_since_vacuum", "int8"},
		{"last_vacuum", "timestamp"},
		{"last_autovacuum", "timestamp"},
		{"last_analyze", "timestamp"},
		{"last_autoanalyze", "timestamp"},
		{"vacuum_count", "int8"},
		{"autovacuum_count", "int8"},
		{"analyze_count", "int8"},
		{"autoanalyze_count", "int8"},
	},
	Values: []string{
		"123456",
		"",
		"",
		"0",
		"NULL",
		"0",
		"0",
		"NULL",
		"0",
		"0",
		"0",
		"0",
		"0",
		"0",
		"1",
		"0",
		"0",
		"0",
		"NULL",
		"NULL",
		"NULL",
		"NULL",
		"0",
		"0",
		"0",
		"0",
	},
}

var PG_STAT_GSSAPI_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"pid", "int4"},
		{"gss_authenticated", "bool"},
		{"principal", "text"},
		{"encrypted", "bool"},
		{"credentials_delegated", "bool"},
	},
}

var PG_AUTH_MEMBERS_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"oid", "text"},
		{"roleid", "oid"},
		{"member", "oid"},
		{"grantor", "oid"},
		{"admin_option", "bool"},
		{"inherit_option", "bool"},
		{"set_option", "bool"},
	},
}

var PG_STAT_ACTIVITY_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"datid", "oid"},
		{"datname", "text"},
		{"pid", "int4"},
		{"usesysid", "oid"},
		{"usename", "text"},
		{"application_name", "text"},
		{"client_addr", "inet"},
		{"client_hostname", "text"},
		{"client_port", "int4"},
		{"backend_start", "timestamp"},
		{"xact_start", "timestamp"},
		{"query_start", "timestamp"},
		{"state_change", "timestamp"},
		{"wait_event_type", "text"},
		{"wait_event", "text"},
		{"state", "text"},
		{"backend_xid", "int8"},
		{"backend_xmin", "int8"},
		{"query", "text"},
		{"backend_type", "text"},
	},
}

var PG_VIEWS_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"schemaname", "text"},
		{"viewname", "text"},
		{"viewowner", "text"},
		{"definition", "text"},
	},
}

var PG_MATVIEWS_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"schemaname", "text"},
		{"matviewname", "text"},
		{"matviewowner", "text"},
		{"tablespace", "text"},
		{"hasindexes", "bool"},
		{"ispopulated", "bool"},
		{"definition", "text"},
	},
}

var PG_OPCLASS_DEFINITION = TableDefinition{
	Columns: []ColumnDefinition{
		{"oid", "oid"},
		{"opcmethod", "oid"},
		{"opcname", "text"},
		{"opcnamespace", "oid"},
		{"opcowner", "oid"},
		{"opcfamily", "oid"},
		{"opcintype", "oid"},
		{"opcdefault", "bool"},
		{"opckeytype", "oid"},
	},
}

var PG_SYSTEM_TABLES = NewSet([]string{
	"pg_aggregate",
	"pg_am",
	"pg_amop",
	"pg_amproc",
	"pg_attrdef",
	"pg_attribute",
	"pg_auth_members",
	"pg_authid",
	"pg_cast",
	"pg_class",
	"pg_collation",
	"pg_constraint",
	"pg_conversion",
	"pg_database",
	"pg_db_role_setting",
	"pg_default_acl",
	"pg_depend",
	"pg_description",
	"pg_enum",
	"pg_event_trigger",
	"pg_extension",
	"pg_foreign_data_wrapper",
	"pg_foreign_server",
	"pg_foreign_table",
	"pg_index",
	"pg_inherits",
	"pg_init_privs",
	"pg_language",
	"pg_largeobject",
	"pg_largeobject_metadata",
	"pg_matviews",
	"pg_namespace",
	"pg_opclass",
	"pg_operator",
	"pg_opfamily",
	"pg_parameter_acl",
	"pg_partitioned_table",
	"pg_policy",
	"pg_proc",
	"pg_publication",
	"pg_publication_namespace",
	"pg_publication_rel",
	"pg_user",
	"pg_range",
	"pg_replication_origin",
	"pg_replication_slots",
	"pg_rewrite",
	"pg_roles",
	"pg_seclabel",
	"pg_sequence",
	"pg_shadow",
	"pg_shdepend",
	"pg_shdescription",
	"pg_shseclabel",
	"pg_statistic",
	"pg_statistic_ext",
	"pg_statistic_ext_data",
	"pg_subscription",
	"pg_subscription_rel",
	"pg_tablespace",
	"pg_transform",
	"pg_trigger",
	"pg_ts_config",
	"pg_ts_config_map",
	"pg_ts_dict",
	"pg_ts_parser",
	"pg_ts_template",
	"pg_type",
	"pg_user_mapping",
	"pg_views",
})

var PG_SYSTEM_VIEWS = NewSet([]string{
	"pg_stat_activity",
	"pg_stat_replication",
	"pg_stat_wal_receiver",
	"pg_stat_recovery_prefetch",
	"pg_stat_subscription",
	"pg_stat_ssl",
	"pg_stat_gssapi",
	"pg_stat_progress_analyze",
	"pg_stat_progress_create_index",
	"pg_stat_progress_vacuum",
	"pg_stat_progress_cluster",
	"pg_stat_progress_basebackup",
	"pg_stat_progress_copy",
	"pg_stat_archiver",
	"pg_stat_bgwriter",
	"pg_stat_checkpointer",
	"pg_stat_database",
	"pg_stat_database_conflicts",
	"pg_stat_io",
	"pg_stat_replication_slots",
	"pg_stat_slru",
	"pg_stat_subscription_stats",
	"pg_stat_wal",
	"pg_stat_all_tables",
	"pg_stat_sys_tables",
	"pg_stat_user_tables",
	"pg_stat_xact_all_tables",
	"pg_stat_xact_sys_tables",
	"pg_stat_xact_user_tables",
	"pg_stat_all_indexes",
	"pg_stat_sys_indexes",
	"pg_stat_user_indexes",
	"pg_stat_user_functions",
	"pg_stat_xact_user_functions",
	"pg_statio_all_tables",
	"pg_statio_sys_tables",
	"pg_statio_user_tables",
	"pg_statio_all_indexes",
	"pg_statio_sys_indexes",
	"pg_statio_user_indexes",
	"pg_statio_all_sequences",
	"pg_statio_sys_sequences",
	"pg_statio_user_sequences",
})

var PG_SYSTEM_FUNCTIONS = NewSet([]string{
	"pg_get_expr",
	"pg_get_keywords",
	"pg_is_in_recovery",
	"pg_show_all_settings",
})
