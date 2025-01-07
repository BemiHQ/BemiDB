package main

const (
	PG_SCHEMA_INFORMATION_SCHEMA = "information_schema"
	PG_SCHEMA_PG_CATALOG         = "pg_catalog"
	PG_SCHEMA_PUBLIC             = "public"

	PG_FUNCTION_ARRAY_TO_STRING      = "array_to_string"
	PG_FUNCTION_ARRAY_UPPER          = "array_upper"
	PG_FUNCTION_PG_EXPANDARRAY       = "_pg_expandarray"
	PG_FUNCTION_PG_GET_EXPR          = "pg_get_expr"
	PG_FUNCTION_PG_GET_INDEXDEF      = "pg_get_indexdef"
	PG_FUNCTION_PG_GET_KEYWORDS      = "pg_get_keywords"
	PG_FUNCTION_PG_IS_IN_RECOVERY    = "pg_is_in_recovery"
	PG_FUNCTION_PG_SHOW_ALL_SETTINGS = "pg_show_all_settings"
	PG_FUNCTION_QUOTE_INDENT         = "quote_ident"
	PG_FUNCTION_ROW_TO_JSON          = "row_to_json"
	PG_FUNCTION_SET_CONFIG           = "set_config"

	PG_TABLE_PG_AUTH_MEMBERS       = "pg_auth_members"
	PG_TABLE_PG_CLASS              = "pg_class"
	PG_TABLE_PG_DATABASE           = "pg_database"
	PG_TABLE_PG_EXTENSION          = "pg_extension"
	PG_TABLE_PG_INHERITS           = "pg_inherits"
	PG_TABLE_PG_MATVIEWS           = "pg_matviews"
	PG_TABLE_PG_NAMESPACE          = "pg_namespace"
	PG_TABLE_PG_REPLICATION_SLOTS  = "pg_replication_slots"
	PG_TABLE_PG_ROLES              = "pg_roles"
	PG_TABLE_PG_SHADOW             = "pg_shadow"
	PG_TABLE_PG_SHDESCRIPTION      = "pg_shdescription"
	PG_TABLE_PG_STATIO_USER_TABLES = "pg_statio_user_tables"
	PG_TABLE_PG_STAT_ACTIVITY      = "pg_stat_activity"
	PG_TABLE_PG_STAT_GSSAPI        = "pg_stat_gssapi"
	PG_TABLE_PG_STAT_USER_TABLES   = "pg_stat_user_tables"
	PG_TABLE_PG_USER               = "pg_user"
	PG_TABLE_TABLES                = "tables"
)

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
