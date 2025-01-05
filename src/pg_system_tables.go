package main

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type PgColumnDef struct {
	Name    string
	PgType  uint32
	Default string
}

var PG_SHADOW_COLUMNS = []PgColumnDef{
	{"usename", pgtype.NameOID, ""},
	{"usesysid", pgtype.OIDOID, "10"},
	{"usecreatedb", pgtype.BoolOID, "FALSE"},
	{"usesuper", pgtype.BoolOID, "FALSE"},
	{"userepl", pgtype.BoolOID, "TRUE"},
	{"usebypassrls", pgtype.BoolOID, "FALSE"},
	{"passwd", pgtype.TextOID, ""},
	{"valuntil", pgtype.TimestamptzOID, "NULL"},
	{"useconfig", pgtype.TextArrayOID, "NULL"},
}

var PG_ROLES_COLUMNS = []PgColumnDef{
	{"oid", pgtype.OIDOID, "10"},
	{"rolname", pgtype.NameOID, ""},
	{"rolsuper", pgtype.BoolOID, "true"},
	{"rolinherit", pgtype.BoolOID, "true"},
	{"rolcreaterole", pgtype.BoolOID, "true"},
	{"rolcreatedb", pgtype.BoolOID, "true"},
	{"rolcanlogin", pgtype.BoolOID, "true"},
	{"rolreplication", pgtype.BoolOID, "false"},
	{"rolconnlimit", pgtype.Int4OID, "-1"},
	{"rolpassword", pgtype.TextOID, "NULL"},
	{"rolvaliduntil", pgtype.TimestamptzOID, "NULL"},
	{"rolbypassrls", pgtype.BoolOID, "false"},
	{"rolconfig", pgtype.TextArrayOID, "NULL"},
}

var PG_DATABASE_COLUMNS = []PgColumnDef{
	{"oid", pgtype.OIDOID, "16388"},
	{"datname", pgtype.NameOID, ""},
	{"datdba", pgtype.OIDOID, "10"},
	{"encoding", pgtype.Int4OID, "6"},
	{"datlocprovider", pgtype.TextOID, "c"},
	{"datistemplate", pgtype.BoolOID, "FALSE"},
	{"datallowconn", pgtype.BoolOID, "TRUE"},
	{"datconnlimit", pgtype.Int4OID, "-1"},
	{"datfrozenxid", pgtype.Int8OID, "722"},
	{"datminmxid", pgtype.Int8OID, "1"},
	{"dattablespace", pgtype.OIDOID, "1663"},
	{"datcollate", pgtype.TextOID, "en_US.UTF-8"},
	{"datctype", pgtype.TextOID, "en_US.UTF-8"},
	{"datlocale", pgtype.TextOID, "NULL"},
	{"daticurules", pgtype.TextOID, "NULL"},
	{"datcollversion", pgtype.TextOID, "NULL"},
	{"datacl", pgtype.TextArrayOID, "NULL"},
}

var PG_USER_COLUMNS = []PgColumnDef{
	{"usename", pgtype.TextOID, ""},
	{"usesysid", pgtype.OIDOID, "10"},
	{"usecreatedb", pgtype.BoolOID, "t"},
	{"usesuper", pgtype.BoolOID, "t"},
	{"userepl", pgtype.BoolOID, "t"},
	{"usebypassrls", pgtype.BoolOID, "t"},
	{"passwd", pgtype.TextOID, ""},
	{"valuntil", pgtype.TimestamptzOID, "NULL"},
	{"useconfig", pgtype.TextArrayOID, "NULL"},
}

var PG_EXTENSION_COLUMNS = []PgColumnDef{
	{"oid", pgtype.OIDOID, "13823"},
	{"extname", pgtype.NameOID, "plpgsql"},
	{"extowner", pgtype.OIDOID, "10"},
	{"extnamespace", pgtype.OIDOID, "11"},
	{"extrelocatable", pgtype.BoolOID, "false"},
	{"extversion", pgtype.TextOID, "1.0"},
	{"extconfig", pgtype.OIDArrayOID, "NULL"},
	{"extcondition", pgtype.TextArrayOID, "NULL"},
}

var PG_INHERITS_COLUMNS = []PgColumnDef{
	{"inhrelid", pgtype.OIDOID, ""},
	{"inhparent", pgtype.OIDOID, ""},
	{"inhseqno", pgtype.Int4OID, ""},
	{"inhdetachpending", pgtype.BoolOID, ""},
}

var PG_SHDESCRIPTION_COLUMNS = []PgColumnDef{
	{"objoid", pgtype.OIDOID, ""},
	{"classoid", pgtype.OIDOID, ""},
	{"description", pgtype.TextOID, ""},
}

var PG_STATIO_USER_TABLES_COLUMNS = []PgColumnDef{
	{"relid", pgtype.OIDOID, ""},
	{"schemaname", pgtype.NameOID, ""},
	{"relname", pgtype.NameOID, ""},
	{"heap_blks_read", pgtype.Int8OID, ""},
	{"heap_blks_hit", pgtype.Int8OID, ""},
	{"idx_blks_read", pgtype.Int8OID, ""},
	{"idx_blks_hit", pgtype.Int8OID, ""},
	{"toast_blks_read", pgtype.Int8OID, ""},
	{"toast_blks_hit", pgtype.Int8OID, ""},
	{"tidx_blks_read", pgtype.Int8OID, ""},
	{"tidx_blks_hit", pgtype.Int8OID, ""},
}

var PG_REPLICATION_SLOTS_COLUMNS = []PgColumnDef{
	{"slot_name", pgtype.NameOID, ""},
	{"plugin", pgtype.NameOID, ""},
	{"slot_type", pgtype.TextOID, ""},
	{"datoid", pgtype.OIDOID, ""},
	{"database", pgtype.NameOID, ""},
	{"temporary", pgtype.BoolOID, ""},
	{"active", pgtype.BoolOID, ""},
	{"active_pid", pgtype.Int4OID, ""},
	{"xmin", pgtype.Int8OID, ""},
	{"catalog_xmin", pgtype.Int8OID, ""},
	{"restart_lsn", pgtype.TextOID, ""},
	{"confirmed_flush_lsn", pgtype.TextOID, ""},
	{"wal_status", pgtype.TextOID, ""},
	{"safe_wal_size", pgtype.Int8OID, ""},
	{"two_phase", pgtype.BoolOID, ""},
	{"conflicting", pgtype.BoolOID, ""},
}

var PG_STAT_GSSAPI_COLUMNS = []PgColumnDef{
	{"pid", pgtype.Int4OID, ""},
	{"gss_authenticated", pgtype.BoolOID, ""},
	{"principal", pgtype.TextOID, ""},
	{"encrypted", pgtype.BoolOID, ""},
	{"credentials_delegated", pgtype.BoolOID, ""},
}

var PG_AUTH_MEMBERS_COLUMNS = []PgColumnDef{
	{"oid", pgtype.OIDOID, ""},
	{"roleid", pgtype.OIDOID, ""},
	{"member", pgtype.OIDOID, ""},
	{"grantor", pgtype.OIDOID, ""},
	{"admin_option", pgtype.BoolOID, ""},
	{"inherit_option", pgtype.BoolOID, ""},
	{"set_option", pgtype.BoolOID, ""},
}

var PG_STAT_ACTIVITY_COLUMNS = []PgColumnDef{
	{"datid", pgtype.OIDOID, ""},
	{"datname", pgtype.NameOID, ""},
	{"pid", pgtype.Int4OID, ""},
	{"usesysid", pgtype.OIDOID, ""},
	{"usename", pgtype.NameOID, ""},
	{"application_name", pgtype.TextOID, ""},
	{"client_addr", pgtype.InetOID, ""},
	{"client_hostname", pgtype.TextOID, ""},
	{"client_port", pgtype.Int4OID, ""},
	{"backend_start", pgtype.TimestamptzOID, ""},
	{"xact_start", pgtype.TimestamptzOID, ""},
	{"query_start", pgtype.TimestamptzOID, ""},
	{"state_change", pgtype.TimestamptzOID, ""},
	{"wait_event_type", pgtype.TextOID, ""},
	{"wait_event", pgtype.TextOID, ""},
	{"state", pgtype.TextOID, ""},
	{"backend_xid", pgtype.Int8OID, ""},
	{"backend_xmin", pgtype.Int8OID, ""},
	{"query", pgtype.TextOID, ""},
	{"backend_type", pgtype.TextOID, ""},
}
