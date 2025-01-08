package main

import (
	"github.com/jackc/pgx/v5/pgtype"
)

// TableDefinition represents a system table structure
type TableDefinition struct {
	Columns []PgColumnDef
	HasRows bool // Whether the table should contain any rows
}

type PgColumnDef struct {
	Name   string
	PgType uint32
	Value  *string // nil = NULL, omitted = no rows
}

func str(s string) *string {
	return &s
}

var PG_SHADOW_TABLE = TableDefinition{
	HasRows: true,
	Columns: []PgColumnDef{
		{"usename", pgtype.NameOID, str("")}, // Will be filled with user
		{"usesysid", pgtype.OIDOID, str("10")},
		{"usecreatedb", pgtype.BoolOID, str("FALSE")},
		{"usesuper", pgtype.BoolOID, str("FALSE")},
		{"userepl", pgtype.BoolOID, str("TRUE")},
		{"usebypassrls", pgtype.BoolOID, str("FALSE")},
		{"passwd", pgtype.TextOID, str("")},
		{"valuntil", pgtype.TimestamptzOID, nil},
		{"useconfig", pgtype.TextArrayOID, nil},
	},
}

var PG_INHERITS_TABLE = TableDefinition{
	HasRows: false, // This table should be empty
	Columns: []PgColumnDef{
		{"inhrelid", pgtype.OIDOID, nil},
		{"inhparent", pgtype.OIDOID, nil},
		{"inhseqno", pgtype.Int4OID, nil},
		{"inhdetachpending", pgtype.BoolOID, nil},
	},
}

var PG_ROLES_TABLE = TableDefinition{
	HasRows: true,
	Columns: []PgColumnDef{
		{"oid", pgtype.OIDOID, str("10")},
		{"rolname", pgtype.NameOID, str("")},
		{"rolsuper", pgtype.BoolOID, str("true")},
		{"rolinherit", pgtype.BoolOID, str("true")},
		{"rolcreaterole", pgtype.BoolOID, str("true")},
		{"rolcreatedb", pgtype.BoolOID, str("true")},
		{"rolcanlogin", pgtype.BoolOID, str("true")},
		{"rolreplication", pgtype.BoolOID, str("false")},
		{"rolconnlimit", pgtype.Int4OID, str("-1")},
		{"rolpassword", pgtype.TextOID, nil},
		{"rolvaliduntil", pgtype.TimestamptzOID, nil},
		{"rolbypassrls", pgtype.BoolOID, str("false")},
		{"rolconfig", pgtype.TextArrayOID, nil},
	},
}

var PG_DATABASE_TABLE = TableDefinition{
	HasRows: true,
	Columns: []PgColumnDef{
		{"oid", pgtype.OIDOID, str("16388")},
		{"datname", pgtype.NameOID, str("")},
		{"datdba", pgtype.OIDOID, str("10")},
		{"encoding", pgtype.Int4OID, str("6")},
		{"datlocprovider", pgtype.TextOID, str("c")},
		{"datistemplate", pgtype.BoolOID, str("FALSE")},
		{"datallowconn", pgtype.BoolOID, str("TRUE")},
		{"datconnlimit", pgtype.Int4OID, str("-1")},
		{"datfrozenxid", pgtype.Int8OID, str("722")},
		{"datminmxid", pgtype.Int8OID, str("1")},
		{"dattablespace", pgtype.OIDOID, str("1663")},
		{"datcollate", pgtype.TextOID, str("en_US.UTF-8")},
		{"datctype", pgtype.TextOID, str("en_US.UTF-8")},
		{"datlocale", pgtype.TextOID, nil},
		{"daticurules", pgtype.TextOID, nil},
		{"datcollversion", pgtype.TextOID, nil},
		{"datacl", pgtype.TextArrayOID, nil},
	},
}

var PG_USER_TABLE = TableDefinition{
	HasRows: true,
	Columns: []PgColumnDef{
		{"usename", pgtype.TextOID, str("")},
		{"usesysid", pgtype.OIDOID, str("10")},
		{"usecreatedb", pgtype.BoolOID, str("t")},
		{"usesuper", pgtype.BoolOID, str("t")},
		{"userepl", pgtype.BoolOID, str("t")},
		{"usebypassrls", pgtype.BoolOID, str("t")},
		{"passwd", pgtype.TextOID, str("")},
		{"valuntil", pgtype.TimestamptzOID, nil},
		{"useconfig", pgtype.TextArrayOID, nil},
	},
}

var PG_EXTENSION_TABLE = TableDefinition{
	HasRows: true,
	Columns: []PgColumnDef{
		{"oid", pgtype.OIDOID, str("13823")},
		{"extname", pgtype.NameOID, str("plpgsql")},
		{"extowner", pgtype.OIDOID, str("10")},
		{"extnamespace", pgtype.OIDOID, str("11")},
		{"extrelocatable", pgtype.BoolOID, str("false")},
		{"extversion", pgtype.TextOID, str("1.0")},
		{"extconfig", pgtype.OIDArrayOID, nil},
		{"extcondition", pgtype.TextArrayOID, nil},
	},
}

var PG_SHDESCRIPTION_TABLE = TableDefinition{
	HasRows: false,
	Columns: []PgColumnDef{
		{"objoid", pgtype.OIDOID, nil},
		{"classoid", pgtype.OIDOID, nil},
		{"description", pgtype.TextOID, nil},
	},
}

var PG_STATIO_USER_TABLES_TABLE = TableDefinition{
	HasRows: false,
	Columns: []PgColumnDef{
		{"relid", pgtype.OIDOID, nil},
		{"schemaname", pgtype.NameOID, nil},
		{"relname", pgtype.NameOID, nil},
		{"heap_blks_read", pgtype.Int8OID, nil},
		{"heap_blks_hit", pgtype.Int8OID, nil},
		{"idx_blks_read", pgtype.Int8OID, nil},
		{"idx_blks_hit", pgtype.Int8OID, nil},
		{"toast_blks_read", pgtype.Int8OID, nil},
		{"toast_blks_hit", pgtype.Int8OID, nil},
		{"tidx_blks_read", pgtype.Int8OID, nil},
		{"tidx_blks_hit", pgtype.Int8OID, nil},
	},
}

var PG_REPLICATION_SLOTS_TABLE = TableDefinition{
	HasRows: false,
	Columns: []PgColumnDef{
		{"slot_name", pgtype.NameOID, nil},
		{"plugin", pgtype.NameOID, nil},
		{"slot_type", pgtype.TextOID, nil},
		{"datoid", pgtype.OIDOID, nil},
		{"database", pgtype.NameOID, nil},
		{"temporary", pgtype.BoolOID, nil},
		{"active", pgtype.BoolOID, nil},
		{"active_pid", pgtype.Int4OID, nil},
		{"xmin", pgtype.Int8OID, nil},
		{"catalog_xmin", pgtype.Int8OID, nil},
		{"restart_lsn", pgtype.TextOID, nil},
		{"confirmed_flush_lsn", pgtype.TextOID, nil},
		{"wal_status", pgtype.TextOID, nil},
		{"safe_wal_size", pgtype.Int8OID, nil},
		{"two_phase", pgtype.BoolOID, nil},
		{"conflicting", pgtype.BoolOID, nil},
	},
}

var PG_STAT_GSSAPI_TABLE = TableDefinition{
	HasRows: false,
	Columns: []PgColumnDef{
		{"pid", pgtype.Int4OID, nil},
		{"gss_authenticated", pgtype.BoolOID, nil},
		{"principal", pgtype.TextOID, nil},
		{"encrypted", pgtype.BoolOID, nil},
		{"credentials_delegated", pgtype.BoolOID, nil},
	},
}

var PG_AUTH_MEMBERS_TABLE = TableDefinition{
	HasRows: false,
	Columns: []PgColumnDef{
		{"oid", pgtype.OIDOID, nil},
		{"roleid", pgtype.OIDOID, nil},
		{"member", pgtype.OIDOID, nil},
		{"grantor", pgtype.OIDOID, nil},
		{"admin_option", pgtype.BoolOID, nil},
		{"inherit_option", pgtype.BoolOID, nil},
		{"set_option", pgtype.BoolOID, nil},
	},
}

var PG_STAT_ACTIVITY_TABLE = TableDefinition{
	HasRows: false,
	Columns: []PgColumnDef{
		{"datid", pgtype.OIDOID, nil},
		{"datname", pgtype.NameOID, nil},
		{"pid", pgtype.Int4OID, nil},
		{"usesysid", pgtype.OIDOID, nil},
		{"usename", pgtype.NameOID, nil},
		{"application_name", pgtype.TextOID, nil},
		{"client_addr", pgtype.InetOID, nil},
		{"client_hostname", pgtype.TextOID, nil},
		{"client_port", pgtype.Int4OID, nil},
		{"backend_start", pgtype.TimestamptzOID, nil},
		{"xact_start", pgtype.TimestamptzOID, nil},
		{"query_start", pgtype.TimestamptzOID, nil},
		{"state_change", pgtype.TimestamptzOID, nil},
		{"wait_event_type", pgtype.TextOID, nil},
		{"wait_event", pgtype.TextOID, nil},
		{"state", pgtype.TextOID, nil},
		{"backend_xid", pgtype.Int8OID, nil},
		{"backend_xmin", pgtype.Int8OID, nil},
		{"query", pgtype.TextOID, nil},
		{"backend_type", pgtype.TextOID, nil},
	},
}
