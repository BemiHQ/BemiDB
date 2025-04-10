package main

import (
	"testing"
)

func TestCopyFromPgTableSql(t *testing.T) {
	config := &Config{}
	syncer := NewSyncerTable(config)
	pgSchemaTable := PgSchemaTable{Schema: "public", Table: "users"}

	t.Run("Full refresh", func(t *testing.T) {
		// [**************************************************************************************************]
		// 0                                                                                           curr max xmin
		t.Run("Runs a full refresh if there is no previous internalTableMetadata", func(t *testing.T) {
			internalTableMetadata := InternalTableMetadata{}
			currentTxid := int64(100)

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" ORDER BY xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})

		// [**************************************************************************************************]
		// 0                                                                                           curr max xmin
		t.Run("Runs a full refresh after successful full sync", func(t *testing.T) {
			previousMaxXmin := uint32(500)
			initialTxid := int64(800)
			currentTxid := int64(1000)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFull, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" ORDER BY xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})

		// [***********************|########################|************************|************************]
		// 0                 prev max xmin        curr wrap-around txid      init (wrap-around) txid        32^2
		t.Run("Runs a full refresh if an overlapping wrap-around occurred during a full sync", func(t *testing.T) {
			previousMaxXmin := uint32(2_000_000_000)
			currentTxid := int64(3_000_000_000) + (int64(1) << 32)
			initialTxid := int64(4_000_000_000)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFullInProgress, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" ORDER BY xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})
	})

	t.Run("Continued in-progress refresh without a wrap-around", func(t *testing.T) {
		// [-----------------------|************************|************************|************************]
		// 0                 prev max xmin      init (wrap-around) txid    curr (wrap-around) txid          32^2
		t.Run("Continues a full refresh refresh before reaching the initial txid", func(t *testing.T) {
			previousMaxXmin := uint32(1_000_000_000)
			initialTxid := int64(2_000_000_000) + (int64(1) << 32)
			currentTxid := int64(3_000_000_000) + (int64(1) << 32)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFullInProgress, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" WHERE xmin::text::bigint >= 1000000000 ORDER BY xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})

		// [-----------------------|------------------------|************************|************************]
		// 0            init (wrap-around) txid       prev max xmin        curr (wrap-around) txid          32^2
		t.Run("Continues a full refresh refresh after reaching the initial txid", func(t *testing.T) {
			initialTxid := int64(1_000_000_000)
			previousMaxXmin := uint32(2_000_000_000)
			currentTxid := int64(3_000_000_000)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFullInProgress, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" WHERE xmin::text::bigint >= 2000000000 ORDER BY xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})
	})

	t.Run("Continued in-progress refresh with a wrap-around", func(t *testing.T) {
		// [***********************|------------------------|************************|************************]
		// 0             curr wrap-around txid        prev max xmin       init (wrap-around) txid           32^2
		t.Run("Continues a full refresh refresh before reaching the initial txid", func(t *testing.T) {
			currentTxid := int64(1_000_000_000) + (int64(1) << 32)
			previousMaxXmin := uint32(2_000_000_000)
			initialTxid := int64(3_000_000_000)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFullInProgress, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" WHERE xmin::text::bigint >= 2000000000 OR xmin::text::bigint <= 1000000000 ORDER BY xmin::text::bigint <= 1000000000 ASC, xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})

		// [***********************|------------------------|------------------------|************************]
		// 0             curr wrap-around txid     init (wrap-around) txid     prev max xmin                32^2
		t.Run("Continues a full refresh refresh after reaching the initial txid", func(t *testing.T) {
			currentTxid := int64(1_000_000_000) + (int64(1) << 32)
			initialTxid := int64(2_000_000_000)
			previousMaxXmin := uint32(3_000_000_000)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFullInProgress, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" WHERE xmin::text::bigint >= 3000000000 OR xmin::text::bigint <= 1000000000 ORDER BY xmin::text::bigint <= 1000000000 ASC, xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})

		// [***********************|************************|------------------------|************************]
		// 0            init (wrap-around) txid    curr wrap-around txid       prev max xmin                32^2
		t.Run("Continues a full refresh refresh after the current wrapped-around txid exceeds the initial txid", func(t *testing.T) {
			initialTxid := int64(1_000_000_000)
			currentTxid := int64(2_000_000_000) + (int64(1) << 32)
			previousMaxXmin := uint32(3_000_000_000)
			internalTableMetadata := InternalTableMetadata{LastRefreshMode: RefreshModeFullInProgress, LastTxid: initialTxid, MaxXmin: &previousMaxXmin}

			sql := syncer.CopyFromPgTableSql(pgSchemaTable, internalTableMetadata, currentTxid)

			expected := "COPY (SELECT *, xmin::text::bigint AS xmin FROM \"public\".\"users\" WHERE xmin::text::bigint >= 3000000000 OR xmin::text::bigint <= 2000000000 ORDER BY xmin::text::bigint <= 2000000000 ASC, xmin::text::bigint ASC) TO STDOUT WITH CSV HEADER NULL 'BEMIDB_NULL'"
			if sql != expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
			}
		})
	})
}
