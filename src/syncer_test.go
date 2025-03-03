package main

import "testing"

func TestShouldSyncTable(t *testing.T) {
	t.Run("returns true when no filters are set", func(t *testing.T) {
		config := &Config{
			Pg: PgConfig{
				DatabaseUrl: "postgres://user:pass@localhost:5432/db",
			},
		}
		syncer := NewSyncer(config)
		pgSchemaTable := PgSchemaTable{Schema: "public", Table: "users"}

		if !syncer.shouldSyncTable(pgSchemaTable) {
			t.Error("Expected shouldSyncTable to return true when no filters are set")
		}
	})

	t.Run("respects include filter", func(t *testing.T) {
		config := &Config{
			Pg: PgConfig{
				DatabaseUrl:   "postgres://user:pass@localhost:5432/db",
				IncludeTables: []string{"public.users", "public.orders"},
			},
		}
		syncer := NewSyncer(config)

		pgSchemaTableIncluded := PgSchemaTable{Schema: "public", Table: "users"}
		if !syncer.shouldSyncTable(pgSchemaTableIncluded) {
			t.Error("Expected shouldSyncTable to return true for included table")
		}

		pgSchemaTableExcluded := PgSchemaTable{Schema: "public", Table: "secrets"}
		if syncer.shouldSyncTable(pgSchemaTableExcluded) {
			t.Error("Expected shouldSyncTable to return false for non-included table")
		}
	})

	t.Run("respects exclude filter", func(t *testing.T) {
		config := &Config{
			Pg: PgConfig{
				DatabaseUrl:   "postgres://user:pass@localhost:5432/db",
				ExcludeTables: []string{"public.secrets", "public.cache"},
			},
		}
		syncer := NewSyncer(config)

		pgSchemaTableIncluded := PgSchemaTable{Schema: "public", Table: "users"}
		if !syncer.shouldSyncTable(pgSchemaTableIncluded) {
			t.Error("Expected shouldSyncTable to return true for non-excluded table")
		}

		pgSchemaTableExcluded := PgSchemaTable{Schema: "public", Table: "secrets"}
		if syncer.shouldSyncTable(pgSchemaTableExcluded) {
			t.Error("Expected shouldSyncTable to return false for excluded table")
		}
	})
}
