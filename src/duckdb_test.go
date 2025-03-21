package main

import (
	"context"
	"testing"
)

func TestNewDuckdb(t *testing.T) {
	t.Run("Creates a new DuckDB instance", func(t *testing.T) {
		config := loadTestConfig()

		duckdb := NewDuckdb(config, false)
		defer duckdb.Close()

		rows, err := duckdb.QueryContext(context.Background(), "SELECT 1")
		if err != nil {
			t.Errorf("Expected query to succeed")
		}
		defer rows.Close()

		for rows.Next() {
			var result int
			err = rows.Scan(&result)
			if err != nil {
				t.Errorf("Expected query to return a result")
			}
			if result != 1 {
				t.Errorf("Expected query result to be 1, got %d", result)
			}
		}
	})
}
