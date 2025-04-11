package main

import (
	"context"
	"io"
	"strings"
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

func TestExecFile(t *testing.T) {
	t.Run("Executes SQL file", func(t *testing.T) {
		config := loadTestConfig()
		duckdb := NewDuckdb(config, false)
		defer duckdb.Close()
		fileContent := strings.Join([]string{
			"CREATE TABLE test (id INTEGER);",
			"INSERT INTO test VALUES (1);",
		}, "\n")
		file := io.NopCloser(strings.NewReader(fileContent))

		duckdb.ExecFile(file)

		rows, err := duckdb.QueryContext(context.Background(), "SELECT COUNT(*) FROM test")
		if err != nil {
			t.Errorf("Expected query to succeed")
		}
		defer rows.Close()
		var count int
		rows.Next()
		err = rows.Scan(&count)
		if err != nil {
			t.Errorf("Expected query to return a result")
		}
		if count != 1 {
			t.Errorf("Expected query result to be 1, got %d", count)
		}
	})
}
