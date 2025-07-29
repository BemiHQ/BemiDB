package common

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	CONNECTION_TIMEOUT = 30 * time.Second
)

type Postgres struct {
	Conn   *pgx.Conn
	Config *BaseConfig
}

func NewPostgres(config *BaseConfig, databaseUrl string) *Postgres {
	ctx, cancel := context.WithTimeout(context.Background(), CONNECTION_TIMEOUT)
	defer cancel()

	conn, err := pgx.Connect(ctx, urlEncodePassword(databaseUrl))
	PanicIfError(config, err)

	return &Postgres{
		Config: config,
		Conn:   conn,
	}
}

func (client *Postgres) Close() {
	err := client.Conn.Close(context.Background())
	PanicIfError(client.Config, err)
}

func (client *Postgres) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	LogDebug(client.Config, "Postgres query:", query, args)
	return client.Conn.Query(ctx, query, args...)
}

func (client *Postgres) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	LogDebug(client.Config, "Postgres query:", query, args)
	return client.Conn.QueryRow(ctx, query, args...)
}

func (client *Postgres) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	LogDebug(client.Config, "Postgres exec:", query, args)
	return client.Conn.Exec(ctx, query, args...)
}

// Example:
// - From postgres://username:pas$:wor^d#@host:port/database
// - To postgres://username:pas%24%3Awor%5Ed%23@host:port/database
func urlEncodePassword(pgDatabaseUrl string) string {
	// No credentials
	if !strings.Contains(pgDatabaseUrl, "@") {
		return pgDatabaseUrl
	}

	password := strings.TrimPrefix(pgDatabaseUrl, "postgresql://")
	password = strings.TrimPrefix(password, "postgres://")
	passwordEndIndex := strings.LastIndex(password, "@")
	password = password[:passwordEndIndex]

	// Credentials without password
	if !strings.Contains(password, ":") {
		return pgDatabaseUrl
	}

	_, password, _ = strings.Cut(password, ":")
	encodedPassword := url.QueryEscape(password)

	// Password is already encoded
	if encodedPassword == password {
		return pgDatabaseUrl
	}

	return strings.Replace(pgDatabaseUrl, ":"+password+"@", ":"+encodedPassword+"@", 1)
}
