module github.com/BemiHQ/BemiDB/src/syncer-postgres

go 1.24.4

require (
	github.com/BemiHQ/BemiDB/src/syncer-common v0.0.1
	github.com/jackc/pgx/v5 v5.7.4
	github.com/nats-io/nats.go v1.43.0
)

replace github.com/BemiHQ/BemiDB/src/syncer-common => ../syncer-common

require (
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/trinodb/trino-go-client v0.323.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/net v0.24.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)
