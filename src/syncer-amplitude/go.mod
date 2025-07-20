module github.com/BemiHQ/bemidb-services/syncers/syncer-amplitude

go 1.24.4

require github.com/BemiHQ/BemiDB/src/syncer-common v0.0.1

replace github.com/BemiHQ/BemiDB/src/syncer-common => ../syncer-common

require (
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/trinodb/trino-go-client v0.323.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/net v0.24.0 // indirect
)
