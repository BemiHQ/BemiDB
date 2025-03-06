sh:
	devbox --env-file .env shell

install:
	devbox run "cd src && go mod tidy"

up:
	devbox run --env-file .env "cd src && go run ."

.PHONY: build
build:
	rm -rf build/bemidb-* && \
		devbox run "./scripts/build-darwin.sh" && \
		./scripts/build-linux.sh

build-local:
	rm -rf build/bemidb-* && \
		cd src && go build -o ../build/bemidb-darwin-arm64

sync:
	devbox run --env-file .env "cd src && go run . sync"

test:
	devbox run "cd src && go test ./..."

test-function:
	devbox run "cd src && go test ./... -run $(FUNC)"

debug:
	devbox run "cd src && dlv test github.com/BemiHQ/BemiDB"

lint:
	devbox run "cd src && go fmt && deadcode . && staticcheck ."

console:
	devbox run "cd src && gore"

outdated:
	devbox run "cd src && go list -u -m -f '{{if and .Update (not .Indirect)}}{{.}}{{end}}' all"

.PHONY: benchmark
benchmark:
	devbox run "time psql postgres://127.0.0.1:54321/bemidb < ./benchmark/queries.sql"

pg-init:
	devbox run initdb &&
		sed -i "s/#log_statement = 'none'/log_statement = 'all'/g" ./.devbox/virtenv/postgresql/data/postgresql.conf && \
		sed -i "s/#logging_collector = off/logging_collector = on/g" ./.devbox/virtenv/postgresql/data/postgresql.conf && \
		sed -i "s/#log_directory = 'log'/log_directory = 'log'/g" ./.devbox/virtenv/postgresql/data/postgresql.conf

pg-up:
	devbox services start postgresql

pg-create:
	devbox run "(dropdb tpch || true) && \
		createdb tpch && \
		./benchmark/scripts/load-pg-data.sh"

pg-index:
	devbox run "psql postgres://127.0.0.1:5432/tpch -f ./benchmark/data/create-indexes.ddl"

pg-benchmark:
	devbox run "psql postgres://127.0.0.1:5432/tpch -c 'ANALYZE VERBOSE' && \
		time psql postgres://127.0.0.1:5432/tpch < ./benchmark/queries.sql"

pg-down:
	devbox services stop postgresql

pg-logs:
	tail -f .devbox/virtenv/postgresql/data/log/postgresql-*.log

pg-sniff:
	sudo tshark -i lo0 -f 'tcp port 5432' -d tcp.port==5432,pgsql -O pgsql

tpch-install:
	devbox run "cd benchmark && \
		rm -rf tpch-kit && \
		git clone https://github.com/gregrahn/tpch-kit.git && \
		cd tpch-kit/dbgen && \
		make MACHINE=$$MACHINE DATABASE=POSTGRESQL"

tpch-generate:
	devbox run "./benchmark/scripts/generate-data.sh"

sniff:
	sudo tshark -i lo0 -f 'tcp port 54321' -d tcp.port==54321,pgsql -O pgsql
