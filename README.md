# BemiDB

BemiDB is a Postgres read replica optimized for analytics.
It consists of a single binary that seamlessly connects to a Postgres database, replicates the data in a compressed columnar format, and allows you to run complex queries using its Postgres-compatible analytical query engine.

![BemiDB](/img/BemiDB.gif)

## Contents

- [Highlights](#highlights)
- [Use cases](#use-cases)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Benchmark](#benchmark)
- [Data type mapping](#data-type-mapping)
- [Alternatives](#alternatives)
- [Development](#development)
- [License](#license)

## Highlights

- **Performance**: runs analytical queries up to 2000x faster than Postgres.
- **Single Binary**: consists of a single binary that can be run on any machine.
- **Postgres Replication**: automatically syncs data from Postgres databases.
- **Compressed Data**: uses an open columnar format for tables with 4x compression.
- **Scalable Storage**: storage is separated from compute and supports a local disk or S3.
- **Query Engine**: leverages a query engine optimized for analytical workloads.
- **Postgres-Compatible**: integrates with services and tools in the Postgres ecosystem.
- **Open-Source**: released under an OSI-approved license.

## Use cases

- **Run complex analytical queries like it's your Postgres database**. Without worrying about performance impact and indexing.
- **Simplify your data stack down to a single binary**. No complex setup, no data movement, no weird acronyms like CDC, ETL, DW.
- **Integrate with Postgres-compatible tools and services**. Querying and visualizing data with BI tools, notebooks, and ORMs.
- **Automatically centralize all data in a data lakehouse**. Using Iceberg tables with Parquet data files on object storage.
- **Continuously archive data from your Postgres database**. Keeping and querying historical data without affecting the main database.

## Quickstart

Install BemiDB:

```sh
curl -sSL https://raw.githubusercontent.com/BemiHQ/BemiDB/refs/heads/main/scripts/install.sh | bash
```

Sync data from a Postgres database:

```sh
./bemidb --pg-database-url postgres://postgres:postgres@localhost:5432/dbname sync
```

Then run BemiDB database:

```sh
./bemidb start
```

Run Postgres queries on top of the BemiDB database:

```sh
# List all tables
psql postgres://localhost:54321/bemidb -c "SELECT table_schema, table_name FROM information_schema.tables"

# Query a table
psql postgres://localhost:54321/bemidb -c "SELECT COUNT(*) FROM [table_name]"
```

<a name="docker"></a>
<details>
<summary><b>Running in a Docker container</b></summary>

```sh
# Download the latest Docker image
docker pull ghcr.io/bemihq/bemidb:latest

# Sync data from a Postgres database
docker run \
  -e PG_DATABASE_URL=postgres://postgres:postgres@host.docker.internal:5432/dbname \
  ghcr.io/bemihq/bemidb:latest sync

# Start the BemiDB database
docker run ghcr.io/bemihq/bemidb:latest start
```
</details>

<a name="kubernetes"></a>
<details>
<summary><b>Running in a Kubernetes cluster</b></summary>

You can run 2 deployments in parallel, one for data syncing from a Postgres database and another for running the BemiDB database.

In that case, you'd need to set up a shared volume or a shared S3 bucket between the two deployments for Iceberg data.
See the [Configuration](#configuration) section for more details.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bemidb-sync
  namespace: default
  labels:
    app.kubernetes.io/name: bemidb-sync
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app.kubernetes.io/name: bemidb-sync
    spec:
      containers:
      - name: bemidb
        image: ghcr.io/bemihq/bemidb:latest
        command: ["sync"]
        env:
        - name: PG_DATABASE_URL
          value: "postgres://postgres:postgres@postgres-host:5432/dbname"
        - name: PG_SYNC_INTERVAL
          value: "1h"
        ...
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bemidb-start
  namespace: default
  labels:
    app.kubernetes.io/name: bemidb-start
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app.kubernetes.io/name: bemidb-start
    spec:
      containers:
      - name: bemidb
        image: ghcr.io/bemihq/bemidb:latest
        command: ["start"]
        ...
```

</details>

## Configuration

### Storage configuration

#### Local disk storage

By default, BemiDB stores data on the local disk.
Here is an example of running BemiDB with default settings and storing data in a local `iceberg` directory:

```sh
./bemidb \
  --storage-type LOCAL \
  --storage-path ./iceberg \ # Data stored in ./iceberg/*
  start
```

#### S3 block storage

BemiDB natively supports S3 storage. You can specify the S3 settings using the following flags:

```sh
./bemidb \
  --storage-type S3 \
  --storage-path iceberg \ # Data stored in s3://[AWS_S3_BUCKET]/iceberg/*
  --aws-region [AWS_REGION] \
  --aws-s3-bucket [AWS_S3_BUCKET] \
  --aws-access-key-id [AWS_ACCESS_KEY_ID] \
  --aws-secret-access-key [AWS_SECRET_ACCESS_KEY] \
  start
```

<a name="iam"></a>
<details>
<summary><b>AWS IAM policy example</b></summary>

Here is the minimal IAM policy required for BemiDB to work with S3:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::[AWS_S3_BUCKET]",
                "arn:aws:s3:::[AWS_S3_BUCKET]/*"
            ]
        }
    ]
}
```
</details>

<a name="minio"></a>
<details>
<summary><b>MinIO object storage example</b></summary>

BemiDB can work with various S3-compatible object storage solutions, such as MinIO.

1. You can run MinIO locally:

```sh
minio server ./minio-data
# API: http://192.168.68.102:9000  http://127.0.0.1:9000
#    RootUser: minioadmin
#    RootPass: minioadmin
# WebUI: http://192.168.68.102:65218 http://127.0.0.1:65218
#    RootUser: minioadmin
#    RootPass: minioadmin
```

2. Open the MinIO WebUI and create a bucket, for example, `bemidb-bucket`.

3. Run BemiDB with the following command:

```sh
./bemidb \
  --storage-type S3 \
  --storage-path iceberg \
  --aws-s3-bucket bemidb-bucket \
  --aws-s3-endpoint 127.0.0.1:9000 \
  --aws-region us-east-1 \
  --aws-access-key-id minioadmin \
  --aws-secret-access-key minioadmin \
  sync
```

</details>

### Syncing configuration

#### Periodic data syncing

To sync data periodically from a Postgres database:

```sh
./bemidb \
  --pg-sync-interval 1h \ # Supported units: h, m, s
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync
```

#### Selective table syncing

By default, BemiDB syncs all tables from the Postgres database. To include and sync only specific tables from your Postgres database:

```sh
./bemidb \
  --pg-include-tables public.billing_*,public.users \ # A comma-separated list of tables to include, supports wildcards (*)
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync
```

To exclude specific tables during the sync:

```sh
./bemidb \
  --pg-exclude-tables public.*_logs,public.cache \ # A comma-separated list of tables to exclude, supports wildcards (*)
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync
```

Note: if a table matches both `--pg-include-tables` and `--pg-exclude-tables`, it will be excluded.

For example, to include all tables in the `public` schema except for the `public.cache` table:

```sh
./bemidb \
  --pg-include-tables public.* \     # Include all tables in the public schema
  --pg-exclude-tables public.cache \ # Except for the public.cache table
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync
```

#### Incremental data syncing

By default, BemiDB performs a full refresh of the table data during each sync.
For large tables, you can enable incremental syncing to only refresh the rows that have been inserted or updated since the last sync:

```sh
./bemidb \
  --pg-include-tables * \                                   # Sync all tables with a full refresh
  --pg-incrementally-refreshed-tables public.transactions \ # Refresh only the public.transactions table incrementally
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync
```

Note: incremental refresh is currently limited to INSERT/UPDATE-modified tables and doesn't detect DELETEd rows.
I.e., in BemiDB, these tables become append-only.

#### Sharded data syncing

BemiDB allows running multiple sync processes independently, each responsible its own set of tables ("shards") from the same Postgres database:

```sh
./bemidb \
  --pg-include-tables public.table1,public.table2 \
  --pg-preserve-unsynced \ # Don't delete the existing tables in BemiDB that are not part of this sync (public.table3 and public.table4)
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync

./bemidb \
  --pg-include-tables public.table3,public.table4 \
  --pg-preserve-unsynced \ # Don't delete the existing tables in BemiDB that are not part of this sync (public.table1 and public.table2)
  --pg-database-url postgres://postgres:postgres@localhost:5432/dbname \
  sync
```

#### Syncing from multiple Postgres databases

BemiDB supports syncing data from multiple Postgres databases into the same BemiDB database by allowing prefixing schemas.

For example, if two Postgres databases `db1` and `db2` contain `public` schemas, you can prefix them as follows:

```sh
./bemidb \
  --pg-schema-prefix db1_ \ # Prefix all db1 database schemas with db1_
  --pg-preserve-unsynced \  # Don't delete db2 schemas in BemiDB
  --pg-database-url postgres://postgres:postgres@localhost:5432/db1 \
  sync

./bemidb \
  --pg-schema-prefix db2_ \ # Prefix all db2 database schemas with db2_
  --pg-preserve-unsynced \  # Don't delete db1 schemas in BemiDB
  --pg-database-url postgres://postgres:postgres@localhost:5432/db2 \
  sync
```

Then you can query and join tables from both Postgres databases in the same BemiDB database:

```sh
./bemidb start

psql postgres://localhost:54321/bemidb -c \
  "SELECT * FROM db1_public.[TABLE] JOIN db2_public.[TABLE] ON ..."
```

### Configuration options

#### `sync` command options

| CLI argument                          | Environment variable                | Default value | Description                                                                                                                                              |
|---------------------------------------|-------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--pg-database-url`                   | `PG_DATABASE_URL`                   | Required      | PostgreSQL database URL to sync                                                                                                                          |
| `--pg-sync-interval`                  | `PG_SYNC_INTERVAL`                  |               | Interval between syncs. Valid units: `h`, `m`, `s`                                                                                                       |
| `--pg-exclude-tables`                 | `PG_EXCLUDE_TABLES`                 |               | List of tables to exclude from sync. Comma-separated `schema.table`. May contain wildcards (`*`)                                                         |
| `--pg-include-tables`                 | `PG_INCLUDE_TABLES`                 |               | List of tables to include in sync. Comma-separated `schema.table`. May contain wildcards (`*`)                                                           |
| `--pg-incrementally-refreshed-tables` | `PG_INCREMENTALLY_REFRESHED_TABLES` |               | List of tables to refresh incrementally, currently limited to INSERT/UPDATE-modified tables. Comma-separated `schema.table`. May contain wildcards (`*`) |
| `--pg-schema-prefix`                  | `PG_SCHEMA_PREFIX`                  |               | Prefix for PostgreSQL schema names                                                                                                                       |
| `--pg-preserve-unsynced`              | `PG_PRESERVE_UNSYNCED`              | `false`       | Do not delete the existing tables in BemiDB that are not part of the sync                                                                                |

#### `start` command options

| CLI argument  | Environment variable | Default value | Description                            |
|---------------|----------------------|---------------|----------------------------------------|
| `--host`      | `BEMIDB_HOST`        | `127.0.0.1`   | Host for BemiDB to listen on           |
| `--port`      | `BEMIDB_PORT`        | `54321`       | Port for BemiDB to listen on           |
| `--database`  | `BEMIDB_DATABASE`    | `bemidb`      | Database name                          |
| `--init-sql ` | `BEMIDB_INIT_SQL`    | `./init.sql`  | Path to the initialization SQL file    |
| `--user`      | `BEMIDB_USER`        |               | Database user. Allows any if empty     |
| `--password`  | `BEMIDB_PASSWORD`    |               | Database password. Allows any if empty |

#### Storage options

| CLI argument              | Environment variable    | Default value                   | Description                                                                                              |
|---------------------------|-------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------|
| `--storage-type`          | `BEMIDB_STORAGE_TYPE`   | `LOCAL`                         | Storage type: `LOCAL` or `S3`                                                                            |
| `--storage-path`          | `BEMIDB_STORAGE_PATH`   | `iceberg`                       | Path to the storage folder                                                                               |
| `--aws-s3-endpoint`       | `AWS_S3_ENDPOINT`       | `s3.amazonaws.com`              | AWS S3 endpoint                                                                                          |
| `--aws-region`            | `AWS_REGION`            | Required with `S3` storage type | AWS region                                                                                               |
| `--aws-s3-bucket`         | `AWS_S3_BUCKET`         | Required with `S3` storage type | AWS S3 bucket name                                                                                       |
| `--aws-access-key-id`     | `AWS_ACCESS_KEY_ID`     |                                 | AWS access key ID. If empty, tries to fetch AWS SDK credentials in this order: config file, STS, SSO     |
| `--aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` |                                 | AWS secret access key. If empty, tries to fetch AWS SDK credentials in this order: config file, STS, SSO |

#### Other options

| CLI argument                   | Environment variable                 | Default value | Description                                                             |
|--------------------------------|--------------------------------------|---------------|-------------------------------------------------------------------------|
| `--log-level`                  | `BEMIDB_LOG_LEVEL`                   | `INFO`        | Log level: `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`                    |
| `--disable-anonymous-analytics`| `BEMIDB_DISABLE_ANONYMOUS_ANALYTICS` | `false`       | Disable collection of anonymous usage metadata (OS type, database host) |

Note: CLI arguments take precedence over environment variables. I.e. you can override the environment variables with CLI arguments.

## Architecture

BemiDB consists of the following main components:

- **Database Server**: implements the [Postgres protocol](https://www.postgresql.org/docs/current/protocol.html) to enable Postgres compatibility.
- **Query Engine**: embeds the [DuckDB](https://duckdb.org/) query engine to run analytical queries.
- **Storage Layer**: uses the [Iceberg](https://iceberg.apache.org/) table format to store data in columnar compressed Parquet files.
- **Postgres Connector**: connects to a Postgres databases to sync tables' schema and data.

<img src="/img/architecture.png" alt="Architecture" width="720px">

## Benchmark

BemiDB is optimized for analytical workloads and can run complex queries up to 2000x faster than Postgres.

On the TPC-H benchmark with 22 sequential queries, BemiDB outperforms Postgres by a significant margin:

* Scale factor: 0.1
  * BemiDB unindexed: 2.3s 👍
  * Postgres unindexed: 1h23m13s 👎 (2,170x slower)
  * Postgres indexed: 1.5s 👍 (99.97% bottleneck reduction)
* Scale factor: 1.0
  * BemiDB unindexed: 25.6s 👍
  * Postgres unindexed: ∞ 👎 (infinitely slower)
  * Postgres indexed: 1h34m40s 👎 (220x slower)

See the [benchmark](/benchmark) directory for more details.

## Data type mapping

Primitive data types are mapped as follows:

| PostgreSQL                                                  | Parquet                                           | Iceberg                          |
|-------------------------------------------------------------|---------------------------------------------------|----------------------------------|
| `bool`                                                      | `BOOLEAN`                                         | `boolean`                        |
| `varchar`, `text`, `bpchar`, `bit`                          | `BYTE_ARRAY` (`UTF8`)                             | `string`                         |
| `int2`, `int4`                                              | `INT32`                                           | `int`                            |
| `int8`                                                      | `INT64`                                           | `long`                           |
| `xid`                                                       | `INT32` (`UINT_32`)                               | `int`                            |
| `xid8`                                                      | `INT64` (`UINT_64`)                               | `long`                           |
| `float4`, `float8`                                          | `FLOAT`                                           | `float`                          |
| `numeric`                                                   | `FIXED_LEN_BYTE_ARRAY` (`DECIMAL`)                | `decimal(P, S)`                  |
| `date`                                                      | `INT32` (`DATE`)                                  | `date`                           |
| `time`, `timetz`                                            | `INT64` (`TIME_MICROS` / `TIME_MILLIS`)           | `time`                           |
| `timestamp`                                                 | `INT64` (`TIMESTAMP_MICROS` / `TIMESTAMP_MILLIS`) | `timestamp` / `timestamp_ns`     |
| `timestamptz`                                               | `INT64` (`TIMESTAMP_MICROS` / `TIMESTAMP_MILLIS`) | `timestamptz` / `timestamptz_ns` |
| `uuid`                                                      | `BYTE_ARRAY` (`UTF8`)                             | `uuid`                           |
| `bytea`                                                     | `BYTE_ARRAY` (`UTF8`)                             | `binary`                         |
| `interval`                                                  | `BYTE_ARRAY` (`UTF8`)                             | `string`                         |
| `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle` | `BYTE_ARRAY` (`UTF8`)                             | `string`                         |
| `cidr`, `inet`, `macaddr`, `macaddr8`                       | `BYTE_ARRAY` (`UTF8`)                             | `string`                         |
| `tsvector`, `xml`, `pg_snapshot`                            | `BYTE_ARRAY` (`UTF8`)                             | `string`                         |
| `json`, `jsonb`                                             | `BYTE_ARRAY` (`UTF8`)                             | `string` (JSON logical type)     |
| `_*` (array)                                                | `LIST` `*`                                        | `list`                           |
| `*` (user-defined type)                                     | `BYTE_ARRAY` (`UTF8`)                             | `string`                         |

Note that Postgres `json` and `jsonb` types are implemented as JSON logical types and stored as strings (Parquet and Iceberg don't support unstructured data types).
You can query JSON columns using standard operators, for example:

```sql
SELECT * FROM [TABLE] WHERE [JSON_COLUMN]->>'[JSON_KEY]' = '[JSON_VALUE]';
```

## Alternatives

#### BemiDB vs PostgreSQL

PostgreSQL pros:

- It is the most loved general-purpose transactional (OLTP) database 💛
- Capable of running analytical queries at small scale

PostgreSQL cons:

- Slow for analytical (OLAP) queries on medium and large datasets
- Requires creating indexes for specific analytical queries, which impacts the "write" performance for transactional queries
- Materialized views as a "cache" require manual maintenance and become increasingly slow to refresh as the data grows
- Further tuning may not be possible if executing various ad-hoc analytical queries

#### BemiDB vs PostgreSQL extensions

PostgreSQL extensions pros:

- There is a wide range of extensions available in the PostgreSQL ecosystem
- Open-source community driven

PostgreSQL extensions cons:

- Performance overhead when running analytical queries affecting transactional queries
- Limited support for installable extensions in managed PostgreSQL services (for example, AWS Aurora [allowlist](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/AuroraPostgreSQL.Extensions.html#AuroraPostgreSQL.Extensions.16))
- Increased PostgreSQL maintenance complexity when upgrading versions
- Require manual data syncing and schema mapping if data is stored in a different format

Main types of extensions for analytics:

- Foreign data wrapper extensions (parquet_fdw, parquet_s3_fdw, etc.)
  - Pros: allow querying external data sources like columnar Parquet files directly from PostgreSQL
  - Cons: use not optimized for analytics query engines
- OLAP query engine extensions (pg_duckdb, pg_analytics, etc.)
  - Pros: integrate an analytical query engine directly into PostgreSQL
  - Cons: cumbersome to use (creating foreign tables, calling custom functions), data layer is not integrated and optimized

#### BemiDB vs DuckDB

DuckDB pros:

- Designed for OLAP use cases
- Easy to run with a single binary

DuckDB cons:

- Limited support in the data ecosystem like notebooks, BI tools, etc.
- Requires manual data syncing and schema mapping for best performance
- Limited features compared to a full-fledged database: no support for writing into Iceberg tables, reading from Iceberg according to the spec, etc.

#### BemiDB vs real-time OLAP databases (ClickHouse, Druid, etc.)

Real-time OLAP databases pros:

- High-performance optimized for real-time analytics

Real-time OLAP databases cons:

- Require expertise to set up and manage distributed systems
- Limitations on data mutability
- Steeper learning curve
- Require manual data syncing and schema mapping

#### BemiDB vs big data query engines (Spark, Trino, etc.)

Big data query engines pros:

- Distributed SQL query engines for big data analytics

Big data query engines cons:

- Complex to set up and manage a distributed query engine (ZooKeeper, JVM, etc.)
- Don't have a storage layer themselves
- Require manual data syncing and schema mapping

#### BemiDB vs proprietary solutions (Snowflake, Redshift, BigQuery, Databricks, etc.)

Proprietary solutions pros:

- Fully managed cloud data warehouses and lakehouses optimized for OLAP

Proprietary solutions cons:

- Can be expensive compared to other alternatives
- Vendor lock-in and limited control over the data
- Require separate systems for data syncing and schema mapping

---

For a more detailed comparison of different approaches to running analytics with PostgreSQL, check out our [blog post](https://blog.bemi.io/analytics-with-postgresql/).

## Development

We develop BemiDB using [Devbox](https://www.jetify.com/devbox) to ensure a consistent development environment without relying on Docker.

To start developing BemiDB and run tests, follow these steps:

```sh
cp .env.sample .env
make install
make test
```

To run BemiDB locally, use the following command:

```sh
make up
```

To sync data from a Postgres database, use the following command:

```sh
make sync
```

## License

Distributed under the terms of the [AGPL-3.0 License](/LICENSE). If you need to modify and distribute the code, please release it to contribute back to the open-source community.
