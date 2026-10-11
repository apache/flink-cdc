---
title: "GaussDB DWS"
weight: 6
type: docs
aliases:
- /connectors/pipeline-connectors/dws
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# GaussDB DWS Connector

The GaussDB DWS pipeline sink writes Flink CDC events through the official DWS client. The default `auto` mode lets that client choose UPSERT or COPY-based execution for each supported batch. Target tables must have primary keys.

The sink provides at-least-once recovery and final convergence when the source is replayable. It does not provide checkpoint-transaction visibility or exactly-once delivery. A checkpoint flushes the official client before writer state is snapshotted.

## Example

```yaml
source:
  type: values
  name: Values Source

sink:
  type: dws
  name: GaussDB DWS Sink
  jdbc-url: jdbc:gaussdb://127.0.0.1:8000/postgres?connectTimeout=10&socketTimeout=60
  username: gaussdb
  password: INJECT_WITH_APPROVED_SECRET_MECHANISM
  schema: public
  sink.enable-delete: true
  write-mode: auto
  auto-batch-flush-size: 30000
  auto-flush-max-interval: 3s
  dws.client.write.force-flush-size: 40000
  dws.client.write.buffer.all-max-bytes: 128MiB
  dws.client.write.buffer.table-max-bytes: 64MiB
  dws.client.write.buffer.partition-max-bytes: 32MiB

pipeline:
  name: Values to GaussDB DWS Pipeline
  parallelism: 4
```

The password example is a placeholder. Supply credentials through the deployment platform's approved secret mechanism; the pipeline parser does not promise environment-variable expansion.

## Connector Options

### Connection, naming, and table behavior

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `type` | yes | — | Must be `dws`. |
| `jdbc-url` | yes | — | `jdbc:gaussdb://` URL including the target database. Existing unrelated query parameters are preserved. Missing `connectTimeout`/`socketTimeout` are added as 10/60 seconds. |
| `username` / `password` | yes | — | DWS credentials. They are never included in the connector's effective-configuration log. |
| `schema` | no | `public` | Default schema for table identifiers without an explicit schema. |
| `case-sensitive` | no | `true` | Preserve identifier case. When `false`, schema, table, column, and key identifiers are normalized to lower case. Embedded quote characters are rejected. |
| `local-time-zone` | no | pipeline value, then system default | Time zone used for timestamp conversion. |
| `driver` | no | `com.huawei.gauss200.jdbc.Driver` | This is the only accepted driver. |
| `sink.enable-delete` | no | `true` | Controls independent DELETE events. Retractions generated for primary-key changes are always executed. |
| `enable-dn-partition` | no | `false` | Adds DDL distribution semantics. This is not native-client DirectDN mode. |
| `distribution-key` | no | — | Comma-separated existing columns. Required when DN partitioning is enabled and rejected when it is disabled. |

### Write, retry, and timeout options

| Option | Default | Description |
| --- | --- | --- |
| `write-mode` | `auto` | `auto`, `upsert`, `copy_upsert`, or `copy_merge` (case-insensitive). Alias: `dws.client.write.mode`. `auto` is recommended. |
| `enable-auto-flush` | `true` | Enables normal native batch/time triggers. `false` still retains finite force, connector budget, checkpoint, and schema flushes. |
| `auto-batch-flush-size` | `30000` | Positive native auto-flush batch size. Alias: `dws.client.write.auto-flush-size`. |
| `auto-flush-max-interval` | `3s` | Positive native auto-flush interval. Alias: `dws.client.write.auto-flush-max-interval`. |
| `dws.client.write.thread-size` | `1` | Positive official-client worker count. Raising it only enables client-side cross-table concurrency. |
| `dws.client.write.use-copy-size` | `1000` | Positive AUTO-to-COPY threshold for compatible same-column batches; it does not guarantee COPY for every type. |
| `dws.client.write.force-flush-size` | `40000` | Finite safety flush threshold; must be at least the auto batch size when auto flush is enabled. |
| `sink.max-retries` | `3` | Total attempts, at least 1. Alias: `dws.client.retry.max-times`. |
| `dws.client.retry.sleep-base-time` | `1s` | Non-negative retry base delay. |
| `dws.client.retry.sleep-random-time` | `300ms` | Positive retry jitter. |
| `dws.client.timeout.task` | `10min` | Positive native task timeout; not a hard deadline for the whole flush. |
| `dws.client.timeout.statement` | `5min` | Positive DWS statement timeout. |

If a primary option and its alias are both present, their normalized values must be equal. Conflicts fail during connector creation instead of being silently overwritten.

### Buffer budgets

| Option | Default | Scope |
| --- | --- | --- |
| `dws.client.write.buffer.all-max-bytes` | `128MiB` | Estimated total buffered bytes per sink writer/client. |
| `dws.client.write.buffer.table-max-bytes` | `64MiB` | Estimated bytes per table and writer; must not exceed the all-table budget. |
| `dws.client.write.buffer.partition-max-bytes` | `32MiB` | Estimated bytes for the connector-fixed native partition; must not exceed the table budget. |

These are conservative accounting budgets, not JVM heap limits. The estimate includes binary values, keys, and container overhead because the native record-size metric omits `byte[]`. A single estimated record above the effective per-table/partition or all-table budget is rejected before native commit. When the next legal record would exceed an accumulated budget, the writer synchronously flushes first and clears counters only after success. The writer does not keep a second copy of records.

### Connection lifetime compatibility

| Option | Default | Description |
| --- | --- | --- |
| `connectionMaxUseTimeSeconds` | `3600` | Legacy seconds form. `connectionMaxUseTimeThreshold` is a compatibility synonym; native alias: `dws.client.jdbc.max.use-time` (Duration). |
| `connectionMaxIdleMs` | `60000` | Legacy milliseconds form; native alias: `dws.client.jdbc.max.idle` (Duration). |
| `connectionTimeOut` | URL default 10s | Legacy milliseconds form for JDBC `connectTimeout`; an explicit value must be positive and divisible by 1000. |
| `connectionSocketTimeout` | URL default 60s | Legacy milliseconds form for JDBC `socketTimeout`; an explicit value must be positive and divisible by 1000. |

An existing URL timeout and a legacy option may be supplied together only when they are equal after unit conversion. JDBC connect/socket timeouts do not constitute a global flush deadline.

### Explicitly rejected options

The connector rejects `sink-table` (use pipeline routing), `sink.parallelism` (use `pipeline.parallelism`), `connectionSize` (use `dws.client.write.thread-size`), `logSwitch=true`, all `connectionPool*`/pool-monitor options, `connectionMaxUseCount`, and native partition-policy/min/max overrides. Arbitrary `dws.client.*` pass-through, DirectDN, compare-field, partial-update, and conflict-ignore settings are not supported.

## Delivery, ordering, schema, and observability

- A complete distribution must contain the updated common event model, serializer, runtime partition operators, composer, and DWS connector. Deploying only the connector JAR is unsafe because primary-key-changing UPDATE events use a typed retraction event.
- The DWS sink opts into primary-key update splitting. The retraction is partitioned by the old key and the insertion by the new key, retaining multiple writers per table and per-key ordering. Other sinks keep their prior event stream. This does not create global ordering across unrelated keys.
- CREATE and schema-evolution events refresh the official client's table-schema cache before the connector publishes its new converter. DROP removes both caches; TRUNCATE retains schema. Unsupported schema changes fail the pipeline.
- Mixed-case identifiers are supported according to `case-sensitive`; this is not a promise that every quoted or otherwise illegal identifier is accepted.
- Flink metrics expose `dws.acceptedRecords`, `dws.flushCount`, `dws.conservativeBufferedBytes`, `dws.lastFlushDurationMillis`, and `dws.firstAsyncFailure`; standard sink metrics expose flush-confirmed sent records/bytes and definite synchronous send errors. “Written” means the synchronous native flush returned, not transactional checkpoint visibility. Native buffer metrics that are unavailable are not synthesized.

## Data Type Mapping

| Flink CDC type | GaussDB DWS type | Notes |
| --- | --- | --- |
| BOOLEAN | BOOLEAN | |
| TINYINT, SMALLINT | SMALLINT | |
| INTEGER | INTEGER | |
| BIGINT | BIGINT | |
| FLOAT | REAL | |
| DOUBLE | DOUBLE PRECISION | |
| DECIMAL | DECIMAL(p, s) | Source precision and scale are preserved. |
| CHAR | CHAR(n) | |
| VARCHAR | VARCHAR(n) or TEXT | Very large VARCHAR maps to TEXT. |
| BINARY, VARBINARY | BYTEA | Included in conservative buffer accounting. |
| DATE | DATE | |
| TIME | TIME(p) | Maximum precision 6. |
| TIMESTAMP | TIMESTAMP(p) | Maximum precision 6. |
| TIMESTAMP_LTZ, TIMESTAMP_TZ | TIMESTAMPTZ(p) | Maximum precision 6. |
| ARRAY | TEXT | |
| MAP, ROW | JSON | |

## Migrating from the staging/committer writer

The native-client writer uses a new state protocol and cannot restore a savepoint created by the old staging-table committer. Do not bypass this check with `allowNonRestoredState` or a changed sink operator UID: either can discard unapplied committables.

1. Drain and stop the old job. Retain its artifact, checkpoint/savepoint, source offsets, old target, staging tables, and resource-ownership inventory.
2. Create an empty target dedicated to the migration.
3. Run a consistent full snapshot with the new connector, then resume incremental replay while retaining source logs.
4. Compare every primary key and field, including deletes and primary-key changes, with an independent query.
5. Switch consumers only after explicit authorization. Retain the old target and replay boundary through the rollback window.

To roll back, stop the new job while preserving diagnostics, then restore the old artifact against the old target and retained source boundary. Neither job may delete resources it does not own; cleanup needs separate authorization.

The native-client route offers at-least-once recovery and convergence on replayable sources. It does not provide checkpoint-transaction visibility or direct state interchange with the old committer.

{{< top >}}
