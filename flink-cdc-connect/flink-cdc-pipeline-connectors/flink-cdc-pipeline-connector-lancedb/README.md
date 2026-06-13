# Flink CDC Pipeline Connector for LanceDB

This connector writes Flink CDC Pipeline events to Lance datasets. In Java, the
implementation uses `com.lancedb:lance-core`, so the target is a Lance dataset
path such as `/data/lancedb/inventory_products.lance` or an object-store path
supported by Lance storage options.

## Supported Semantics

Lance datasets are append-oriented. This connector does not pretend that Lance is
a transactional row-store database.

| CDC event | `append-only` | `append-with-metadata` | `reject` |
| --- | --- | --- | --- |
| INSERT | Append `after` row | Append `after` row with CDC metadata | Reject |
| REPLACE | Append `after` row | Append `after` row with CDC metadata | Reject |
| UPDATE | Reject | Append `after` row with CDC metadata | Reject |
| DELETE | Reject | Append `before` row with CDC metadata | Reject |

When `sink.changelog-mode=append-with-metadata`, the connector adds:

| Column | Type | Meaning |
| --- | --- | --- |
| `_cdc_op` | string | Source operation name |
| `_cdc_deleted` | boolean | `true` for DELETE events |
| `_cdc_event_time` | bigint | Connector-side event processing time in milliseconds |

For latest-row queries, downstream readers should use these metadata columns and
the source primary key to build a merge-on-read view.

## Example

```yaml
sink:
  type: lancedb
  root.path: /data/lancedb
  sink.changelog-mode: append-with-metadata
  sink.flush.max-rows: 1000
  sink.batch.max-rows-per-commit: 1000
  sink.flush.interval: 1s
  sink.max-retries: 3
  write.max-rows-per-file: 1048576
  write.max-rows-per-group: 1024
  write.max-bytes-per-file: 1073741824
```

With the default path derivation, source table `inventory.products` is written to
`/data/lancedb/inventory_products.lance`.

Use explicit mapping when names might collide or when object-store paths are
needed:

```yaml
sink:
  type: lancedb
  root.path: /data/lancedb
  table.path.mapping: inventory.products:/data/lancedb/products.lance;inventory.orders:s3://bucket/lance/orders.lance
  storage.options.region: us-east-1
```

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `root.path` | Yes | none | Root directory for derived Lance dataset paths. |
| `table.path.mapping` | No | empty | `sourceTable:targetPath` pairs separated by `;`. |
| `table.name-normalize.enabled` | No | `true` | Normalize source table IDs when deriving dataset names. |
| `create-table.enabled` | No | `true` | Create missing Lance datasets on CREATE_TABLE. |
| `schema-validation.enabled` | No | `true` | Validate existing dataset schema on CREATE_TABLE. |
| `schema.evolution.enabled` | No | `false` | Enable compatible ADD_COLUMN at the end of the schema. |
| `sink.changelog-mode` | No | `append-only` | `append-only`, `append-with-metadata`, or `reject`. |
| `sink.flush.max-rows` | No | `1000` | Maximum buffered rows before flush. |
| `sink.batch.max-rows-per-commit` | No | `1000` | Maximum rows written in one Lance commit. Lower this to reduce retry blast radius on object storage or high-conflict workloads. |
| `sink.flush.interval` | No | `1s` | Maximum buffered time before flush. |
| `sink.max-retries` | No | `3` | Maximum retries for retryable write failures. |
| `sink.retry.backoff` | No | `500ms` | Backoff between retries. |
| `write.max-rows-per-file` | No | `1048576` | Lance `WriteParams` max rows per file. |
| `write.max-rows-per-group` | No | `1024` | Lance `WriteParams` max rows per group. |
| `write.max-bytes-per-file` | No | `1073741824` | Lance `WriteParams` max bytes per file. |
| `write.enable-stable-row-ids` | No | `true` | Lance stable row IDs setting. |
| `write.mode` | No | `CREATE` | Lance dataset creation mode. |
| `timezone` | No | `UTC` | Timezone for timestamp conversion. |
| `storage.options.*` | No | empty | Lance storage options passed through to `WriteParams`. |

## Type Mapping

| Flink CDC type | Arrow/Lance type |
| --- | --- |
| BOOLEAN | Bool |
| TINYINT | Int8 |
| SMALLINT | Int16 |
| INT | Int32 |
| BIGINT | Int64 |
| FLOAT | Float32 |
| DOUBLE | Float64 |
| CHAR / VARCHAR / STRING | Utf8 |
| BINARY / VARBINARY / BYTES | Binary |
| DECIMAL(p, s) | Decimal128(p, s) |
| DATE | Date32 |
| TIME | Time32 millisecond |
| TIMESTAMP / TIMESTAMP_LTZ / TIMESTAMP_TZ | Timestamp microsecond |
| ARRAY of string/boolean/integer/float/double | List |

`MAP`, `ROW`, `VARIANT`, nested arrays, arrays of decimal/time/binary, and schema
changes other than CREATE_TABLE and optional ADD_COLUMN are rejected.

## Production Notes

- The connector hashes events by target dataset path so the same dataset is
  routed to one sink subtask, reducing Lance commit conflicts.
- Do not map multiple source tables to the same dataset path. The factory and
  runtime metadata layer reject duplicate ownership.
- Existing Lance schemas are validated strictly by field order, field name,
  Arrow type, nullability, and nested list element type when
  `schema-validation.enabled=true`.
- Default mode is append-only and rejects UPDATE/DELETE. Use
  `append-with-metadata` only when downstream readers are designed for CDC logs.
- Lance Java native libraries must match the JVM architecture. On macOS arm64,
  use an arm64 JDK. The bundled `lance-core` artifact supports macOS arm64,
  Linux x86_64, and Linux aarch64.
- Tune `sink.flush.max-rows`, `sink.batch.max-rows-per-commit`,
  `write.max-rows-per-file`, and `write.max-rows-per-group` together. Smaller
  commit batches reduce retry cost; larger file/group settings reduce small-file
  pressure.
- This connector provides at-least-once writes. It flushes on checkpoint calls,
  but it does not implement two-phase commit.
- For object storage, configure the required `storage.options.*` keys and test
  credentials in the same runtime environment as Flink.

## Verification

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-lancedb test
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-lancedb -Dtest=LanceDbSinkITCase test
```

`LanceDbSinkITCase` runs only when the Lance native library can be loaded for the
current JVM architecture.
