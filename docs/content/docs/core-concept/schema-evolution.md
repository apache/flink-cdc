---
title: "Schema Evolution"
weight: 7
type: docs
aliases:
  - /core-concept/schema-evolution/
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

# Definition

**Schema Evolution** feature could synchronize upstream schema DDL changes to downstream, including creating new table, appending new columns, renaming columns or changing column types, dropping columns, truncating and dropping tables.

## Parameters

Schema evolution behavior could be specified with the following pipeline option:

```yaml
pipeline:
  schema.change.behavior: evolve
```

`schema.change.behavior` is of enum type, and could be set to `exception`, `evolve`, `try_evolve`, `lenient` or `ignore`.

## Behaviors

### Exception Mode

In this mode, all schema change behaviors are forbidden. An exception will be thrown from `SchemaOperator` once it was captured.
This is useful when your downstream sink is not expected to handle any schema changes.

### Evolve Mode

In this mode, CDC pipeline schema operator will apply all upstream schema change events to downstream sink.
If the attempt fails, an exception will be thrown from the `SchemaRegistry` and trigger a global failover.

### TryEvolve Mode

In this mode, schema operator will also try to apply upstream schema change events to downstream sink.
However, if specific schema change events are not supported by downstream sink, the failure will be tolerated and `SchemaOperator` will try to convert all following data records in case of schema discrepancy.

> Warning: such data casting and converting isn't guaranteed to be lossless. Some fields with incompatible data types might be lost. 

### Lenient Mode

In this mode, schema operator will convert all upstream schema change events to downstream sink after converting them to ensure no data will be lost.
For example, an `AlterColumnTypeEvent` will be converted to two individual schema change events including `RenameColumnEvent` and `AddColumnEvent`:
Previous column (with the unchanged type) will be kept and a new column (with the new type) will be added.

This is the default schema evolution behavior.

> Notice: In this mode, `TruncateTableEvent` and `DropTableEvent` will not be sent to downstream to avoid unexpected data loss. Such behavior could be overridden by [Per-Event Type Control](#per-event-type-control).

### Ignore Mode

In this mode, all schema change events will be silently swallowed by `SchemaOperator` and never attempt to apply them to downstream sink.
This is useful when your downstream sink is unready for any schema changes, but wants to keep receiving data from unchanged columns.

## Existing Table Schema Expansion

Set the sink option `existing-table.schema-expansion.mode` to control how the framework handles the initial `CreateTableEvent` when the target table already exists. The default is `OFF`. For sinks that implement this capability, the framework may add missing non-key physical columns as nullable columns and safely widen non-key column types. Derived DDL events are logged.

| Mode | Behavior on an existing target table | Behavior when the target table is missing | Failure handling |
|---|---|---|---|
| `OFF` | No check or expansion; the sink's original behavior applies | Sink creates the table | N/A |
| `CHECK` | Validate that every upstream column can be contained by the target table, without issuing any DDL | Fails the job; the table must be created externally | Any incompatibility, read failure, or missing capability fails the job with an aggregated error |
| `TRY_EXPAND` | Check and best-effort apply safe DDL, then verify the result by reading the target schema back | Sink creates the table | Failures of this mechanism are logged and delegated to the sink's original behavior |
| `EXPAND` | Check and apply safe DDL, then verify the result by reading the target schema back | Sink creates the table | Any incompatibility, unsupported DDL, execution or verification failure fails the job |

`CHECK` never issues DDL, so it is independent of `include.schema.changes` and of the sink's DDL capabilities. It guards the initial table state and runs **regardless of `schema.change.behavior`** (including `IGNORE` and `EXCEPTION`); `TRY_EXPAND` and `EXPAND` skip the framework-side initial handling when `schema.change.behavior` is `IGNORE` or `EXCEPTION`. Note that `CHECK` only constrains the initial table handling: subsequent source schema changes are still controlled by `schema.change.behavior`, so it is not a job-wide "never issue DDL" switch. When the check fails, the aggregated error lists every difference (table, column, upstream type vs. target type) together with suggested `ALTER TABLE` repair statements that can be reviewed and adjusted to the target system's dialect.

`TRY_EXPAND` swallows failures of this mechanism only; it neither hides errors from the sink's own schema handling nor guarantees that all upstream columns end up in the target table after a failed expansion.

```yaml
sink:
  type: paimon
  existing-table.schema-expansion.mode: "EXPAND"
```

> Note: `existing-table.schema-expansion.enabled` is no longer supported. Use `existing-table.schema-expansion.mode` with one of `OFF`, `CHECK`, `TRY_EXPAND`, `EXPAND` instead; the previous `enabled: true` maps to `TRY_EXPAND`. Quote the mode value to avoid the bare `OFF` scalar being parsed as a YAML boolean.

## Per-Event Type Control

Sometimes, it may not be suitable to synchronize all schema change events to downstream.
For example, allowing `AddColumnEvent` but disallowing `DropColumnEvent` is a common scenario to avoid deleting existing data.
This could be achieved by setting `include.schema.changes` and `exclude.schema.changes` option in `sink` block.

### Options

| Option Key               | meaning                                                                                                    | optional/required |
|--------------------------|------------------------------------------------------------------------------------------------------------|-------------------|
| `include.schema.changes` | Schema change event types to be included. Include all types by default if not specified.                   | optional          |
| `exclude.schema.changes` | Schema change event types **not** to be included. It has a higher priority than `include.schema.changes`. | optional          |

> In Lenient mode, `TruncateTableEvent` and `DropTableEvent` will be ignored by default. In any other mode, no events will be ignored by default.

> `CreateTableEvent` is the foundation for all subsequent schema change processing. When `include.schema.changes` is explicitly specified, `create.table` will be automatically added unless the user explicitly excludes it via `exclude.schema.changes`.

Here's a full list of configurable schema change event types:

| Event Type          | Description                  |
|---------------------|------------------------------|
| `add.column`        | Add a new column to a table. |
| `alter.column.type` | Change the type of column.   |
| `create.table`      | Create a new table.          |
| `drop.column`       | Drop a column.               |
| `drop.table`        | Drop a table.                |
| `rename.column`     | Rename a column.             |
| `truncate.table`    | Truncate a table.            |

Partial matching is supported. For example, passing `drop` into the options above is equivalent to passing `drop.column` and `drop.table`.

### Example

The following YAML configuration is set to include `CreateTableEvent` and column related events, except `DropColumnEvent`.

```yaml
sink:
  include.schema.changes: [create.table, column] # This matches CreateTable, AddColumn, AlterColumnType, RenameColumn, and DropColumn Events
  exclude.schema.changes: [drop.column] # This excludes DropColumn Events
```
