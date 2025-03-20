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

## Per-Event Type Control

Sometimes, it may not be suitable to synchronize all schema change events to downstream.
For example, allowing `AddColumnEvent` but disallowing `DropColumnEvent` is a common scenario to avoid deleting existing data.
This could be achieved by setting `include.schema.changes` and `exclude.schema.changes` option in `sink` block.

### Options

| Option Key               | meaning                                                                                                   | optional/required |
|--------------------------|-----------------------------------------------------------------------------------------------------------|-------------------|
| `include.schema.changes` | Schema change event types to be included. Include all types by default if not specified.                  | optional          |
| `exclude.schema.changes` | Schema change event types **not** to be included. It has a higher priority than `include.schema.changes`. | optional          |

> In Lenient mode, `TruncateTableEvent` and `DropTableEvent` will be ignored by default. In any other mode, no events will be ignored by default.

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
