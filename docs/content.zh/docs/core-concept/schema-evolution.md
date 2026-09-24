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

# 定义

**Schema Evolution** 功能可以用于将上游的 DDL 变更事件同步到下游，例如创建新表、添加新列、重命名列或更改列类型、删除列、截断和删除表等。

## 参数

Schema Evolution 的行为可以通过配置以下参数来设定：

```yaml
pipeline:
  schema.change.behavior: evolve
```

`schema.change.behavior` 是一个枚举类型，可以被设定为 `exception`、`evolve`、`try_evolve`、`lenient`、或 `ignore`。

## Schema Evolution 行为

### Exception 模式

在此模式下，所有结构变更行为均不被允许。
一旦收到表结构变更事件，`SchemaOperator` 就会抛出异常。
当您的下游接收器不能处理任何架构更改时，可以使用此模式。

### Evolve 模式

在此模式下，`SchemaOperator` 会将所有上游架构更改事件应用于下游接收器。
如果尝试失败，则会从 `SchemaRegistry` 抛出异常并触发全局的故障重启。

### TryEvolve 模式

在此模式下，架构运算符还将尝试将上游架构更改事件应用于下游接收器。
但是，如果下游接收器不支持特定的架构更改事件并报告失败，
`SchemaOperator` 会容忍这一事件，并且在出现上下游表结构差异的情况下，尝试转换所有后续数据记录。

> 警告：此类数据转换和转换不能保证无损。某些数据类型不兼容的字段可能会丢失。

### Lenient 模式

在此模式下，架构操作员将在转换所有上游架构更改事件后将其转换为下游接收器，以确保不会丢失任何数据。
例如，`AlterColumnTypeEvent` 将被转换为两个单独的架构更改事件 `RenameColumnEvent` 和 `AddColumnEvent`：
保留上一列（具有更改前的类型），并添加一个新列（具有新类型）。

这是默认的架构演变行为。

> 注意：在此模式下，`TruncateTableEvent` 和 `DropTableEvent` 默认不会被发送到下游，以避免意外的数据丢失。这一行为可以通过配置 [按类型配置行为](#按类型配置行为) 调整。

### Ignore 模式

在此模式下，所有架构更改事件都将被 `SchemaOperator` 默默接收，并且永远不会尝试将它们应用于下游接收器。
当您的下游接收器尚未准备好进行任何架构更改，但想要继续从未更改的列中接收数据时，这很有用。

## 已有目标表的安全 Schema 扩展

设置 Sink 选项 `existing-table.schema-expansion.mode` 来控制框架在初始 `CreateTableEvent` 遇到已有目标表时的处理方式，默认值为 `DISABLED`。对于实现了该能力的 Sink，框架可能将缺失的普通非键物理列按 nullable 补充，并安全拓宽普通非键列类型；派生的 DDL 事件会记录在日志中。

这是由 Flink CDC 框架直接消费的 `sink` 级选项。连接器创建前，框架会从 Sink 配置中移除该选项，因此它不会作为配置属性传递到连接器的 `MetadataApplier`。与 `catalog.properties.*` 不同，该选项不能嵌套在连接器专属属性下；将其写入 `catalog.properties.existing-table.schema-expansion.mode` 不会生效。

| 选项 | 作用范围 | 是否传递给连接器 |
|---|---|---|
| `existing-table.schema-expansion.mode` | Flink CDC 框架 | 否 |
| `catalog.properties.*` | 连接器专属 Catalog 选项 | 是 |

只有其 `MetadataApplier` 实现了 `ExistingTableSchemaExpansionSupport` 的 Sink 才支持扩展。当前实现该能力的 pipeline connector 是 Paimon 和 Fluss。显式配置任何非 `DISABLED` 模式时，如果当前 connector 缺少扩展能力，作业都会立即失败。

该能力仅对流式作业生效。当 `execution.runtime-mode` 为 `BATCH` 时，该选项会被忽略并打印告警日志，此时沿用 Sink 原生的 Schema 处理。

| 模式 | 已有目标表 | 目标表不存在 | 失败处理 |
|---|---|---|---|
| `DISABLED` | 不检查、不扩展，保持 Sink 原行为 | Sink 原生建表 | 不适用 |
| `CHECK` | 只校验每个上游列能否被目标表容纳、且主键与 pipeline 一致，不执行任何 DDL | 作业失败，目标表需由外部创建 | 任何不兼容、读取失败或能力缺失都会以聚合错误使作业失败 |
| `TRY_EXPAND` | 检查并尽力执行安全 DDL，随后读回目标 schema 验证 | Sink 原生建表 | 缺少扩展能力时作业失败；本机制的其他失败仅记录日志并交给 Sink 原行为 |
| `EXPAND` | 检查并执行安全 DDL，随后读回目标 schema 验证 | Sink 原生建表 | 任何不兼容、DDL 不支持、执行或验证失败都会使作业失败 |

`CHECK` 不执行任何 DDL，因此不受 `include.schema.changes` 和 Sink DDL 能力的影响。它守护初始表状态，**不受 `schema.change.behavior` 控制**（包括 `IGNORE` 和 `EXCEPTION` 也会执行检查）；`TRY_EXPAND` 和 `EXPAND` 在 `schema.change.behavior` 为 `IGNORE` 或 `EXCEPTION` 时跳过框架侧初始处理。注意：`CHECK` 只约束已有目标表的初始处理，后续源端 Schema 变更仍由 `schema.change.behavior` 控制，因此它不是全作业级别的“永不执行 DDL”开关。检查失败时，聚合错误会列出每处差异（表、列、上游类型与目标类型），并附方言无关的 `ALTER TABLE` 修复 SQL 模板，需按目标连接器方言调整并人工确认后执行。

扩展不会重新对齐表键。框架会将 pipeline 的主键与已有目标表的主键进行比较（比较时忽略两侧的分区列，因为 Paimon 等连接器会把分区列存入主键），不一致会作为不兼容项上报：`CHECK` 与 `EXPAND` 会直接使作业失败；`TRY_EXPAND` 则完全跳过扩展（不会对一张主键永远无法匹配的表执行 DDL），并把 `CreateTableEvent` 交由 Sink 处理，因此只打印告警，最终由连接器自身的键校验决定。若希望主键不一致时作业必须失败，请使用 `CHECK` 或 `EXPAND`。分区键只在 pipeline 自身声明时才比较（例如通过 `PARTITION BY` 转换，或源端会上报分区信息）：多数源并不声明，因此外部自行分区的目标表不会被判为不兼容。

`TRY_EXPAND` 仅在确认 connector 支持该能力后吞掉本机制自身的失败：它既不屏蔽 Sink 自身 Schema 处理抛出的错误，也不保证扩展失败后所有上游列都能落入目标表。

```yaml
sink:
  type: paimon
  existing-table.schema-expansion.mode: "EXPAND"
```

模式值必须使用字符串（例如 `"DISABLED"` 或 `"EXPAND"`）；布尔值将不被接受。

## 按类型配置行为

有时，将所有架构更改事件同步到下游可能并不合适。
例如，允许 `AddColumnEvent` 但禁止 `DropColumnEvent` 是一种常见的情况，可以避免删除已有的数据。
这可以通过在 `sink` 块中设置 `include.schema.changes` 和 `exclude.schema.changes` 选项来实现。

### 选项

| Option Key               | 注释                                    | 是否可选 |
|--------------------------|---------------------------------------|------|
| `include.schema.changes` | 要应用的结构变更事件类型。如果未指定，则默认包含所有类型。          | 是    |
| `exclude.schema.changes` | 不希望应用的结构变更事件类型。其优先级高于 `include.schema.changes`。 | 是    |

> 在 Lenient 模式下，`TruncateTableEvent` 和 `DropTableEvent` 默认会被忽略。在任何其他模式下，默认不会忽略任何事件。

> `CreateTableEvent` 是所有后续 schema 变更处理的基础。当显式指定 `include.schema.changes` 时，`create.table` 会被自动添加，除非用户通过 `exclude.schema.changes` 明确将其排除。

以下是可配置架构变更事件类型的完整列表：

| 事件类型                | 注释           |
|---------------------|--------------|
| `add.column`        | 向表中追加一列。     |
| `alter.column.type` | 变更某一列的数据类型。  |
| `create.table`      | 创建一张新表。      |
| `drop.column`       | 删除某一列。       |
| `drop.table`        | 删除某张表。       |
| `rename.column`     | 修改某一列的名字。    |
| `truncate.table`    | 清除某张表中的全部数据。 |

支持部分匹配。例如，将 `drop` 传入上面的选项相当于同时传入 `drop.column` 和 `drop.table`。

### 例子

下面的 YAML 配置设置为包括 `CreateTableEvent` 和列相关事件，但 `DropColumnEvent` 除外。

```yaml
sink:
  include.schema.changes: [create.table, column] # 匹配了 CreateTable、AddColumn、AlterColumnType、RenameColumn、和 DropColumn 事件
  exclude.schema.changes: [drop.column] # 排除了 DropColumn 事件
```
