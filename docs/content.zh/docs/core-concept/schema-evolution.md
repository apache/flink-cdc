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

> 注意：在此模式下，`TruncateTableEvent` 和 `DropTableEvent` 默认不会被发送到下游，以避免意外的数据丢失。这一行为可以通过配置 [Per-Event Type Control](#per-event-type-control) 调整。

### Ignore 模式

在此模式下，所有架构更改事件都将被 `SchemaOperator` 默默接收，并且永远不会尝试将它们应用于下游接收器。
当您的下游接收器尚未准备好进行任何架构更改，但想要继续从未更改的列中接收数据时，这很有用。

## 按类型配置行为

有时，将所有架构更改事件同步到下游可能并不合适。
例如，允许 `AddColumnEvent` 但禁止 `DropColumnEvent` 是一种常见的情况，可以避免删除已有的数据。
这可以通过在 `sink` 块中设置 `include.schema.changes` 和 `exclude.schema.changes` 选项来实现。

### 选项

| Option Key               | 注释                                              | 是否可选 |
|--------------------------|-------------------------------------------------|------|
| `include.schema.changes` | 要应用的结构变更事件类型。如果未指定，则默认包含所有类型。                   | 是    |
| `exclude.schema.changes` | 不希望应用的结构变更事件类型。其优先级高于 `include.schema.changes`。 | 是    |

> 在 Lenient 模式下，`TruncateTableEvent` 和 `DropTableEvent` 默认会被忽略。在任何其他模式下，默认不会忽略任何事件。

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
