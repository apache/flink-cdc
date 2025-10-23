---
title: "理解 Flink CDC API"
weight: 1
type: docs
aliases:
  - /developer-guide/understand-flink-cdc-api
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

# 理解 Flink CDC API

如果你计划构建自己的 Flink CDC 连接器，或打算为 Flink CDC 项目做出贡献，我们建议
你深入了解 Flink CDC 涉及的 API。这里将介绍其中一些关键概念与接口，以帮助你进行开发工作。

## 事件 Event

在 Flink CDC 的上下文中，事件（event）是 Flink 数据流中的一种特殊记录。
它描述了外部系统数据源端（[source](../core-concept/data-source.md) side）捕获到的变更，
经由 Flink CDC 内部构建的算子处理与转换后，
再传递给下游的数据终端（[sink](../core-concept/data-sink.md) side），最终写入或应用到外部系统中。

每个事件都包含两个核心部分：表 ID（table ID），标识事件所属的表；以及负载（payload），即事件携带的数据内容。
根据负载类型的不同，事件可以分为以下几类：

### 数据变更事件 DataChangeEvent

数据变更事件（DataChangeEvent）描述了源端的数据变化，包含五个字段：

- `Table ID`：所属表的 ID
- `Before`：变更前的数据快照
- `After`：变更后的数据快照
- `Operation type`：此变更操作的类型
- `Meta`：此变更的元信息

对于操作类型（operation type）字段，我们预定义了 4 种类型：

- Insert：新数据插入，`before = null`，`after = 新数据`
- Delete：数据删除，`before = 被删除的数据`，`after = null`
- Update：已有数据更新，`before = 更新前的数据`，`after = 更新后的数据`
- Replace：

### 模式变更事件（SchemaChangeEvent）

模式变更事件（SchemaChangeEvent）描述了源端的模式变化。与数据变更事件相比，
模式变更事件的负载描述了外部系统中表结构的变化，包括以下几种：

- `AddColumnEvent`：新增列
- `AlterColumnTypeEvent`：列类型修改
- `CreateTableEvent`：新表的创建。
  也用于描述预先发送的数据变更事件（DataChangeEvent）的模式
- `DropColumnEvent`：删除列
- `RenameColumnEvent`：重命名列

### 事件流（Flow of Events）

你可能注意到，数据变更事件（DataChangeEvent）并不携带其表结构信息（schema）。
这种设计减少数据变更事件体积和序列化的开销，但也意味着事件不能自解释。
那么，框架如何解释数据变更事件呢？

为了解决这个问题，框架对事件流（flow of events）增加了一个要求：
若某张表对框架而言是新表，则必须在发送任何 `DataChangeEvent` 前发送
一个 `CreateTableEvent`；若表的模式发生了变化，则必须在任何
新的 `DataChangeEvent` 前发送一个 `SchemaChangeEvent`。
这种事件顺序确保框架在处理数据变更前已经了解模式定义。

{{< img src="/fig/flow-of-events.png" alt="Flow of Events" >}}

## 数据源（Data Source）

数据源可视为 `EventSource` 和 `MetadataAccessor` 的工厂，
用于构建从外部系统捕获变更并提供元数据的运行时实现。

`EventSource` 是一个 Flink 源，它读取外部系统的变更，将其转换为事件，
然后发送到下游的 Flink 算子。你可以参考 [Flink 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/)
来了解其内部机制以及如何实现一个 Flink 源。

`MetadataAccessor` 作为外部系统的元数据读取器，例如列出命名空间（namespace）、
模式（schema）与表（table），并提供给定表 ID 的模式（表结构）定义。

## 数据汇（Data Sink)

与数据源（data source）对称，数据汇（data Sink）由 `EventSink` 和 `MetadataApplier` 组成，
用于写入数据变更事件并将模式变更（元数据变更）应用至外部系统。

`EventSink` 是一个 Flink Sink，它接收来自上游算子的变更事件，并将其应用到外部系统中。
目前我们仅支持 Flink 的 Sink V2 API。

`MetadataApplier` 则负责处理模式变更。框架从源端接收到模式变更事件，经内部同步和刷新操作，
该应用器将这些结构变更应用至外部系统。