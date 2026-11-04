---
title: "示例与最佳实践"
slug: "/docs/examples-and-best-practices"
sidebar_label: "概览"
sidebar_position: 1
hide_title: true
displayed_sidebar: docs
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

# 示例与最佳实践

本节聚焦于 Flink CDC YAML 数据摄入作业的实际应用场景：

- 数据库数据实时同步到数据湖和消息队列。
- 表筛选、数据转换与表路由。
- 数据摄入过程中的 AI 数据增强。
- 数据摄入作业调优与故障排查。

## 示例

- [MySQL 数据实时入湖 Paimon](mysql-to-paimon.md)：合并订单分表，并同步表结构和数据变更。
- [MySQL 变更实时分发投递到 Kafka](mysql-to-kafka.md)：将表变更写入独立 Topic，并按主键分区。
- [PostgreSQL 数据实时入湖 Iceberg](postgres-to-iceberg.md)：配置逻辑复制，并同步全量快照、行级变更和新增字段。
- [表筛选与新增表同步](table-selection-and-new-tables.md)：排除临时表、不停机捕获新建表，并从 Savepoint 恢复以纳入历史表。
- [元数据列、计算列与软删除](metadata-and-soft-deletes.md)：补充元数据、计算日期分区，并保留删除标记。
- [AI Function 配置与实时文本标注](ai-text-labeling.md)：配置模型，并在摄入时完成反馈标注、摘要生成和脱敏。

作业语法和支持的连接器请参见[数据管道](../core-concept/data-pipeline.md)和
[Pipeline 连接器](../connectors/pipeline-connectors/overview.md)。
模型配置和可用的 AI 函数请参见 [AI 模型](../core-concept/ai-model.md)。
