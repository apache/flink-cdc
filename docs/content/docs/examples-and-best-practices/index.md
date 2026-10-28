---
title: "Examples and Best Practices"
slug: "/docs/examples-and-best-practices"
sidebar_label: "Overview"
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

# Examples and Best Practices

This section focuses on practical data ingestion scenarios with Flink CDC YAML pipelines:

- Synchronizing databases into data lakes and message queues.
- Selecting tables, transforming records, and routing tables.
- Enriching data with AI functions during ingestion.
- Tuning and troubleshooting data ingestion pipelines.

## Examples

- [Real-time MySQL Ingestion into Paimon](mysql-to-paimon.md): merge order tables and synchronize schema and data changes.
- [Real-time MySQL Change Distribution to Kafka](mysql-to-kafka.md): publish table changes to separate topics and partition them by primary key.
- [Real-time PostgreSQL Ingestion into Iceberg](postgres-to-iceberg.md): configure logical replication and synchronize snapshots, row changes, and an added column.
- [Table Selection and Adding Tables](table-selection-and-new-tables.md): exclude temporary tables, capture new tables, and include historical tables through savepoint restore.
- [Metadata Columns, Computed Columns, and Soft Deletes](metadata-and-soft-deletes.md): enrich records, compute date partitions, and preserve deletion markers.
- [AI Function Configuration and Real-time Text Labeling](ai-text-labeling.md): configure a model and label, summarize, and mask feedback during ingestion.

For pipeline syntax and supported connectors, see [Data Pipeline](../core-concept/data-pipeline.md)
and [Pipeline Connectors](../connectors/pipeline-connectors/overview.md).
For model configuration and available AI functions, see [AI Model](../core-concept/ai-model.md).
