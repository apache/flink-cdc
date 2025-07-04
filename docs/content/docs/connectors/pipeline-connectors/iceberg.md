---
title: "Iceberg"
weight: 9
type: docs
aliases:
- /connectors/pipeline-connectors/iceberg
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

# Iceberg Pipeline Connector

The Iceberg Pipeline Connector functions as a *Data Sink* for data pipelines, enabling data writes to [Apache Iceberg](https://iceberg.apache.org) tables. This document explains how to configure the connector.

## Key Capabilities
* **Automatic Table Creation:**
creates Iceberg tables dynamically when they do not exist
* **Schema Synchronization:**
propagates schema changes (e.g., column additions) from source systems to Iceberg
* **Data Replication:**
supports both batch and streaming data synchronization

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Iceberg can be defined as follows:

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: 127.0.0.1
  port: 3306
  username: admin
  password: pass
  tables: adb.\.*, bdb.user_table_[0-9]+, [app|web].order_\.*
  server-id: 5401-5404

sink:
  type: iceberg
  name: Iceberg Sink
  catalog.properties.type: hadoop
  catalog.properties.warehouse: /path/warehouse

pipeline:
  name: MySQL to Iceberg Pipeline
  parallelism: 2
```

Pipeline Connector Options
----------------
<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>type</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>iceberg</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>catalog.properties.type</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Metastore of Iceberg catalog, supports <code>hadoop</code> and <code>hive</code>.</td>
    </tr>
    <tr>
      <td>catalog.properties.warehouse</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The warehouse root path of catalog.</td>
    </tr>
    <tr>
      <td>catalog.properties.uri</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Uri of metastore server.</td>
    </tr>
    <tr>
      <td>partition.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Partition keys for each partitioned table. Allow setting multiple primary keys for multiTables. Tables are separated by ';', and partition keys are separated by ','. For example, we can set <code>partition.key</code> of two tables using 'testdb.table1:id1,id2;testdb.table2:name'.</td>
    </tr>
    <tr>
      <td>catalog.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Iceberg catalog options to the pipeline，See <a href="https://iceberg.apache.org/docs/nightly/flink-configuration/#catalog-configuration">Iceberg catalog options</a>. </td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Iceberg table options to the pipeline，See <a href="https://iceberg.apache.org/docs/nightly/configuration/#write-properties">Iceberg table options</a>. </td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* The source table must have a primary key. Tables with no primary key are not supported.

* Exactly-once semantics are not supported. The connector uses at-least-once + the table's primary key for idempotent writing.

Data Type Mapping
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">Iceberg type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>TIMESTAMP_LTZ</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
