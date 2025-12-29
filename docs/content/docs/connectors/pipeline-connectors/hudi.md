---
title: "Hudi"
weight: 10
type: docs
aliases:
- /connectors/pipeline-connectors/hudi
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

# Hudi Pipeline Connector

The Hudi Pipeline Connector functions as a *Data Sink* for data pipelines, enabling data writes to [Apache Hudi](https://hudi.apache.org) tables. This document explains how to configure the connector.

## What can the connector do?
* Create table automatically if not exist
* Schema change synchronization
* Data synchronization

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Hudi can be defined as follows:

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: 127.0.0.1
  port: 3306
  username: admin
  password: pass
  tables: adb.\.*, bdb.user_table_[0-9]+
  server-id: 5401-5404

sink:
  type: hudi 
  name: Hudi Sink
  path: /path/warehouse
  hoodie.table.type: MERGE_ON_READ

transform:
  - source-table: adb.\.*
    table-options: ordering.fields=ts1
  - source-table: bdb.user_table_[0-9]+
    table-options: ordering.fields=ts2

pipeline:
  name: MySQL to Hudi Pipeline
  parallelism: 4
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
      <td>Specify what connector to use, here should be <code>hudi</code></td>
    </tr>
    <tr>
      <td>path</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify base path for all <code>hudi</code> tables</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink</td>
    </tr>
    <tr>
      <td>hoodie.table.type</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Type of table to write, currently only <code>MERGE_ON_READ</code> is supported</td>
    </tr>
    <tr>
      <td>index.type</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Index type of Flink write job, currently only <code>BUCKET</code> is supported</td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Hudi table options to the pipeline，See <a href="https://hudi.apache.org/docs/configurations/#FLINK_SQL">Hudi table options</a></td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* The source table must have a primary key. Users can use <code>transform</code> rule to override the primary key configuration or set up ordering fields configuration.
* Only MERGE_ON_READ table with simple BUCKET index is supported now.
* Exactly-once semantics are not supported. The connector uses at-least-once + the table's primary key for idempotent writing.

Data Type Mapping
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">Hudi type</th>
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
      <td>TIME</td>
      <td>TIME</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY</td>
      <td>BINARY</td>
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
