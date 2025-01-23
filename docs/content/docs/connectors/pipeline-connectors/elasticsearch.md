---
title: "Elasticsearch"
weight: 7
type: docs
aliases:
- /connectors/pipeline-connectors/elasticsearch
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

# Elasticsearch Pipeline Connector

The Elasticsearch Pipeline connector can be used as the *Data Sink* of the pipeline, and write data to Elasticsearch. This document describes how to set up the Elasticsearch Pipeline connector.


How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Elasticsearch can be defined as follows:

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
  type: elasticsearch
  name: Elasticsearch Sink
  hosts: http://127.0.0.1:9092,http://127.0.0.1:9093
  
route:
  - source-table: adb.\.*
    sink-table: default_index
    description: sync adb.\.* table to default_index

pipeline:
  name: MySQL to Elasticsearch Pipeline
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
      <td>Specify what connector to use, here should be <code>'elasticsearch'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>hosts</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>One or more Elasticsearch hosts to connect to, e.g. 'http://host_name:9092,http://host_name:9093'.</td>
    </tr>
    <tr>
      <td>version</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">7</td>
      <td>Integer</td>
      <td>Specify what connector to use, valid values are:
      <ul>
        <li>6: connect to Elasticsearch 6.x cluster.</li>
        <li>7: connect to Elasticsearch 7.x cluster.</li>
        <li>8: connect to Elasticsearch 8.x cluster.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td>username</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The username for Elasticsearch authentication.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The password for Elasticsearch authentication.</td>
    </tr>
    <tr>
      <td>batch.size.max</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Integer</td>
      <td>Maximum number of buffered actions per bulk request. Can be set to '0' to disable it.</td>
    </tr>
    <tr>
      <td>inflight.requests.max</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5</td>
      <td>Integer</td>
      <td>The maximum number of concurrent requests that the sink will try to execute.</td>
    </tr>
    <tr>
      <td>buffered.requests.max</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>The maximum number of requests to keep in the in-memory buffer.</td>
    </tr>
    <tr>
      <td>batch.size.max.bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5242880</td>
      <td>Long</td>
      <td>The maximum size of batch requests in bytes.</td>
    </tr>
    <tr>
      <td>buffer.time.max.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum time to wait for incomplete batches before flushing.</td>
    </tr>
    <tr>
      <td>record.size.max.bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10485760</td>
      <td>Long</td>
      <td>The maximum size of a single record in bytes.</td>
    </tr>
    <tr>
      <td>sharding.suffix.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Sharding suffix key for each table, allow setting sharding suffix key for multiTables.Default sink table name is test_table${suffix_key}.Default sharding column is first partition column.Tables are separated by ';'.Table and column are separated by ':'.For example, we can set sharding.suffix.key by 'table1:col1;table2:col2'.</td>
    </tr>
    <tr>
      <td>sharding.suffix.separator</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">"_"</td>
      <td>String</td>
      <td>Separator for sharding suffix in table names, allow defining the separator between table name and sharding suffix. Default value is '_'. For example, if set to '-', the default table name would be test_table-${suffix}</td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* The written index of Elasticsearch will be `namespace.schemaName.tableName` string of TableId，this can be changed using route function of pipeline.

* No support for automatic Elasticsearch index creation.

Data Type Mapping
----------------
Elasticsearch stores document in a JSON string. So the data type mapping is between Flink CDC data type and JSON data type.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">JSON type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>STRING</td>
      <td>with format: date (yyyy-MM-dd), example: 2024-10-21</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>STRING</td>
      <td>with format: date-time (yyyy-MM-dd HH:mm:ss.SSSSSS, with UTC time zone), example: 2024-10-21 14:10:56.000000</td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>STRING</td>
      <td>with format: date-time (yyyy-MM-dd HH:mm:ss.SSSSSS, with UTC time zone), example: 2024-10-21 14:10:56.000000</td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>ARRAY</td>
      <td></td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}