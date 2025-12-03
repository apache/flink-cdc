---
title: "Fluss"
weight: 4
type: docs
aliases:
- /connectors/pipeline-connectors/fluss
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

# Fluss Pipeline Connector

The Fluss Pipeline connector can be used as the *Data Sink* of the pipeline, and write data to [Fluss](https://fluss.apache.org). This document describes how to set up the Fluss Pipeline connector.

## What can the connector do?
* Create table automatically if not exist
* Data synchronization

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Fluss can be defined as follows:

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
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123
  # Security-related properties for the Fluss client
  properties.client.security.protocol: sasl
  properties.client.security.sasl.mechanism: PLAIN
  properties.client.security.sasl.username: developer
  properties.client.security.sasl.password: developer-pass

pipeline:
  name: MySQL to Fluss Pipeline
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
      <td>Specify what connector to use, here should be <code>'fluss'</code>. </td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>bootstrap.servers</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The bootstrap servers for the Fluss sink connection. </td>
    </tr>
    <tr>
      <td>bucket.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specific the distribution policy of each Fluss table.Tables are separated by ';', and bucket keys are separated by ','. 
          Format: database1.table1:key1,key2;database1.table2:key3. Data will be distributed to each bucket according to the hash value of bucket-key (It must be a subset of the primary keys excluding partition keys of the primary key table). 
          If the table has a primary key and a bucket key is not specified, the bucket key will be used as primary key(excluding the partition key).
          If the table has no primary key and the bucket key is not specified, the data will be distributed to each bucket randomly. </td>
    </tr>
    <tr>
      <td>bucket.num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The number of buckets of each Fluss table.Tables are separated by ';'.Format: database1.table1:4;database1.table2:8. </td>
    </tr>
    <tr>
      <td>properties.table.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass options of Fluss table to pipeline，See <a href="https://fluss.apache.org/docs/engine-flink/options/#storage-options">Fluss table options</a>. </td>
    </tr>
    <tr>
      <td>properties.client.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass options of Fluss client to pipeline，See <a href="https://fluss.apache.org/docs/engine-flink/options/#write-options">Fluss client options</a>. </td>
    </tr>
    </tbody>
</table>    
</div>

## Usage Notes

* Support Fluss primary key table and log table.

* For creating table automatically
  * There is no partition key
  * The number of buckets is controlled by `bucket.num`
  * The distribution keys are controlled by option `bucket.key`. For primary key table and a bucket key is not specified, the bucket key will be used as primary key(excluding the partition key). For log table has no primary key and the bucket key is not specified, the data will be distributed to each bucket randomly. 

* Not support schema change synchronization.If you want to ignore schema change, use `schema.change.behavior: IGNORE`.

* For data synchronization, the pipeline connector uses [Fluss Java Client](https://fluss.apache.org/docs/apis/java-client/)
  to write data to Fluss.

Data Type Mapping
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Flink CDC type</th>
        <th class="text-left">Fluss type</th>
        <th class="text-left" style="width:60%;">Note</th>
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
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY(n)</td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARBINARY(N)</td>
      <td>BYTES</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
