---
title: "MaxCompute"
weight: 7
type: docs
aliases:
  - /connectors/maxcompute
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

# MaxCompute Connector

MaxCompute connector can be used as the *Data Sink* of the pipeline, and write data
to [MaxCompute](https://www.aliyun.com/product/odps). This document describes how to set up the MaxCompute connector.

## What can the connector do?

* Create table automatically if not exist
* Schema change synchronization
* Data synchronization

## Example

The pipeline for reading data from MySQL and sink to MaxCompute can be defined as follows:

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
  type: maxcompute
  name: MaxCompute Sink
  access-id: ak
  access-key: sk
  endpoint: endpoint
  project: flink_cdc
  buckets-num: 8

pipeline:
  name: MySQL to MaxCompute Pipeline
  parallelism: 2
```

## Connector Options

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
      <td>Specify what connector to use, here should be <code>'maxcompute'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>access-id</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>AccessKey ID of Alibaba Cloud account or RAM user. You can enter <a href="https://ram.console.aliyun.com/manage/ak">
            AccessKey management page</a> Obtain AccessKey ID.</td>
    </tr>
    <tr>
      <td>access-key</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>AccessKey Secret corresponding to AccessKey ID. You can enter <a href="https://ram.console.aliyun.com/manage/ak">
            AccessKey management page</a> Obtain AccessKey Secret.</td>
    </tr>
    <tr>
      <td>endpoint</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The connection address for the MaxCompute service. You need to configure the Endpoint based on the region selected when creating the MaxCompute project and the network connection method. For values corresponding to each region and network, please refer to <a href="https://help.aliyun.com/zh/maxcompute/user-guide/endpoints">Endpoint</a>.</td>
    </tr>
    <tr>
      <td>project</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the MaxCompute project. You can log in to the <a href="https://maxcompute.console.aliyun.com/">MaxCompute console</a> and obtain the MaxCompute project name on the Workspace > Project Management page.</td>
    </tr>
    <tr>
      <td>tunnel.endpoint</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The connection address for the MaxCompute Tunnel service. Typically, this configuration can be auto-routed based on the region where the specified project is located. It is used only in special network environments such as when using a proxy.</td>
    </tr>
    <tr>
      <td>quota.name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the exclusive resource group for MaxCompute data transfer. If not specified, the shared resource group is used. For details, refer to <a href="https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts">Using exclusive resource groups for Maxcompute</a></td>
    </tr>
    <tr>
      <td>sts-token</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>When using a temporary access token (STS Token) issued by a RAM role for authentication, this parameter must be specified.</td>
    </tr>
    <tr>
      <td>buckets-num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>The number of buckets used when auto-creating MaxCompute Delta tables. For usage, refer to <a href="https://help.aliyun.com/zh/maxcompute/user-guide/transaction-table2-0-overview">Delta Table Overview</a></td>
    </tr>
    <tr>
      <td>compress.algorithm</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">zlib</td>
      <td>String</td>
      <td>The data compression algorithm used when writing to MaxCompute. Currently supports <code>raw</code> (no compression), <code>zlib</code>, <code>lz4</code>, and <code>snappy</code>.</td>
    </tr>
    <tr>
      <td>total.buffer-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">64MB</td>
      <td>String</td>
      <td>The size of the data buffer in memory, by partition level (for non-partitioned tables, by table level). Buffers for different partitions (tables) are independent, and data is written to MaxCompute when the threshold is reached.</td>
    </tr>
    <tr>
      <td>bucket.buffer-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">4MB</td>
      <td>String</td>
      <td>The size of the data buffer in memory, by bucket level. This is effective only when writing to Delta tables. Buffers for different data buckets are independent, and the bucket data is written to MaxCompute when the threshold is reached.</td>
    </tr>
    <tr>
      <td>commit.thread-num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>The number of partitions (tables) that can be processed simultaneously during the checkpoint stage.</td>
    </tr>
    <tr>
      <td>flush.concurrent-num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">4</td>
      <td>Integer</td>
      <td>The number of buckets that can be written to MaxCompute simultaneously. This is effective only when writing to Delta tables.</td>
    </tr>
    </tbody>
</table>    
</div>

## Usage Instructions

* The connector supports automatic table creation, automatically mapping the location relationship and data types between MaxCompute tables and source tables (see the mapping table below). When the source table has a primary key, a MaxCompute Delta table is automatically created; otherwise, a regular MaxCompute table (Append table) is created.
* When writing to a regular MaxCompute table (Append table), the delete operation will be ignored, and the update operation will be treated as an insert operation.
* Currently, only at-least-once is supported. Delta tables can achieve idempotent writes due to their primary key characteristics.
* For synchronization of table structure changes:
    * A new column can only be added as the last column.
    * Modifying a column type can only be changed to a compatible type. For compatible types, refer to[ALTER TABLE](https://help.aliyun.com/zh/maxcompute/user-guide/alter-table)

## Table Location Mapping
When the connector automatically creates tables, it uses the following mapping relationship to map the location information of the source tables to the location of the MaxCompute tables. Note that when the MaxCompute project does not support the Schema model, each synchronization task can only synchronize one MySQL Database. (The same applies to other DataSources, the connector will ignore the TableId.namespace information)

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Abstract in Flink CDC	</th>
        <th class="text-left">MaxCompute Location</th>
        <th class="text-left" style="width:60%;">MySQL Location</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>project in the configuration file</td>
      <td>project</td>
      <td>(none)</td>
    </tr>
    <tr>
      <td>TableId.namespace</td>
      <td>schema (Only when the MaxCompute project supports the Schema model. If not supported, this configuration will be ignored)</td>
      <td>database</td>
    </tr>
    <tr>
      <td>TableId.tableName</td>
      <td>table</td>
      <td>table</td>
    </tr>
    </tbody>
</table>
</div>

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Flink Type</th>
        <th class="text-left">MaxCompute Type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR/VARCHAR</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>BINARY/VARBINARY</td>
      <td>BINARY</td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>INTEGER</td>
      <td>INTEGER</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>TIME_WITHOUT_TIME_ZONE</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIMESTAMP_WITHOUT_TIME_ZONE</td>
      <td>TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP_WITH_LOCAL_TIME_ZONE</td>
      <td>TIMESTAMP</td>
    </tr>
    <tr>
      <td>TIMESTAMP_WITH_TIME_ZONE</td>
      <td>TIMESTAMP</td>
    </tr>
    <tr>
    <tr>
      <td>ARRAY</td>
      <td>ARRAY</td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>MAP</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>STRUCT</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
