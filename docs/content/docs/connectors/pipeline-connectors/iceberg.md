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

### Hadoop Catalog Example

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

### AWS Glue Catalog Example

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
  catalog.properties.type: glue
  catalog.properties.warehouse: s3://my-bucket/warehouse
  catalog.properties.io-impl: org.apache.iceberg.aws.s3.S3FileIO
  catalog.properties.client.region: us-east-1
  catalog.properties.glue.skip-archive: true

pipeline:
  name: MySQL to Iceberg via Glue Pipeline
  parallelism: 2
```

***Note:***
Depending on the catalog type, you may need to add extra JARs manually and pass them with the `--jar` argument of Flink CDC CLI when submitting YAML pipeline jobs.

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Catalog Type</th>
        <th class="text-left">Dependency Item</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>all</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-flink-runtime-1.20">org.apache.iceberg:iceberg-flink-runtime-1.20</a></td>
        <td>Iceberg Flink runtime. Required for all catalog types when not pre-installed in the runtime environment (e.g., standalone Flink clusters).</td>
      </tr>
      <tr>
        <td>hadoop</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.flink/flink-shaded-hadoop-2-uber/2.8.3-10.0">org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0</a></td>
        <td>Provides Hadoop filesystem dependencies.</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws">org.apache.iceberg:iceberg-aws</a></td>
        <td>Provides AWS Glue Catalog and S3 FileIO implementation.</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/software.amazon.awssdk/bundle">software.amazon.awssdk:bundle</a></td>
        <td>AWS SDK bundle required by iceberg-aws.</td>
      </tr>
    </tbody>
</table>
</div>

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
      <td>conditionally required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Metastore type of Iceberg catalog, supports <code>hadoop</code>, <code>hive</code>, and <code>glue</code>. Either this option or <code>catalog.properties.catalog-impl</code> must be set.</td>
    </tr>
    <tr>
      <td>catalog.properties.catalog-impl</td>
      <td>conditionally required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom catalog implementation class, e.g. <code>org.apache.iceberg.aws.glue.GlueCatalog</code>. Either this option or <code>catalog.properties.type</code> must be set.</td>
    </tr>
    <tr>
      <td>catalog.properties.warehouse</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The warehouse root path of the Iceberg catalog, used by all catalog types. For <code>hadoop</code> and <code>hive</code> catalogs, this is typically a local or distributed filesystem path. For <code>glue</code> catalog, this is typically an object storage path like <code>s3://my-bucket/warehouse</code>.</td>
    </tr>
    <tr>
      <td>catalog.properties.uri</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>URI of the metastore server (e.g. Hive Metastore thrift URI).</td>
    </tr>
    <tr>
      <td>catalog.properties.io-impl</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom FileIO implementation class. For AWS S3, use <code>org.apache.iceberg.aws.s3.S3FileIO</code>.</td>
    </tr>
    <tr>
      <td>catalog.properties.client.region</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS region for the Glue catalog client (e.g. <code>us-east-1</code>).</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The Glue catalog ID (AWS account ID). By default, the caller's AWS account ID is used.</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-archive</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to skip archiving older table versions in Glue.</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-name-validation</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to skip name validation for Glue catalog.</td>
    </tr>
    <tr>
      <td>partition.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Partition keys for each partitioned table. Allow setting multiple primary keys for multiTables. Tables are separated by ';', and partition keys are separated by ','. For example, we can set <code>partition.key</code> of two tables using 'testdb.table1:id1,id2;testdb.table2:name'. For partition transforms, we can set <code>partition.key</code> using 'testdb.table1:truncate[10](id);testdb.table2:hour(create_time);testdb.table3:day(create_time);testdb.table4:month(create_time);testdb.table5:year(create_time);testdb.table6:bucket[10](create_time)'.</td>
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
    <tr>
      <td>hadoop.conf.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Hadoop <code>Configuration</code> options used by Iceberg catalog/table operations. The prefix <code>hadoop.conf.</code> will be stripped. For example, <code>hadoop.conf.fs.s3a.endpoint</code>.</td>
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
