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

# Iceberg Pipeline 连接器

Iceberg Pipeline 连接器作为数据管道的 *Data Sink*，支持将数据写入 [Apache Iceberg](https://iceberg.apache.org) 表。本文档介绍如何配置该连接器。

## 核心能力
* **自动建表：**
当 Iceberg 表不存在时，自动动态创建
* **Schema 同步：**
将上游数据源的 Schema 变更（例如新增列）自动同步到 Iceberg 表
* **数据同步：**
支持批处理和流式数据同步

Pipeline 创建方式
----------------

以下示例展示了如何定义一个从 MySQL 读取数据并写入 Iceberg 的数据管道：

### Hadoop Catalog 示例

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

### AWS Glue Catalog 示例

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

***注意：***
根据所使用的 Catalog 类型，可能需要手动添加额外的 JAR 依赖，并在使用 Flink CDC CLI 提交 YAML 管道作业时通过 `--jar` 参数传入。

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Catalog 类型</th>
        <th class="text-left">依赖项</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>所有类型</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-flink-runtime-1.20">org.apache.iceberg:iceberg-flink-runtime-1.20</a></td>
        <td>Iceberg Flink 运行时依赖。当运行环境中未预装 Iceberg 时（例如独立部署的 Flink 集群），所有 Catalog 类型均需要此依赖。</td>
      </tr>
      <tr>
        <td>hadoop</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.flink/flink-shaded-hadoop-2-uber/2.8.3-10.0">org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0</a></td>
        <td>提供 Hadoop 文件系统相关依赖。</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws">org.apache.iceberg:iceberg-aws</a></td>
        <td>提供 AWS Glue Catalog 和 S3 FileIO 实现。</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/software.amazon.awssdk/bundle">software.amazon.awssdk:bundle</a></td>
        <td>iceberg-aws 所依赖的 AWS SDK。</td>
      </tr>
    </tbody>
</table>
</div>

Pipeline 连接器选项
----------------
<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">选项</th>
        <th class="text-left" style="width: 8%">是否必填</th>
        <th class="text-left" style="width: 7%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>type</td>
      <td>必填</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>指定使用的连接器类型，此处应为 <code>iceberg</code>。</td>
    </tr>
    <tr>
      <td>name</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Sink 的名称。</td>
    </tr>
    <tr>
      <td>catalog.properties.type</td>
      <td>条件必填</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Iceberg Catalog 的元数据存储类型，支持 <code>hadoop</code>、<code>hive</code> 和 <code>glue</code>。此选项与 <code>catalog.properties.catalog-impl</code> 必须设置其一。</td>
    </tr>
    <tr>
      <td>catalog.properties.catalog-impl</td>
      <td>条件必填</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>自定义 Catalog 实现类，例如 <code>org.apache.iceberg.aws.glue.GlueCatalog</code>。此选项与 <code>catalog.properties.type</code> 必须设置其一。</td>
    </tr>
    <tr>
      <td>catalog.properties.warehouse</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Iceberg Catalog 的仓库根路径，适用于所有 Catalog 类型。对于 <code>hadoop</code> 和 <code>hive</code> Catalog，通常为本地或分布式文件系统路径；对于 <code>glue</code> Catalog，通常为对象存储路径，例如 <code>s3://my-bucket/warehouse</code>。</td>
    </tr>
    <tr>
      <td>catalog.properties.uri</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>元数据服务 URI（例如 Hive Metastore 的 thrift URI）。</td>
    </tr>
    <tr>
      <td>catalog.properties.io-impl</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>自定义 FileIO 实现类。使用 AWS S3 时，请设置为 <code>org.apache.iceberg.aws.s3.S3FileIO</code>。</td>
    </tr>
    <tr>
      <td>catalog.properties.client.region</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Glue Catalog 客户端的 AWS 区域（例如 <code>us-east-1</code>）。</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.id</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Glue Catalog ID（即 AWS 账户 ID）。默认使用调用者的 AWS 账户 ID。</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-archive</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否跳过在 Glue 中归档旧版本的表元数据。</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-name-validation</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否跳过 Glue Catalog 的名称校验。</td>
    </tr>
    <tr>
      <td>partition.key</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>各分区表的分区键。支持为多张表设置不同的分区键，表之间以 <code>;</code> 分隔，分区键之间以 <code>,</code> 分隔。例如，可以通过 <code>testdb.table1:id1,id2;testdb.table2:name</code> 为两张表设置分区键。对于分区转换，可以使用以下语法：<code>testdb.table1:truncate[10](id);testdb.table2:hour(create_time);testdb.table3:day(create_time);testdb.table4:month(create_time);testdb.table5:year(create_time);testdb.table6:bucket[10](create_time)</code>。</td>
    </tr>
    <tr>
      <td>catalog.properties.*</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>透传 Iceberg Catalog 选项到管道，详见 <a href="https://iceberg.apache.org/docs/nightly/flink-configuration/#catalog-configuration">Iceberg Catalog 配置</a>。</td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>透传 Iceberg 表选项到管道，详见 <a href="https://iceberg.apache.org/docs/nightly/configuration/#write-properties">Iceberg 表配置</a>。</td>
    </tr>
    </tbody>
</table>    
</div>

使用须知
--------

* 源表必须包含主键，不支持无主键的表。

* 不支持精确一次（Exactly-Once）语义。连接器采用至少一次（At-Least-Once）+ 主键幂等写入的方式保证数据一致性。

数据类型映射
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC 类型</th>
        <th class="text-left">Iceberg 类型</th>
        <th class="text-left" style="width:60%;">说明</th>
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
