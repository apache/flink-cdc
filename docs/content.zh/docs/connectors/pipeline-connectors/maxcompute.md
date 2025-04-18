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

MaxCompute Pipeline 连接器可以用作 Pipeline 的 *Data Sink*，将数据写入[MaxCompute](https://www.aliyun.com/product/odps)。
本文档介绍如何设置 MaxCompute Pipeline 连接器。

## 连接器的功能

* 自动建表
* 表结构变更同步
* 数据实时同步

## 示例

从 MySQL 读取数据同步到 MaxCompute 的 Pipeline 可以定义如下：

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

## 连接器配置项

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
      <td>指定要使用的连接器, 这里需要设置成 <code>'maxcompute'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称.</td>
    </tr>
    <tr>
      <td>access-id</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>阿里云账号或RAM用户的AccessKey ID。您可以进入<a href="https://ram.console.aliyun.com/manage/ak">
            AccessKey管理页面</a> 获取AccessKey ID。</td>
    </tr>
    <tr>
      <td>access-key</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>AccessKey ID对应的AccessKey Secret。您可以进入<a href="https://ram.console.aliyun.com/manage/ak">
            AccessKey管理页面</a> 获取AccessKey Secret。</td>
    </tr>
    <tr>
      <td>endpoint</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MaxCompute服务的连接地址。您需要根据创建MaxCompute项目时选择的地域以及网络连接方式配置Endpoint。各地域及网络对应的Endpoint值，请参见<a href="https://help.aliyun.com/zh/maxcompute/user-guide/endpoints">
           Endpoint</a>。</td>
    </tr>
    <tr>
      <td>project</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MaxCompute项目名称。您可以登录<a href="https://maxcompute.console.aliyun.com/">
           MaxCompute控制台</a>，在 工作区 > 项目管理 页面获取MaxCompute项目名称。</td>
    </tr>
    <tr>
      <td>tunnel.endpoint</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MaxCompute Tunnel服务的连接地址，通常这项配置可以根据指定的project所在的region进行自动路由。仅在使用代理等特殊网络环境下使用该配置。</td>
    </tr>
    <tr>
      <td>quota.name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MaxCompute 数据传输使用的独享资源组名称，如不指定该配置，则使用共享资源组。详情可以参考<a href="https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts">
           使用 Maxcompute 独享资源组</a></td>
    </tr>
    <tr>
      <td>sts-token</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>当使用RAM角色颁发的短时有效的访问令牌（STS Token）进行鉴权时，需要指定该参数。</td>
    </tr>
    <tr>
      <td>buckets-num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>自动创建 MaxCompute Delta 表时使用的桶数。使用方式可以参考 <a href="https://help.aliyun.com/zh/maxcompute/user-guide/transaction-table2-0-overview">
           Delta Table 概述</a></td>
    </tr>
    <tr>
      <td>compress.algorithm</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">zlib</td>
      <td>String</td>
      <td>写入MaxCompute时使用的数据压缩算法，当前支持<code>raw</code>（不进行压缩），<code>zlib</code>, <code>lz4</code>和<code>snappy</code>。</td>
    </tr>
    <tr>
      <td>total.buffer-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">64MB</td>
      <td>String</td>
      <td>内存中缓冲的数据量大小，单位为分区级（非分区表单位为表级），不同分区（表）的缓冲区相互独立，达到阈值后数据写入到MaxCompute。</td>
    </tr>
    <tr>
      <td>bucket.buffer-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">4MB</td>
      <td>String</td>
      <td>内存中缓冲的数据量大小，单位为桶级，仅写入 Delta 表时生效。不同数据桶的缓冲区相互独立，达到阈值后将该桶数据写入到MaxCompute。</td>
    </tr>
    <tr>
      <td>commit.thread-num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>checkpoint阶段，能够同时处理的分区（表）数量。</td>
    </tr>
    <tr>
      <td>flush.concurrent-num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">4</td>
      <td>Integer</td>
      <td>写入数据到MaxCompute时，能够同时写入的桶数量。仅写入 Delta 表时生效。</td>
    </tr>
    </tbody>
</table>    
</div>

## 使用说明

* 链接器 支持自动建表，将MaxCompute表与源表的位置关系、数据类型进行自动映射（参见下文映射表），当源表有主键时，自动创建
  MaxCompute Delta 表，否则创建普通 MaxCompute 表（Append表）
* 当写入普通 MaxCompute 表（Append表）时，会忽略`delete`操作，`update`操作会被视为`insert`操作
* 目前仅支持 at-least-once，Delta 表由于主键特性能够实现幂等写
* 对于表结构变更同步
    * 新增列只能添加到最后一列
    * 修改列类型，只能修改为兼容的类型。兼容表可以参考[ALTER TABLE](https://help.aliyun.com/zh/maxcompute/user-guide/alter-table)

## 表位置映射

链接器自动建表时，使用如下映射关系，将源表的位置信息映射到MaxCompute表的位置。注意，当MaxCompute项目不支持Schema模型时，每个同步任务仅能同步一个Mysql
Database。（其他Datasource同理，链接器会忽略TableId.namespace信息）

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Flink CDC 中抽象</th>
        <th class="text-left">MaxCompute 位置</th>
        <th class="text-left" style="width:60%;">Mysql 位置</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>配置文件中project</td>
      <td>project</td>
      <td>(none)</td>
    </tr>
    <tr>
      <td>TableId.namespace</td>
      <td>schema（仅当MaxCompute项目支持Schema模型时，如不支持，将忽略该配置）</td>
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

## 数据类型映射

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
