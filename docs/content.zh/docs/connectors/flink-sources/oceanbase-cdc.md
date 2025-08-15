---
title: "OceanBase"
weight: 9
type: docs
aliases:
- /connectors/flink-sources/oceanbase-cdc
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

# OceanBase CDC 连接器

OceanBase CDC 连接器允许从 OceanBase 读取快照数据和增量数据。本文介绍了如何设置 OceanBase CDC 连接器以对 OceanBase 进行 SQL 查询。


### OceanBase CDC 方案

名词解释:

- *OceanBase CE*: OceanBase 社区版。OceanBase 的开源版本，兼容 MySQL https://github.com/oceanbase/oceanbase 。
- *OceanBase EE*: OceanBase 企业版。OceanBase 的商业版本，支持 MySQL 和 Oracle 两种兼容模式 https://www.oceanbase.com 。
- *OceanBase Cloud*: OceanBase 云数据库 https://www.oceanbase.com/product/cloud 。
- *Binlog Service CE*: OceanBase Binlog 服务社区版。OceanBase 社区版的一个兼容 MySQL 复制协议的解决方案，详情见[文档](https://www.oceanbase.com/docs/oblogproxy-doc)。
- *Binlog Service EE*: OceanBase Binlog 服务企业版。OceanBase 企业版 MySQL 模式的一个兼容 MySQL 复制协议的解决方案，仅可在阿里云使用，详情见[操作指南](https://www.alibabacloud.com/help/zh/apsaradb-for-oceanbase/latest/binlog-overview)。
- *MySQL Driver*: `mysql-connector-java`，可用于 OceanBase 社区版和 OceanBase 企业版 MySQL 模式。
- *OceanBase Driver*: OceanBase JDBC 驱动，支持所有版本的 MySQL 和 Oracle 兼容模式 https://github.com/oceanbase/obconnector-j 。

OceanBase CDC 源端读取方案：

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">数据库类型</th>
                <th class="text-left">支持的驱动</th>
                <th class="text-left">CDC 连接器</th>
                <th class="text-left">其他用到的组件</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td rowspan="2">OceanBase CE</td>
                <td>
                    MySQL Driver: 8.0.x <br>
                </td>
                <td>OceanBase CDC Connector</td>
                <td>Binlog Service CE</td>
            </tr>
            <tr>
                <td>MySQL Driver: 8.0.x</td>
                <td>MySQL CDC Connector</td>
                <td>Binlog Service CE</td>
            </tr>
            <tr>
                <td rowspan="2">OceanBase EE (MySQL 模式)</td>
                <td>
                    MySQL Driver: 8.0.x <br>
                </td>
                <td>OceanBase CDC Connector</td>
                <td>Binlog Service EE</td>
            </tr>
            <tr>
                <td>MySQL Driver: 8.0.x</td>
                <td>MySQL CDC Connector</td>
                <td>Binlog Service EE</td>
            </tr>
            <tr>
                <td>OceanBase EE (Oracle 模式)</td>
                <td></td>
                <td>暂不支持 OceanBase Oracle 兼容模式下的增量订阅服务，请联系企业技术支持。</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</div>

**注意事项**: oceanbase-cdc 连接器 从 3.5 版本开始进行了以下大的改动：
- 先前基于 OceanBase Log Proxy 服务实现的连接器已被正式移除，当前将仅支持连接到 OceanBase Binlog 服务。
- 当前版本 oceanbase-cdc 将基于 mysql-cdc 连接器实现，主要改进了对 OceanBase Binlog 服务的兼容性，包含一些 Bug 修复，推荐使用。
- 由于 OceanBase Binlog 服务兼容 MySQL 复制协议，仍然支持使用 [MySQL CDC](mysql-cdc.md) 连接器连接到 OceanBase Binlog 服务。
- 暂不支持 OceanBase Oracle 兼容模式下的增量订阅服务，请联系企业技术支持。


依赖
------------

为了使用 OceanBase CDC 连接器，您必须提供相关的依赖信息。以下依赖信息适用于使用自动构建工具（如 Maven 或 SBT）构建的项目和带有 SQL JAR 包的 SQL 客户端。

### Maven dependency

{{< artifact flink-connector-oceanbase-cdc >}}

### SQL Client JAR

```下载链接仅在已发布版本可用，请在文档网站左下角选择浏览已发布的版本。```

下载[flink-sql-connector-oceanbase-cdc-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-oceanbase-cdc/{{< param Version >}}/flink-sql-connector-oceanbase-cdc-{{< param Version >}}.jar)  到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-oceanbase-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oceanbase-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

由于 MySQL Driver 使用的开源协议与 Flink CDC 项目不兼容，我们无法在 jar 包中提供驱动。 您可能需要手动配置以下依赖：

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">依赖名称</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.27">mysql:mysql-connector-java:8.0.27</a></td>
        <td>用于连接到 OceanBase 数据库的 MySQL 租户。</td>
      </tr>
    </tbody>
</table>
</div>

部署 OceanBase 数据库和 Binlog 服务
----------------------

1. 按照 [文档](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000002012734) 部署 OceanBase 数据库。
2. 按照 [文档](https://www.oceanbase.com/docs/common-oblogproxy-doc-1000000003053708) 部署 OceanBase Binlog 服务。


使用文档
----------------

**注意事项**：
- 当前版本 oceanbase-cdc 连接器基于 mysql-cdc 连接器实现，只修改了部分内部实现。外部接口、使用参数等 和 mysql-cdc 基本保持一致，使用文档也可参考[MySQL CDC 使用文档](mysql-cdc.md)。

### 创建 OceanBase CDC 表

OceanBase CDC 表可以定义如下：

```sql
-- 每 3 秒做一次 checkpoint，用于测试，生产配置建议5到10分钟                      
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- 在 Flink SQL中注册 OceanBase 表 'orders'
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'oceanbase-cdc',
     'hostname' = 'localhost',
     'port' = '2881',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'mydb',
     'table-name' = 'orders');
  
-- 从订单表读取全量数据(快照)和增量数据(binlog)
Flink SQL> SELECT * FROM orders;
```

连接器选项
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器, 这里应该是 <code>'oceanbase-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> OceanBase 服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 OceanBase 数据库服务器时要使用的 OceanBase 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 OceanBase 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 OceanBase 服务器的数据库名称。数据库名称还支持正则表达式，以监视多个与正则表达式匹配的表。</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 OceanBase 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。注意：OceanBase CDC 连接器在正则匹配表名时，会把用户填写的 database-name， table-name 通过字符串 `\\.` 连接成一个全路径的正则表达式，然后使用该正则表达式和 OceanBase 数据库中表的全限定名进行正则匹配。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2881</td>
      <td>Integer</td>
      <td> OceanBase 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>server-id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>读取数据使用的 server id，server id 可以是个整数或者一个整数范围，比如 '5400' 或 '5400-5408', 
      建议在 'scan.incremental.snapshot.enabled' 参数为启用时，配置成整数范围。 默认情况下，连接器会在 5400 和 6400 之间生成一个随机数，但是我们建议用户明确指定 Server id。
      </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.enabled</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">true</td>
          <td>Boolean</td>
          <td>增量快照是一种读取表快照的新机制，与旧的快照机制相比，
              增量快照有许多优点，包括：
              （1）在快照读取期间，Source 支持并发读取，
              （2）在快照读取期间，Source 支持进行 chunk 粒度的 checkpoint，
              （3）在快照读取之前，Source 不需要数据库锁权限。
              如果希望 Source 并行运行，则每个并行 Readers 都应该具有唯一的 Server id，所以
              Server id 必须是类似 `5400-6400` 的范围，并且该范围必须大于并行度。
              请查阅 <a href="./mysql-cdc.md#增量快照读取">增量快照读取</a> 章节了解更多详细信息。
          </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。</td>
    </tr>
    <tr>
          <td>scan.snapshot.fetch.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1024</td>
          <td>Integer</td>
          <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>表快照的分片键，在读取表的快照时，被捕获的表会按分片键拆分为多个分片。  
              默认情况下，分片键是主键的第一列。可以使用非主键列作为分片键，但这可能会导致查询性能下降。  
              <br>  
              <b>警告：</b> 使用非主键列作为分片键可能会导致数据不一致。请参阅 <a href="./mysql-cdc.md#警告">增量快照读取</a> 章节了解详细信息。  
          </td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td> OceanBase CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"earliest-offset"，"latest-offset"，"specific-offset"，"timestamp" 和 "snapshot"。
           请查阅 <a href="./mysql-cdc.md#启动模式">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.file</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 binlog 文件名。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.pos</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 binlog 文件位置。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.gtid-set</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 GTID 集合。</td>
    </tr>
    <tr>
      <td>scan.startup.timestamp-millis</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在 "timestamp" 启动模式下，启动位点的毫秒时间戳。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-events</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的事件数量。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的数据行数量。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区， 例如： "Asia/Shanghai". 
          它控制 OceanBase 中的时间戳类型如何转换为字符串。
          更多请参考 <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types"> 这里</a>.
          如果没有设置，则使用ZoneId.systemDefault()来确定服务器时区。
      </td>
    </tr>
    <tr>
      <td>debezium.min.row.
      count.to.stream.result</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>	
       在快照操作期间，连接器将查询每个包含的表，以生成该表中所有行的读取事件。 此参数确定 OceanBase 连接是否将表的所有结果拉入内存（速度很快，但需要大量内存）， 或者结果是否需要流式传输（传输速度可能较慢，但适用于非常大的表）。 该值指定了在连接器对结果进行流式处理之前，表必须包含的最小行数，默认值为1000。将此参数设置为`0`以跳过所有表大小检查，并始终在快照期间对所有结果进行流式处理。</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>连接器在尝试连接到 OceanBase 数据库服务器后超时前应等待的最长时间。该时长不能少于250毫秒。</td>
    </tr>    
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>连接器应重试以建立 OceanBase 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
          <td>connection.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>Integer</td>
          <td>连接池大小。</td>
    </tr>
    <tr>
          <td>jdbc.properties.*</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>String</td>
          <td>传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'.</td>
    </tr>
    <tr>
          <td>heartbeat.interval</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>用于跟踪最新可用 binlog 偏移的发送心跳事件的间隔。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 OceanBase 服务器捕获数据更改。
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-connector-properties"> Debezium 的  MySQL 连接器属性</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。</td>
    </tr>
    <tr>
      <td>scan.parse.online.schema.changes.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否尝试解析由 <a href="https://github.com/github/gh-ost">gh-ost</a> 或 <a href="https://docs.percona.com/percona-toolkit/pt-online-schema-change.html">pt-osc</a> 工具生成的表结构变更事件。
        这些工具会在变更表结构时，将变更语句应用到“影子表”之上，并稍后将其与主表进行交换，以达到表结构变更的目的。
        <br>
        这是一项实验性功能。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        快照读取阶段是否先分配 UnboundedChunk。<br>
        这有助于降低 TaskManager 在快照阶段同步最后一个chunk时遇到内存溢出 (OOM) 的风险。<br> 
        这是一项实验特性，默认为 false。
      </td>
    </tr>
    <tr>
      <td>scan.read-changelog-as-append-only.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否将 changelog 数据流转换为 append-only 数据流。<br>
        仅在需要保存上游表删除消息等特殊场景下开启使用，比如在逻辑删除场景下，用户不允许物理删除下游消息，此时使用该特性，并配合 row_kind 元数据字段，下游可以先保存所有明细数据，再通过 row_kind 字段判断是否进行逻辑删除。<br>
        参数取值如下：<br>
          <li>true：所有类型的消息（包括INSERT、DELETE、UPDATE_BEFORE、UPDATE_AFTER）都会转换成 INSERT 类型的消息。</li>
          <li>false（默认）：所有类型的消息都保持原样下发。</li>
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill 。<br>
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 binlog 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些 binlog 事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的 binlog 事件应进行特殊处理。
    </tr>
    <tr>
      <td>use.legacy.json.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否使用 legacy JSON 格式来转换 Binlog 中的 JSON 类型的数据。 <br>
          这代表着是否使用 legacy JSON 格式来转换 Binlog 中的 JSON 类型的数据。
          如果用户配置 'use.legacy.json.format' = 'true'，则从 Binlog 中转换 JSON 类型的数据时，会移除值之前的空格和逗号之后的空格。例如，
          Binlog 中 JSON 类型的数据 {"key1": "value1", "key2": "value2"} 会被转换为 {"key1":"value1","key2":"value2"}。
          如果设置 'use.legacy.json.format' = 'false'， 这条数据会被转换为 {"key1": "value1", "key2": "value2"}， 也就是 key 和 value 前的空格都会被保留。
      </td>
    </tr>
    </tbody>
</table>
</div>

支持的元数据
----------------

下表中的元数据可以在 DDL 中作为只读（虚拟）meta 列声明。

<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">Key</th>
       <th class="text-left" style="width: 30%">DataType</th>
       <th class="text-left" style="width: 55%">Description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的表名称。</td>
    </tr>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是 binlog 读取记录，该值将始终为0。</td>
    </tr>
    <tr>
      <td>row_kind</td>
      <td>STRING NOT NULL</td>
      <td>当前记录的变更类型。<br>
         注意：如果 Source 算子选择为每条记录输出 row_kind 列，则下游 SQL 操作符在处理回撤时可能会由于此新添加的列而无法比较，导致出现非确定性更新问题。建议仅在简单的同步作业中使用此元数据列。<br>
         '+I' 表示 INSERT 消息，'-D' 表示 DELETE 消息，'-U' 表示 UPDATE_BEFORE 消息，'+U' 表示 UPDATE_AFTER 消息。</td>
    </tr>
  </tbody>
</table>

下述创建表示例展示元数据列的用法：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    operation STRING METADATA FROM 'row_kind' VIRTUAL,
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'oceanbase-cdc',
    'hostname' = 'localhost',
    'port' = '2881',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);
```

下述创建表示例展示使用正则表达式匹配多张库表的用法：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    operation STRING METADATA FROM 'row_kind' VIRTUAL,
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'oceanbase-cdc',
    'hostname' = 'localhost',
    'port' = '2881',
    'username' = 'root',
    'password' = '123456',
    'database-name' = '(^(test).*|^(tpc).*|txc|.*[p$]|t{2})',
    'table-name' = '(t[5-8]|tt)'
);
```
<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">匹配示例</th>
       <th class="text-left" style="width: 30%">表达式</th>
       <th class="text-left" style="width: 55%">描述</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>前缀匹配</td>
      <td>^(test).*</td>
      <td>匹配前缀为test的数据库名或表名，例如test1、test2等。</td>
    </tr>
    <tr>
      <td>后缀匹配</td>
      <td>.*[p$]</td>
      <td>匹配后缀为p的数据库名或表名，例如cdcp、edcp等。</td>
    </tr>
    <tr>
      <td>特定匹配</td>
      <td>txc</td>
      <td>匹配具体的数据库名或表名。</td>
    </tr>
  </tbody>
</table>

进行库表匹配时，会使用正则表达式 `database-name\\.table-name` 来与OceanBase表的全限定名做匹配，所以该例子使用 `(^(test).*|^(tpc).*|txc|.*[p$]|t{2})\\.(t[5-8]|tt)`，可以匹配到表 txc.tt、test2.test5。


数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">OceanBase type<a href="https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000003383166"></a></th>
        <th class="text-left" style="width:10%;">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
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
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED<br>
        TINYINT UNSIGNED ZEROFILL
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED<br>
        SMALLINT UNSIGNED ZEROFILL
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED<br>
        INT UNSIGNED ZEROFILL<br>
        MEDIUMINT UNSIGNED<br>
        MEDIUMINT UNSIGNED ZEROFILL
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
   <tr>
      <td>
        BIGINT UNSIGNED<br>
        BIGINT UNSIGNED ZEROFILL<br>
        SERIAL
      </td>
      <td>DECIMAL(20, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        FLOAT UNSIGNED<br>
        FLOAT UNSIGNED ZEROFILL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL<br>
        REAL UNSIGNED<br>
        REAL UNSIGNED ZEROFILL<br>
        DOUBLE<br>
        DOUBLE UNSIGNED<br>
        DOUBLE UNSIGNED ZEROFILL<br>
        DOUBLE PRECISION<br>
        DOUBLE PRECISION UNSIGNED<br>
        DOUBLE PRECISION UNSIGNED ZEROFILL
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where p <= 38<br>
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where 38 < p <= 65<br>
      </td>
      <td>STRING</td>
      <td>在 OceanBase MySQL 兼容模式中，十进制数据类型的精度高达 65，但在 Flink 中，十进制数据类型的精度仅限于 38。所以，如果定义精度大于 38 的十进制列，则应将其映射到字符串以避免精度损失。</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
        TINYINT(1)<br>
        BIT(1)
        </td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]<br>
        DATETIME [(p)]
      </td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIT(n)
      </td>
      <td>BINARY(⌈n/8⌉)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
      <td>VARBINARY(N)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYTEXT<br>
        TEXT<br>
        MEDIUMTEXT<br>
        LONGTEXT<br>
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYBLOB<br>
        BLOB<br>
        MEDIUMBLOB<br>
        LONGBLOB<br>
      </td>
      <td>BYTES</td>
      <td>目前，对于 BLOB 数据类型，仅支持长度不大于 2147483647（2**31-1）的 blob。 </td>
    </tr>
    <tr>
      <td>
        YEAR
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        ENUM
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        JSON
      </td>
      <td>STRING</td>
      <td> JSON 数据类型将在 Flink 中转换为 JSON 格式的字符串。</td>
    </tr>
    <tr>
      <td>
        SET
      </td>
      <td>ARRAY&lt;STRING&gt;</td>
      <td>因为 SET 数据类型是一个字符串对象，可以有零个或多个值 
          它应该始终映射到字符串数组。
      </td>
    </tr>
    <tr>
      <td>
       GEOMETRY<br>
       POINT<br>
       LINESTRING<br>
       POLYGON<br>
       MULTIPOINT<br>
       MULTILINESTRING<br>
       MULTIPOLYGON<br>
       GEOMETRYCOLLECTION<br>
      </td>
      <td>
        STRING
      </td>
      <td>
      空间数据类型将转换为具有固定 Json 格式的字符串。
      请参考 <a href="#a-name-id-003-a">空间数据类型映射</a> 章节了解更多详细信息。
      </td>
    </tr>
    </tbody>
</table>
</div>

### 空间数据类型映射<a name="空间数据类型映射" id="003"></a>

除`GEOMETRYCOLLECTION`之外的空间数据类型都会转换为 Json 字符串，格式固定，如：<br>
```json
{"srid": 0 , "type": "xxx", "coordinates": [0, 0]}
```
字段`srid`标识定义几何体的 SRS，如果未指定 SRID，则 SRID 0 是新几何体值的默认值。

字段`type`标识空间数据类型，例如`POINT`/`LINESTRING`/`POLYGON`。

字段`coordinates`表示空间数据的`坐标`。

对于`GEOMETRYCOLLECTION`，它将转换为 Json 字符串，格式固定，如：<br>
```json
{"srid": 0 , "type": "GeometryCollection", "geometries": [{"type":"Point","coordinates":[10,10]}]}
```

`Geometrics`字段是一个包含所有空间数据的数组。

不同空间数据类型映射的示例如下：
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Spatial data</th>
        <th class="text-left">Json String converted in Flink</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>POINT(1 1)</td>
        <td>{"coordinates":[1,1],"type":"Point","srid":0}</td>
      </tr>
      <tr>
        <td>LINESTRING(3 0, 3 3, 3 5)</td>
        <td>{"coordinates":[[3,0],[3,3],[3,5]],"type":"LineString","srid":0}</td>
      </tr>
      <tr>
        <td>POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))</td>
        <td>{"coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],"type":"Polygon","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOINT((1 1),(2 2))</td>
        <td>{"coordinates":[[1,1],[2,2]],"type":"MultiPoint","srid":0}</td>
      </tr>
      <tr>
        <td>MultiLineString((1 1,2 2,3 3),(4 4,5 5))</td>
        <td>{"coordinates":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],"type":"MultiLineString","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))</td>
        <td>{"coordinates":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],"type":"MultiPolygon","srid":0}</td>
      </tr>
      <tr>
        <td>GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))</td>
        <td>{"geometries":[{"type":"Point","coordinates":[10,10]},{"type":"Point","coordinates":[30,30]},{"type":"LineString","coordinates":[[15,15],[20,20]]}],"type":"GeometryCollection","srid":0}</td>
      </tr>
    </tbody>
</table>
</div>

{{< top >}}
