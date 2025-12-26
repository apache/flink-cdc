---
title: "ORACLE"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/oracle
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

# Oracle Connector

Oracle CDC Pipeline 连接器允许从 Oracle 数据库读取快照数据和增量数据，并提供端到端的整库数据同步能力。 本文描述了如何设置 Oracle CDC Pipeline 连接器。


## 示例

从 Oracle 读取数据同步到 Doris 的 Pipeline 可以定义如下：

```yaml
source:
   type: oracle
   name: Oracle Source
   hostname: 127.0.0.1
   port: 1521
   username: debezium
   password: password
   database: ORCLDB
   tables: testdb.\.*, testdb.user_table_[0-9]+, [app|web].order_\.*

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: password

pipeline:
   name: Oracle to Doris Pipeline
   parallelism: 4
```

## 连接器配置项

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
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> Oracle 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>required</td>
      <td style="word-wrap: break-word;">1521</td>
      <td>Integer</td>
      <td>Oracle 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 Oracle 数据库服务器时要使用的 Oracle 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 Oracle 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 Oracle 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。<br>
          需要注意的是，点号（.）被视为数据库和表名的分隔符。 如果需要在正则表达式中使用点（.）来匹配任何字符，必须使用反斜杠对点进行转义。<br>
          例如，db0.\.*, db1.user_table_[0-9]+, db[1-2].[app|web]order_\.*</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否发送模式更改事件，下游 sink 可以响应模式变更事件实现表结构同步，默认为true。</td>
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
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td> Oracle CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"latest-offset"。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 Oracle 服务器捕获数据更改。
          例如: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/oracle.html#oracle-connector-properties"> Debezium 的  Oracle 连接器属性</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。<br>
          若 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 默认值变更为 true，可以不用显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = true。</td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td>
        可额外读取的SourceRecord中元数据的列表，后续可直接使用在transform模块，英文逗号 `,` 分割。目前可用值包含：op_ts。
      </td>
    </tr>
    <tr>
      <td>jdbc.url</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> oracle jdbc的url，将优先使用url，如果没有配置url，则使用“jdbc:oracle:sinen:@localhost:1521:orcl”，但oracle 19c url是“jdbc:oracle：thin:@//localhost:1521/pdb1”，因此url属性是选项，以适应不同版本的oracle。</td>
    </tr>
    <tr>
      <td>database</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的Oracle服务器的数据库名称。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区。如果没有设置，则使用ZoneId.systemDefault（）来确定服务器时区。</td>
    </tr>
<tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>
连接池大小。
      </td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30</td>
      <td>Integer</td>
      <td>连接器在尝试连接到oracle数据库服务器后超时前应等待的最长时间。</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>
连接器应重试构建oracle数据库服务器连接的最大重试次数。
      </td>
    </tr>
    <tr>
      <td>heartbeat.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30</td>
      <td>String</td>
      <td>
        发送心跳事件的可选间隔，用于跟踪最新可用的binlog偏移量。
      </td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>
       块元的组大小，如果元大小超过组大小，则元将被分为多个组。
      </td>
    </tr>
    <tr>
      <td>chunk-key.even-distribution.factor.upper-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000.0d</td>
      <td>Double</td>
      <td>
        块密钥分布因子的上限。分布因子用于确定“
桌子是否均匀分布。"
当数据分布均匀时，表块将使用均匀计算优化。”
当查询oracle不均匀时，就会发生拆分。
分布因子可以通过（MAX（id）-MIN（id）+1）/rowCount来计算。
      </td>
    </tr>
    <tr>
      <td>chunk-key.even-distribution.factor.lower-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0.05d</td>
      <td>Double</td>
      <td>
        块密钥分布因子的下限。分布因子用于确定
桌子是否均匀分布。"
当数据分布均匀时，表块将使用均匀计算优化。”
当查询oracle不均匀时，就会发生拆分。
分布因子可以通过（MAX（id）-MIN（id）+1）/rowCount来计算。
      </td>
    </tr>
    <tr>
      <td>debezium.log.mining.strategy</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">online_catalog</td>
      <td>String</td>
      <td>
      日志数据分析或挖掘中的一种策略。  
      </td>
    </tr>
    <tr>
      <td>debezium.log.mining.continuous.mine</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
      原木开采领域中的连续或正在进行的开采过程。
      </td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
      从保存点/检查点还原时是否捕获新添加的表，默认情况下为false。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
      增量快照是一种读取表快照的新机制。“与旧的快照机制相比，增量快照具有许多优点，包括：（1）在快照读取过程中源可以并行，（2）源可以在快照读取期间以块粒度执行检查点，（3）源在快照读取之前不需要获取全局读锁（FLUSH TABLES WITH read lock）。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
      是否在快照读取阶段跳过回填。如果跳过回填，则快照阶段捕获的表上的更改将在稍后的binlog读取阶段被使用，而不是合并到快照中。警告：跳过回填可能会导致数据不一致，因为在快照阶段发生的一些binlog事件可能会被重放（至少只有一次语义承诺）。例如，更新快照中已更新的值，或删除快照中已删除的条目。这些重放的binlog事件应该特别处理。
      </td>
    </tr>
    </tbody>
</table>
</div>

## 启动模式

配置选项`scan.startup.mode`指定 Oracle CDC 使用者的启动模式。有效枚举包括：
- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的归档日志。
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从归档日志的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。

例如，可以在 YAML 配置文件中这样指定启动模式：

```yaml
source:
  type: oracle
  scan.startup.mode: earliest-offset                    # Start from earliest offset
  scan.startup.mode: latest-offset                      # Start from latest offset
  # ...
```

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">Oracle type<a href="https://docs.oracle.com/cd/B13789_01/win.101/b10118/o4o00675.htm"></a></th>
        <th class="text-left" style="width:10%;">CDC type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>NUMBER(p,s)</td>
      <td>DECIMAL(p, s)/BIGINT</td>
      <td>当s 大于 0 时，使用 DECIMAL(p, s)；否则使用 BIGINT。</td>
    </tr>
    <tr>
      <td>
        LONG<br>
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        DATE
      </td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        BINARY_FLOAT<br>
        REAL<br>
      </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
   <tr>
      <td>
        BINARY_DOUBLE<br>
        DOUBLE
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TIMESTAMP(p)
        </td>
      <td>TIMESTAMP [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TIMESTAMP(p) WITH TIME ZONE
      </td>
      <td>TIMESTAMP_TZ [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TIMESTAMP(p) WITH LOCAL TIME ZONE
      </td>
      <td>TIMESTAMP_LTZ [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INTERVAL YEAR(2) TO MONTH<br>
        INTERVAL DAY(3) TO SECOND(2)<br>
        BIGINT
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NCHAR(n)<br>
        CHAR(n)<br>
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)<br>
        VARCHAR2(n)<br>
        NVARCHAR2(n)<br>
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        CLOB<br>
        NLOB<br>
        NCLOB<br>
        SDO_GEOMETRY<br>
        XMLTYPE
      </td>
      <td>STRING</td>
      <td>目前，对于 Oracle 中的 BLOB 数据类型，仅支持长度不大于 2147483647（2**31-1）的 blob。 </td>
    </tr>
    <tr>
      <td>
        BLOB<br>
        RAW<br>
        BFILE
      </td>
      <td>BYTES</td>
      <td> </td>
    </tr>
    <tr>
      <td>
        INTEGER<br>
        SMALLINT<br>
        TINYINT
      </td>
      <td>INT</td>
      <td> </td>
    </tr>
    <tr>
      <td>
        BOOLEAN
      </td>
      <td>BOOLEAN</td>
      <td> </td>
    </tr>
    </tbody>
</table>
</div>
{{< top >}}
