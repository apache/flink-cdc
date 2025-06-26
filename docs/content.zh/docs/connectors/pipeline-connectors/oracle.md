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
   password: dbz
   database: ORCLDB
   tables: adb.\.*, bdb.user_table_[0-9]+, [app|web].order_\.*

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

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
      <td>optional</td>
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
      <td>TIMESTAMP [(6)]</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        BINARY_FLOAT<br>
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
        INTERVAL DAY(3) TO SECOND(2)
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)<br>
        VARCHAR2(n)<br>
        NVARCHAR2(n)<br>
        NCHAR(n)<br>
        CHAR(n)<br>
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        CLOB<br>
        BLOB<br>
        TEXT<br>
        NCLOB<br>
        SDO_GEOMETRY<br>
        XMLTYPE
      </td>
      <td>STRING</td>
      <td>目前，对于 Oracle 中的 BLOB 数据类型，仅支持长度不大于 2147483647（2**31-1）的 blob。 </td>
    </tr>
    </tbody>
</table>
</div>
{{< top >}}
