---
title: "OceanBase"
weight: 7
type: docs
aliases:
- /connectors/pipeline-connectors/oceanbase
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

# OceanBase Connector

OceanBase Pipeline 连接器可以用作 Pipeline 的 *Data Sink*，将数据写入[OceanBase](https://github.com/oceanbase/oceanbase)。 本文档介绍如何设置 OceanBase Pipeline 连接器。

## 连接器的功能
* 自动建表
* 表结构变更同步
* 数据实时同步

## 示例

从 MySQL 读取数据同步到 OceanBase 的 Pipeline 可以定义如下：

```yaml
source:
  type: mysql
  hostname: mysql
  port: 3306
  username: mysqluser
  password: mysqlpw
  tables: mysql_2_oceanbase_test_17l13vc.\.*
  server-id: 5400-5404
  server-time-zone: UTC

sink:
  type: oceanbase
  url: jdbc:mysql://oceanbase:2881/test
  username: root@test
  password: password

pipeline:
  name: MySQL to OceanBase Pipeline
  parallelism: 1
```

## 连接器配置项

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th>参数名</th>
                <th>是否必需</th>
                <th>默认值</th>
                <th>类型</th>
                <th>描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
              <td>type</td>
              <td>是</td>
              <td></td>
              <td>String</td>
              <td>指定要使用的连接器, 这里需要设置成 <code>'oceanbase'</code>.</td>
            </tr>
            <tr>
                <td>url</td>
                <td>是</td>
                <td></td>
                <td>String</td>
                <td>数据库的 JDBC url。</td>
            </tr>
            <tr>
                <td>username</td>
                <td>是</td>
                <td></td>
                <td>String</td>
                <td>连接用户名。</td>
            </tr>
            <tr>
                <td>password</td>
                <td>是</td>
                <td></td>
                <td>String</td>
                <td>连接密码。</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>连接的 schema 名或 db 名。</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>表名。</td>
            </tr>
            <tr>
                <td>driver-class-name</td>
                <td>否</td>
                <td>com.mysql.cj.jdbc.Driver</td>
                <td>String</td>
                <td>驱动类名，默认为 'com.mysql.cj.jdbc.Driver'。同时该connector并不包含对应驱动，需手动引入。</td>
            </tr>
            <tr>
                <td>druid-properties</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>Druid 连接池属性，多个值用分号分隔。</td>
            </tr>
            <tr>
                <td>sync-write</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否开启同步写，设置为 true 时将不使用 buffer 直接写入数据库。</td>
            </tr>
            <tr>
                <td>buffer-flush.interval</td>
                <td>否</td>
                <td>1s</td>
                <td>Duration</td>
                <td>缓冲区刷新周期。设置为 '0' 时将关闭定期刷新。</td>
            </tr>
            <tr>
                <td>buffer-flush.buffer-size</td>
                <td>否</td>
                <td>1000</td>
                <td>Integer</td>
                <td>缓冲区大小。</td>
            </tr>
            <tr>
                <td>max-retries</td>
                <td>否</td>
                <td>3</td>
                <td>Integer</td>
                <td>失败重试次数。</td>
            </tr>
            <tr>
                <td>memstore-check.enabled</td>
                <td>否</td>
                <td>true</td>
                <td>Boolean</td>
                <td>是否开启内存检查。</td>
            </tr>
            <tr>
                <td>memstore-check.threshold</td>
                <td>否</td>
                <td>0.9</td>
                <td>Double</td>
                <td>内存使用的阈值相对最大限制值的比例。</td>
            </tr>
            <tr>
                <td>memstore-check.interval</td>
                <td>否</td>
                <td>30s</td>
                <td>Duration</td>
                <td>内存使用检查周期。</td>
            </tr>
            <tr>
                <td>partition.enabled</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否启用分区计算功能，按照分区来写数据。仅当 'sync-write' 和 'direct-load.enabled' 都为 false 时生效。</td>
                </tr>
            <tr>
                <td>direct-load.enabled</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否开启旁路导入。需要注意旁路导入需要将 sink 的并发度设置为1。</td>
            </tr>
            <tr>
                <td>direct-load.host</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>旁路导入使用的域名或 IP 地址，开启旁路导入时为必填项。</td>
            </tr>
            <tr>
                <td>direct-load.port</td>
                <td>否</td>
                <td>2882</td>
                <td>Integer</td>
                <td>旁路导入使用的 RPC 端口，开启旁路导入时为必填项。</td>
            </tr>
            <tr>
                <td>direct-load.parallel</td>
                <td>否</td>
                <td>8</td>
                <td>Integer</td>
                <td>旁路导入任务的并发度。</td>
            </tr>
            <tr>
                <td>direct-load.max-error-rows</td>
                <td>否</td>
                <td>0</td>
                <td>Long</td>
                <td>旁路导入任务最大可容忍的错误行数目。</td>
            </tr>
            <tr>
                <td>direct-load.dup-action</td>
                <td>否</td>
                <td>REPLACE</td>
                <td>STRING</td>
                <td>旁路导入任务中主键重复时的处理策略。可以是 'STOP_ON_DUP'（本次导入失败），'REPLACE'（替换）或 'IGNORE'（忽略）。</td>
            </tr>
            <tr>
                <td>direct-load.timeout</td>
                <td>否</td>
                <td>7d</td>
                <td>Duration</td>
                <td>旁路导入任务的超时时间。</td>
            </tr>
            <tr>
                <td>direct-load.heartbeat-timeout</td>
                <td>否</td>
                <td>30s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳超时时间。</td>
            </tr>
            </tbody>
        </table>
    </div>

## 使用说明

* 暂仅支持OceanBase的MySQL租户

* at-least-once语义保证，暂不支持 exactly-once

* 对于自动建表
  * 没有分区键
  
* 对于表结构变更同步
  * 暂只支持新增列、重命名列
  * 新增列只能添加到最后一列

* 对于数据同步，pipeline 连接器使用 [OceanBase Sink 连接器](https://github.com/oceanbase/flink-connector-oceanbase)
  将数据写入 OceanBase，具体可以参考 [Sink 文档](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase.md)。

## 数据类型映射
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">OceanBase type under MySQL tenant</th>
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
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n) where n <= 256</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n) where n > 256</td>
      <td>VARCHAR(n)</td>
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
