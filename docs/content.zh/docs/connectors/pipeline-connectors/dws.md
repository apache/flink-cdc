---
title: "GaussDB DWS"
weight: 6
type: docs
aliases:
- /connectors/pipeline-connectors/dws
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

# GaussDB DWS Connector

GaussDB DWS 连接器是一个 Pipeline Sink 连接器，用于将 Flink CDC 事件写入华为云 GaussDB(DWS)。本文描述如何配置 GaussDB DWS Pipeline Sink 连接器。

该 sink 使用 SinkV2 staging table 实现应用层两阶段提交：数据先写入 checkpoint 级 staging table，再由 committer 提交到目标表。数据变更事件要求目标表具备主键，以便 staging 数据可以按主键幂等归并。

## 示例

下面的示例展示了如何将 Values source 的数据写入 GaussDB DWS：

```yaml
source:
   type: values
   name: Values Source

sink:
   type: dws
   name: GaussDB DWS Sink
   jdbc-url: jdbc:gaussdb://127.0.0.1:8000/postgres
   username: gaussdb
   password: password
   schema: public
   sink.enable-delete: true

pipeline:
   name: Values to GaussDB DWS Pipeline
   parallelism: 1
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
      <td>type</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定使用的 sink。对于 GaussDB DWS，设置为 <code>dws</code>。</td>
    </tr>
    <tr>
      <td>jdbc-url</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>GaussDB DWS JDBC URL。目标数据库必须在该 URL 中指定。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 GaussDB DWS 数据库时使用的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 GaussDB DWS 数据库时使用的密码。</td>
    </tr>
    <tr>
      <td>schema</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">public</td>
      <td>String</td>
      <td>当输入表标识不包含 schema 时使用的默认目标 schema。</td>
    </tr>
    <tr>
      <td>sink-table</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>传递给原生 DWS client 的目标表名覆盖配置。</td>
    </tr>
    <tr>
      <td>driver</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">com.huawei.gauss200.jdbc.Driver</td>
      <td>String</td>
      <td>GaussDB DWS JDBC driver class。</td>
    </tr>
    <tr>
      <td>case-sensitive</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>目标标识符是否保留大小写。关闭后，标识符会规范化为小写。</td>
    </tr>
    <tr>
      <td>write-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>兼容性配置项。SinkV2 staging table 写入器按主键归并提交，不使用原生 DWS 写入模式。</td>
    </tr>
    <tr>
      <td>sink.enable-delete</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否将 DELETE 事件转发到 sink。</td>
    </tr>
    <tr>
      <td>sink.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>原生 DWS client 重试次数的兼容性配置项。SinkV2 commit 重试由 Flink committer 重试机制处理。</td>
    </tr>
    <tr>
      <td>sink.parallelism</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>自定义 sink 并行度。未设置时由 planner 自动推导。</td>
    </tr>
    <tr>
      <td>enable-auto-flush</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>兼容性配置项。SinkV2 staging table 写入器在 checkpoint commit 时 flush staged records。</td>
    </tr>
    <tr>
      <td>auto-batch-flush-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>原生 DWS client 自动 flush 阈值的兼容性配置项。</td>
    </tr>
    <tr>
      <td>auto-flush-max-interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3 min</td>
      <td>Duration</td>
      <td>原生 DWS client 自动 flush 间隔的兼容性配置项。</td>
    </tr>
    <tr>
      <td>enable-dn-partition</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否启用华为 DN 分区。</td>
    </tr>
    <tr>
      <td>distribution-key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>DN 分区使用的分布键，多个字段用英文逗号分隔。</td>
    </tr>
    <tr>
      <td>local-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">systemDefault</td>
      <td>String</td>
      <td>时间戳转换使用的会话时区。</td>
    </tr>
    <tr>
      <td>connectionSize</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>DWS client 使用的连接数。</td>
    </tr>
    <tr>
      <td>connectionMaxUseTimeSeconds</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3600</td>
      <td>Integer</td>
      <td>单个连接最大生命周期，单位为秒。</td>
    </tr>
    <tr>
      <td>connectionMaxIdleMs</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">60000</td>
      <td>Integer</td>
      <td>连接最大空闲时间，单位为毫秒。</td>
    </tr>
    <tr>
      <td>connectionTimeOut</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>连接超时时间，单位为毫秒。</td>
    </tr>
    <tr>
      <td>connectionPoolName</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>String</td>
      <td>连接池名称。</td>
    </tr>
    <tr>
      <td>connectionPoolSize</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Integer</td>
      <td>连接池大小。</td>
    </tr>
    <tr>
      <td>connectionPoolTimeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>连接池获取连接超时时间，单位为毫秒。</td>
    </tr>
    <tr>
      <td>connectionSocketTimeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Integer</td>
      <td>Socket 超时时间，单位为毫秒。</td>
    </tr>
    <tr>
      <td>connectionMaxUseCount</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>单个连接最大使用次数。</td>
    </tr>
    <tr>
      <td>needConnectionPoolMonitor</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Boolean</td>
      <td>是否启用连接池监控。</td>
    </tr>
    <tr>
      <td>connectionPoolMonitorPeriod</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>连接池监控输出间隔，单位为毫秒。</td>
    </tr>
    <tr>
      <td>dws.client.write.thread-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>原生 DWS client 写入线程数的兼容性配置项。</td>
    </tr>
    <tr>
      <td>dws.client.write.use-copy-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>原生 DWS client COPY 模式切换阈值的兼容性配置项。</td>
    </tr>
    <tr>
      <td>dws.client.write.force-flush-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">40000</td>
      <td>Integer</td>
      <td>原生 DWS client 强制 flush 阈值的兼容性配置项。</td>
    </tr>
    <tr>
      <td>logSwitch</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否启用 DWS client 日志。</td>
    </tr>
    </tbody>
</table>
</div>

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">Flink CDC Type</th>
        <th class="text-left" style="width:30%;">GaussDB DWS Type</th>
        <th class="text-left" style="width:40%;">说明</th>
      </tr>
    </thead>
    <tbody>
    <tr><td>BOOLEAN</td><td>BOOLEAN</td><td></td></tr>
    <tr><td>TINYINT, SMALLINT</td><td>SMALLINT</td><td></td></tr>
    <tr><td>INTEGER</td><td>INTEGER</td><td></td></tr>
    <tr><td>BIGINT</td><td>BIGINT</td><td></td></tr>
    <tr><td>FLOAT</td><td>REAL</td><td></td></tr>
    <tr><td>DOUBLE</td><td>DOUBLE PRECISION</td><td></td></tr>
    <tr><td>DECIMAL</td><td>DECIMAL(p, s)</td><td>使用来源字段的精度和小数位。</td></tr>
    <tr><td>CHAR</td><td>CHAR(n)</td><td></td></tr>
    <tr><td>VARCHAR</td><td>VARCHAR(n) 或 TEXT</td><td>超大 VARCHAR 字段会映射为 TEXT。</td></tr>
    <tr><td>BINARY, VARBINARY</td><td>BYTEA</td><td></td></tr>
    <tr><td>DATE</td><td>DATE</td><td></td></tr>
    <tr><td>TIME</td><td>TIME(p)</td><td>精度最大为 6。</td></tr>
    <tr><td>TIMESTAMP</td><td>TIMESTAMP(p)</td><td>精度最大为 6。</td></tr>
    <tr><td>TIMESTAMP_LTZ, TIMESTAMP_TZ</td><td>TIMESTAMPTZ(p)</td><td>精度最大为 6。</td></tr>
    <tr><td>ARRAY</td><td>TEXT</td><td></td></tr>
    <tr><td>MAP, ROW</td><td>JSON</td><td></td></tr>
    </tbody>
</table>
</div>

{{< top >}}
