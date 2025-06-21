---
title: "Doris"
weight: 5
type: docs
aliases:
- /connectors/pipeline-connectors/doris
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

# Doris Connector

本文介绍了Pipeline Doris Connector的使用。

## 示例


```yaml
source:
   type: values
   name: ValuesSource

sink:
   type: doris
   name: Doris Sink
   fenodes: 127.0.0.1:8030
   username: root
   password: ""
   table.create.properties.replication_num: 1

pipeline:
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
      <td>指定要使用的Sink, 这里是 <code>'doris'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> PipeLine的名称 </td>
    </tr>
     <tr>
      <td>fenodes</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群FE的Http地址, 比如 127.0.0.1:8030 </td>
    </tr>
     <tr>
      <td>benodes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群BE的Http地址, 比如 127.0.0.1:8040 </td>
    </tr>
    <tr>
      <td>jdbc-url</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群的JDBC地址，比如：jdbc:mysql://127.0.0.1:9030/db</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群的用户名</td>
    </tr> 
    <tr>
      <td>password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群的密码</td>
    </tr>
    <tr>
      <td>auto-redirect</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td> 是否通过FE重定向写入，直连BE写入 </td>
    </tr>
    <tr>
      <td>charset-encoding</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td> Doris Http客户端字符集编码，默认UTF-8 </td>
    </tr>
    <tr>
      <td>sink.enable.batch-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td> 是否使用攒批方式写入Doris </td>
    </tr>
    <tr>
       <td>sink.enable-delete</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td>是否启用删除 </td>
     </tr>
     <tr>
       <td>sink.max-retries</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">3</td>
       <td>Integer</td>
       <td>写入失败后最大重试次数</td>
     </tr>
    <tr>
      <td>sink.flush.queue-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2</td>
      <td>Integer</td>
      <td> 攒批写入的队列大小
      </td>
    </tr>
    <tr>
      <td>sink.buffer-flush.max-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>单个批次最大Flush的记录数</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.max-bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10485760(10MB)</td>
      <td>Integer</td>
      <td>单个批次最大Flush的字节数</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10s</td>
      <td>String</td>
      <td>Flush的间隔时长，超过这个时间，将异步Flush数据</td>
    </tr>
    <tr>
       <td>sink.ignore.update-before</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td>在CDC场景中，当上游的主键与下游的主键不一致时，需要将 update-before 数据作为已删除数据传递给下游，否则数据将无法被删除。默认设置为忽略，即执行 upsert 语义 </td>
     </tr>
    <tr>
      <td>sink.properties.</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>StreamLoad的参数。
        For example: <code> sink.properties.strict_mode: true</code>.
        查看更多关于 <a href="https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/stream-load-manual"> StreamLoad 的属性</a></td> 
      </td>
    </tr>
    <tr>
      <td>table.create.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>创建表的Properties配置。
        For example: <code> table.create.properties.replication_num: 1</code>.
        查看更多关于 <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE"> Doris Table 的属性</a></td> 
      </td>
    </tr>
    <tr>
      <td>table.create.auto-partition.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>创建自动分区表的配置。<br/>
        当前仅支持DATE/DATETIME类型列的AUTO RANGE PARTITION，分区函数为<code>date_trunc</code>，且Doris版本必须大于2.1.6，查看更多关于 <a href="https://doris.apache.org/docs/table-design/data-partitioning/auto-partitioning">Doris自动分区</a><br/>
        支持的属性有：<br/>
        <code> table.create.auto-partition.properties.include</code>包含的经过route后的表集合，用逗号分隔，支持正则表达式；<br/>
        <code> table.create.auto-partition.properties.exclude</code>排除的经过route后的表集合，用逗号分隔，支持正则表达式；<br/>
        <code> table.create.auto-partition.properties.default-partition-key</code>默认分区键；<br/>
        <code> table.create.auto-partition.properties.default-partition-unit</code>默认分区单位；<br/>
        <code> table.create.auto-partition.properties.DB.TABLE.partition-key</code>特定表的分区键，如未配置取默认分区键；<br/>
        <code> table.create.auto-partition.properties.DB.TABLE.partition-unit</code>特定表的分区单位，如未配置取默认分区单位。<br/>
        注意：<br/>
        1: 如果分区键不为DATE/DATETIME类型，则不会创建分区表。<br/>
        2: Doris AUTO RANGE PARTITION不支持NULLABLE列作为分区列，如果您配置的分区键的值为空或者表创建完成后新增了NULLABLE分区列，系统将自动填充默认值（DATE类型为<code>1970-01-01</code>，DATETIME类型为<code>1970-01-01 00:00:00</code>），请选择合适的分区键。
      </td> 
    </tr>
    </tbody>
</table>
</div>

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:10%;">CDC type</th>
        <th class="text-left" style="width:30%;">Doris type</th>
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
      <td>DECIMAL</td>
      <td>DECIMAL</td>
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
      <td>TIMESTAMP [(p)]</td>
      <td>DATETIME [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ [(p)]
      </td>
      <td>DATETIME [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n*3)</td>
      <td>在Doris中，字符串是以UTF-8编码存储的，所以英文字符占1个字节，中文字符占3个字节。这里的长度统一乘3，CHAR最大的长度是255，超过后会自动转为VARCHAR类型</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n*3)</td>
      <td>同上，这里的长度统一乘3，VARCHAR最大的长度是65533，超过后会自动转为STRING类型</td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TIME
      </td>
      <td>STRING</td>
      <td> Doris 不支持 TIME 数据类型，需转换为 STRING 类型以兼容 </td>
    </tr>
    <tr>
      <td>STRING</td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
