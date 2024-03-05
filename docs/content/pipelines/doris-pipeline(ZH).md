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

# Doris Pipeline 连接器

本文介绍了Pipeline Doris Connector的使用


## 示例
----------------

```yaml
source:
  type: values
  name: Values Source

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


## Pipeline选项
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
      <td>sink.enable.batch-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td> 是否使用攒批方式写入Doris </td>
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
      <td>sink.properties.</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>StreamLoad的参数。
        For example: <code> sink.properties.strict_mode: true</code>.
        查看更多关于 <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD/"> StreamLoad的Properties 属性</a></td> 
      </td>
    </tr>
    <tr>
      <td>table.create.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>创建表的Properties配置。
        For example: <code> table.create.properties.replication_num: 1</code>.
        查看更多关于 <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/"> Doris Table 的  Properties 属性</a></td> 
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
        <th class="text-left" style="width:30%;">Doris type<a href="https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Types/BOOLEAN/"></a></th>
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
      <td>STRING</td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>





## 常见问题
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
