# Kafka Pipeline 连接器

Kafka Pipeline 连接器可以用作 Pipeline 的 *Data Sink*，将数据写入[Kafka](https://kafka.apache.org)。 本文档介绍如何设置 Kafka Pipeline 连接器。

## 连接器的功能
* 自动建表
* 表结构变更同步
* 数据实时同步

如何创建 Pipeline
----------------

从 MySQL 读取数据同步到 Kafka 的 Pipeline 可以定义如下：

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
  type: kafka
  name: Kafka Sink
  properties.bootstrap.servers: PLAINTEXT://localhost:62510

pipeline:
  name: MySQL to Kafka Pipeline
  parallelism: 2
```

Pipeline 连接器配置项
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
      <td>指定要使用的连接器, 这里需要设置成 <code>'kafka'</code>。 </td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称。 </td>
    </tr>
    <tr>
      <td>value.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于序列化 Kafka 消息的值部分数据的格式。可选的填写值包括 <a href="https://debezium.io/documentation/reference/stable/integrations/serdes.html">debezium-json</a> 和 <a href="https://github.com/alibaba/canal/wiki">canal-json</a>, 默认值为 `debezium-json`，并且目前不支持用户自定义输出格式。 </td>
    </tr>
    <tr>
      <td>properties.bootstrap.servers</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于建立与 Kafka 集群初始连接的主机/端口对列表。</td>
    </tr>
    <tr>
      <td>topic</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>如果配置了这个参数，所有的消息都会发送到这一个主题。</td>
    </tr>
    <tr>
      <td>sink.add-tableId-to-header-enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Boolean</td>
      <td>如果配置了这个参数，所有的消息都会带上键为 `namespace`, 'schemaName', 'tableName'，值为事件 TableId 里对应的 字符串的 header。</td>
    </tr>
    <tr>
      <td>properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Kafka 支持的参数传递给 pipeline，参考 <a href="https://kafka.apache.org/28/documentation.html#consumerconfigs">Kafka consume options</a>。 </td>
    </tr>
    </tbody>
</table>    
</div>

使用说明
--------

* 写入 Kafka 的 topic 默认会是上游表 `namespace.schemaName.tableName` 对应的字符串，可以通过 pipeline 的 route 功能进行修改。
* 如果配置了 `topic` 参数，所有的消息都会发送到这一个主题。
* 写入 Kafka 的 topic 如果不存在，则会默认创建。

数据类型映射
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">Paimon type</th>
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
      <td>DATETIME</td>
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

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))