# StarRocks Pipeline 连接器

StarRocks Pipeline 连接器可以用作 Pipeline 的 *Data Sink*，将数据写入[StarRocks](https://github.com/StarRocks/starrocks)。 本文档介绍如何设置 StarRocks Pipeline 连接器。

## 连接器的功能
* 自动建表
* 表结构变更同步
* 数据实时同步

如何创建 Pipeline
----------------

从 MySQL 读取数据同步到 StarRocks 的 Pipeline 可以定义如下：

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
  type: starrocks
  name: StarRocks Sink
  jdbc-url: jdbc:mysql://127.0.0.1:9030
  load-url: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
   name: MySQL to StarRocks Pipeline
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
      <td>指定要使用的连接器, 这里需要设置成 <code>'starrocks'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称.</td>
    </tr>
    <tr>
      <td>jdbc-url</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于访问 FE 节点上的 MySQL 服务器。多个地址用英文逗号（,）分隔。格式：`jdbc:mysql://fe_host1:fe_query_port1,fe_host2:fe_query_port2`。</td>
    </tr>
    <tr>
      <td>load-url</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于访问 FE 节点上的 HTTP 服务器。多个地址用英文分号（;）分隔。格式：`fe_host1:fe_http_port1;fe_host2:fe_http_port2`。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>StarRocks 集群的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>StarRocks 集群的用户密码。</td>
    </tr>
    <tr>
      <td>sink.label-prefix</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定 Stream Load 使用的 label 前缀。</td>
    </tr>
    <tr>
      <td>sink.connect.timeout-ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30000</td>
      <td>String</td>
      <td>与 FE 建立 HTTP 连接的超时时间。取值范围：[100, 60000]。</td>
    </tr>
    <tr>
      <td>sink.wait-for-continue.timeout-ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30000</td>
      <td>String</td>
      <td>等待 FE HTTP 100-continue 应答的超时时间。取值范围：[3000, 60000]。</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.max-bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">157286400</td>
      <td>Long</td>
      <td>内存中缓冲的数据量大小，缓冲区由所有导入的表共享，达到阈值后将选择一个或多个表的数据写入到StarRocks。
          达到阈值后取值范围：[64MB, 10GB]。</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.interval-ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">300000</td>
      <td>Long</td>
      <td>每个表缓冲数据发送的间隔，用于控制数据写入 StarRocks 的延迟。单位是毫秒，取值范围：[1000, 3600000]。</td>
    </tr>
    <tr>
      <td>sink.scan-frequency.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50</td>
      <td>Long</td>
      <td>连接器会定期检查每个表是否到达发送间隔，该配置控制检查频率，单位为毫秒。</td>
    </tr>
    <tr>
      <td>sink.io.thread-count</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2</td>
      <td>Integer</td>
      <td>用来执行 Stream Load 的线程数，不同表之间的导入可以并发执行。</td>
    </tr>
    <tr>
      <td>sink.at-least-once.use-transaction-stream-load</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>at-least-once 下是否使用 transaction stream load。</td>
    </tr>
    <tr>
      <td>sink.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Stream Load 的参数，控制 Stream Load 导入行为。例如 参数 `sink.properties.timeout` 用来控制导入的超时时间。
            全部参数和解释请参考 <a href="https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD">
            STREAM LOAD</a>。</td>
    </tr>
    <tr>
      <td>table.create.num-buckets</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>自动创建 StarRocks 表时使用的桶数。对于 StarRocks 2.5 及之后的版本可以不设置，StarRocks 将会
          <a href="https://docs.starrocks.io/zh/docs/table_design/Data_distribution/#%E7%A1%AE%E5%AE%9A%E5%88%86%E6%A1%B6%E6%95%B0%E9%87%8F">
          自动设置分桶数量</a>；对于 StarRocks 2.5 之前的版本必须设置。</td>
    </tr>
    <tr>
      <td>table.create.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>自动创建 StarRocks 表时使用的属性。比如: 如果使用 StarRocks 3.2 及之后的版本，<code>'table.create.properties.fast_schema_evolution' = 'true'</code>
          将会打开 fast schema evolution 功能。 更多信息请参考 
          <a href="https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table/">主键模型</a>。</td> 
    </tr>
    <tr>
      <td>table.schema-change.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30min</td>
      <td>Duration</td>
      <td>StarRocks 侧执行 schema change 的超时时间，必须是秒的整数倍。超时后 StarRocks 将会取消 schema change，从而导致作业失败。</td>
    </tr>
    </tbody>
</table>    
</div>

使用说明
--------

* 只支持主键表，因此源表必须有主键

* 暂不支持 exactly-once，连接器 通过 at-least-once 和主键表实现幂等写

* 对于自动建表
  * 分桶键和主键相同
  * 没有分区键
  * 分桶数由 `table.create.num-buckets` 控制。如果使用的 StarRocks 2.5 及之后的版本可以不设置，StarRocks 能够
    <a href="https://docs.starrocks.io/zh/docs/table_design/Data_distribution/#%E7%A1%AE%E5%AE%9A%E5%88%86%E6%A1%B6%E6%95%B0%E9%87%8F">
    自动设置分桶数量</a>。对于 StarRocks 2.5 之前的版本必须设置，否则无法自动创建表。

* 对于表结构变更同步
  * 只支持增删列
  * 新增列只能添加到最后一列
  * 如果使用 StarRocks 3.2 及之后版本，并且通过连接器来自动建表, 可以通过配置 `table.create.properties.fast_schema_evolution` 为 `true`
    来加速 StarRocks 执行变更。

* 对于数据同步，pipeline 连接器使用 [StarRocks Sink 连接器](https://github.com/StarRocks/starrocks-connector-for-apache-flink)
  将数据写入 StarRocks，具体可以参考 [Sink 文档](https://github.com/StarRocks/starrocks-connector-for-apache-flink/blob/main/docs/content/connector-sink.md)。

数据类型映射
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">StarRocks type</th>
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
      <td>DATETIME</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n) where n <= 85</td>
      <td>CHAR(n * 3)</td>
      <td>CDC 中长度表示字符数，而 StarRocks 中长度表示字节数。根据 UTF-8 编码，一个中文字符占用三个字节，因此 CDC 中的长度对应到 StarRocks
          中为 n * 3。由于 StarRocks CHAR 类型的最大长度为255，所以只有当 CDC 中长度不超过85时，才将 CDC CHAR 映射到 StarRocks CHAR。</td>
    </tr>
    <tr>
      <td>CHAR(n) where n > 85</td>
      <td>VARCHAR(n * 3)</td>
      <td>CDC 中长度表示字符数，而 StarRocks 中长度表示字节数。根据 UTF-8 编码，一个中文字符占用三个字节，因此 CDC 中的长度对应到 StarRocks
          中为 n * 3。由于 StarRocks CHAR 类型的最大长度为255，所以当 CDC 中长度超过85时，才将 CDC CHAR 映射到 StarRocks VARCHAR。</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n * 3)</td>
      <td>CDC 中长度表示字符数，而 StarRocks 中长度表示字节数。根据 UTF-8 编码，一个中文字符占用三个字节，因此 CDC 中的长度对应到 StarRocks
          中为 n * 3。</td>
    </tr>
    </tbody>
</table>
</div>

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))