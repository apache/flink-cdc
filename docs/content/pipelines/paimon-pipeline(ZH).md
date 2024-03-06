# Paimon Pipeline 连接器

Paimon Pipeline 连接器可以用作 Pipeline 的 *Data Sink*，将数据写入[Paimon](https://paimon.apache.org)。 本文档介绍如何设置 Paimon Pipeline 连接器。

## 连接器的功能
* 自动建表
* 表结构变更同步
* 数据实时同步

如何创建 Pipeline
----------------

从 MySQL 读取数据同步到 Paimon 的 Pipeline 可以定义如下：

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
  type: paimon
  name: Paimon Sink
  metastore: filesystem
  warehouse: /path/warehouse

pipeline:
   name: MySQL to Paimon Pipeline
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
      <td>指定要使用的连接器, 这里需要设置成 <code>'paimon'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称.</td>
    </tr>
    <tr>
      <td>catalog.properties.metastore</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于构建 Paimon Catalog 的类型。可选填值 filesystem 或者 hive。</td>
    </tr>
    <tr>
      <td>catalog.properties.warehouse</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Paimon 仓库存储数据的根目录。</td>
    </tr>
    <tr>
      <td>catalog.properties.uri</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Hive metastore 的 uri，在 metastore 设置为 hive 的时候需要。</td>
    </tr>
    <tr>
      <td>commit.user</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>提交数据文件时的用户名， 默认值为 `admin`.</td>
    </tr>
    <tr>
      <td>partition.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>设置每个分区表的分区字段，允许填写成多个分区表的多个分区字段。 不同的表使用 ';'分割， 而不同的字段则使用 ','分割。举个例子， 我们可以为两张表的不同分区字段作如下的设置 'testdb.table1:id1,id2;testdb.table2:name'。</td>
    </tr>
    <tr>
      <td>catalog.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Paimon catalog 支持的参数传递给 pipeline，参考 <a href="https://paimon.apache.org/docs/master/maintenance/configurations/#catalogoptions">Paimon catalog options</a>。 </td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Paimon table 支持的参数传递给 pipeline，参考 <a href="https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions">Paimon table options</a>。 </td>
    </tr>
    </tbody>
</table>    
</div>

使用说明
--------

* 只支持主键表，因此源表必须有主键

* 暂不支持 exactly-once，连接器 通过 at-least-once 和主键表实现幂等写
  
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