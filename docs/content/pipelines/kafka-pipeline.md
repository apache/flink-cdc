# Kafka Pipeline Connector

The Kafka Pipeline connector can be used as the *Data Sink* of the pipeline, and write data to [Kafka](https://kafka.apache.org). This document describes how to set up the Kafka Pipeline connector.

## What can the connector do?
* Data synchronization

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Kafka can be defined as follows:

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

Pipeline Connector Options
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
      <td>Specify what connector to use, here should be <code>'kafka'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>value.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to serialize the value part of Kafka messages. Available options are <a href="https://debezium.io/documentation/reference/stable/integrations/serdes.html">debezium-json</a> and <a href="https://github.com/alibaba/canal/wiki">canal-json</a>, default option is `debezium-json`, and do not support user-defined format now. </td>
    </tr>
    <tr>
      <td>properties.bootstrap.servers</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.</td>
    </tr>
    <tr>
      <td>topic</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>If this parameter is configured, all events will be sent to this topic.</td>
    </tr>
    <tr>
      <td>sink.add-tableId-to-header-enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Boolean</td>
      <td>If this parameter is true, a header with key of 'namespace','schemaName','tableName' will be added for each Kafka record. Default value is false.</td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass options of Kafka table to pipeline，See <a href="https://kafka.apache.org/28/documentation.html#consumerconfigs">Kafka consume options</a>. </td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* The written topic of Kafka will be `namespace.schemaName.tableName` string of TableId，this can be changed using route function of pipeline.

* If the written topic of Kafka is not existed, we will create one automatically.

Data Type Mapping
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
      <td>TIMESTAMP</td>
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