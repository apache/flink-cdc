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

# StarRocks Pipeline Connector

The StarRocks Pipeline connector can be used as the *Data Sink* of the pipeline, and write data to [StarRocks](https://github.com/StarRocks/starrocks). This document describes how to set up the StarRocks Pipeline connector.

## What can the connector do?
* Create table automatically if not exist
* Schema change synchronization
* Data synchronization

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to StarRocks can be defined as follows:

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
      <td>Specify what connector to use, here should be <code>'starrocks'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>jdbc-url</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The address that is used to connect to the MySQL server of the FE. You can specify multiple addresses, which must be separated by a comma (,). Format: `jdbc:mysql://fe_host1:fe_query_port1,fe_host2:fe_query_port2,fe_host3:fe_query_port3`.</td>
    </tr>
    <tr>
      <td>load-url</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (;). Format: `fe_host1:fe_http_port1;fe_host2:fe_http_port2`.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>User name to use when connecting to the StarRocks database.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the StarRocks database.</td>
    </tr>
    <tr>
      <td>sink.label-prefix</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The label prefix used by Stream Load.</td>
    </tr>
    <tr>
      <td>sink.connect.timeout-ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30000</td>
      <td>String</td>
      <td>The timeout for establishing HTTP connection. Valid values: 100 to 60000.</td>
    </tr>
    <tr>
      <td>sink.wait-for-continue.timeout-ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30000</td>
      <td>String</td>
      <td>Timeout in millisecond to wait for 100-continue response from FE http server.
            Valid values: 3000 to 600000.</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.max-bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">157286400</td>
      <td>Long</td>
      <td>The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time.
        The value ranges from 64 MB to 10 GB. This buffer is shared by all tables in the sink. If the buffer 
        is full, the connector will choose one or more tables to flush.</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.interval-ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">300000</td>
      <td>Long</td>
      <td>The interval at which data is flushed for each table. The unit is in millisecond.</td>
    </tr>
    <tr>
      <td>sink.scan-frequency.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50</td>
      <td>Long</td>
      <td>Scan frequency in milliseconds to check whether the buffered data for a table should be flushed
            because of reaching the flush interval.</td>
    </tr>
    <tr>
      <td>sink.io.thread-count</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2</td>
      <td>Integer</td>
      <td>Number of threads used for concurrent stream loads among different tables.</td>
    </tr>
    <tr>
      <td>sink.at-least-once.use-transaction-stream-load</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to use transaction stream load for at-least-once when it's available.</td>
    </tr>
    <tr>
      <td>sink.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The parameters that control Stream Load behavior. For example, the parameter `sink.properties.timeout` 
            specifies the timeout of Stream Load. For a list of supported parameters and their descriptions,
            see <a href="https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD">
            STREAM LOAD</a>.</td>
    </tr>
    <tr>
      <td>table.create.num-buckets</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Number of buckets when creating a StarRocks table automatically. For StarRocks 2.5 or later, it's not required
            to set the option because StarRocks can 
            <a href="https://docs.starrocks.io/docs/table_design/Data_distribution/#determine-the-number-of-buckets">
            determine the number of buckets automatically</a>. For StarRocks prior to 2.5, you must set this option. </td>
    </tr>
    <tr>
      <td>table.create.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Properties used for creating a StarRocks table. For example: <code>'table.create.properties.fast_schema_evolution' = 'true'</code>
          will enable fast schema evolution if you are using StarRocks 3.2 or later. For more information, 
          see <a href="https://docs.starrocks.io/docs/table_design/table_types/primary_key_table">how to create a primary key table</a>.</td> 
    </tr>
    <tr>
      <td>table.schema-change.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30min</td>
      <td>Duration</td>
      <td>Timeout for a schema change on StarRocks side, and must be an integral multiple of 
          seconds. StarRocks will cancel the schema change after timeout which will
          cause the sink failure. </td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* Only support StarRocks primary key table, so the source table must have primary keys.

* Not support exactly-once. The connector uses at-least-once + primary key table for idempotent writing.

* For creating table automatically
  * the distribution keys are the same as the primary keys
  * there is no partition key
  * the number of buckets is controlled by `table.create.num-buckets`. If you are using StarRocks 2.5 or later,
    it's not required to set the option because StarRocks can [determine the number of buckets automatically](https://docs.starrocks.io/docs/table_design/Data_distribution/#determine-the-number-of-buckets),
    otherwise you must set the option.

* For schema change synchronization
  * only supports add/drop columns
  * the new column will always be added to the last position
  * if your StarRocks version is 3.2 or later, and using the connector to create table automatically,
    you can set `table.create.properties.fast_schema_evolution` to `true` to speed up the schema change.

* For data synchronization, the pipeline connector uses [StarRocks Sink Connector](https://github.com/StarRocks/starrocks-connector-for-apache-flink)
  to write data to StarRocks. You can see [sink documentation](https://github.com/StarRocks/starrocks-connector-for-apache-flink/blob/main/docs/content/connector-sink.md)
  for how it works. 

Data Type Mapping
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
      <td>CDC defines the length by characters, and StarRocks defines it by bytes. According to UTF-8, one Chinese 
        character is equal to three bytes, so the length for StarRocks is n * 3. Because the max length of StarRocks
        CHAR is 255, map CDC CHAR to StarRocks CHAR only when the CDC length is no larger than 85.</td>
    </tr>
    <tr>
      <td>CHAR(n) where n > 85</td>
      <td>VARCHAR(n * 3)</td>
      <td>CDC defines the length by characters, and StarRocks defines it by bytes. According to UTF-8, one Chinese 
        character is equal to three bytes, so the length for StarRocks is n * 3. Because the max length of StarRocks
        CHAR is 255, map CDC CHAR to StarRocks VARCHAR if the CDC length is larger than 85.</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n * 3)</td>
      <td>CDC defines the length by characters, and StarRocks defines it by bytes. According to UTF-8, one Chinese 
        character is equal to three bytes, so the length for StarRocks is n * 3.</td>
    </tr>
    </tbody>
</table>
</div>

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))