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

Oracle connector allows reading snapshot data and incremental data from Oracle database and provides end-to-end full-database data synchronization capabilities.
This document describes how to setup the Oracle connector.


## Example

An example of the pipeline for reading data from Oracle and sink to Doris can be defined as follows:

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

## Connector Options

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
      <td> IP address or hostname of the Oracle database server.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1521</td>
      <td>Integer</td>
      <td>Integer port number of the Oracle database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Oracle database to use when connecting to the Oracle database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the Oracle database server.</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the Oracle database to monitor. The table-name also supports regular expressions to monitor multiple tables that satisfy the regular expressions. <br>
          It is important to note that the dot (.) is treated as a delimiter for database and table names. 
          If there is a need to use a dot (.) in a regular expression to match any character, it is necessary to escape the dot with a backslash.<br>
          eg. db0.\.*, db1.user_table_[0-9]+, db[1-2].[app|web]order_\.*</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to send schema change events, so that downstream sinks can respond to schema changes and achieve table structure synchronization.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>The maximum fetch size for per poll when read table snapshot.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for Oracle CDC consumer, valid enumerations are "initial","latest-offset".</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from Oracle server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/oracle.html#oracle-connector-properties">Debezium's Oracle Connector properties</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to close idle readers at the end of the snapshot phase. <br>
          The flink version is required to be greater than or equal to 1.14 when 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' is set to true.<br>
          If the flink version is greater than or equal to 1.15, the default value of 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' has been changed to true,
          so it does not need to be explicitly configured 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'</td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td>
        List of readable metadata from SourceRecord to be passed to downstream and could be used in transform module, split by `,`. Available readable metadata are: op_ts.
      </td>
    </tr>
    </tbody>
</table>
</div>

## Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for Oracle CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest archive log.
- `latest-offset`: On first startup, no snapshots of the monitored database tables are performed, and the connector only starts reading from the end of the archive log, which means the connector can only read data changes after the connector starts.

For example in YAML definition:

```yaml
source:
  type: oracle
  scan.startup.mode: earliest-offset                    # Start from earliest offset
  scan.startup.mode: latest-offset                      # Start from latest offset
  # ...
```

## Data Type Mapping

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
      <td>When s is greater than 0, use DECIMAL (p, s); Otherwise, use BIGINT。</td>
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
      <td>Currently, only blob data types with a length not exceeding 2147483647 (2 * * 31-1) are supported in Oracle. </td>
    </tr>
    </tbody>
</table>
</div>
{{< top >}}
