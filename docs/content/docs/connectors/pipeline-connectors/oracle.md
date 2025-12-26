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
   password: password
   database: ORCLDB
   tables: testdb.\.*, testdb.user_table_[0-9]+, [app|web].order_\.*

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: password

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
      <td>required</td>
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
    <tr>
      <td>jdbc.url</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> The url for oracle jdbc ,the url will be used preferentially,if no url is configured, then use "jdbc:oracle:thin:@localhost:1521:orcl"，but oracle 19c url is "jdbc:oracle:thin:@//localhost:1521/pdb1",so the url property is option to adapt to different versions of Oracle.</td>
    </tr>
    <tr>
      <td>database</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the Oracle server to monitor.</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.</td>
    </tr>
<tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>
        The connection pool size.
      </td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30</td>
      <td>Integer</td>
      <td>The maximum time that the connector should wait after trying to connect to the oracle database server before timing out.</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>
The max retry times that the connector should retry to build oracle database server connection.
      </td>
    </tr>
    <tr>
      <td>heartbeat.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30</td>
      <td>String</td>
      <td>
        Optional interval of sending heartbeat event for tracing the latest available binlog offsets.
      </td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>
        The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.
      </td>
    </tr>
    <tr>
      <td>chunk-key.even-distribution.factor.upper-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000.0d</td>
      <td>Double</td>
      <td>
        The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    table is evenly distribution or not."
                                    The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    and the query oracle for splitting would happen when it is uneven.
                                    The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.
      </td>
    </tr>
    <tr>
      <td>chunk-key.even-distribution.factor.lower-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0.05d</td>
      <td>Double</td>
      <td>
        The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the
                                    table is evenly distribution or not."
                                    The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    and the query oracle for splitting would happen when it is uneven.
                                    The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.
      </td>
    </tr>
    <tr>
      <td>debezium.log.mining.strategy</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">online_catalog</td>
      <td>String</td>
      <td>
      A strategy in log data analysis or mining.  
      </td>
    </tr>
    <tr>
      <td>debezium.log.mining.continuous.mine</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
      A continuous or ongoing mining process in the field of log mining.  
      </td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
      Whether capture the newly added tables when restoring from a savepoint/checkpoint or not, by default is false.  
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
      Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:
                                    (1) source can be parallel during snapshot reading,
                                    (2) source can perform checkpoints in the chunk granularity during snapshot reading,
                                    (3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
      Whether to skip backfill in snapshot reading phase. If backfill is skipped, changes on captured tables during snapshot phase will be consumed later in binlog reading phase instead of being merged into the snapshot.WARNING: Skipping backfill might lead to data inconsistency because some binlog events happened within the snapshot phase might be replayed (only at-least-once semantic is promised). For example updating an already updated value in snapshot, or deleting an already deleted entry in snapshot. These replayed binlog events should be handled specially.
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
      <td>When s is greater than 0, use DECIMAL (p, s); Otherwise, use BIGINT.</td>
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
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        BINARY_FLOAT<br>
        REAL<br>
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
        INTERVAL DAY(3) TO SECOND(2)<br>
        BIGINT
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NCHAR(n)<br>
        CHAR(n)<br>
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)<br>
        VARCHAR2(n)<br>
        NVARCHAR2(n)<br>
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        CLOB<br>
        NLOB<br>
        NCLOB<br>
        SDO_GEOMETRY<br>
        XMLTYPE
      </td>
      <td>STRING</td>
      <td>Currently, only blob data types with a length not exceeding 2147483647 (2 * * 31-1) are supported in Oracle. </td>
    </tr>
    <tr>
      <td>
        BLOB<br>
        RAW<br>
        BFILE
      </td>
      <td>BYTES</td>
      <td> </td>
    </tr>
    <tr>
      <td>
        INTEGER<br>
        SMALLINT<br>
        TINYINT
      </td>
      <td>INT</td>
      <td> </td>
    </tr>
    <tr>
      <td>
        BOOLEAN
      </td>
      <td>BOOLEAN</td>
      <td> </td>
    </tr>
    </tbody>
</table>
</div>
{{< top >}}
