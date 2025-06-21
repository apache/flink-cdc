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

This article introduces of Doris Connector

## Example


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
       <td>type</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Specify the Sink to use, here is <code>'doris'</code>.</td>
     </tr>
     <tr>
       <td>name</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td> Name of PipeLine </td>
     </tr>
      <tr>
       <td>fenodes</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Http address of Doris cluster FE, such as 127.0.0.1:8030 </td>
     </tr>
      <tr>
       <td>benodes</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Http address of Doris cluster BE, such as 127.0.0.1:8040 </td>
     </tr>
     <tr>
       <td>jdbc-url</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>JDBC address of Doris cluster, for example: jdbc:mysql://127.0.0.1:9030/db</td>
     </tr>
     <tr>
       <td>username</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Username of Doris cluster</td>
     </tr>
     <tr>
       <td>password</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Password for Doris cluster</td>
     </tr>
     <tr>
       <td>auto-redirect</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">false</td>
       <td>String</td>
       <td> Whether to write through FE redirection and directly connect to BE to write </td>
     </tr>
     <tr>
       <td>charset-encoding</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">false</td>
       <td>Boolean</td>
       <td> Charset encoding for doris http client, default UTF-8 </td>
     </tr>
     <tr>
       <td>sink.enable.batch-mode</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td> Whether to use the batch method to write to Doris </td>
     </tr>
     <tr>
       <td>sink.enable-delete</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td>Whether to enable the delete function </td>
     </tr>
     <tr>
       <td>sink.max-retries</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">3</td>
       <td>Integer</td>
       <td>The max retry times if writing records to database failed. </td>
     </tr>
     <tr>
       <td>sink.flush.queue-size</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">2</td>
       <td>Integer</td>
       <td> Queue size for batch writing
       </td>
     </tr>
     <tr>
       <td>sink.buffer-flush.max-rows</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">50000</td>
       <td>Integer</td>
       <td>Maximum number of Flush records in a single batch</td>
     </tr>
     <tr>
       <td>sink.buffer-flush.max-bytes</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">10485760(10MB)</td>
       <td>Integer</td>
       <td>Maximum number of bytes flushed in a single batch</td>
     </tr>
     <tr>
       <td>sink.buffer-flush.interval</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">10s</td>
       <td>String</td>
       <td>Flush interval duration. If this time is exceeded, the data will be flushed asynchronously</td>
     </tr>
     <tr>
       <td>sink.ignore.update-before</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td>In the CDC scenario, when the primary key of the upstream is inconsistent with that of the downstream, the update-before data needs to be passed to the downstream as deleted data, otherwise the data cannot be deleted.\n"
                                    + "The default is to ignore, that is, perform upsert semantics.</td>
     </tr>
     <tr>
       <td>sink.properties.</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td> Parameters of StreamLoad.
         For example: <code> sink.properties.strict_mode: true</code>.
         See more about <a href="https://doris.apache.org/docs/dev/data-operate/import/import-way/stream-load-manual"> StreamLoad Properties</a></td>
       </td>
     </tr>
     <tr>
       <td>table.create.properties.*</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Create the Properties configuration of the table.
         For example: <code> table.create.properties.replication_num: 1</code>.
         See more about <a href="https://doris.apache.org/docs/dev/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE"> Doris Table Properties</a></td>
       </td>
     </tr>
    <tr>
      <td>table.create.auto-partition.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Create the auto partition Properties configuration of the table.<br/>
        Currently the partition function only supports date_trunc, and the partition column supports only DATE or DATETIME types, and the version of Doris must greater than 2.1.6. See more about <a href="https://doris.apache.org/docs/table-design/data-partitioning/auto-partitioning">Doris Auto Partitioning</a><br/>
        These properties are supported now：<br/>
        <code> table.create.auto-partition.properties.include</code>A collection of tables after route to include, separated by commas, supports regular expressions;<br/>
        <code> table.create.auto-partition.properties.exclude</code>A collection of tables after route to exclude, separated by commas, supports regular expressions;<br/>
        <code> table.create.auto-partition.properties.default-partition-key</code>The default partition key;<br/>
        <code> table.create.auto-partition.properties.default-partition-unit</code>The default partition unit;<br/>
        <code> table.create.auto-partition.properties.DB.TABLE.partition-key</code>The partition key of a specific table. If not set, the default partition key is used;<br/>
        <code> table.create.auto-partition.properties.DB.TABLE.partition-unit</code>The partition unit of a specific table. If not set, the default partition unit is used.<br/>
        Note:<br/>
        1: If the partition key is not DATE/DATETIME type, auto partition tables won't be created.<br/>
        2: Doris AUTO RANGE PARTITION does not support NULLABLE columns as partition key, if Flink CDC get a NULL value or a NULLABLE partition key was added after the table was created, will automatically fill it with a default value(DATE:<code>1970-01-01</code>, DATETIME:<code>1970-01-01 00:00:00</code>), chose a suitable partition key is very important.
      </td> 
    </tr>
     </tbody>
</table>
</div>

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:10%;">Flink CDC Type</th>
        <th class="text-left" style="width:30%;">Doris Type</th>
        <th class="text-left" style="width:60%;">Note</th>
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
      <td>In Doris, strings are stored in UTF-8 encoding, so English characters occupy 1 byte and Chinese characters occupy 3 bytes. The length here is multiplied by 3. The maximum length of CHAR is 255. Once exceeded, it will automatically be converted to VARCHAR type.</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n*3)</td>
      <td>Same as above. The length here is multiplied by 3. The maximum length of VARCHAR is 65533. Once exceeded, it will automatically be converted to STRING type.</td>
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
      <td>Doris does not support the TIME data type, it needs to be converted to STRING type for compatibility.</td>
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
