---
title: "GaussDB DWS"
weight: 6
type: docs
aliases:
- /connectors/pipeline-connectors/dws
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

# GaussDB DWS Connector

The GaussDB DWS connector is a pipeline sink connector that writes Flink CDC events to Huawei Cloud GaussDB(DWS).
This document describes how to set up the GaussDB DWS pipeline sink connector.

## Example

The following example shows how to write data from a Values source to GaussDB DWS:

```yaml
source:
   type: values
   name: Values Source

sink:
   type: dws
   name: GaussDB DWS Sink
   jdbc-url: jdbc:gaussdb://127.0.0.1:8000/postgres
   username: gaussdb
   password: password
   schema: public
   sink.enable-delete: true

pipeline:
   name: Values to GaussDB DWS Pipeline
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
      <td>Specify the sink to use. For GaussDB DWS, set this option to <code>dws</code>.</td>
    </tr>
    <tr>
      <td>jdbc-url</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>GaussDB DWS JDBC URL. The target database must be specified in this URL.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the GaussDB DWS user used to connect to the database.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password of the GaussDB DWS user used to connect to the database.</td>
    </tr>
    <tr>
      <td>schema</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">public</td>
      <td>String</td>
      <td>Default target schema used when incoming table identifiers do not contain a schema.</td>
    </tr>
    <tr>
      <td>sink-table</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Optional target table name override passed to the native DWS client.</td>
    </tr>
    <tr>
      <td>driver</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">com.huawei.gauss200.jdbc.Driver</td>
      <td>String</td>
      <td>GaussDB DWS JDBC driver class.</td>
    </tr>
    <tr>
      <td>case-sensitive</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether target identifiers preserve case. When disabled, identifiers are normalized to lower case.</td>
    </tr>
    <tr>
      <td>write-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Write mode used by the native DWS client. Supported values include copy_merge, auto, upsert, copy_upsert, copy, update, update_auto, and copy_update.</td>
    </tr>
    <tr>
      <td>sink.enable-delete</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether DELETE events are forwarded to the sink.</td>
    </tr>
    <tr>
      <td>sink.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>Maximum number of retries used by the native DWS client.</td>
    </tr>
    <tr>
      <td>sink.parallelism</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Custom sink parallelism. If unset, the planner derives it automatically.</td>
    </tr>
    <tr>
      <td>enable-auto-flush</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether automatic flush is enabled.</td>
    </tr>
    <tr>
      <td>auto-batch-flush-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>Maximum buffered rows before an automatic flush.</td>
    </tr>
    <tr>
      <td>auto-flush-max-interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3 min</td>
      <td>Duration</td>
      <td>Maximum interval between automatic flushes.</td>
    </tr>
    <tr>
      <td>enable-dn-partition</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether Huawei DN partitioning is enabled.</td>
    </tr>
    <tr>
      <td>distribution-key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Comma-separated distribution keys used for DN partitioning.</td>
    </tr>
    <tr>
      <td>local-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">systemDefault</td>
      <td>String</td>
      <td>Session time zone used for timestamp conversion.</td>
    </tr>
    <tr>
      <td>connectionSize</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>Number of connections used by the DWS client.</td>
    </tr>
    <tr>
      <td>connectionMaxUseTimeSeconds</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3600</td>
      <td>Integer</td>
      <td>Maximum lifetime of a connection in seconds.</td>
    </tr>
    <tr>
      <td>connectionMaxIdleMs</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">60000</td>
      <td>Integer</td>
      <td>Maximum idle time for a connection in milliseconds.</td>
    </tr>
    <tr>
      <td>connectionTimeOut</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>Connection timeout in milliseconds.</td>
    </tr>
    <tr>
      <td>connectionPoolName</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>String</td>
      <td>Connection pool name.</td>
    </tr>
    <tr>
      <td>connectionPoolSize</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Integer</td>
      <td>Connection pool size.</td>
    </tr>
    <tr>
      <td>connectionPoolTimeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>Connection pool timeout in milliseconds.</td>
    </tr>
    <tr>
      <td>connectionSocketTimeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Integer</td>
      <td>Socket timeout in milliseconds.</td>
    </tr>
    <tr>
      <td>connectionMaxUseCount</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>Maximum number of operations per connection.</td>
    </tr>
    <tr>
      <td>needConnectionPoolMonitor</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Boolean</td>
      <td>Whether to enable connection pool monitoring.</td>
    </tr>
    <tr>
      <td>connectionPoolMonitorPeriod</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">native default</td>
      <td>Long</td>
      <td>Connection pool monitor interval in milliseconds.</td>
    </tr>
    <tr>
      <td>dws.client.write.thread-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>Native DWS client write worker thread count.</td>
    </tr>
    <tr>
      <td>dws.client.write.use-copy-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>COPY mode switch threshold used by the native DWS client when batching writes.</td>
    </tr>
    <tr>
      <td>dws.client.write.force-flush-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">40000</td>
      <td>Integer</td>
      <td>Force flush threshold used by the native DWS client.</td>
    </tr>
    <tr>
      <td>logSwitch</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to enable DWS client logging.</td>
    </tr>
    </tbody>
</table>
</div>

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">Flink CDC Type</th>
        <th class="text-left" style="width:30%;">GaussDB DWS Type</th>
        <th class="text-left" style="width:40%;">Note</th>
      </tr>
    </thead>
    <tbody>
    <tr><td>BOOLEAN</td><td>BOOLEAN</td><td></td></tr>
    <tr><td>TINYINT, SMALLINT</td><td>SMALLINT</td><td></td></tr>
    <tr><td>INTEGER</td><td>INTEGER</td><td></td></tr>
    <tr><td>BIGINT</td><td>BIGINT</td><td></td></tr>
    <tr><td>FLOAT</td><td>REAL</td><td></td></tr>
    <tr><td>DOUBLE</td><td>DOUBLE PRECISION</td><td></td></tr>
    <tr><td>DECIMAL</td><td>DECIMAL(p, s)</td><td>Uses the source precision and scale.</td></tr>
    <tr><td>CHAR</td><td>CHAR(n)</td><td></td></tr>
    <tr><td>VARCHAR</td><td>VARCHAR(n) or TEXT</td><td>Very large VARCHAR columns are mapped to TEXT.</td></tr>
    <tr><td>BINARY, VARBINARY</td><td>BYTEA</td><td></td></tr>
    <tr><td>DATE</td><td>DATE</td><td></td></tr>
    <tr><td>TIME</td><td>TIME(p)</td><td>Precision is capped at 6.</td></tr>
    <tr><td>TIMESTAMP</td><td>TIMESTAMP(p)</td><td>Precision is capped at 6.</td></tr>
    <tr><td>TIMESTAMP_LTZ, TIMESTAMP_TZ</td><td>TIMESTAMPTZ(p)</td><td>Precision is capped at 6.</td></tr>
    <tr><td>ARRAY</td><td>TEXT</td><td></td></tr>
    <tr><td>MAP, ROW</td><td>JSON</td><td></td></tr>
    </tbody>
</table>
</div>

{{< top >}}
