---
title: "Elasticsearch"
weight: 5
type: docs
aliases:
- /connectors/pipeline-connectors/elasticsearch
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

# Elasticsearch Connector

This article introduces the Elasticsearch Connector

## Example

```yaml
source:
   type: values
   name: ValuesSource

sink:
   type: elasticsearch
   name: Elasticsearch Sink
   hosts: http://localhost:9200
   version: 8
   username: elastic
   password: changeme

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
       <td>Specify the Sink to use, here is <code>'elasticsearch'</code>.</td>
     </tr>
     <tr>
       <td>name</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Name of Pipeline</td>
     </tr>
     <tr>
       <td>hosts</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>The comma-separated list of Elasticsearch hosts to connect to, e.g., http://localhost:9200.</td>
     </tr>
     <tr>
       <td>username</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>The username for Elasticsearch authentication.</td>
     </tr>
     <tr>
       <td>password</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>The password for Elasticsearch authentication.</td>
     </tr>
     <tr>
       <td>version</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">7</td>
       <td>Integer</td>
       <td>The version of Elasticsearch to connect to (6, 7, or 8).</td>
     </tr>
     <tr>
       <td>batch.size.max</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">500</td>
       <td>Integer</td>
       <td>The maximum number of actions to buffer for each bulk request.</td>
     </tr>
     <tr>
       <td>inflight.requests.max</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">5</td>
       <td>Integer</td>
       <td>The maximum number of concurrent requests that the sink will try to execute.</td>
     </tr>
     <tr>
       <td>buffered.requests.max</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">1000</td>
       <td>Integer</td>
       <td>The maximum number of requests to keep in the in-memory buffer.</td>
     </tr>
     <tr>
       <td>batch.size.max.bytes</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">5242880 (5MB)</td>
       <td>Long</td>
       <td>The maximum size of batch requests in bytes.</td>
     </tr>
     <tr>
       <td>buffer.time.max.ms</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">5000</td>
       <td>Long</td>
       <td>The maximum time to wait for incomplete batches before flushing (in milliseconds).</td>
     </tr>
     <tr>
       <td>record.size.max.bytes</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">10485760 (10MB)</td>
       <td>Long</td>
       <td>The maximum size of a single record in bytes.</td>
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
        <th class="text-left" style="width:30%;">Elasticsearch Type</th>
        <th class="text-left" style="width:60%;">Note</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>text</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>boolean</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY / VARBINARY</td>
      <td>binary</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>scaled_float</td>
      <td>Elasticsearch doesn't have a native decimal type. scaled_float is used as an approximation.</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>byte</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>short</td>
      <td></td>
    </tr>
    <tr>
      <td>INTEGER</td>
      <td>integer</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>long</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>float</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>double</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>date</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>keyword</td>
      <td>Stored as string in format HH:mm:ss.SSSSSS</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>date</td>
      <td>Stored with microsecond precision</td>
    </tr>
    <tr>
      <td>ARRAY</</td>
      <td>array</td>
      <td></td>
    </tr>
    <tr>
      <td>MAP / ROW</td>
      <td>object</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
