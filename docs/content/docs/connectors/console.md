---
title: "Console"
weight: 3
type: docs
aliases:
- /connectors/console
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

# Console Connector

The Console(Value) Pipeline connector can be used as the Data Sink of the pipeline, and write data to Stdout. This document describes how to set up the Value Pipeline connector.

## What can the connector do?
* Create table automatically if not exist
* Schema change synchronization
* Data synchronization
* Print data change event log to stdout

## Example


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
  type: values
  name: Console Sink


pipeline:
   parallelism: 2

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
       <td>Specify the Source/Sink to use, here is <code>'values'</code>.</td>
     </tr>
     <tr>
       <td>name</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td> Name of PipeLine </td>
     </tr>
     <tr>
       <td>print.enabled</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td> True if the Event should be print to console. </td>
     </tr>
     <tr>
       <td>sink.api</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>String</td>
       <td> The sink api on which the sink is based: SinkFunction or SinkV2. </td>
     </tr>
     </tbody>
</table>
</div>

{{< top >}}
