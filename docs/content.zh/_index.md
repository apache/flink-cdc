---
title: Apache Flink CDC
type: docs
bookToc: false
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

#### 

<div style="text-align: center">
  <h1>
    Flink CDC: Change Data Capture Solution Of Apache Flink
  </h1>
<h4 style="color: #696969">Set of source connectors for Apache Flink® directly ingesting changes coming from different databases using Change Data Capture(CDC).</h4>
</div>

Flink CDC integrates Debezium as the engine to capture data changes. So it can fully leverage the ability of Debezium. See more about what is [Debezium](https://github.com/debezium/debezium).

{{< img src="/fig/cdc-flow.png" alt="Stateful Functions" width="50%" >}}

Flink CDC supports ingesting snapshot data and real time changes from databases to Flink® and then transform and sink to various downstream systems.

{{< columns >}}
## Try Flink CDC

If you’re interested in playing around with Flink CDC, check out our [quick
start]({{< ref "docs/try-flink-cdc" >}}). It provides multiple examples to submit and execute a Flink CDC job on a Flink cluster.

<--->

## Get Help with Flink CDC

If you get stuck, check out our [community support
resources](https://flink.apache.org/community.html). In particular, Apache
Flink’s user mailing list is consistently ranked as one of the most active of
any Apache project, and is a great way to get help quickly.

{{< /columns >}}

Flink CDC is developed under the umbrella of [Apache
Flink](https://flink.apache.org/).
