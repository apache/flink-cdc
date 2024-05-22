---
title: "Standalone"
weight: 1
type: docs
aliases:
  - /deployment/standalone
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

# Introduction
Standalone mode is Flink’s simplest deployment mode. This short guide will show you how to download the latest stable version of Flink, install, and run it.     
You will also run an example Flink CDC job and view it in the web UI.


## Preparation

Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows).  
You can refer [overview]({{< ref "docs/connectors/pipeline-connectors/overview" >}}) to check supported versions and download [the binary release](https://flink.apache.org/downloads/) of Flink,
then extract the archive:

```bash
tar -xzf flink-*.tgz
```

You should set `FLINK_HOME` environment variables like:

```bash
export FLINK_HOME=/path/flink-*
```

### Start and stop a local cluster

To start a local cluster, run the bash script that comes with Flink:

```bash
cd /path/flink-*
./bin/start-cluster.sh
```

Flink is now running as a background process. You can check its status with the following command:

```bash
ps aux | grep flink
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081) to view
the Flink dashboard and see that the cluster is up and running.

To quickly stop the cluster and all running components, you can use the provided script:

```bash
./bin/stop-cluster.sh
```

## Set up Flink CDC
Download the tar file of Flink CDC from [release page](https://github.com/apache/flink-cdc/releases), then extract the archive:

```bash
tar -xzf flink-cdc-*.tar.gz
```

Extracted `flink-cdc` contains four directories: `bin`,`lib`,`log` and `conf`.

Download the connector jars from [release page](https://github.com/apache/flink-cdc/releases), and move it to the `lib` directory.    
Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on specific branch by yourself.


## Submit a Flink CDC Job
Here is an example file for synchronizing the entire database `mysql-to-doris.yaml`：

```yaml
################################################################################
# Description: Sync MySQL all tables to Doris
################################################################################
source:
 type: mysql
 hostname: localhost
 port: 3306
 username: root
 password: 123456
 tables: app_db.\.*
 server-id: 5400-5404
 server-time-zone: UTC

sink:
 type: doris
 fenodes: 127.0.0.1:8030
 username: root
 password: ""

pipeline:
 name: Sync MySQL Database to Doris
 parallelism: 2
```

You need to modify the configuration file according to your needs, refer to connectors more information.
- [MySQL pipeline connector]({{< ref "docs/connectors/pipeline-connectors/mysql.md" >}})
- [Apache Doris pipeline connector]({{< ref "docs/connectors/pipeline-connectors/doris.md" >}})

Finally, submit job to Flink Standalone cluster using Cli.

```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh mysql-to-doris.yaml
```

After successful submission, the return information is as follows：

```bash
Pipeline has been submitted to cluster.
Job ID: ae30f4580f1918bebf16752d4963dc54
Job Description: Sync MySQL Database to Doris
```

Then you can find a job named `Sync MySQL Database to Doris` running through Flink Web UI. 