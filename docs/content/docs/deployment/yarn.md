---
title: "YARN"
weight: 2
type: docs
aliases:
  - /deployment/yarn
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

[Apache Hadoop YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) is a resource provider popular with many data processing frameworks.
Flink services are submitted to YARN's ResourceManager, which spawns containers on machines managed by YARN NodeManagers. Flink deploys its JobManager and TaskManager instances into such containers.

Flink can dynamically allocate and de-allocate TaskManager resources depending on the number of processing slots required by the job(s) running on the JobManager.

## Preparation

This *Getting Started* section assumes a functional YARN environment, starting from version 2.10.2. YARN environments are provided most conveniently through services such as Amazon EMR, Google Cloud DataProc or products like Cloudera. [Manually setting up a YARN environment locally](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) or [on a cluster](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html) is not recommended for following through this *Getting Started* tutorial.

- Make sure your YARN cluster is ready for accepting Flink applications by running `yarn top`. It should show no error messages.
- Download a recent Flink distribution from the [download page](https://flink.apache.org/downloads/) and unpack it.
- **Important** Make sure that the `HADOOP_CLASSPATH` environment variable is set up (it can be checked by running `echo $HADOOP_CLASSPATH`). If not, set it up using

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```
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

## Session Mode

### Starting a Flink Session on YARN

Once you've made sure that the `HADOOP_CLASSPATH` environment variable is set, you can launch a Flink on YARN session:

```bash
# we assume to be in the root directory of 
# the unzipped Flink distribution

# export HADOOP_CLASSPATH
export HADOOP_CLASSPATH=`hadoop classpath`

# Start YARN session
./bin/yarn-session.sh --detached

# Stop YARN session (replace the application id based 
# on the output of the yarn-session.sh command)
echo "stop" | ./bin/yarn-session.sh -id application_XXXXX_XXX
```

After starting YARN session, you can now access the Flink Web UI through the URL printed in the last lines of the command output, or through the YARN ResourceManager web UI.

Then, you need to add some configs to your flink-conf.yaml:

```yaml
rest.bind-port: {{REST_PORT}}
rest.address: {{NODE_IP}}
execution.target: yarn-session
yarn.application.id: {{YARN_APPLICATION_ID}}
```

{{REST_PORT}} and {{NODE_IP}} should be replaced by the actual values of your JobManager Web Interface, and {{YARN_APPLICATION_ID}} should be replaced by the actual YARN application ID of Flink.

### Set up Flink CDC
Download the tar file of Flink CDC from [release page](https://github.com/apache/flink-cdc/releases), then extract the archive:

```bash
tar -xzf flink-cdc-*.tar.gz
```

Extracted `flink-cdc` contains four directories: `bin`,`lib`,`log` and `conf`.

Download the connector jars from [release page](https://github.com/apache/flink-cdc/releases), and move it to the `lib` directory.    
Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on specific branch by yourself.

### Submit a Flink CDC Job
Here is an example file for synchronizing the entire database `mysql-to-doris.yaml`:

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

You need to modify the configuration file according to your needs.
Finally, submit job to Flink Yarn Session cluster using Cli.

```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh mysql-to-doris.yaml
```

After successful submission, the return information is as follows:

```bash
Pipeline has been submitted to cluster.
Job ID: ae30f4580f1918bebf16752d4963dc54
Job Description: Sync MySQL Database to Doris
```

You can find a job named `Sync MySQL Database to Doris` running through Flink Web UI.

# Yarn Application Mode
The Yarn Application mode is the recommended approach for running Flink jobs on a Yarn cluster. It offers more flexible resource management and allocation, enabling better utilization of cluster resources.

To submit a job to a Flink Yarn Application cluster using the CLI:
```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh -t yarn-application -Dexecution.checkpointing.interval=2min mysql-to-doris.yaml
````
Or resuming Flink-CDC job from Savepoint:
```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh -t yarn-application -s hdfs:///flink/savepoint-1537 -Dexecution.checkpointing.interval=2min mysql-to-doris.yaml
```
After successful submission, the return information is as follows:
```bash
Pipeline has been submitted to cluster.
Job ID: application_1728995081590_1254
Job Description: submit job successful
```
You can find a application_id `application_1728995081590_1254` running through Yarn Web UI.

