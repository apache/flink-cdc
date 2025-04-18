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

# 介绍

[Apache Hadoop YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) 是众多数据处理框架所青睐的资源提供者。
Flink 服务被提交至 YARN 的 ResourceManager，后者会在由 YARN NodeManager 管理的机器上生成 container。Flink 将其 JobManager 和 TaskManager 实例部署到这些 container 中。

Flink 可以根据在 JobManager 上运行的作业处理所需的 slot 数量，动态分配和释放 TaskManager 资源。

## 准备工作

此“入门指南”部分假定从 2.10.2 版本起具备功能可用的 YARN 环境。YARN 环境可以通过像亚马逊 EMR、谷歌云 DataProc 等服务或 Cloudera 等产品来搭建。不建议在[本地](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html)或[集群](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html)上手动设置 YARN 环境来完成本入门教程。

- 通过运行 `yarn top` 无错误信息显示以确保你的 YARN 集群准备好接受 Flink 应用程序的提交。
- 从[下载页面](https://flink.apache.org/downloads/)下载最新的 Flink 发行版并解压缩。
- 一定要确保设置了 `HADOOP_CLASSPATH` 环境变量（可以通过运行 `echo $HADOOP_CLASSPATH` 来检查）。如果没有，请使用以下命令进行设置。

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```
Flink 在所有类 UNIX 的环境中运行，即在 Linux、Mac OS X 以及（针对 Windows 的）Cygwin 上运行。
你可以参考[概览]({{< ref "docs/connectors/pipeline-connectors/overview" >}})来检查支持的版本并下载[Flink二进制版本](https://flink.apache.org/downloads/)，
然后解压文件：
```bash
tar -xzf flink-*.tgz
```
你需要设置 `FLINK_HOME` 环境变量，比如：

```bash
export FLINK_HOME=/path/flink-*
```

## Session 模式

### 在 YARN 启动一个Flink Session

确保已设置 `HADOOP_CLASSPATH` 环境变量，即可在 YARN 会话启动一个 Flink 任务：

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

启动 Yarn 会话之后，即可通过命令输出最后一行打印的 URL 或者 YARN ResourceManager Web UI 访问 Flink Web UI。

然后，需要向 flink-conf.yaml 添加一些配置：

```yaml
rest.bind-port: {{REST_PORT}}
rest.address: {{NODE_IP}}
execution.target: yarn-session
yarn.application.id: {{YARN_APPLICATION_ID}}
```

{{REST_PORT}} 和 {{NODE_IP}} 需要替换为 JobManager Web 接口的实际值，{{YARN_APPLICATION_ID}} 则需替换为 Flink 实际的 Yarn 应用 ID。

### 配置 Flink CDC
从[发布页面](https://github.com/apache/flink-cdc/releases)下载 Flink CDC 的 tar 文件，然后提取该归档文件：

```bash
tar -xzf flink-cdc-*.tar.gz
```

提取后的 `flink-cdc` 包含四个目录: `bin`，`lib`，`log` 和 `conf`。

从[发布页面](https://github.com/apache/flink-cdc/releases)下载连接器 jar，并将其移动至 `lib` 目录中。
下载链接仅适用于稳定版本，SNAPSHOT 依赖项需要自己基于特定分支进行构建。

### 提交 Flink CDC 任务
下面是一个用于整库同步的示例文件 `mysql-to-doris.yaml`：

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

你可以按需修改配置文件。
最后，通过 Cli 将作业提交至 Flink Yarn Session 集群。

```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh mysql-to-doris.yaml
```

提交成功将返回如下信息：

```bash
Pipeline has been submitted to cluster.
Job ID: ae30f4580f1918bebf16752d4963dc54
Job Description: Sync MySQL Database to Doris
```

你可以通过 Flink Web UI 找到一个名为 `Sync MySQL Database to Doris` 的作业。

# Yarn Application 模式
Yarn Application 模式是 Yarn 集群上运行 Flink 作业的推荐模式。对资源的管理和分配更加灵活，可以更好地利用集群资源。

通过Cli将作业提交至 Flink Yarn Application 集群。
```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh -t yarn-application -Dexecution.checkpointing.interval=2min mysql-to-doris.yaml
```
或者从savepoint恢复Flink-CDC作业：
```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh -t yarn-application -s hdfs:///flink/savepoint-1537 -Dexecution.checkpointing.interval=2min mysql-to-doris.yaml
```
提交成功将返回如下信息：
```bash
Pipeline has been submitted to cluster.
Job ID: application_1728995081590_1254
Job Description: submit job successful
```
你可以通过 Yarn Web UI 找到一个application_id为 `application_1728995081590_1254` 的作业。
