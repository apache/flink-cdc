---
title: "Kubernetes"
weight: 3
type: docs
aliases:
  - /deployment/kubernetes
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

Kubernetes 是一个流行的容器编排系统，用于自动化计算机应用程序的部署、扩展和管理。  
Flink 的原生 Kubernetes 集成允许你直接在运行中的 Kubernetes 集群上部署 Flink。  
此外，因为 Flink 可以直接与 Kubernetes 进行通信，所以 Flink 它能够根据所需资源动态分配和释放 TaskManagers。

Apache Flink 还提供了一个 Kubernetes 操作器，用于在 Kubernetes 上管理 Flink 集群。它支持独立和原生部署模式，大大简化了在 Kubernetes 上部署、配置和管理 Flink 资源的生命周期。

有关更多信息，请参考 [Flink Kubernetes Operator 文档](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/zh/docs/concepts/overview/)。

## 准备工作

本文档假设你有一个运行中的 Kubernetes 集群，满足以下要求：

- Kubernetes >= 1.9。
- KubeConfig，能够列出、创建和删除 pods 和 services，配置在 `~/.kube/config` 中。你可以通过运行 `kubectl auth can-i <list|create|edit|delete> pods` 来验证权限。
- 启用 Kubernetes DNS。
- `default` 服务账户具有 [RBAC](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/deployment/resource-providers/native_kubernetes/) 创建和删除 pods 的权限。

如果你在设置 Kubernetes 集群时遇到问题，请查看 [如何设置 Kubernetes 集群](https://kubernetes.io/zh-cn/docs/setup/)。

## Session 模式

Flink 可以在所有类 UNIX 环境中运行，即 Linux、Mac OS X 和 Cygwin（适用于 Windows）。  
你可以参考 [概述]({{< ref "docs/connectors/pipeline-connectors/overview" >}}) 查看支持的版本并下载 [Flink 的二进制发行版](https://flink.apache.org/downloads/)，然后解压归档文件：

```bash
tar -xzf flink-*.tgz
```

你应该设置 `FLINK_HOME` 环境变量，如下所示：

```bash
export FLINK_HOME=/path/flink-*
```

### 启动 Session 集群

如果要在 Kubernetes 上启动 Session 集群，请运行附带 Flink 的 bash 脚本：

```bash
cd /path/flink-*
./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster
```

成功启动后，返回信息如下：

```
org.apache.flink.kubernetes.utils.KubernetesUtils            [] - Kubernetes deployment requires a fixed port. Configuration blob.server.port will be set to 6124
org.apache.flink.kubernetes.utils.KubernetesUtils            [] - Kubernetes deployment requires a fixed port. Configuration taskmanager.rpc.port will be set to 6122
org.apache.flink.kubernetes.KubernetesClusterDescriptor      [] - Please note that Flink client operations(e.g. cancel, list, stop, savepoint, etc.) won't work from outside the Kubernetes cluster since 'kubernetes.rest-service.exposed.type' has been set to ClusterIP.
org.apache.flink.kubernetes.KubernetesClusterDescriptor      [] - Create flink session cluster my-first-flink-cluster successfully, JobManager Web Interface: http://my-first-flink-cluster-rest.default:8081
```

{{< hint info >}}
请参考 [Flink 文档](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui) 来暴露 Flink 的 Web UI 和 REST 端点。  
你应该确保 REST 端点可以被用来提交节点访问。
{{< /hint >}}

接下来，你需要将以下两个配置添加到你的 flink-conf.yaml 中：

```yaml
rest.bind-port: {{REST_PORT}}
rest.address: {{NODE_IP}}
```

{{REST_PORT}} 和 {{NODE_IP}} 应替换为你的 JobManager Web 界面的实际值。

### 设置 Flink CDC

从 [发布页面](https://github.com/apache/flink-cdc/releases) 下载 Flink CDC 的 tar 文件，然后解压归档文件：

```bash
tar -xzf flink-cdc-*.tar.gz
```

解压后的 `flink-cdc` 包含四个目录：`bin`、`lib`、`log` 和 `conf`。

从 [发布页面](https://github.com/apache/flink-cdc/releases) 下载连接器 jar 文件，并将其移动到 `lib` 目录。  
下载链接仅适用于稳定版本，SNAPSHOT 依赖项需要根据特定分支自行构建。

### 提交 Flink CDC 作业

以下是一个示例文件，用于同步整个数据库 `mysql-to-doris.yaml`：

```yaml
################################################################################
# 描述：将 MySQL 所有表同步到 Doris
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

你需要根据自己的需求修改配置文件，更多信息请参考连接器。
- [MySQL 管道连接器]({{< ref "docs/connectors/pipeline-connectors/mysql.md" >}})
- [Apache Doris 管道连接器]({{< ref "docs/connectors/pipeline-connectors/doris.md" >}})

最后，使用 CLI 向 Flink Standalone 集群提交作业。

```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh mysql-to-doris.yaml
```

成功提交后，返回信息如下：

```bash
Pipeline has been submitted to cluster.
Job ID: ae30f4580f1918bebf16752d4963dc54
Job Description: Sync MySQL Database to Doris
```

然后你可以通过 Flink Web UI 找到一个正在运行的作业，名字为 `Sync MySQL Database to Doris`。

{{< hint info >}}
请注意，当前不支持使用 **native application mode** 和 **Flink Kubernetes operator** 提交作业。
{{< /hint >}}