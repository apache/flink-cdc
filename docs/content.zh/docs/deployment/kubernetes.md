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

# 简介

Kubernetes（K8s）是一种流行的容器编排系统，用于自动化部署、扩展和管理应用程序。Flink 的原生 Kubernetes 集成允许您直接在正在运行的 Kubernetes 集群上部署 Flink。此外，Flink 能够根据所需资源动态分配和取消分配 TaskManager，因为它可以直接与 Kubernetes 通信。

Apache Flink 还提供了 Kubernetes Operator，用于管理 Kubernetes 上的 Flink 集群。它支持独立部署和原生部署模式，极大简化了 Flink 在 Kubernetes 上的部署、配置和生命周期管理。

更多信息请参考：[Flink Kubernetes Operator 文档](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/)。

## 准备

假设您正在运行的 Kubernetes 集群满足以下要求：

- Kubernetes 版本 >= 1.9。
- KubeConfig，作为列出、创建、删除 pods 和 services 权限的入口，可通过 `~/.kube/config` 进行配置。您可以通过运行命令：`kubectl auth can-i <list|create|edit|delete> pods` 来验证权限。
- 已启用 Kubernetes DNS。
- `default` 用户具有创建、删除 pods 的 [RBAC](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#rbac) 权限。

如果您在配置 Kubernetes 集群时遇到问题，请参考：[如何配置 Kubernetes 集群](https://kubernetes.io/docs/setup/)。

## Session 模式 {#session模式}

Flink 可以在所有类 UNIX 环境上运行，即 Linux、Mac OS X 和 Cygwin（适用于 Windows）。\
您可以参考 [overview]({{< ref "docs/connectors/pipeline-connectors/overview" >}})页面，查看支持的 Flink 版本并下载[发行包](https://flink.apache.org/downloads/)，然后解压：

```bash
tar -xzf flink-*.tgz
```

设置 `FLINK_HOME` 环境变量：

```bash
export FLINK_HOME=/path/flink-*
```

### 启动 Session 集群 {#启动session集群}

要在 k8s 上启动 Session 集群，请运行 Flink 附带的 bash 脚本：

```bash
cd /path/flink-*
./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster
```

成功启动集群后，返回如下信息：

```
org.apache.flink.kubernetes.utils.KubernetesUtils            [] - Kubernetes deployment requires a fixed port. Configuration blob.server.port will be set to 6124
org.apache.flink.kubernetes.utils.KubernetesUtils            [] - Kubernetes deployment requires a fixed port. Configuration taskmanager.rpc.port will be set to 6122
org.apache.flink.kubernetes.KubernetesClusterDescriptor      [] - Please note that Flink client operations(e.g. cancel, list, stop, savepoint, etc.) won't work from outside the Kubernetes cluster since 'kubernetes.rest-service.exposed.type' has been set to ClusterIP.
org.apache.flink.kubernetes.KubernetesClusterDescriptor      [] - Create flink session cluster my-first-flink-cluster successfully, JobManager Web Interface: http://my-first-flink-cluster-rest.default:8081
```

{{< hint info >}}  
请参考 [Flink 文档](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui)来暴露 Flink Web UI 和 REST 端口。\
请确保您提交作业的节点可以访问 REST 端口。\
{{< /hint >}}  
然后，将以下两个配置添加到 flink-conf.yaml 中：

```yaml
rest.bind-port: {{REST_PORT}}
rest.address: {{NODE_IP}}
```

{{REST_PORT}} 和 {{NODE_IP}} 替换为 JobManager Web 界面的实际值。

### 配置 Flink CDC {#配置flink-cdc}
从[发行页面](https://github.com/apache/flink-cdc/releases)下载 Flink CDC 的 tar 文件并解压：

```bash
tar -xzf flink-cdc-*.tar.gz
```

解压后的 `flink-cdc` 包含四个目录：`bin`，`lib`，`log` 和 `conf`。

从[发行页面](https://github.com/apache/flink-cdc/releases)下载连接器，并移动到 `lib` 路径下。\
下载链接仅适用于稳定版本，SNAPSHOT 依赖项需要您根据特定分支自行构建。

### 提交 Flink CDC 作业 {#提交flink-cdc作业}
以下是 mysql 整库同步到 doris 的示例配置文件：`mysql-to-doris.yaml`

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

请参考连接器信息，按需修改配置文件。
- [MySQL Pipeline 连接器]({{< ref "docs/connectors/pipeline-connectors/mysql.md" >}})
- [Apache Doris Pipeline 连接器]({{< ref "docs/connectors/pipeline-connectors/doris.md" >}})

最后，通过 CLI 将作业提交到 Flink Standalone 集群。

```bash
cd /path/flink-cdc-*
./bin/flink-cdc.sh mysql-to-doris.yaml
```

成功提交作业后，返回如下信息：

```bash
Pipeline has been submitted to cluster.
Job ID: ae30f4580f1918bebf16752d4963dc54
Job Description: Sync MySQL Database to Doris
```

通过 Flink Web UI，您可以找到一个名为 `Sync MySQL Database to Doris` 的作业正在运行。

## Kubernetes Operator 模式 {#kubernetes-operator模式}
假设您已经在 K8s 集群上部署 [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/)，您只需构建自定义的 Flink CDC Docker 镜像即可。

### 构建自定义 Docker 镜像 {#构建自定义docker镜像}
1. 从[发行页面](https://github.com/apache/flink-cdc/releases)下载 Flink CDC 的 tar 文件和需要的连接器，并移动到 Docker 镜像构建目录。\
   假设您的 Docker 构建目录为 `/opt/docker/flink-cdc`，此时该目录下的文件结构如下：
    ```text
    /opt/docker/flink-cdc
        ├── flink-cdc-{{< param Version >}}-bin.tar.gz
        ├── flink-cdc-pipeline-connector-doris-{{< param Version >}}.jar
        ├── flink-cdc-pipeline-connector-mysql-{{< param Version >}}.jar
        ├── mysql-connector-java-8.0.27.jar
        └── ...
    ```
2. 创建 Dockerfile 文件，从官方 `flink` 镜像构建出自定义镜像并添加 Flink CDC 的依赖。
    ```shell script
    FROM flink:1.18.0-java8
    ADD *.jar $FLINK_HOME/lib/
    ADD flink-cdc*.tar.gz $FLINK_HOME/
    RUN mv $FLINK_HOME/flink-cdc-{{< param Version >}}/lib/flink-cdc-dist-{{< param Version >}}.jar $FLINK_HOME/lib/
    ```
   Docker 镜像构建目录最终如下：
    ```text
    /opt/docker/flink-cdc
        ├── Dockerfile
        ├── flink-cdc-{{< param Version >}}-bin.tar.gz
        ├── flink-cdc-pipeline-connector-doris-{{< param Version >}}.jar
        ├── flink-cdc-pipeline-connector-mysql-{{< param Version >}}.jar
        ├── mysql-connector-java-8.0.27.jar
        └── ...
    ```
3. 构建自定义镜像并推送至仓库
    ```bash
   docker build -t flink-cdc-pipeline:{{< param Version >}} .
   
   docker push flink-cdc-pipeline:{{< param Version >}}
   ```

### 创建 ConfigMap 用于挂载 Flink CDC 配置文件 {#创建configmap用于挂载flink-cdc配置文件}
以下是一个示例文件，请修改其中对应的连接参数为实际值：
```yaml
---
apiVersion: v1
data:
  flink-cdc.yaml: |-
      parallelism: 4
      schema.change.behavior: EVOLVE
  mysql-to-doris.yaml: |-
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
kind: ConfigMap
metadata:
  name: flink-cdc-pipeline-configmap
```

### 创建 FlinkDeployment YAML 文件 {#创建flinkdeployment-yaml文件}
以下是示例文件 `flink-cdc-pipeline-job.yaml`：
```yaml
---
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-cdc-pipeline-job
spec:
  flinkConfiguration:
    classloader.resolve-order: parent-first
    state.checkpoints.dir: 'file:///tmp/checkpoints'
    state.savepoints.dir: 'file:///tmp/savepoints'
  flinkVersion: v1_18
  image: 'flink-cdc-pipeline:{{< param Version >}}'
  imagePullPolicy: Always
  job:
    args:
      - '--use-mini-cluster'
      - /opt/flink/flink-cdc-{{< param Version >}}/conf/mysql-to-doris.yaml
    entryClass: org.apache.flink.cdc.cli.CliFrontend
    jarURI: 'local:///opt/flink/flink-cdc-{{< param Version >}}/lib/flink-cdc-dist-{{< param Version >}}.jar'
    parallelism: 1
    state: running
    upgradeMode: savepoint
  jobManager:
    replicas: 1
    resource:
      cpu: 1
      memory: 1024m
  podTemplate:
    apiVersion: v1
    kind: Pod
    spec:
      containers:
        # don't modify this name
        - name: flink-main-container
          volumeMounts:
            - mountPath: /opt/flink/flink-cdc-{{< param Version >}}/conf
              name: flink-cdc-pipeline-config
      volumes:
        - configMap:
            name: flink-cdc-pipeline-configmap
          name: flink-cdc-pipeline-config
  restartNonce: 0
  serviceAccount: flink
  taskManager:
    resource:
      cpu: 1
      memory: 1024m
```
{{< hint info >}}
1. 由于 Flink 的类加载机制，参数 `classloader.resolve-order` 必须设置为 `parent-first`。
2. Flink CDC 默认提交作业到远程 Flink 集群，在 Operator 模式下，您需要通过指定 `--use-mini-cluster` 参数在 pod 内部启动一个 Standalone Flink 集群。\
{{< /hint >}}

### 提交 Flink CDC 作业 {#提交flink-cdc作业-1}
ConfigMap 和 FlinkDeployment YAML 文件创建完成后，即可通过 kubectl 提交作业到 Operator：
```bash
kubectl apply -f flink-cdc-pipeline-job.yaml
```

成功提交作业后，返回信息如下：
```shell
flinkdeployment.flink.apache.org/flink-cdc-pipeline-job created
```
如您需要查看日志、暴露 Flink Web UI 等，请参考：[Flink Kubernetes Operator 文档](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/)。


{{< hint info >}}  
请注意，目前不支持使用**native application mode**提交作业。  
{{< /hint >}}