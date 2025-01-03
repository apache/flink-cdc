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

# Introduction

Kubernetes is a popular container-orchestration system for automating computer application deployment, scaling, and management.
Flink's native Kubernetes integration allows you to directly deploy Flink on a running Kubernetes cluster.
Moreover, Flink is able to dynamically allocate and de-allocate TaskManagers depending on the required resources because it can directly talk to Kubernetes.

Apache Flink also provides a Kubernetes operator for managing Flink clusters on Kubernetes. It supports both standalone and native deployment mode and greatly simplifies deployment, configuration and the life cycle management of Flink resources on Kubernetes.

For more information, please refer to the [Flink Kubernetes Operator documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/).

## Preparation

The doc assumes a running Kubernetes cluster fulfilling the following requirements:

- Kubernetes >= 1.9.
- KubeConfig, which has access to list, create, delete pods and services, configurable via `~/.kube/config`. You can verify permissions by running `kubectl auth can-i <list|create|edit|delete> pods`.
- Enabled Kubernetes DNS.
- `default` service account with [RBAC](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#rbac) permissions to create, delete pods.

If you have problems setting up a Kubernetes cluster, please take a look at [how to setup a Kubernetes cluster](https://kubernetes.io/docs/setup/).

## Session Mode

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

### Start a session cluster

To start a session cluster on k8s, run the bash script that comes with Flink:

```bash
cd /path/flink-*
./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster
```

After successful startup, the return information is as follows：

```
org.apache.flink.kubernetes.utils.KubernetesUtils            [] - Kubernetes deployment requires a fixed port. Configuration blob.server.port will be set to 6124
org.apache.flink.kubernetes.utils.KubernetesUtils            [] - Kubernetes deployment requires a fixed port. Configuration taskmanager.rpc.port will be set to 6122
org.apache.flink.kubernetes.KubernetesClusterDescriptor      [] - Please note that Flink client operations(e.g. cancel, list, stop, savepoint, etc.) won't work from outside the Kubernetes cluster since 'kubernetes.rest-service.exposed.type' has been set to ClusterIP.
org.apache.flink.kubernetes.KubernetesClusterDescriptor      [] - Create flink session cluster my-first-flink-cluster successfully, JobManager Web Interface: http://my-first-flink-cluster-rest.default:8081
```

{{< hint info >}}  
please refer to [Flink documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#accessing-flinks-web-ui) to expose Flink’s Web UI and REST endpoint.   
You should ensure that REST endpoint can be accessed by the node of your submission.  
{{< /hint >}}  
Then, you need to add these two config to your flink-conf.yaml:

```yaml
rest.bind-port: {{REST_PORT}}
rest.address: {{NODE_IP}}
```

{{REST_PORT}} and {{NODE_IP}} should be replaced by the actual values of your JobManager Web Interface.

### Set up Flink CDC
Download the tar file of Flink CDC from [release page](https://github.com/apache/flink-cdc/releases), then extract the archive:

```bash
tar -xzf flink-cdc-*.tar.gz
```

Extracted `flink-cdc` contains four directories: `bin`,`lib`,`log` and `conf`.

Download the connector jars from [release page](https://github.com/apache/flink-cdc/releases), and move it to the `lib` directory.    
Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on specific branch by yourself.

### Submit a Flink CDC Job
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

## Kubernetes Operator Mode
The doc assumes a [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/) has been deployed on your K8s cluster, then you only need to build a Docker image of Flink CDC.

### Build a custom Docker image
1. Download the tar file of Flink CDC and needed connectors from [release page](https://github.com/apache/flink-cdc/releases), then move them to the docker image build directory.  
   Assume that your docker image build directory is `/opt/docker/flink-cdc`, The structure of this directory is as follow：
    ```text
    /opt/docker/flink-cdc
        ├── flink-cdc-{{< param Version >}}-bin.tar.gz
        ├── flink-cdc-pipeline-connector-doris-{{< param Version >}}.jar
        ├── flink-cdc-pipeline-connector-mysql-{{< param Version >}}.jar
        ├── mysql-connector-java-8.0.27.jar
        └── ...
    ```
2. Create a Dockerfile to build a custom image from the `flink` official image and add Flink CDC dependencies.
    ```shell script
    FROM flink:1.18.0-java8
    ADD *.jar $FLINK_HOME/lib/
    ADD flink-cdc*.tar.gz $FLINK_HOME/
    RUN mv $FLINK_HOME/flink-cdc-{{< param Version >}}/lib/flink-cdc-dist-{{< param Version >}}.jar $FLINK_HOME/lib/
    ```
   Finally, The structure is as follow：
    ```text
    /opt/docker/flink-cdc
        ├── Dockerfile
        ├── flink-cdc-{{< param Version >}}-bin.tar.gz
        ├── flink-cdc-pipeline-connector-doris-{{< param Version >}}.jar
        ├── flink-cdc-pipeline-connector-mysql-{{< param Version >}}.jar
        ├── mysql-connector-java-8.0.27.jar
        └── ...
    ```
3. Build the custom Docker image then push.
    ```bash
   docker build -t flink-cdc-pipeline:{{< param Version >}} .
   
   docker push flink-cdc-pipeline:{{< param Version >}}
   ```

### Create a ConfigMap for mounting Flink CDC configuration files
Here is an example file, please change the connection parameters into your actual values:
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

### Create a FlinkDeployment YAML
Here is an example file `flink-cdc-pipeline-job.yaml`：
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
    jarURI: 'local:///opt/flink/lib/flink-cdc-dist-{{< param Version >}}.jar'
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
1. Due to Flink's class loader, the parameter of `classloader.resolve-order` must be `parent-first`.
2. Flink CDC submits a job to a remote Flink cluster by default, you should start a Standalone Flink cluster in the pod by `--use-mini-cluster` in Operator mode.  
{{< /hint >}}

### Submit a Flink CDC Job
After the ConfigMap and FlinkDeployment YAML are created, you can submit the Flink CDC job to the Operator through kubectl like：
```bash
kubectl apply -f flink-cdc-pipeline-job.yaml
```

After successful submission, the return information is as follows：
```shell
flinkdeployment.flink.apache.org/flink-cdc-pipeline-job created
```
If you want to trace the logs or expose the Flink Web UI, please refer to: [Flink Kubernetes Operator documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/concepts/overview/)。

{{< hint info >}}  
Please note that submitting with **native application mode** is not supported for now.  
{{< /hint >}}