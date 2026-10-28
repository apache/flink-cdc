---
title: "AI Function 配置与实时文本标注"
slug: "/docs/examples-and-best-practices/ai-text-labeling"
sidebar_position: 7
hide_title: true
displayed_sidebar: docs
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

# AI Function 配置与实时文本标注

本文介绍如何在 Flink CDC YAML 数据摄入作业中使用 AI Function，
对 MySQL 中的客户反馈进行话题分类、客诉升级风险判断、摘要生成和联系信息脱敏，并将结果写入 Paimon。

## 准备 Flink 环境

准备一台安装了 Docker Compose 和 Java 11 的 Linux 或 macOS 计算机，以及一个 Flink Standalone 集群。
根据下表选择 Flink 发行包及其对应的依赖：

| Flink 发行包 | 所需依赖 |
|--------------|----------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-1.20/flink-cdc-pipeline-connector-paimon-@@VERSION@@-1.20.jar)<br />[AI 模型客户端](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-model-openai-compatible/@@VERSION@@-1.20/flink-cdc-pipeline-model-openai-compatible-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-2.2/flink-cdc-pipeline-connector-paimon-@@VERSION@@-2.2.jar)<br />[AI 模型客户端](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-model-openai-compatible/@@VERSION@@-2.2/flink-cdc-pipeline-model-openai-compatible-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)<br />[面向 Flink 2.1 的 Paimon SQL 2.1.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.1.0/paimon-flink-2.1-2.1.0.jar) |

查询环境中的 Paimon SQL 连接器与 Pipeline 连接器内置的 Paimon 使用相同版本。
下载并解压选定的发行包，将以下路径替换为实际安装目录：

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

将表中的 MySQL 和 Paimon Pipeline 连接器 JAR 放入 `$FLINK_CDC_HOME/lib`。

启动 Flink 前，将表中的 MySQL JDBC 驱动放入 `$FLINK_HOME/lib`。
将 Hadoop JAR 保存为 `$FLINK_HOME/opt/flink-shaded-hadoop.jar`。
在查询环境中，将 Paimon SQL 连接器保存为 `$FLINK_HOME/opt/paimon-sql-connector.jar`。
下文在提交摄入作业和验证查询时，通过 `--jar` 附加 Hadoop 依赖。
如果 `$FLINK_HOME/lib` 中已有该 Hadoop JAR，请在启动集群前将其移至上述 `opt` 路径。
按作业加载该依赖，可以避免它与 Flink 自身命令行所用的依赖发生冲突。

在 `$FLINK_HOME/conf/config.yaml` 中添加或更新以下配置。
如果文件中已有 `execution`、`jobmanager` 和 `taskmanager` 节点，请将配置合并到对应节点中：

```yaml
execution:
  checkpointing:
    interval: 3s
jobmanager:
  memory:
    process:
      size: 2048m
    jvm-metaspace:
      size: 512m
taskmanager:
  memory:
    process:
      size: 2048m
    jvm-metaspace:
      size: 512m
  numberOfTaskSlots: 4
```

Checkpoint 用于提交 Paimon 数据，使其对查询可见。
配置四个 Task Slot，以便摄入作业和验证查询同时运行。
上述内存配置为本流程中的多次验证查询预留空间。
启动集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备模型服务

准备一个支持 OpenAI 兼容 `chat/completions` 协议和 JSON 对象响应的模型服务。
Flink 客户端和 TaskManager 都需要能访问该服务。

将后文配置中的 `endpoint`、`model` 和 `api-key` 替换为服务的实际配置。
`endpoint` 填服务的基础地址，例如 `https://api.example.com/v1`，不填写完整的生成请求路径。
`model` 填该服务提供的模型名称。

凭据通过部署环境的配置机制提供；本例中的值仅为占位符。

## 准备客户反馈

新建一个工作目录，将以下内容保存为 `compose.yaml`：

```yaml
services:
  mysql:
    image: mysql:8.0
    platform: linux/amd64
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: tutorial
      MYSQL_ROOT_HOST: "%"
    command:
      - --server-id=1
      - --log-bin=mysql-bin
      - --binlog-format=ROW
      - --binlog-row-image=FULL
      - --default-time-zone=+00:00
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-ptutorial"]
      interval: 2s
      timeout: 5s
      retries: 30
```

启动 MySQL 并进入客户端：

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

创建源表和两条反馈记录：

```sql
CREATE DATABASE inventory;
USE inventory;

CREATE TABLE feedback (
  id BIGINT NOT NULL,
  content TEXT NOT NULL,
  PRIMARY KEY (id)
);
INSERT INTO feedback VALUES
  (101, 'My blender broke after one day. I will post a public complaint. Email me at alice@example.org.'),
  (102, 'My order arrived three days late. Email me at bob@example.org.');
```

保留客户端，供后续执行变更。Shell 命令请在另一个终端中执行。

## 配置模型与 AI Function

将以下配置保存为 `ai-labeling.yaml`，并填写模型服务配置：

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: inventory.feedback
  server-id: 5400-5404
  server-time-zone: UTC

transform:
  - source-table: inventory.feedback
    projection: >-
      id,
      AI_CLASSIFY('text_model', content, 'quality,delivery,service,other')['category'] AS category,
      AI_COMPLETE('text_model', content, 'Return high if the feedback explicitly threatens escalation or public exposure; otherwise return low. Put only high or low in result.')['result'] AS risk,
      AI_SUMMARIZE('text_model', content, 80)['summary'] AS summary,
      AI_MASK('text_model', content, 'email,phone')['masked_text'] AS masked_content

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: file:///tmp/flink-cdc-ai-labeling/warehouse

pipeline:
  name: AI text labeling during ingestion
  parallelism: 2
  model:
    - name: text_model
      type: openai-compatible
      options:
        model: <model-name>
        endpoint: https://api.example.com/v1
        api-key: <api-key>
        system-prompt: >-
          Use English in generated text. Follow the task-specific JSON shape.
          Never include email addresses or phone numbers in summaries.
          In masked_text, replace each email address or phone number with [REDACTED].
        temperature: 0
        max-tokens: 256
        response-format: json_object
        error-handling-strategy: retry
        retry-num: 3
        retry-fallback-strategy: failover
        retry-backoff-base-interval: 1 s
```

`pipeline.model` 声明模型客户端，Transform 表达式通过其中的 `name` 引用模型：

| 配置项 | 作用 |
|--------|------|
| `name: text_model` | 作业内的模型别名，作为各 AI 函数的第一个参数。 |
| `type: openai-compatible` | 从下载的 JAR 中选择模型客户端实现。 |
| `options.model` | 实际发送给服务的模型名称。 |
| `options.endpoint`、`options.api-key` | 服务基础地址与认证凭据。 |
| `options.system-prompt` | 添加在函数专用提示词之前的公共指令。 |
| `response-format: json_object` | 请求服务返回 JSON 对象。 |
| `retry-num: 3`、`retry-fallback-strategy: failover` | 模型请求失败时最多尝试三次，耗尽后让任务失败。 |

四个函数分别处理以下标注场景：

| 函数 | 任务 | 使用的 JSON 字段 |
|------|------|------------------|
| `AI_CLASSIFY` | 从逗号分隔的话题标签中选择一类。 | `category` |
| `AI_COMPLETE` | 按自定义提示词判断 high/low 客诉升级风险。 | `result` |
| `AI_SUMMARIZE` | 生成不超过 80 个字符的摘要。 | `summary` |
| `AI_MASK` | 替换邮箱地址和电话号码。 | `masked_text` |

文本 AI 函数返回 `VARIANT` 值，`['字段名']` 用于选择所需字段，并直接写入 Paimon 的 `VARIANT` 列。

投影仅保留主键和选定的 AI 输出，不将原始反馈文本及脱敏函数识别到的实体详情写入目标表。

提交作业并记下 Job ID：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" ai-labeling.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

等待初始模型调用和一次成功的 Checkpoint 完成。

## 验证初始标注

下文查询使用 Flink 2.2 SQL Client。Flink 1.20 摄入作业也能写入这些列，
但查询时需要使用支持 Paimon `VARIANT` 的读取端。

启动 SQL Client：

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

创建指向同一仓库的 Catalog，并查询结果：

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG lake WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = 'file:///tmp/flink-cdc-ai-labeling/warehouse',
  'cache-enabled' = 'false'
);
USE CATALOG lake;

SELECT id,
       category,
       risk,
       summary,
       masked_content
FROM inventory.feedback
ORDER BY id;
```

检查输出的结果是否符合预期。

## 验证增量标注

在 MySQL 客户端中，将第一条投诉改为问题已解决的反馈，删除第二条记录，并新增一条配送投诉：

```sql
UPDATE inventory.feedback
SET content = 'The replacement blender works well. Thank you. Email me at alice@example.org.'
WHERE id = 101;
DELETE FROM inventory.feedback WHERE id = 102;
INSERT INTO inventory.feedback VALUES
  (103, 'The courier lost my parcel. Email me at carol@example.org.');
```

等待变更处理及后续 Checkpoint 完成，再次查询 Paimon：

```sql
SELECT id,
       category,
       risk,
       summary,
       masked_content
FROM inventory.feedback
ORDER BY id;
```

检查输出的结果是否符合预期。

## 清理环境

将 `<job-id>` 替换为摄入作业的 Job ID，取消作业并停止本示例的服务：

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```
