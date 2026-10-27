---
title: "AI Function Configuration and Real-time Text Labeling"
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

# AI Function Configuration and Real-time Text Labeling

This article shows how to use AI Functions in a Flink CDC YAML ingestion pipeline
to classify MySQL customer feedback, flag escalation risk, generate summaries,
and mask contact details before writing the results to Paimon.

## Prepare the Flink environment

Prepare a Linux or macOS machine with Docker Compose, Java 11, and a Flink standalone cluster.
Choose a Flink distribution and the corresponding dependencies from the following table:

| Flink distribution | Required dependencies |
|--------------------|-----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Paimon Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-1.20/flink-cdc-pipeline-connector-paimon-@@VERSION@@-1.20.jar)<br />[AI model client](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-model-openai-compatible/@@VERSION@@-1.20/flink-cdc-pipeline-model-openai-compatible-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Paimon Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-2.2/flink-cdc-pipeline-connector-paimon-@@VERSION@@-2.2.jar)<br />[AI model client](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-model-openai-compatible/@@VERSION@@-2.2/flink-cdc-pipeline-model-openai-compatible-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)<br />[Paimon SQL 2.1.0 for Flink 2.1](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.1.0/paimon-flink-2.1-2.1.0.jar) |

The Paimon SQL connector for the query environment matches the Paimon version bundled with the Pipeline connector.
Download and extract the selected distributions, then set the following paths to your installation directories:

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

Place the MySQL and Paimon Pipeline connectors and AI model client JARs from the table in `$FLINK_CDC_HOME/lib`.

Place the MySQL JDBC driver from the table in `$FLINK_HOME/lib` before starting Flink.
Save the Hadoop JAR as `$FLINK_HOME/opt/flink-shaded-hadoop.jar`.
In the query environment, save the Paimon SQL connector as `$FLINK_HOME/opt/paimon-sql-connector.jar`.
The commands below add Hadoop with `--jar` when submitting the pipeline and verification queries.
If this Hadoop JAR is already in `$FLINK_HOME/lib`, move it to the `opt` path above before starting the cluster.
Loading it as a job dependency avoids conflicts with the libraries used by Flink's own CLI.

In `$FLINK_HOME/conf/config.yaml`, add or update the following settings.
Merge them into existing `execution`, `jobmanager`, and `taskmanager` sections if those sections already exist:

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

Checkpointing makes Paimon commits visible to readers.
Four task slots allow the ingestion pipeline and verification queries to run together.
These memory settings accommodate the verification queries in this walkthrough.
Start the cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

The [Flink Web UI](http://localhost:8081/) should show an available TaskManager.

## Prepare the model service

Prepare a model endpoint that supports the OpenAI-compatible `chat/completions` protocol and JSON-object responses.
The Flink client and TaskManagers must be able to reach it.

In the pipeline below, replace `endpoint`, `model`, and `api-key` with the actual service settings.
The endpoint is the service's base URL, such as `https://api.example.com/v1`, rather than the complete completion path.
Use the model name offered by that service.

Supply the credential through your deployment's configuration mechanism.
The value in this example is a placeholder.

## Prepare customer feedback

Save the following content as `compose.yaml` in a new working directory:

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

Start MySQL and open its client:

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

Create the source table and two feedback records:

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

Keep the client open for subsequent changes. Run shell commands in another terminal.

## Configure the model and AI functions

Save the following configuration as `ai-labeling.yaml` and fill in the model service settings:

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

The `pipeline.model` block declares the model client, and the Transform expressions refer to its `name`:

| Setting | Purpose |
|---------|---------|
| `name: text_model` | The local alias passed as the first argument to each AI function. |
| `type: openai-compatible` | Selects the model client implementation from the downloaded JAR. |
| `options.model` | The actual model name sent to the service. |
| `options.endpoint`, `options.api-key` | The base URL and authentication credential. |
| `options.system-prompt` | Shared instructions added before the function-specific prompt. |
| `response-format: json_object` | Requests a JSON-object response from the service. |
| `retry-num: 3`, `retry-fallback-strategy: failover` | Makes at most three attempts for a failed model request, then fails the task. |

The four functions cover the labeling scenarios:

| Function | Task | JSON field used |
|----------|------|-----------------|
| `AI_CLASSIFY` | Choose one of the comma-separated topic labels. | `category` |
| `AI_COMPLETE` | Apply the custom high/low escalation-risk prompt. | `result` |
| `AI_SUMMARIZE` | Produce a summary of at most 80 characters. | `summary` |
| `AI_MASK` | Replace email addresses and phone numbers. | `masked_text` |

Text AI functions return `VARIANT` values. The `['field']` expression selects the required field, which is written directly to a Paimon `VARIANT` column.

Only the primary key and selected AI outputs are projected.
The original feedback text and the masking function's detected-entity details are not written to the target table.

Submit the pipeline and record its Job ID:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" ai-labeling.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Wait for the initial model calls and a successful checkpoint.

## Verify the initial labels

The queries below use the Flink 2.2 SQL Client. A Flink 1.20 ingestion job can write these columns,
but querying them requires a reader with Paimon `VARIANT` support.

Open the SQL Client:

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Create a catalog pointing to the same warehouse and query the results:

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

Check whether the output matches the expected results.

## Verify incremental labeling

In the MySQL client, replace the first complaint with resolved feedback, delete the second record, and add another delivery complaint:

```sql
UPDATE inventory.feedback
SET content = 'The replacement blender works well. Thank you. Email me at alice@example.org.'
WHERE id = 101;
DELETE FROM inventory.feedback WHERE id = 102;
INSERT INTO inventory.feedback VALUES
  (103, 'The courier lost my parcel. Email me at carol@example.org.');
```

After the changes have been processed and a subsequent checkpoint completes, query Paimon again:

```sql
SELECT id,
       category,
       risk,
       summary,
       masked_content
FROM inventory.feedback
ORDER BY id;
```

Check whether the output matches the expected results.

## Clean up

Replace `<job-id>` with the ingestion Job ID, cancel the job, and stop the tutorial services:

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```

The warehouse remains under `/tmp/flink-cdc-ai-labeling/warehouse`.
Use an empty warehouse when starting this example again from scratch.
