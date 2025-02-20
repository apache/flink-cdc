---
title: "CdcUp Quickstart Guide"
weight: 3
type: docs
aliases:
- /get-started/quickstart/cdc-up-quickstart-guide
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

# Setting-up a CDC Pipeline Environment with CdcUp CLI

Flink CDC now provides a CdcUp CLI utility to start a playground environment and run Flink CDC jobs quickly.

You will need a working Docker and Docker Compose V2 environment to use it.

## Step-by-step Guide

1. Open a terminal and run `git clone https://github.com/apache/flink-cdc.git --depth=1` to retrieve a copy of Flink CDC source code.

2. Run `cd flink-cdc/tools/cdcup/` to enter CDC directory and run `./cdcup.sh` to use the "cdc-up" tool to start a playground environment. You should see the following output:

```
Usage: ./cdcup.sh { init | up | pipeline <yaml> | flink | mysql | stop | down | help }

Commands:
    * init:
        Initialize a playground environment, and generate configuration files.

    * up:
        Start docker containers. This may take a while before database is ready.

    * pipeline <yaml>:
        Submit a YAML pipeline job.

    * flink:
        Print Flink Web dashboard URL.

    * mysql:
        Open MySQL console.

    * stop:
        Stop all running playground containers.

    * down:
        Stop and remove containers, networks, and volumes.

    * help:
        Print this message.
```

3. Run `./cdcup.sh init`, waiting for pulling base docker image, and you should see the following output:

```
🎉 Welcome to cdc-up quickstart wizard!
   There are a few questions to ask before getting started:
🐿️ Which Flink version would you like to use? (Press ↑/↓ arrow to move and Enter to select)
  1.17.2
  1.18.1
  1.19.2
‣ 1.20.1
```

Use the arrow keys to navigate and press Enter to select specified Flink and CDC version, source, and sink connectors.

4. Run `./cdcup.sh up` to boot-up docker containers, and wait for them to be ready. Some sink connectors (like Doris and StarRocks) need some time to initialize and ready for handling requests.

5. If you're choosing MySQL as data source, you need to run `./cdcup.sh mysql` to open a MySQL session and create at least one table.

For example, the following SQL commands create a database and a table, insert some test data, and verify the result:

```sql
-- initialize db and table
CREATE DATABASE cdc_playground;
USE cdc_playground;
CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(32));

-- insert test data
INSERT INTO test_table VALUES (1, 'alice'), (2, 'bob'), (3, 'cicada'), (4, 'derrida');

-- verify if it has been successfully inserted
SELECT * FROM test_table;
```

6. Run `./cdcup.sh pipeline pipeline-definition.yaml` to submit the pipeline job. You may also edit the pipeline definition file for further configurations.

7. Run `./cdcup.sh flink` and navigate to the printed URL to access the Flink Web UI.

```
$ ./cdcup.sh flink
🚩 Visit Flink Dashboard at: http://localhost:33448
```