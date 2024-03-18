---
title: "Data Sink"
weight: 3
type: docs
aliases:
  - /core-concept/data-sink/
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

# Definition
**Data Sink** is used to apply schema changes and write change data to external systems.    
A Data Sink can write to multiple tables simultaneously.

# Parameters
To describe a data sink, the follows are required:

| parameter                   | meaning                                                                                         | optional/required |
|-----------------------------|-------------------------------------------------------------------------------------------------|-------------------|
| type                        | The type of the sink, such as doris or starrocks.                                               | required          |
| name                        | The name of the sink, which is user-defined (a default value provided).                         | optional          |
| configurations of Data Sink | Configurations to build the Data Sink e.g. connection configurations and sink table properties. | optional          |

# Example
We could use this yaml file to define a doris sink:
```yaml
sink:
    type: doris
    name: doris-sink           	# Optional parameter for description purpose
    fenodes: 127.0.0.1:8030
    username: root
    password: ""
    table.create.properties.replication_num: 1      	# Optional parameter for advanced functionalities
```