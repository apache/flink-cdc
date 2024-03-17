---
title: "Data Source"
weight: 2
type: docs
aliases:
  - /core-concept/data-source/
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
**Data Source** is used to access metadata and read the changed data from external systems.   
A Data Source can read data from multiple tables simultaneously.

# Parameters
To describe a data source, the follows are required:

| parameter                     | meaning                                                                                             | optional/required |
|-------------------------------|-----------------------------------------------------------------------------------------------------|-------------------|
| type                          | The type of the source, such as mysql.                                                              | required          |
| name                          | The name of the source, which is user-defined (a default value provided).                           | optional          |
| configurations of Data Source | Configurations to build the Data Source e.g. connection configurations and source table properties. | optional          |

# Example
We could use yaml files to define a mysql source:
```yaml
source:
    type: mysql
    name: mysql-source   #optional，description information
    host: localhost
    port: 3306
    username: admin
    password: pass
    tables: adb.*, bdb.user_table_[0-9]+, [app|web]_order_\.*
```