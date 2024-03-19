---
title: "Table ID"
weight: 4
type: docs
aliases:
  - /core-concept/table-id/
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
When connecting to external systems, it is necessary to establish a mapping relationship with the storage objects of the external system. This is what **Table Id** refers to.

# Example
To be compatible with most external systems, the Table Id is represented by a 3-tuple : (namespace, schemaName, tableName).   
Connectors should establish the mapping between Table Id and storage objects in external systems.

The following table lists the parts in table Id of different data systems:

| data system           | parts in tableId         | String example      |
|-----------------------|--------------------------|---------------------|
| Oracle/PostgreSQL     | database, schema, table  | mydb.default.orders |
| MySQL/Doris/StarRocks | database, table          | mydb.orders         |
| Kafka                 | topic                    | orders              |
