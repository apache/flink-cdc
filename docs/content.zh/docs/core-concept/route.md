---
title: "Route"
weight: 6
type: docs
aliases:
  - /core-concept/route/
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
**Route** specifies the rule of matching a list of source-table and mapping to sink-table. The most typical scenario is the merge of sub-databases and sub-tables, routing multiple upstream source tables to the same sink table.

# Parameters
To describe a route, the follows are required:  

| parameter      | meaning                                                                                     | optional/required |
|----------------|---------------------------------------------------------------------------------------------|-------------------|
| source-table   | Source table id, supports regular expressions                                               | required          |
| sink-table     | Sink table id, supports symbol replacement                                                  | required          |
| replace-symbol | Special symbol in sink-table for pattern replacing, will be replaced by original table name | optional          |
| description    | Routing rule description(a default value provided)                                          | optional          |

A route module can contain a list of source-table/sink-table rules.

# Example
## Route one Data Source table to one Data Sink table
if synchronize the table `web_order` in the database `mydb` to a Doris table `ods_web_order`, we can use this yaml file to define this route：

```yaml
route:
  - source-table: mydb.web_order
    sink-table: mydb.ods_web_order
    description: sync table to one destination table with given prefix ods_
```

## Route multiple Data Source tables to one Data Sink table
What's more, if you want to synchronize the sharding tables in the database `mydb` to a Doris table `ods_web_order`, we can use this yaml file to define this route：
```yaml
route:
  - source-table: mydb\.*
    sink-table: mydb.ods_web_order
    description: sync sharding tables to one destination table
```

## Complex Route via combining route rules
What's more, if you want to specify many different mapping rules, we can use this yaml file to define this route：
```yaml
route:
  - source-table: mydb.orders
    sink-table: ods_db.ods_orders
    description: sync orders table to orders
  - source-table: mydb.shipments
    sink-table: ods_db.ods_shipments
    description: sync shipments table to ods_shipments
  - source-table: mydb.products
    sink-table: ods_db.ods_products
    description: sync products table to ods_products
```

## Pattern Replacement in routing rules

If you'd like to route source tables and rename them to sink tables with specific patterns, `replace-symbol` could be used to resemble source table names like this:

```yaml
route:
  - source-table: source_db.\.*
    sink-table: sink_db.<>
    replace-symbol: <>
    description: route all tables in source_db to sink_db
```

Then, all tables including `source_db.XXX` will be routed to `sink_db.XXX` without hassle.