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

# Route Mode
By default, all matching route rules are applied to a table. You can configure the `route-mode` option in the pipeline configuration to change this behavior:

| Value        | Description                                                            |
|--------------|------------------------------------------------------------------------|
| `ALL_MATCH`  | Apply all matching route rules to a table. This is the default mode.  |
| `FIRST_MATCH`| Apply only the first matching route rule and stop evaluation.          |

For example, to use `FIRST_MATCH` mode:

```yaml
pipeline:
  name: Sync MySQL Database to Doris
  parallelism: 2
  route-mode: FIRST_MATCH
```

{{< hint info >}}

When using `FIRST_MATCH` mode, route rules are evaluated in the order they are defined. The first rule that matches the source table will be applied, and subsequent rules will be skipped.

{{< /hint >}}

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

## Advanced: RegExp Capturing & Replacement Rules

It is also possible to create capturing groups in `source-table` fields like this:

```yaml
route:
  - source-table: db_(\.*).(\.*)_tbl
    sink-table: sink_db_$1.sink_table_$2
```

Here we create two capturing groups matching database suffix and table prefix.

For upstream table `db_foo.bar_tbl`, capturing group `(foo, bar)` will be extracted and bound to `$1` and `$2`.
As a result, such table will be routed to downstream table `sink_db_foo.sink_table_bar`.

{{< hint info >}}

Standard RegExp capturing could not be used with `replace-symbol` options.

{{< /hint >}}

# Compatibility Notes — Sink-Table Suffix Leak from Prefix Matches

For a `source-table` rule, `TableIdRouter` first checks whether the regex matches the *entire*
source table id, and only then runs the substitution. The exact routing behavior is therefore
sensitive to a specific interaction between full-match and prefix-match:

- The regex matches the full source table id (so the rule applies at all), **and**
- The regex can also match a *shorter prefix* of the same source table id (so the substitution
  would consume only part of the id).

When both hold, the substitution in Flink CDC 3.6.0 was performed against the shorter prefix
match, leaving the unmatched tail to be appended to the sink-table name. This commonly occurs
when the regex has multiple alternatives ordered from shorter to longer — for example
`table_1|table_10`, `([1-9]|1[0-6])`, or `[1-9]|1[0-6]` without parentheses — regardless of
whether the rule uses capturing groups or `$N` back-references.

Starting with Flink CDC 3.7.0, the substitution consumes the entire source table id, eliminating
the suffix leak. The table below shows side-by-side what the same configuration routes to in
3.6.0 versus 3.7.0.

| Source table id       | source-table                              | sink-table                          | Routed to in 3.6.0                          | Routed to in 3.7.0                  |
|-----------------------|-------------------------------------------|-------------------------------------|---------------------------------------------|-------------------------------------|
| `new_db_6.table_1`    | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix`                   | `new_db_6.table_1_suffix`           |
| `new_db_6.table_13`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix3`                  | `new_db_6.table_13_suffix`          |
| `new_db_6.table_13`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_merged`             | `new_db_6.table_merged3`                    | `new_db_6.table_merged`             |
| `new_db_6.table_14`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_merged`             | `new_db_6.table_merged4`                    | `new_db_6.table_merged`             |

Reading the table:

- The first row shows a single-digit source (`table_1`) where the two versions happen to agree,
  because the shorter-prefix match happens to consume the entire id, leaving nothing to leak.
- The second row shows the `$N`-back-reference case: in 3.6.0 the captured value is truncated
  to the first matched digit (`1`) and the trailing characters are concatenated after the
  `_suffix` literal; in 3.7.0 the full multi-digit value is captured.
- The third and fourth rows show the no-back-reference case: in 3.6.0 each matched source is
  suffixed by its own unmatched tail (`merged3`, `merged4`, ...), effectively producing one
  sink-table per source table; in 3.7.0 all matched sources collapse onto the single declared
  sink-table.

## Compatibility Impact on Existing Pipelines

The change affects any `source-table` rule that admits a shorter prefix match alongside a full
match on the same source table id. In practice this is most often a regex with alternation
branches ordered from shorter to longer, with or without capturing groups and with or without
`$N` back-references in the sink-table template. Rules that use only literal table names or the
`replace-symbol` option are unaffected.

For pipelines that *are* affected, upgrading from 3.6.0 to 3.7.0 may produce this kind of
behavior change:

- **Previously (mis-)routed sources land on a different sink-table id.**
  - *With a `$N` back-reference:* the captured value becomes the full substring instead of the
    truncated prefix (for example `..._table_13_suffix` instead of the old `..._table_1_suffix3`).
  - *Without a `$N` back-reference:* every previously-matched source is now routed to the single
    declared sink-table (for example `new_db_6.table_merged` instead of
    `new_db_6.table_merged3`).

  Either way, downstream tables that were created under the old (incorrect) name are no longer
  the target of the pipeline. Reviewers should decide whether to rename the affected downstream
  tables or to migrate the existing data into the new (correct) sink tables.

In short, before upgrading, audit your route rules for any `source-table` regex whose shorter
prefix matches form a strict subset of its full matches on the same ids, confirm that the
rule's matching set is exactly what you intend, and — if any pipeline previously depended on
the 3.6.0 suffix leak — decide whether to rename the affected downstream tables or to migrate
the data into the new (correct) sink tables.
