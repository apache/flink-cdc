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

# Compatibility Notes — Multi-Character Capturing Groups

When a `source-table` rule uses a capturing group that can match more than one character — for
example `new_db_6.table_([1-9]|1[0-6])`, which captures both single- and two-digit numeric
suffixes — the routing behavior differs between Flink CDC 3.6.0 and 3.7.0. This section
illustrates the behavior change and points out which existing pipelines may need to be reviewed
before upgrading.

## Behavior in Flink CDC 3.6.0

In Flink CDC 3.6.0, a source-table regex with a multi-character capturing group could be
matched against only a *prefix* of the source table id. The unmatched tail of the source table
id would then be appended to the sink-table template, producing incorrect sink names.

## Behavior in Flink CDC 3.7.0

Starting with Flink CDC 3.7.0, the source-table regex must consume the entire source table id,
and every capturing group — including multi-character ones — is captured in full. The table below
shows side-by-side what the same configuration routes to in 3.6.0 versus 3.7.0.

| Source table id       | source-table                              | sink-table                          | Routed to in 3.6.0                          | Routed to in 3.7.0                  |
|-----------------------|-------------------------------------------|-------------------------------------|---------------------------------------------|-------------------------------------|
| `new_db_6.table_1`    | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix`                   | `new_db_6.table_1_suffix`           |
| `new_db_6.table_13`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix3`                  | `new_db_6.table_13_suffix`          |
| `new_db_6.table_17`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix7`                  | `new_db_6.table_17` (no match)      |
| `new_db_6.table_13`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_merged`             | `new_db_6.table_merged3`                    | `new_db_6.table_merged`             |
| `new_db_6.table_14`   | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_merged`             | `new_db_6.table_merged4`                    | `new_db_6.table_merged`             |

Reading the table:

- The first row shows a single-digit source (`table_1`) where the two versions happen to agree,
  because there is no leftover tail to leak into the sink-table name.
- The second and third rows show the `$N`-back-reference case: in 3.6.0 the captured value is
  truncated to the first matched digit (`1`) and the trailing characters are concatenated after
  the `_suffix` literal; in 3.7.0 the full multi-digit value is captured, and (for `table_17`)
  the source-table id is not routed at all when the regex does not match the whole id.
- The fourth and fifth rows show the no-back-reference case: in 3.6.0 each matched source is
  suffixed by its own unmatched tail (`merged3`, `merged4`, ...), effectively producing one
  sink-table per source table; in 3.7.0 all matched sources collapse onto the single declared
  sink-table.

## Compatibility Impact on Existing Pipelines

The change only affects pipelines whose `source-table` rule contains a capturing group that can
match more than one character (for example `([1-9]|1[0-6])`, `(db_[0-9]+)`, `(\w{2,4})`, etc.).
Rules that use only single-character capturing groups, literal table names, or the
`replace-symbol` option are unaffected.

For pipelines that *are* affected, two kinds of behavior change may be observed when upgrading
from 3.6.0 to 3.7.0:

- **Previously (mis-)routed sources land on a different sink-table id.**
  - *With a `$N` back-reference:* the captured value becomes the full multi-character substring
    instead of the truncated one (for example `..._table_13_suffix` instead of the old
    `..._table_1_suffix3`).
  - *Without a `$N` back-reference:* every previously-matched source is now routed to the single
    declared sink-table (for example `new_db_6.table_merged` instead of
    `new_db_6.table_merged3`).

  Either way, downstream tables that were created under the old (incorrect) name are no longer
  the target of the pipeline. Reviewers should decide whether to rename the affected downstream
  tables or to migrate the existing data into the new (correct) sink tables.

- **Source tables that previously matched the rule only partially are no longer routed.** They
  fall through to the default no-route behavior and keep their original source table id. This
  is normally the desired behavior, but if any pipeline relied on the 3.6.0 partial-matching
  misrouting, that pipeline needs to be updated explicitly.

In short, before upgrading, audit your route rules for any `source-table` regex that contains a
multi-character capturing group, confirm that the rule's matching set is exactly what you
intend, and — if any pipeline previously depended on the 3.6.0 misrouting — decide whether to
rename the affected downstream tables or to migrate the data into the new (correct) sink tables.
