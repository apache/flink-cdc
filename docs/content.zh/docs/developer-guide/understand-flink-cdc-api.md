---
title: "理解 Flink CDC API"
weight: 1
type: docs
aliases:
  - /developer-guide/understand-flink-cdc-api
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

# Understand Flink CDC API

If you are planning to build your own Flink CDC connectors, or considering
contributing to Flink CDC, you might want to hava a deeper look at the APIs of
Flink CDC. This document will go through some important concepts and interfaces
in order to help you with your development.

## Event

An event under the context of Flink CDC is a special kind of record in Flink's
data stream. It describes the captured changes in the external system on source
side, gets processed and transformed by internal operators built by Flink CDC,
and finally passed to data sink then write or applied to the external system on
sink side.

Each change event contains the table ID it belongs to, and the payload that the
event carries. Based on the type of payload, we categorize events into these
kinds:

### DataChangeEvent

DataChangeEvent describes data changes in the source. It consists of 5 fields

- `Table ID`: table ID it belongs to
- `Before`: pre-image of the data
- `After`: post-image of the data
- `Operation type`: type of the change operation
- `Meta`: metadata of the change

For the operation type field, we pre-define 4 operation types:

- Insert: new data entry, with `before = null` and `after = new data`
- Delete: removal of data, with `before = removed` data and `after = null`
- Update: update of existed data, with `before = data before change`
  and `after = data after change`
- Replace:

### SchemaChangeEvent

SchemaChangeEvent describes schema changes in the source. Compared to
DataChangeEvent, the payload of SchemaChangeEvent describes changes in the table
structure in the external system, including:

- `AddColumnEvent`: new column in the table
- `AlterColumnTypeEvent`: type change of a column
- `CreateTableEvent`: creation of a new table. Also used to describe the schema
  of
  a pre-emitted DataChangeEvent
- `DropColumnEvent`: removal of a column
- `RenameColumnEvent`: name change of a column

### Flow of Events

As you may have noticed, data change event doesn't have its schema bound with
it. This reduces the size of data change event and the overhead of
serialization, but makes it not self-descriptive Then how does the framework
know how to interpret the data change event?

To resolve the problem, the framework adds a requirement to the flow of events:
a `CreateTableEvent` must be emitted before any `DataChangeEvent` if a table is
new to the framework, and `SchemaChangeEvent` must be emitted before any
`DataChangeEvent` if the schema of a table is changed. This requirement makes
sure that the framework has been aware of the schema before processing any data
changes.

{{< img src="/fig/flow-of-events.png" alt="Flow of Events" >}}

## Data Source

Data source works as a factory of `EventSource` and `MetadataAccessor`,
constructing runtime implementations of source that captures changes from
external system and provides metadata.

`EventSource` is a Flink source that reads changes, converts them to events
, then emits to downstream Flink operators. You can refer
to [Flink documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/)
to learn internals and how to implement a Flink source.

`MetadataAccessor` serves as the metadata reader of the external system, by
listing namespaces, schemas and tables, and provide the table schema (table
structure) of the given table ID.

## Data Sink

Symmetrical with data source, data sink consists of `EventSink`
and `MetadataApplier`, which writes data change events and apply schema
changes (metadata changes) to external system.

`EventSink` is a Flink sink that receives change event from upstream operator,
and apply them to the external system. Currently we only support Flink's Sink V2
API.

`MetadataApplier` will be used to handle schema changes. When the framework
receives schema change event from source, after making some internal
synchronizations and flushes, it will apply the schema change to
external system via this applier. 
