# 核心概念

## 概念介绍

在 Flink CDC 3.0 框架中，外部系统中的存储对象统一映射为表（Table），因此外部系统存储对象的变更也映射为表的变更。与传统的表概念类似，每个表有唯一的 Table ID，有自己的表结构（Schema）。

为兼容大部分外部系统，Table ID 由 namespace、schemaName, table 三部分组成，连接器需要建立 Table ID 与外部系统存储对象的映射，如 MySQL / Doris 的映射关系为：(null, database, table)，Kafka 的映射关系为：(null, null, topic)。

在 Flink CDC 3.0 框架中流转的数据类型称为 Event，表示某个 table 产生的变更事件，每个 event 都标记了其变更的目标 Table ID。Event 分为 SchemaChangeEvent 和 DataChangeEvent，分别表示 table 的表结构变更和数据变更。

数据源连接器称为 Data Source。Data Source，捕获上游系统中存储对象的变更，并将其转化为 event 输出至同步任务中，还通过 MetadataAccessor 供框架对上游系统的元信息进行读取。数据目标连接器称为 Data Sink，Data Sink 在收到 event 后，会将变更应用在目标系统的存储对象中，实现与上游系统的数据同步，并且可以通过 MetadataApplier 将上游的元信息变更应用在目标系统中。

由于 event 是以流水线方式从上游经过处理后发送至下游，所以数据同步任务又被称为 Data Pipeline。一条 Data Pipeline 由 Data Source、路由（Route）、变换（Transform）、Data Sink 共同组成，其中，transform 可以将 event 进行一定程度的修改，route 可以将 event 对应的 table ID 进行重映射。

## 数据源

数据源，数据源用来访问元数据和读取外部系统的变更数据，一个数据源可以同时读取多张表。

- type：source 类型，如 mysql、postgres
- name：source 名称，用户自定义（可选、提供默认值）
- 其他 source 的自定义配置

整库同步的能力来自于 Flink source 的实现。在获取用户配置后，Flink source connector 需要能够按照指定的数据类型向下游发送某个数据库中的全部数据。

## 数据目的

数据目的，数据目的用来向外部系统修改元数据和写入变更数据，一个数据目的可以写多张表。

- type：sink 类型，如 mysql、postgres
- name：sink 名称，用户自定义（可选、提供默认值）
- 其他 sink 自定义配置

## 转换

数据变换，针对给定表中的每一行数据，指定如何对变更数据进行转换:

- source-table：源表 id，支持正则
- sql-expr： 对源表里的数据执行表达式求值
- description：转换表达式描述，用户自定义（可选、提供默认值）

```yml

transform:
  - source-table: mydb.app_order_\.*
    projection: id, order_id, TO_UPPER(product_name)
    filter: id > 10 AND order_id > 100
    description: project fields from source table    	#可选参数，描述信息
  - source-table: mydb.web_order_\.*
    projection: CONCAT(id, order_id) as uniq_id, *
    filter: uniq_id > 10
    description: add new uniq_id for each row        	#可选参数，描述信息
```

## 路由

路由，针对每一个变更 event，指定该 event 的目标 table ID。最典型的应用场景为分库分表合并，将多张上游源表路由至同一张结果表中。

- source-table：源表 id，支持正则
- sink-table：结果表 id，支持正则
- description：路由规则描述，用户自定义（可选、提供默认值）

```yml

route:
  - source-table: mydb.app_order_\.*
    sink-table: app-order-topic
    description:  将所有分片表同步到一个目标表
  - source-table: mydb.web_order
    sink-table: odsdb.ods_web_order
    description: 同步表到一个带有给定前缀 ods_ 的目标表
```

## 数据管道

- name: pipeline 名称，会作为作业名提交至 Flink 集群
- 其他对高级功能的处理方式，比如自动建表、schema evolution 等
