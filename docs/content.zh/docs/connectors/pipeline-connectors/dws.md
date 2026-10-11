---
title: "GaussDB DWS"
weight: 6
type: docs
aliases:
- /connectors/pipeline-connectors/dws
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

# GaussDB DWS Connector

GaussDB DWS Pipeline Sink 通过官方 DWS 客户端写入 Flink CDC 事件。默认 `auto` 模式由官方客户端按受支持的批次选择 UPSERT 或 COPY 路径。目标表必须有主键。

该 sink 在源端可重放时提供至少一次恢复和最终收敛，不提供 checkpoint 事务可见性或 exactly-once。checkpoint 会在 writer state 快照前同步 flush 官方客户端。

## 示例

```yaml
source:
  type: values
  name: Values Source

sink:
  type: dws
  name: GaussDB DWS Sink
  jdbc-url: jdbc:gaussdb://127.0.0.1:8000/postgres?connectTimeout=10&socketTimeout=60
  username: gaussdb
  password: INJECT_WITH_APPROVED_SECRET_MECHANISM
  schema: public
  sink.enable-delete: true
  write-mode: auto
  auto-batch-flush-size: 30000
  auto-flush-max-interval: 3s
  dws.client.write.force-flush-size: 40000
  dws.client.write.buffer.all-max-bytes: 128MiB
  dws.client.write.buffer.table-max-bytes: 64MiB
  dws.client.write.buffer.partition-max-bytes: 32MiB

pipeline:
  name: Values to GaussDB DWS Pipeline
  parallelism: 4
```

示例中的密码是占位符。请通过部署平台批准的凭据注入机制提供密码；Pipeline 解析器不保证自动展开环境变量。

## 连接器配置项

### 连接、命名与表行为

| 配置项 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `type` | 是 | — | 必须为 `dws`。 |
| `jdbc-url` | 是 | — | 包含目标数据库的 `jdbc:gaussdb://` URL。保留无关 query 参数；缺少 `connectTimeout`/`socketTimeout` 时分别补 10/60 秒。 |
| `username` / `password` | 是 | — | DWS 凭据。connector 的生效配置日志不会输出它们。 |
| `schema` | 否 | `public` | 事件表标识不含 schema 时使用的默认值。 |
| `case-sensitive` | 否 | `true` | 是否保留标识符大小写。为 `false` 时 schema、表、列和主键统一转为小写；包含双引号字符的标识符会被拒绝。 |
| `local-time-zone` | 否 | Pipeline 值，其次系统默认值 | 时间戳转换使用的时区。 |
| `driver` | 否 | `com.huawei.gauss200.jdbc.Driver` | 仅接受该驱动。 |
| `sink.enable-delete` | 否 | `true` | 控制独立 DELETE；主键变更产生的撤回始终执行。 |
| `enable-dn-partition` | 否 | `false` | 控制 DDL distribution 语义，不是客户端 DirectDN 模式。 |
| `distribution-key` | 否 | — | 逗号分隔的已有列。开启 DN partition 时必填，关闭时显式配置会被拒绝。 |

### 写入、重试与超时

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `write-mode` | `auto` | 支持 `auto`、`upsert`、`copy_upsert`、`copy_merge`，大小写不敏感；别名为 `dws.client.write.mode`。推荐 `auto`。 |
| `enable-auto-flush` | `true` | 控制常规 native 批次/时间触发。设为 `false` 后仍保留有限 force、connector 预算、checkpoint 和 schema flush。 |
| `auto-batch-flush-size` | `30000` | 正整数；别名为 `dws.client.write.auto-flush-size`。 |
| `auto-flush-max-interval` | `3s` | 正 Duration；别名为 `dws.client.write.auto-flush-max-interval`。 |
| `dws.client.write.thread-size` | `1` | 官方客户端 worker 数。增大后只提供客户端跨表并发。 |
| `dws.client.write.use-copy-size` | `1000` | 兼容同列批次的 AUTO-to-COPY 阈值；不保证所有类型都走 COPY。 |
| `dws.client.write.force-flush-size` | `40000` | 有限安全 flush 阈值；自动 flush 开启时不得小于 auto batch。 |
| `sink.max-retries` | `3` | 总尝试次数，至少 1；别名为 `dws.client.retry.max-times`。 |
| `dws.client.retry.sleep-base-time` | `1s` | 非负重试基础等待。 |
| `dws.client.retry.sleep-random-time` | `300ms` | 正数随机重试抖动。 |
| `dws.client.timeout.task` | `10min` | 正数 native task timeout，不是整个 flush 的硬 deadline。 |
| `dws.client.timeout.statement` | `5min` | 正数 DWS statement timeout。 |

主配置和别名同时出现时，归一化后的值必须相等；冲突会在 connector 创建阶段失败，不会静默覆盖。

### 缓冲预算

| 配置项 | 默认值 | 范围 |
| --- | --- | --- |
| `dws.client.write.buffer.all-max-bytes` | `128MiB` | 每个 sink writer/client 的估算总缓冲。 |
| `dws.client.write.buffer.table-max-bytes` | `64MiB` | 每表每 writer 的估算缓冲，不得大于总预算。 |
| `dws.client.write.buffer.partition-max-bytes` | `32MiB` | connector 固定 native partition 的估算缓冲，不得大于表预算。 |

这些值是保守计数预算，不是 JVM heap 硬上限。由于 native record-size 指标遗漏 `byte[]`，估算会计入二进制值、主键和容器开销。单条估算值超过有效表/partition 或总预算时，在 native commit 前拒绝；累计加入下一条会超限时先同步 flush，只有成功才清零计数。writer 不维护第二份记录缓存。

### 连接生命周期兼容项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `connectionMaxUseTimeSeconds` | `3600` | 旧秒单位配置；`connectionMaxUseTimeThreshold` 为兼容同义键，native Duration 别名为 `dws.client.jdbc.max.use-time`。 |
| `connectionMaxIdleMs` | `60000` | 旧毫秒单位配置；native Duration 别名为 `dws.client.jdbc.max.idle`。 |
| `connectionTimeOut` | URL 默认 10s | JDBC `connectTimeout` 的旧毫秒配置；显式值必须为正数且能被 1000 整除。 |
| `connectionSocketTimeout` | URL 默认 60s | JDBC `socketTimeout` 的旧毫秒配置；显式值必须为正数且能被 1000 整除。 |

URL 已有 timeout 与旧配置可同时提供，但单位转换后必须相等。JDBC connect/socket timeout 不构成全局 flush deadline。

### 明确拒绝的配置

connector 会拒绝 `sink-table`（改用 Pipeline route）、`sink.parallelism`（改用 `pipeline.parallelism`）、`connectionSize`（改用 `dws.client.write.thread-size`）、`logSwitch=true`、所有 `connectionPool*`/连接池监控项、`connectionMaxUseCount` 以及 native partition-policy/min/max 覆盖。不支持任意 `dws.client.*` 透传、DirectDN、compare-field、partial-update 或 ignore-on-conflict。

## 投递、顺序、Schema 与可观测性

- 完整制品必须同时包含更新后的 common event model、serializer、runtime partition operators、composer 与 DWS connector。只替换 connector JAR 不安全，因为主键变更 UPDATE 使用新的 typed retraction 事件。
- DWS sink 显式启用主键变更拆分：撤回按旧键分区，插入按新键分区，因此保留单表多 writer 与逐键顺序；其他 sink 的事件流不变。这不会建立不相关 key 之间的全局顺序。
- CREATE 和 schema evolution 在 connector 发布新 converter 前刷新官方客户端 schema cache；DROP 同时移除两层 cache，TRUNCATE 保留 schema。不支持的 schema 变更会令 Pipeline 失败。
- 混合大小写按 `case-sensitive` 合同处理，但不承诺接受所有 quoted 或非法标识符。
- Flink 指标提供 `dws.acceptedRecords`、`dws.flushCount`、`dws.conservativeBufferedBytes`、`dws.lastFlushDurationMillis`、`dws.firstAsyncFailure`；标准 sink 指标提供 flush 已确认记录/字节和确定的同步发送错误。“written”仅表示同步 native flush 返回，不表示 checkpoint 事务可见。不可获得的 native buffer 指标不会被伪造。

## 数据类型映射

| Flink CDC 类型 | GaussDB DWS 类型 | 说明 |
| --- | --- | --- |
| BOOLEAN | BOOLEAN | |
| TINYINT, SMALLINT | SMALLINT | |
| INTEGER | INTEGER | |
| BIGINT | BIGINT | |
| FLOAT | REAL | |
| DOUBLE | DOUBLE PRECISION | |
| DECIMAL | DECIMAL(p, s) | 保留来源精度和小数位。 |
| CHAR | CHAR(n) | |
| VARCHAR | VARCHAR(n) 或 TEXT | 超大 VARCHAR 映射为 TEXT。 |
| BINARY, VARBINARY | BYTEA | 纳入保守缓冲计数。 |
| DATE | DATE | |
| TIME | TIME(p) | 最大精度为 6。 |
| TIMESTAMP | TIMESTAMP(p) | 最大精度为 6。 |
| TIMESTAMP_LTZ, TIMESTAMP_TZ | TIMESTAMPTZ(p) | 最大精度为 6。 |
| ARRAY | TEXT | |
| MAP, ROW | JSON | |

## 从 staging/committer 写入器迁移

原生客户端写入器使用新的状态协议，不能恢复旧 staging-table committer 创建的 savepoint。不要通过 `allowNonRestoredState` 或修改 sink operator UID 绕过检查，否则可能丢弃尚未应用的 committable。

1. drain 并停止旧任务，保留旧制品、checkpoint/savepoint、源端位点、旧目标表、staging table 和资源归属清单。
2. 创建本次迁移独占的空目标。
3. 新 connector 先执行一致性全量快照，再衔接增量重放，并继续保留源端日志。
4. 使用独立查询逐主键、逐字段核对，覆盖删除与主键变更。
5. 获得明确授权后再切换消费端；回退窗口结束前保留旧目标和可重放边界。

回退时先停止新任务并保留诊断状态，再使用旧制品、旧目标和保留的源端边界恢复。新旧任务都不得删除不属于自己的资源；清理需要单独授权。

原生客户端路线提供至少一次恢复和可重放源上的最终收敛，不提供 checkpoint 事务可见性，也不支持与旧 committer 状态直接互载。

{{< top >}}
