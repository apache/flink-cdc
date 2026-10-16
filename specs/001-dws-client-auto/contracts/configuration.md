# Contract: DWS Pipeline 配置

本文件定义升级后的目标合同，不表示当前代码已实现。connector identifier 保持 `dws`。不提供任意 `dws.client.*` 透传：仅接受下列白名单，避免覆盖顺序、安全及 lifecycle 固定项。

## 解析规则

1. 显式键优于默认；同义键都显式提供时，先统一单位/大小写，等值接受并发一次弃用提示，不等值在启动前拒绝。禁止使用默认值误判用户输入冲突。
2. 所有正数项拒绝零/负数/溢出；Duration 必须带可解析单位。连接 URL 内重复关键参数、无法解释的冲突拒绝，不靠字符串拼接覆盖。
3. 未列出的原生参数和已删除的占位项明确失败并给替代方法，不接受后忽略。
4. 日志仅输出安全白名单的生效值；密码、含凭据的 URL、记录正文与异常中完整 SQL 参数不得输出。

## 已有连接、CDC 和表配置

| Pipeline 键 | 升级后行为与默认 |
| --- | --- |
| jdbc-url / username / password | 必填；URL 必须为支持的 DWS JDBC 协议；只在初始化/连接时使用凭据 |
| case-sensitive | 保留默认 true，映射 WRITE_TABLE_FIELD_CASE_SENSITIVE；false 沿用既有归一化 |
| schema | 保留 public，事件显式 schema 优先；与 DDL、writer 和目标 hash 标识保持一致 |
| local-time-zone | 保留 sink 显式值→pipeline→systemDefault 的优先级 |
| sink.enable-delete | 默认 true；只作用于独立 DELETE，不作用于变键 UPDATE_BEFORE |
| enable-dn-partition / distribution-key | 保留 DDL distribution 语义和默认 false/无；不等于客户端 DirectDN。新增严格校验：显式 true 必须给有效列；false 时显式 key 拒绝并提示开关。当前实现会直接省略子句，该变化须列入迁移说明 |
| driver | 默认且仅支持 com.huawei.gauss200.jdbc.Driver；其他值显式拒绝，不能实际忽略 |
| logSwitch | 默认 false；不启用会记录业务行/凭据的官方详细日志，显式 true 暂拒绝并指引使用安全 connector 指标/诊断 |
| sink-table | 显式配置拒绝；使用 Pipeline route 指定目标表，避免多表共写歧义 |
| sink.parallelism | 显式配置拒绝；使用 pipeline.parallelism，后者实际控制 hash/下游拓扑 |

## 写入参数与别名

表中“原生别名”也在白名单内；只接受列出的别名，不自动开放官方所有 legacy 名称。

| Pipeline 键 | 原生别名/映射 | 新默认 | 验证 |
| --- | --- | --- | --- |
| write-mode | dws.client.write.mode | auto | 允许 auto/upsert/copy_upsert/copy_merge，大小写不敏感 |
| auto-batch-flush-size | dws.client.write.auto-flush-size | 30000 | 正整数；原声明默认 50000 的变化须发布说明 |
| auto-flush-max-interval | dws.client.write.auto-flush-max-interval | 3s | 正 Duration；原声明默认 3min 的变化须发布说明 |
| enable-auto-flush | connector 策略 | true | false 时关闭常規批量/时间触发，保留 finite force/内存触发与 checkpoint/schema flush |
| dws.client.write.thread-size | WRITE_THREAD_SIZE | 1 | 正整数；原声明默认 3，增大只允许客户端跨表执行并发 |
| dws.client.write.use-copy-size | WRITE_USE_COPY_BATCH_SIZE | 1000 | 正整数；按同列记录组触发，不承诺所有列类型都走 COPY |
| dws.client.write.force-flush-size | WRITE_FORCE_FLUSH_BATCH_SIZE | 40000 | 正整数；自动刷新开启时不得小于 auto-batch-flush-size |
| sink.max-retries | dws.client.retry.max-times | 3 | 总尝试次数，至少 1；不是失败后额外重试数 |
| dws.client.retry.sleep-base-time | RETRY_SLEEP_BASE_TIME | 1s | 非负 Duration |
| dws.client.retry.sleep-random-time | RETRY_SLEEP_RANDOM_TIME | 300ms | 至少 1ms，防 native 取模零 |
| dws.client.timeout.task | TIMEOUT_TASK | 10min | 有限正 Duration；不是整个 flush 的硬 deadline |
| dws.client.timeout.statement | TIMEOUT_SQL_STATEMENT | 5min | 有限正 Duration；作用于连接的 statement_timeout |

COPY、UPDATE、UPDATE_AUTO、COPY_UPDATE 不属于本完整 CDC upsert 合同，显式提供时拒绝并提示以上四种模式；以前“校验通过但不生效”不等于这些模式已有语义保证。compareField、partial update、ignore-on-conflict 不开放。

enable-auto-flush=false 时设置 interval=0、native batch=解析后的有限 force 阈值。达到这个安全上限时允许后台或业务线程提交；这不是禁用所有批量保护。不能设 batch=MAX_INT：原生 forceFlush 仍依赖 batch/interval 条件，会因此失效。日志明确普通 auto-batch/interval 被替代为安全触发，不静默忽略。显式旧 auto=50000 且自动刷新开启、未增大 force 时，拒绝并提示同时设 force>=50000，而不是悄悄改变显式值。

显式 UPSERT 下，将未公开的 WRITE_FORCE_FLUSH_UPSERT_BATCH_SIZE 同步设置为解析后的有限 force 值，并测试 client 构造后的实际配置，避免 native 默认 10000 覆盖用户 force。不为此再增加一个公开参数。

## 缓冲预算（新增公开项）

| 键 | 默认 | 验证 |
| --- | --- | --- |
| dws.client.write.buffer.all-max-bytes | 128MiB | 每 writer/client 的估算缓存总预算 |
| dws.client.write.buffer.table-max-bytes | 64MiB | 每表预算，<=all |
| dws.client.write.buffer.partition-max-bytes | 32MiB | 每 native partition 预算，<=table |

单位按明确二进制字节转换为 native Memory，均须正数。使用既有 MemorySize/字节解析能力，调用 typed `with(..., new Memory(bytes))`，不依赖官方字符串解析是否认识 `MiB`。超出单 partition 预算的单条记录在提交前按估算大小明确拒绝，避免无限攒批；该校验并不限制上游反序列化前的输入大小。

这是缓存估算预算，不是 JVM 堆硬上限。多 writer 总开销、inflight action、字段对象、COPY 编码等必须计入压测。实施不得仅从 native 缓存计数降为零推断实际内存已经释放。

原生 Record.getByteSize 不包含 byte[]，不能直接作为唯一保护。全部 setObject 后、commit 前，通过公开 Operate.getRecord 及转换后的值保守估算（包括 byte[] 长度、PK 派生和容器开销）。维护总计/分表“最近一次成功显式 flush 后已接受字节”的数字计数；下一条将超 all 或 min(table,partition) 预算时先同步 flush，成功才清零并提交，异步成功不扣计数。超限单条在 commit 前拒绝。这不是第二个数据缓冲，只是对原生低估的计数保护。

固定不暴露的顺序项：CN、DYNAMIC、partition-min=max=1；用户设置 dws.client.write.partition-policy/min/max 时拒绝。客户端 native 多 partition 不与 Flink 多 writer 混为一谈。

## 连接与废弃项的完整处理

| 旧键 | 处理 |
| --- | --- |
| connectionSize | 显式配置拒绝；改用 dws.client.write.thread-size；不假装连接数与线程数完全等价 |
| connectionMaxUseTimeSeconds | 按 Pipeline 既有文档的秒解释，映射 dws.client.jdbc.max.use-time，默认 3600s；弃用提示 |
| connectionMaxUseTimeThreshold（新兼容键） | 秒，映射同上；不调用废弃 builder |
| connectionMaxIdleMs | 毫秒映射 dws.client.jdbc.max.idle，默认 60000ms |
| connectionTimeOut | 历史毫秒映射 URL connectTimeout 秒，必须正且为 1000 的整数倍；新建未配置采用 10s，不沿用旧未生效 300000ms 默认 |
| connectionSocketTimeout | 历史毫秒映射 URL socketTimeout 秒，必须正且为 1000 的整数倍；新建未配置采用 60s，显式 0 拒绝（不允许无限等待） |
| connectionPoolName | 显式拒绝；官方 BINLOG 池项，不是新写入路径 |
| connectionPoolSize | 同上 |
| connectionPoolTimeout | 同上 |
| connectionMaxUseCount | 同上 |
| needConnectionPoolMonitor | 同上 |
| connectionPoolMonitorPeriod | 同上 |

接受原生 duration 别名 `dws.client.jdbc.max.use-time` / `dws.client.jdbc.max.idle`，按前述冲突规则解析。URL 已有 connectTimeout/socketTimeout 时保留有效显式值；与旧毫秒别名同配须精确相等，否则拒绝。实施用结构化参数解析合并，不重写无关 URL 参数，不记录最终含敏感信息 URL。

URL 单位秒来自 JDBC 8.6.1-200 字节码验证；不要套用 Druid/BINLOG 毫秒配置。Socket read timeout 不覆盖所有发送阻塞；COPY 黑洞与取消仍是发布门槛，不能承诺一个未实现的全局 flush deadline。

## 数据与可观测性固定约束

- 保持完整行 upsert、PK delete、现有时区与类型转换；不得自动追加非法字符替换、截断策略。固定 WRITE_FORMAT_STRING_U0000=false（原生默认 true 会删除 NUL），并关闭 compatible-illegal-chars；这些项不能被透传覆盖。不支持的 NUL 输入明确失败，服务端若有不可禁用的静默数据变更设置则阻断严格正确性认证。
- 不开放原生详细数据打印，connector 的异常包装只含 tableId/op/字段类型等必要上下文，不串接完整 Event。
- 至少提供 accepted/written/failed records、flush duration/count、首错、重试诊断、已配置模式与可验证的实际写入路径；native buffer estimate 与 JVM heap 分开命名，不把前者称为实际堆使用。
- native 无公开 getter 的统计不得通过反射或复制执行引擎获取。使用已有 native metrics/受控 SQL 观测完成验收；无法实时提供的指标明确标为不可用，不伪造精确值。

## 示例（实施后验证使用）

下列仅是 sink/pipeline 片段，需配现有受支持 source、主键表、route 和凭据注入流程；不保证解析器会自动展开环境变量。

```yaml
sink:
  type: dws
  jdbc-url: jdbc:gaussdb://DWS_HOST:8000/DWS_DATABASE?connectTimeout=10&socketTimeout=60
  username: DWS_USERNAME
  password: INJECT_WITH_APPROVED_SECRET_MECHANISM
  write-mode: auto
  case-sensitive: true
  auto-batch-flush-size: 30000
  auto-flush-max-interval: 3s
  dws.client.write.force-flush-size: 40000
  dws.client.write.buffer.all-max-bytes: 128MiB
  dws.client.write.buffer.table-max-bytes: 64MiB
  dws.client.write.buffer.partition-max-bytes: 32MiB
pipeline:
  parallelism: 4
```
