# Research: DWS 官方客户端 AUTO 路线

核查日期：2026-10-10。基线：FLINK-39327，`3e88f65854cf3f96276456c58698fd09532bf0a0`。本轮没有真实数据库运行验证。

## 证据来源

- 仓库当前路径：DWS `sink/DwsDataSink.java:59-69` 进入自研 SinkV2；`factory/DwsDataSinkFactory.java:83-103,146-153` 仅校验模式，连接对象只传 URL/用户/密码。声明参数不等于已经生效。
- 发布制品：[dws-client 2.1.0.6 POM](https://repo.maven.apache.org/maven2/com/huaweicloud/dws/dws-client/2.1.0.6/dws-client-2.1.0.6.pom)。会话已下载并检查 POM/sources JAR，包含 JDBC 8.6.1-200、Druid 1.2.23、metrics 4.2.25。
- 官方：[写入建议](https://support.huaweicloud.com/tg-dws/dws_07_0184.html)、[版本记录](https://support.huaweicloud.com/tg-dws/dws_07_0178.html)、[客户端 API/配置](https://support.huaweicloud.com/tg-dws/dws_07_0175.html)。版本记录当前展示到 2.1.0.5；不将未公布的 2.1.0.6 特有收益写成已确认事实。
- 后文 native 路径相对发布 sources JAR 的 `com/huaweicloud/dws/client/`；仓库路径相对根目录。临时下载源码不成为项目依赖。

## R1 — 直接 client，保留 Pipeline SinkV2

**Decision**：直接依赖 dws-client 2.1.0.6，移除 dws-connector-flink、DwsConnectionOptions 及 legacy DwsSinkFunction；保留 Pipeline SinkV2 和已有 Flink2 compat。

**Rationale**：当前生产 writer 不调用原生客户端，只升级版本不会接入 AUTO。官方 legacy sink 也不能自动适配 CDC schema barrier，且增加 Flink2 链接风险。配置可序列化，client/连接/回调只能在 writer 初始化。

**Alternatives considered**：仅升级 POM、回退 SinkFunction、保留双引擎均不选。

## R2 — AUTO 与大小写修复范围

**Decision**：默认 AUTO；显式支持 UPSERT/COPY_UPSERT/COPY_MERGE，不因大小写开关改变模式，不自行拼接 merge SQL。

**Rationale**：官方建议 AUTO；2.1.0.2 记录明确修复 COPY_MERGE 大小写问题。2.0.0.6 `util/JdbcUtil:519-528` 的 ON/SET/VALUES 引用未 quote；2.1.0.6 `:763-777` 已引用。该已知缺陷在源码层面修复，真实 DWS 回归仍必需。

**Limits**：compareField 条件引用仍有独立问题，本次不暴露；IdentifierUtil 仅包裹引号，包含内嵌双引号的标识符先明确拒绝。`handler/PutActionHandler:161-197` 按同列集合批量判断阈值，bytea 可强制 UPSERT；验证 COPY 必须选无 bytea 的完整同列数据并观察实际 SQL 分支，不能只读配置。

**Alternatives considered**：默认强制 COPY_MERGE 不是官方 AUTO；只测 SQL 字符串不足以认证。

## R3 — 错误、flush 与超时

**Decision**：write 使用 `Operate.commit()` 异步提交；checkpoint/schema/end 同步 `client.flush()`。native onError 记录首错并重抛；writer 每次 write/flush/close 与 1 秒周期 mailbox 检查首错。客户端负责批次/重试，connector 不叠加行缓存或提交线程池。

**Rationale**：`worker/WaitActionTaskExecutor:91-96` 在回调正常返回时可能吞失败；未完成 future 会重新排队，flush 并没有统一 task deadline。`worker/ConnectionProvider:168-169` 初始化 session 时设置 statement_timeout，是真正写入会话超时。

**JDBC evidence**：精确 8.6.1-200 JAR 的 PGProperty/ConnectionFactoryImpl 字节码确认 URL connectTimeout/socketTimeout 单位为秒，默认 10/0；乘 1000 后传 socket。PGStream.setNetworkTimeout 使用 Socket.setSoTimeout，只约束读取，不是发送 deadline。JAR SHA-256：`6e2fd372ad6f835f2b8a38ca85e402f10d1b6cd39728e24afed4c25093753489`。

**Limits**：retry.max-times 至少 1，表示总尝试次数（0 会跳过执行）；retry random 至少 1ms（存在取模零风险）。有限 connect/socket/statement timeout 加故障注入是首版策略；大 COPY 发送阻塞、黑洞、取消/close 有限结束仍属阻断发布门槛。若失败，实施必须修复终止路径，不可仅给 Future 设置超时后假装底层写入停止。

**Alternatives considered**：日志吞错、每条 syncCommit、第二提交池、把 task timeout 当整个 flush 截止时间均不选。

## R4 — 用户选择单表多 writer，分区前拆分主键更新

**Decision**：DataSink 新增 default-false `requiresPrimaryKeyUpdateSplit()`，DWS 开启；三类 pre-partition 在主键实际变化时发 UPDATE_BEFORE(old) + REPLACE(new)，再各自 hash。typed UPDATE_BEFORE 追加在 enum 尾部，不占用户 meta。

**Rationale**：默认 hash 对 UPDATE 使用 after（`DefaultDataChangeEventHashFunctionProvider:65-73`）；旧 writer 在新键分区删除旧键（`DwsWriter:218-222`），另一 writer 的延迟旧键写入可使旧键复活。拆分使 I(A)→U(A,B)→I(A) 在 A 分区成为 I、撤回、I。同 callback 输出两半，checkpoint 不插入 callback 中间；重放仍以相同规则归一化。

**Compatibility**：DataChangeEvent factory/opTypeString、默认 hash、serializer/copy 和所有沿途 operation switch 必须覆盖新操作；保留旧四种 tag/布局，增加黄金字节测试。常规、分布式、批模式都覆盖，其他 sink 能力默认 false。整体部署 common/runtime/connector，不能混新旧事件二进制。

**Limits**：比较 PK 原值而非 hash，binary 使用内容等值。Distributed 在 schema/route 前分区；多源合表的目标主键域必须不重叠或已有统一顺序，拆分不能提供跨源冲突键全序。两半不提供跨 writer 原子可见性。可检测的不满足路由前提的配置要拒绝，其他前提在上线核对中证明。

**Alternatives considered**：按表固定 writer 已由用户否决；metadata 魔法键、所有 UPDATE 都删除重插、全局改变其他 connector 均不选。

## R5 — native 顺序与内存

**Decision**：每 writer 一个 client，CN/DYNAMIC，native partition-min=max=1；跨 Flink writer 按 PK 并行，线程可跨表工作。显式设置三层缓冲预算，不新建 connector 缓冲。

**Rationale**：`collector/partition/DynamicSizePolicy:25-68` 按分布列 hash，无分布列时可轮询，不保证按主键。`PartitionCollector:126-133` 的 preAction 只串联本 partition。DwsClient 构造器默认可按每 client JVM 堆 40% 派生预算，多 writer 会叠加。

**Limits**：RecordBuffer 转为 action 后会从缓存计数扣除，仍有在途/编码/对象开销；native budget 不是硬 heap 限额。关闭常规定时刷新仍保留 force/内存保护。宽行和多表限速场景必须实测，超预算单行显式失败。

**实现陷阱**：RecordBuffer.forceFlush 仍依赖 flush() 的 batch/interval 条件，不能把 batch 设为 MAX_INT 来关闭自动刷新；false 模式使用 interval=0、batch=有限 force 阈值。DwsClient 构造器对显式 UPSERT 会把 force>=10000 改为 force-flush-upsert-size，后者必须同步配置为解析后的 force，避免静默覆盖。

**二进制预算修正**：`model/Record.getObjByteSize:190-216` 不计 byte[]，PK 派生还会增加对象开销。因此使用公开 Operate.getRecord 在 setObject 完成、commit 前进行保守估算，计入 byte[]/PK/容器开销。writer 只维护“上次成功显式 flush 后接受的估算字节数”及分表数字计数，达到预算前同步 flush，成功才清零；不依赖异步计数扣减，不保存额外行队列。这个保守保护和真实 heap 压测一起覆盖 native 累计低估。

**Alternatives considered**：默认 DirectDN 或 native 多 partition 不选；前者扩大部署/schema 限制，后者增加顺序证明成本。

## R6 — 大小写 DDL 后刷新需适配

**Decision**：沿用 pipeline flush→DDL→schema 通知；writer removeTableSchema 后 getTableSchema 按正确 TableConfig 重载，并更新 getter/PK 索引。DROP 仅清缓存；不直接调用 updateTableSchema。

**Rationale**：`CacheUtil.updateTableCache:215` 使用不带 TableConfig 的 JdbcUtil.getTableSchema，默认 caseSensitive=false；正常 loader 则传配置。这是 merge SQL 修复之外的独立风险。

**Recovery**：`DataSinkWriterOperator:176-183,238-245` 从恢复的 SchemaRegistry 补 Create；SchemaCoordinator 持久化 evolved/original schemas。验证这条路径，不保存第二份 schema state。必须覆盖大小写 ALTER 后首条、TTL 失效、恢复首条和空闲 writer。

**Alternatives considered**：等待 TTL、复制 native cache/SQL、每次 DDL 重建全 client 均不作为默认。

## R7 — 参数与转换

**Decision**：白名单映射或显式拒绝；公开默认改为 30000 条/3s/force 40000，显式旧值按契约处理；完整规则见 configuration.md。沿用已支持类型/时区规则，delete 只设置主键，不照搬 legacy 给所有字段 setObject 的做法。

**Rationale**：当前 factory 仅传连接三元组；sink-table/sink.parallelism 未被消费。多数 connectionPool* 是官方 BINLOG 参数，不是写入池。旧连接存活时间在 Pipeline 文档为秒，按 Duration.ofSeconds 映射，避开官方废弃 legacy 单位歧义。

**数据保护**：WRITE_FORMAT_STRING_U0000 原生默认 true，StringType 会静默删除 NUL；新配置必须固定 false，同时关闭 compatible-illegal-chars。NUL 无法由目标接受时明确失败，不能悄悄改值。

**Alternatives considered**：任意 native 参数透传、接受但忽略、静默非法字符替换均不选。

## R8 — 状态迁移与回退

**Decision**：最小 writer protocol marker v2，显式拒绝旧 v1；无 committer。首版迁移为核清旧提交并停止后，对独立空目标做一致全量+增量，再核对切换。回退用有证明的完整日志重放边界，否则同样空目标重同步。

**Rationale**：旧 writer v1 只有 jobId，在途数据依赖 committable/staging。去掉 committer 后只恢复源点位会漏数据；allowNonRestoredState 不代表安全。对已有脏目标直接重快照也会残留源端已删键。

**Alternatives considered**：通用自动 savepoint 转换、自动丢弃/清理旧状态、启动清遗留表均不选；特定来源点位迁移不预设支持。

## R9 — 验证

**Decision**：reactor/公共序列化兼容、两代 runtime 链接、openGauss smoke、真实 DWS 分层留证；新旧性能始终同时校验数据。

**Rationale**：此前本地构建被 reactor/JDK/Scala 边界阻断，不能记作 DWS 通过。现有 DwsContainer 是 openGauss 3.0.3、测试并行度 1，不证明真实 DWS 或多 writer。DWS 模块没有 flink2 版本覆盖，实施必须补齐后检查 effective-pom。

**Alternatives considered**：版本号升级即完成、单次峰值、mock 全绿或 -Pflink2 名称存在即兼容，均不选。
