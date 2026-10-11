# Quickstart: 升级后的验证与发布门槛

本指南供实施完成后执行。本轮只完成方案，下面的新增测试类、fixture 和环境变量读取逻辑尚需实施；不得把命令示例当作已运行结果。

## 1. 前置条件与安全范围

- 仓库根目录执行 Maven；Flink 1.20.3 使用项目支持的 JDK 11/17（首轮 JDK 11），Flink 2.2.0 使用 JDK 17。先核对 `java -version`、`mvn -version`，不要使用此前触发 Scala bridge 问题的 JDK 21。
- 完成 DWS 模块直接客户端依赖与 flink2 profile 改造；整套 common/runtime/composer/connector 使用同一制品版本。
- 单元/harness 无需数据库；现有容器需要 Docker，但它是 openGauss 3.0.3，不是 DWS。
- 真实 DWS 测试只使用已授权隔离库/schema，有创建/删除测试表及故障代理权限。无权限只跑不变更环境的检查，不借测试名操作生产表。
- 新增 `DwsNativePipelineITCase` 外部 fixture 从 `DWS_TEST_JDBC_URL`、`DWS_TEST_USERNAME`、`DWS_TEST_PASSWORD`、`DWS_TEST_SCHEMA` 读取配置。凭据通过已批准的秘密注入流程设置，不写入 YAML/代码/命令历史。显式选择此类而缺变量必须失败，不允许 skip 后显示绿色。
- 日志和报告只含脱敏信息、制品摘要、环境版本、计数/校验摘要、SQL 模式标识；移除现有测试基类打印密码的代码。

## 2. 构建、依赖与公开契约

以下命令从仓库根目录执行；使用 root reactor 的 `-am`，不要只构建孤立模块导致缺失未安装的本项目依赖。输出写入本次执行证据目录，再检查真实执行的测试数和失败数。

```bash
java -version
mvn -version
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws -am clean test -Dtest='Dws*Test,PrePartitionOperatorTest,DataChangeEventSerializerTest' -Dsurefire.failIfNoSpecifiedTests=false
```

新增测试保持 Dws*Test 命名或更新上面的选择器；不得仅匹配旧用例。另执行 common/runtime/composer 全量单元测试，证明未 opt-in sinks 的事件流没有改变：

```bash
mvn -pl flink-cdc-common,flink-cdc-runtime,flink-cdc-composer -am test
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws -am package -DskipTests
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws help:effective-pom
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws dependency:tree -Dincludes='com.huaweicloud.dws:*,org.apache.flink:*,io.dropwizard.metrics:*'
```

依赖解析若要求已安装 reactor artifact，使用同配置的 `-am install -DskipTests` 准备本地依赖后重跑 tree；不是 deploy，不上传仓库。保存 effective POM 与最终 JAR 内容，确认 direct client=2.1.0.6、JDBC=8.6.1-200、metrics=4.2.25，没有 dws-connector-flink、旧 RichSinkFunction 类或重复驱动打包。核对 SPI/shade 资源及 NOTICE。

在 JDK 17 下重复 clean 构建，不能共用上一代残留 class：

```bash
mvn -Pflink2 -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws -am clean test -Dtest='Dws*Test,PrePartitionOperatorTest,DataChangeEventSerializerTest' -Dsurefire.failIfNoSpecifiedTests=false
mvn -Pflink2 -pl flink-cdc-common,flink-cdc-runtime,flink-cdc-composer -am test
mvn -Pflink2 -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws help:effective-pom
mvn -Pflink2 -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws dependency:tree -Dincludes='com.huaweicloud.dws:*,org.apache.flink:*'
```

预期 DWS/runtime/composer 的有效 Flink 依赖及测试环境都是 2.2.0；若 DWS 仍出现 1.20.3，profile 改造未完成，不能宣称 2.x 通过。还需最终 JAR 在真实 Flink 1/2 小集群加载运行，单纯编译不证明无 NoSuchMethodError。

## 3. 必须先建立的回归用例

| 场景 | 操作/注入 | 必须观察 |
| --- | --- | --- |
| 旧键乱序 | 并行 2/4，令旧键 writer 延迟；I(A)→U(A,B)→I(A)；关闭独立 DELETE 再测 | A 为最后重建值，B 为更新值，无旧值复活/误删 |
| 回撤事件 | enum/serializer golden、copy、object reuse、hash 碰撞、复合/二进制 PK | 旧四事件字节不变；新事件往返正确；PK 比较不是 hash 比较 |
| 三拓扑 | Regular、Distributed、Batch，各有变键与不变键；schema 更改/恢复 | 启用时拆分，仅变键拆；其他 sink 默认事件数/分区完全不变 |
| 首错传播 | native 后台失败后不再输入；flush/close 同时失败 | 1 秒检查周期内触发 mailbox 失败调度（允许调度延迟），根因不被 close 覆盖 |
| 恢复 schema | 新 marker restore 后第一条数据无显式 Create 输入 | runtime 补 Create，getter/目标 metadata 正确；不新增 schema state 副本 |
| 旧状态拒绝 | 旧 v1 marker、pending committable、移除 committer、未知 marker | 明确拒绝；不通过 allowNonRestoredState 绕过 |
| 参数全表 | configuration.md 每个公开键、旧键、别名同值/冲突/单位边界 | 生效或明确拒绝；retry=0/URL timeout=0 失败；UPSERT 构造后 force 不被改写；禁自动刷新后 finite force 仍触发 |
| 转换 | NULL、Unicode、timestamp/LTZ、decimal、binary、复合 PK、NUL/非法字符 | delete 只发 PK；UPSERT/AUTO-COPY 的 NUL 不被静默删除，不支持则明确失败 |

## 4. 真实 DWS 验证

先完成外部 fixture。以下按指定类运行 Surefire test，不使用 `-Dit.test`：当前根 POM 的 ITCase 是 Surefire 的 integration-test execution，不是 Failsafe。

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws -am test -Dtest=DwsNativePipelineITCase -Dsurefire.failIfNoSpecifiedTests=false
mvn -Pflink2 -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws -am test -Dtest=DwsNativePipelineITCase -Dsurefire.failIfNoSpecifiedTests=false
```

两代切换仍要按第 2 节先 clean 并确认有效依赖。测试报告必须显示此类真实执行，不能因拼错类名而零用例通过。

矩阵：大小写 true/false × AUTO/COPY_MERGE。大小写敏感用混合 schema、表名、普通列、复合主键；大小写不敏感用符合既有归一化规则的目标。AUTO 使用同列完整行、无 bytea，批次低于 1000 和达到/超过 1000 各测；通过脱敏 SQL/action 类型证据分别看到 UPSERT/COPY_UPSERT，MERGE 组看到 COPY_MERGE。另用 bytea 验证官方回退，不把它算作 COPY 已覆盖。

每组覆盖 I/U/D/REPLACE、删除后重建、变键、多次同键更新；逐键逐字段和参考事件 reducer 比对，不能只比较行数。七类 schema 事件各有成功/失败用例；重点是 mixed-case ADD/RENAME/ALTER 后的第一条 AUTO/MERGE 写入、TTL 刷新、DDL 成功而 checkpoint 失败后的恢复。

## 5. 故障、内存与性能

### 五个恢复窗口

缓冲中、写入中、数据库已接收但响应丢失、checkpoint 确认前、确认后，各至少一次故障恢复；最终零缺失/多余键/字段差异。源端有序日志保留完整，并记录恢复点。额外覆盖新版本相同协议 rescale 2→4→2，包括新 writer 空 marker 分配；所有旧执行实例结束后再恢复，防止旧 writer 与新 writer 并存。

### 有限等待

测试连接失败、socket 读黑洞、大 COPY 发送阻塞、数据库锁等待、checkpoint 超时与任务取消。测试前固定 connect/socket/statement、重试与 Flink 取消时限，记录实际失败/释放时间。未按有限时限退出、关闭后仍不可控写入或泄漏线程/连接，均阻断发布；不能仅断言 Future 超时就算通过。

### SC-005 初始冻结基线

- 一台 TM、2 个 writer slot、JVM heap 2GiB；每 client 缓存 128/64/32MiB，native partition=1，write thread=1。
- 固定种子生成至少两张表，窄行约 1KiB 和宽行约 64KiB 两组；完整可重放事件日志；下游限速至输入目标速率的约 50%，持续至少 30 分钟。记录源实际背压后的速率，不能要求缓存无限吸收未处理输入。
- 记录 native 缓存估算、在途 action 数、JVM used heap/GC 后 retained heap、吞吐和 flush 时间。native 估算的阈值越界容差固定为一条最大合法记录；不把该阈值拿来约束整个 JVM。
- 无 OOM；在初始 5 分钟暖机后，GC 后 retained heap 的后 10 分钟中位数不得比前 10 分钟高出 128MiB，且观察到背压或配置限定的受控失败。该数值是计划初始验收预算，若环境需调整，先更新记录再运行，不可事后改标准掩盖泄漏。
- 同测 enable-auto-flush=false、多表、超大单条拒绝；显式说明估算预算不包含上游已经反序列化的巨型记录。
- 必测超过单 partition 预算的单条 binary，断言 Operate.commit 未调用且目标不存在该记录；再测多条各自合法、累计超预算的 binary，验证保守数字计数触发同步 flush/背压和真实 heap 有界，不能用 native 漏计 byte[] 的指标判定通过。

### SC-006 对照

旧 staging 路线和新 AUTO 各用相同集群、表结构、数据、资源、并行度和 correctness oracle，稀疏/批量各至少三次。报告中保留事务可见性差异；记录吞吐、p50/p95 延迟、峰值 heap、checkpoint 时间、实际写入路径、CPU/网络瓶颈。性能回退必须解释并由维护者在发布前评估，不以单次峰值宣称优化完成。

## 6. 迁移与回退演练

按 [C6](contracts/events-and-lifecycle.md#c6--状态兼容与首次迁移) 在隔离环境各演练一次，旧任务含在途数据、已删除旧键和主键修改。先核清旧提交再停，新任务向空目标完整快照+增量，逐键核对后模拟切换；回退使用有证明的旧日志边界，否则独立空目标重同步。

证据包括旧/新制品摘要、恢复点/日志保留、状态拒绝结果、源目标校验、在途资源归属及清理候选。没有授权不清理、不切生产消费端；误删业务或其他任务资源必须为零。

## 7. 发布结果表

每项记录 PASS/FAIL/BLOCKED、证据位置与环境；不能将 BLOCKED 计为通过。

| 规格指标 | 证据 |
| --- | --- |
| SC-001 | 五类事件序列正常/恢复的逐键逐字段结果 |
| SC-002 | 四组大小写/模式矩阵与实际 SQL 路径 |
| SC-003 | 五窗口恢复、空闲首错、取消释放 |
| SC-004 | 全量配置合同、别名/单位/冲突测试 |
| SC-005 | 30 分钟限速预算、背压、heap/在途证据 |
| SC-006 | 稀疏/批量各三轮新旧对照 |
| SC-007 | 含在途数据的迁移与回退各一次 |
| SC-008 | 七类 schema success/failure、两代 Flink 真实加载和恢复 |

## 当前状态（2026-10-11）

- 本地 JDK 11 已完成 DWS clean 单元门禁（61 tests）以及 common（79）、runtime（882，1 skipped）、values（5）和 composer（18）的 clean reactor 回归，均无失败/错误。
- AUTO writer、typed 撤回与分区、异步首错、schema cache reload、旧状态拒绝、全量配置、缓冲补偿、安全指标、中英文文档及最终 JAR 静态检查已有本地证据；详见 `validation/`。
- 真实 DWS 的写入/DDL/故障/迁移/性能场景尚未执行，因为本环境没有 `DWS_TEST_*` 授权配置；Flink 1 小集群及 Flink 2/JDK 17 门禁也仍为 BLOCKED。不得把本地测试解释为发布就绪，最终状态见 `validation/release-readiness.md`。
