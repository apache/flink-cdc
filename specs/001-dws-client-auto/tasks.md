# Tasks: DWS 官方客户端 AUTO 写入升级

**Input**: [spec.md](spec.md)、[plan.md](plan.md)、[research.md](research.md)、[data-model.md](data-model.md)、[配置契约](contracts/configuration.md)、[事件契约](contracts/events-and-lifecycle.md)、[quickstart.md](quickstart.md)。

**Branch**: `FLINK-39327`。本轮仅生成待办，所有复选框保持未完成；没有开始 Java/POM 改造或运行数据库验收。

**Tests**: 规格 SC-001～008 明确要求测试，故包含单元、operator harness、真实 DWS、故障和迁移任务。先建立可复现的失败测试，再实现；编译失败只能证明 API 尚缺，不能代替行为反例。

**Organization**: Setup → Foundation → US1(P1) → US2(P1) → US3(P1) → US5(P1) → US4(P2) → Cross-cutting。保留规格原故事编号，不因排序改变 US4/US5 身份。

## Format and paths

- 所有路径相对仓库根目录，任务中给出完整路径；标注“新增”的文件由实施创建，其余优先扩展现有文件。
- `[P]` 只表示指定阶段前置完成后、同批任务之间文件独立；不是允许跳过依赖，也不是自动启动子代理。共享 DwsWriter/factory/真实测试 fixture 的任务串行。
- `validation/*.md` 是实施任务未来生成的脱敏证据，不在本轮伪造。原始大日志保存在执行 session；证据记录版本/摘要/测试数/结果/日志定位，不能写凭据或业务行。
- 缺真实 DWS、旧制品或授权时记录 BLOCKED，不勾完成，不把 openGauss/mock 当真实验收。
- 任何数据库清空、生产切换、遗留资源删除、commit/push 都需要对应授权；本清单不提供自动授权。

## Phase 1: Setup — 冻结基线与测试资源

**Purpose**: 保留旧行为/状态证据，建立有独立 oracle 的隔离验证入口。T001 先行，T002/T003 可并行，随后 T004。
- [X] T001 在 `specs/001-dws-client-auto/validation/baseline.md`（新增）记录基线 commit、工作区差异、JDK/Maven、两代 Flink 版本与旧制品 SHA-256，并保存当前 dependency tree 和构建阻塞签名；不重置现有改动、不将基线构建失败记为 DWS 测试通过。
- [X] T002 [P] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/utils/DwsExpectedState.java`（新增）建立独立于 writer 实现的事件序列 oracle，覆盖复合/二进制键、变键、最终删除与删除后重建；记录比较键/字段而非只比较行数的方法。
- [X] T003 [P] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/utils/DwsNativeTestEnvironment.java`（新增）实现隔离外部 DWS fixture：读取 quickstart 的四个 DWS_TEST_* 变量，显式选中测试而配置缺失必须失败；同时删除 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/utils/DwsSinkTestBase.java` 的密码/完整敏感 URL 打印，限定资源清理归属。
- [X] T004 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/resources/compatibility/legacy-writer-state-v1.hex` 与 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/resources/compatibility/legacy-committable-v1.hex`（新增）从基线真实 serializer 生成脱敏黄金样本，并在 `specs/001-dws-client-auto/validation/baseline.md` 记录来源/摘要和可复现旧任务在途场景；先保留样本再删除旧协议类。

**Checkpoint**: 基线可追溯，oracle 与隔离环境契约已就绪；没有向生产写入。

## Phase 2: Foundational — 直接客户端依赖与最小安全配置

**Purpose**: 所有故事的编译、转换与初始化基础。T005/T006 为独立测试批，T007～T010 串行；本阶段中间提交状态不发布，结束必须恢复可构建。
- [X] T005 [P] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/DwsRecordConverterTest.java`（新增）为现有 DwsUtils/SinkFunction 转换建立回归 oracle：NULL、Unicode、decimal、binary、timestamp/LTZ、时区、复合 PK；验证 delete 只设置 PK、NUL 不被新客户端默认静默删除。
- [X] T006 [P] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/DwsClientDefaultsTest.java`（新增）冻结 AUTO/30000/3s/40000、CN 单 native partition、128/64/32MiB、有限 retry/timeout、NUL/illegal-chars=false 的构造后配置断言，并断言显式 UPSERT 不改写 force、关闭自动刷新仍有 finite force。
- [X] T007 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsRecordConverter.java`（新增小型 helper）复用 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/utils/DwsUtils.java` 的已有类型语义，分离完整行 write 与 PK-only delete；随后删除 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsSinkFunction.java` 和 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/DwsSinkFunctionTest.java`，不新增另一套运行引擎。
- [X] T008 修改 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/pom.xml` 为直接 dws-client:2.1.0.6，移除 dws-connector-flink，收敛 JDBC 8.6.1-200/metrics 4.2.25/Druid 1.2.23，补与 runtime/composer 一致的 flink2 profile 和 compat 配置；同步检查 shade 的重复驱动/legacy Flink API/Lombok 包含规则。
- [X] T009 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/factory/DwsDataSinkFactory.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsDataSink.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsDataSinkOptions.java` 移除 DwsConnectionOptions/import 常量，改为传输纯配置快照；落实 T006 安全默认、四种模式白名单与 credential/time-zone/default-schema 规则，暂不支持的显式参数必须拒绝而非忽略；旧生产 writer 的切换留给 US1。
- [ ] T010 按 `specs/001-dws-client-auto/quickstart.md` 对 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/pom.xml` 执行两代 clean reactor 编译及 T005/T006，记录 effective POM、依赖树与结果到 `specs/001-dws-client-auto/validation/foundation.md`（新增）；DWS 2.x 依赖仍为 1.20.3 或残留 DwsConnectionOptions 时不得进入后续故事。

**Checkpoint**: 直接 client 与转换基础可构建、最小配置安全；尚未声称 native writer 完成。

## Phase 3: US1 — 默认 AUTO 与最终数据正确（P1，隔离 MVP）

**Goal**: 单一路线 native SinkV2、完整 CDC、保留单表多 writer，通过分区前拆分修复主键变化的旧键顺序。

**Independent Test**: 无 bytea 的同列记录低于/达到 COPY 阈值分别观察 UPSERT/COPY_UPSERT；并行 2/4 执行 I(A)→U(A,B)→I(A)，含独立 delete 关闭、复合 PK、重复键及无 PK 拒绝，逐键逐字段一致。不开启能力的其他 sinks 行为不变。

### Tests first

- [X] T011 [P] [US1] 扩展 `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/serializer/event/DataChangeEventSerializerTest.java`，冻结旧四类事件黄金字节并添加 UPDATE_BEFORE serialize/deserialize/copy/对象复用失败用例；覆盖 before-only 结构、meta 保留和旧枚举顺序。
- [X] T012 [P] [US1] 扩展 `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/partitioning/PrePartitionOperatorTest.java`，为三拓扑建立变键/非变键、hash 碰撞、binary 深等值、schema getter 刷新、同 callback 两半、无 PK/缺 before/after、opt-out 不变的失败用例。
- [X] T013 [P] [US1] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterEventTest.java`（新增）定义 native 调用契约：完整行 upsert、PK-only delete、撤回无条件执行、独立 DELETE 遵循开关、未拆分变键 UPDATE 拒绝、无效 PK 写入前失败。

### Implementation and integration

- [X] T014 [US1] 在 `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/sink/DataSink.java` 增加 default-false requiresPrimaryKeyUpdateSplit；在 `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/event/OperationType.java` 尾部追加 UPDATE_BEFORE，`flink-cdc-common/src/main/java/org/apache/flink/cdc/common/event/DataChangeEvent.java` 增加 factory/opTypeString，`flink-cdc-common/src/main/java/org/apache/flink/cdc/common/sink/DefaultDataChangeEventHashFunctionProvider.java` 对撤回使用 before；不改变其他操作定义。
- [X] T015 [US1] 在 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/serializer/event/DataChangeEventSerializer.java` 增加撤回的读写/深拷贝分支，保持旧四事件 tag/布局；通过 T011 黄金测试，禁止借此次升级给所有旧事件添加新 envelope。
- [X] T016 [US1] 在 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/partitioning/PrimaryKeyUpdateSplitter.java`（新增、小型共享 helper）实现按 Schema 创建 PK getters、原值/二进制深比较与 typed 撤回+REPLACE 输出；无状态/无 metadata 魔法键，输入无效 fail closed，缓存仍由 operator 所有。
- [X] T017 [P] [US1] 在 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/partitioning/RegularPrePartitionOperator.java` 接入能力与 splitter，复用 evolved schema 查询，schema 变化同时刷新 hash/getters；先拆再 hash，同 callback 发两半，保留旧构造器默认 false。
- [X] T018 [P] [US1] 在 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/partitioning/DistributedPrePartitionOperator.java` 接入能力与 splitter，复用 schemaMap，在分区前输出撤回/REPLACE并保留 source partition 标识；不声称 route 后冲突主键获得跨源全序。
- [X] T019 [P] [US1] 在 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/partitioning/BatchRegularPrePartitionOperator.java` 接入能力与 splitter，基于 Create schema 保持对象所有权和两个输出顺序；默认 false 路径的事件数/分区保持不变。
- [X] T020 [US1] 在 `flink-cdc-composer/src/main/java/org/apache/flink/cdc/composer/flink/FlinkPipelineComposer.java`、`flink-cdc-composer/src/main/java/org/apache/flink/cdc/composer/flink/translator/PartitioningTranslator.java` 透传能力至三拓扑，在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsDataSink.java` 显式开启；扩展 `flink-cdc-composer/src/test/java/org/apache/flink/cdc/composer/flink/FlinkPipelineComposerTest.java` 证明其他 sinks 默认不启用，旧方法重载仍可调用。
- [X] T021 [US1] 改写 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsSink.java` 和 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 接入每 writer 单 DwsClient，采用 T009 配置/转换与单 native partition；实现事件映射和基本同步 flush/close，不再 writer 内跨键删除；在 native client 初始化处应用并验证 T006 固定安全值。
- [X] T022 [US1] 移除 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsCommitter.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsCommittable.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsCommittableSerializer.java` 的生产引用与旧实现，更新 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsSinkTest.java`；审计 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsSqlUtils.java` 的 DDL 使用再保留/归位必要工具，并替换仅验证旧协议的测试，保留 T004 黄金证据。
- [ ] T023 [US1] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsNativePipelineITCase.java`（新增）使用 T002/T003 实现无 bytea AUTO 小/大批实际分支、I/U/D/REPLACE、复合键、变键及旧键 writer 人工延迟用例；使用 pipeline.parallelism=2/4，断言删除关闭不阻断撤回。
- [ ] T024 [US1] 运行 T011～T023 的单元/harness 和真实隔离 DWS 基础用例，将每键/字段 oracle、实际模式及 opt-out 回归记录到 `specs/001-dws-client-auto/validation/us1.md`（新增）；mock/openGauss 结果单独标注，环境缺失时真实部分保持 BLOCKED。

**Checkpoint**: US1 是可演示的隔离 MVP，不可用于生产升级；后续恢复、schema、预算与迁移门槛仍未完成。

## Phase 4: US2 — 故障重放后的最终一致（P1）

**Goal**: 明确 flush/checkpoint 边界、首错传播、协议 marker 与新版本恢复；失败不能被日志吞掉。

**Independent Test**: 五个故障窗口恢复均收敛；空闲异步失败被 mailbox 感知；end-of-input 无尾部遗漏；新协议 2→4→2 rescale（含空 marker 分配）正确；黑洞/取消在冻结时限内退出。

### Tests first

- [X] T025 [P] [US2] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterLifecycleTest.java`（新增）编写异步首错/空闲 timer/mailbox、flush 前后失败、snapshot/end-of-input、close suppressed 根因的失败用例，确保 onError 正常 return 会被测试识别为错误。
- [ ] T026 [P] [US2] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsRecoveryITCase.java`（新增）建立五窗口可控故障与新协议 rescale 2→4→2 场景，复用独立 oracle；包括源有序日志保留、旧执行实例全部退出、schema 恢复首条、全任务 marker 来源记录。

### Implementation and integration

- [X] T027 [US2] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterState.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterStateSerializer.java` 和 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterStateSerializerTest.java` 实现/验证 serializer v2 固定 marker，接入 StatefulSink/restore；接受合法 scale-up 空分配及多个一致 marker，不存 schema/行/jobId，不把单 writer marker 当全任务来源证明。
- [X] T028 [US2] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 接入 native onError 首错原子记录并重抛、1s mailbox 健康检查、write/flush/close 首错检查及幂等资源释放；停止 timer，保留初始失败、关闭失败 suppressed，不在 native 线程调用 operator 生命周期。
- [X] T029 [US2] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 和 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsSink.java` 完成 flush(false/true) 与 snapshot 的等待顺序；checkpoint/schema/end 仅在全部已接受写入成功后推进，失败不生成成功 marker、不吞记录；通过 T025。
- [ ] T030 [US2] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsRecoveryITCase.java` 加入连接失败、socket 读黑洞、大 COPY 发送阻塞、锁等待、checkpoint 超时及 cancel/close 场景，冻结驱动/statement/retry/取消时限；若 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 的实际终止路径不满足则修复或阻断，不用 Future 超时冒充底层取消。
- [ ] T031 [US2] 在两代有效 Flink 环境执行 T026/T030 和 lifecycle 用例，将五窗口、空闲失败、rescale、线程/连接释放及最终一致证据记录到 `specs/001-dws-client-auto/validation/us2.md`（新增）；说明至少一次及中间可见性，不声明 checkpoint 原子提交。

**Checkpoint**: 新协议正常恢复和失败传播可验证；旧协议拒绝/迁移由 US5 完成。

## Phase 5: US3 — 大小写与 schema 演进（P1）

**Goal**: AUTO/COPY_MERGE 在正确字段上执行，DDL 前后严格隔离旧数据与新 metadata。

**Independent Test**: case-sensitive true/false × AUTO/COPY_MERGE 四组均通过；七类 schema success/failure、mixed-case DDL 后首条、TTL 与恢复首条无旧结构写入。

### Tests first

- [X] T032 [P] [US3] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterSchemaTest.java`（新增）编写 mixed-case reload、TTL、DROP 不重载、TRUNCATE、getter/PK 索引切换测试；显式区分 removeTableSchema+getTableSchema 与有缺陷的 updateTableSchema 路径。
- [ ] T033 [P] [US3] 扩展 `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/operators/sink/DataSinkOperatorWithSchemaEvolveTest.java` 与 `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/operators/schema/common/SchemaDerivatorTest.java`，测试 flush→DDL→schema→data、任阶段失败停止、恢复补 Create，以及 UPDATE_BEFORE 的投影/coercion/meta 保留。

### Implementation and integration

- [X] T034 [US3] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/utils/DwsUtils.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsMetadataApplier.java` 统一 writer/DDL 标识符处理，保留 insensitive 归一化；内嵌引号等未支持名称明确拒绝，不自行重写 native MERGE SQL，不因 case-sensitive 改模式。
- [X] T035 [US3] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 实现 schema 通知后的 removeTableSchema+按 TableConfig reload、getter/PK 缓存更新及 native collector 新 schema 切换；CREATE/ADD/ALTER_TYPE/DROP_COLUMN/RENAME_COLUMN/TRUNCATE/DROP 分别处理，不新增 RENAME_TABLE。
- [ ] T036 [US3] 核对 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/sink/DataSinkWriterOperator.java` 的既有 flush/恢复补 Create 与 `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/schema/common/SchemaDerivator.java` 的新事件传播，仅修复测试证实的缺口；扫描沿途 operation switch，不新增 schema 持久副本；通过 T033。
- [ ] T037 [US3] 扩展并运行 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsNativePipelineITCase.java` 的四组大小写/模式和七类 DDL success/failure（含 DDL 已成功但 checkpoint 失败），两代环境证据保存到 `specs/001-dws-client-auto/validation/us3.md`（新增）；bytea 回退另测，不能拿它证明 COPY_MERGE 已覆盖。

**Checkpoint**: 原 merge 引用修复和 connector cache 刷新共同通过真实回归，不能只凭版本记录勾选完成。

## Phase 6: US5 — 现有任务迁移与回退（P1）

**Goal**: 明确拒绝旧 savepoint 直升，保全在途数据，提供空目标重同步及有证据的回退流程。

**Independent Test**: 旧 v1/未知 state 与未映射 committer 被拒绝；隔离旧任务含在途/删除/变键数据，迁移和回退各一次，逐键一致且没有误删其他任务资源。此阶段证明功能流程；全配置集成后还须最终回归。

### Tests first

- [X] T038 [P] [US5] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsMigrationCompatibilityTest.java`（新增）消费 T004 黄金样本，验证旧 writer v1/未知 marker/旧 pending committable 不被静默接受，合法新协议空分配不被误拒；测试不得依靠删除输入状态获得绿色。
- [X] T039 [P] [US5] 在 `docs/content.zh/docs/connectors/pipeline-connectors/dws.md` 和 `docs/content/docs/connectors/pipeline-connectors/dws.md` 增加迁移/回退小节：旧任务 drain 与归属清单、禁止 allowNonRestoredState/改 UID 绕过、空目标一致快照+增量、源日志保留、核对/切换授权和遗留清理边界；不把脏目标重跑 snapshot 写为通用迁移。

### Implementation and integration

- [X] T040 [US5] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterStateSerializer.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsSink.java` 落实旧 v1/未知 marker 显式迁移诊断，联合 Flink 默认未映射状态失败机制验证；在 `specs/001-dws-client-auto/validation/migration-preflight.md`（新增）列出整套制品/state 来源人工核对项，明确运行时不能单凭空 marker 检测所有 UID 绕过。
- [ ] T041 [US5] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsMigrationITCase.java`（新增）构建隔离迁移/回退验证场景，读取 T001 已核验旧制品及在途场景作为前置输入，复用外部 fixture/oracle；缺少旧制品或授权则明确失败/BLOCKED，不自动下载执行任意旧包、不自动清空用户目标。
- [ ] T042 [US5] 执行 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsMigrationITCase.java` 的在途迁移与回退各一次，把旧提交核对、空目标重同步、删除/变键结果和零误删证据写入 `specs/001-dws-client-auto/validation/us5.md`（新增）；实际切生产消费端与删除旧资源不在任务授权内。

**Checkpoint**: 已有可演练路线和旧状态拒绝，不承诺新旧状态双向直载。

## Phase 7: US4 — 参数真实生效与内存受控（P2，生产必需）

**Goal**: 完成全量配置映射/拒绝、字节预算补偿及可观测性；验证调优效果，不凭单次峰值承诺性能。

**Independent Test**: 每公开项至少一个生效/拒绝测试，别名/单位/冲突零静默忽略；二进制单条提交前拒绝、累计预算刷新；30分钟限速无 OOM、背压/受控失败；新旧稀疏/批量各三次对比。

### Tests first

- [X] T043 [P] [US4] 扩展 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/factory/DwsDataSinkFactoryTest.java` 对照 configuration.md 全表参数化测试：默认/显式/同义等值/冲突、URL 秒与旧毫秒、禁用项、单位/范围/溢出、distribution 组合、sink.parallelism/sink-table 指引，以及无意外 native 透传。
- [X] T044 [P] [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterBufferTest.java`（新增）先复现 native 漏计 byte[]，测试单条超限 commit 不调用、累计多表/all/partition 限额、PK/对象开销、flush 成功才清零、关闭定时仍 force、UPSERT 构造不覆盖 force。
- [X] T045 [P] [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriterMetricsTest.java`（新增）测试 accepted/written/failed 与 flush 指标、首错/模式诊断及其精确/估算边界；用测试凭据和记录哨兵断言正常/错误/初始化日志不泄漏，缺原生指标不能伪造数值。

### Implementation and integration

- [X] T046 [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsDataSinkOptions.java` 完成 configuration.md 的公开键/别名/三层预算/defaults，给已拒绝占位参数和模式明确迁移提示；不存在任意 dws.client.* 开放，不暴露 DirectDN/compareField/partial-update。
- [X] T047 [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/factory/DwsDataSinkFactory.java` 完成全表解析/范围/冲突、连接寿命秒/idle毫秒及 URL connect/socket 秒转换，结构化合并不覆写无关参数；明确拒绝 connectionPool*、connectionSize、日志开关等契约禁用项，distribution 列有效性在 schema 可用时再校验。
- [X] T048 [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 的 client 构造及 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/DwsDataSink.java` 的配置传递中应用 typed Memory、固定 CN/partition=1、false 自动刷新时 interval=0/batch=finite force、隐藏 UPSERT force 同值及 NUL/illegal-chars=false；验证构造后实际值，禁止静默回退。
- [X] T049 [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 实现 commit 前保守大小预检和 writer/分表数字计数，补 byte[]、PK、容器开销；将超 all/min(table,partition) 时先同步 flush，成功才清零；单条超限拒绝，异步成功不扣计数，不额外缓存记录。
- [X] T050 [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/java/org/apache/flink/cdc/connectors/dws/sink/v2/DwsWriter.java` 接入安全指标/生效配置/首错和模式诊断，重试与实际 SQL 路径只用公开 native 能力或受控测试观测；严格区分 native buffer estimate、保守计数与 JVM heap，不反射 private 字段，不串接完整 Event。
- [ ] T051 [US4] 在 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsBufferITCase.java`（新增）实现多表限速、窄/宽/binary、禁自动刷新、超大记录及固定 seed 重放场景；在 `specs/001-dws-client-auto/validation/performance-protocol.md`（新增）预先冻结 quickstart 资源/预算/容差、旧新两路线稀疏/批量各三轮对照和采样口径。
- [ ] T052 [US4] 运行全表配置测试和 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsBufferITCase.java`，完成至少30分钟限速及新旧各三轮稀疏/批量比较，将正确性、延迟/吞吐/heap/checkpoint、实际分支与性能回退解释写入 `specs/001-dws-client-auto/validation/us4.md`（新增）；不事后调整容差或把 native 漏计当真实内存。

**Checkpoint**: 调优行为与预算在真实运行中有证据；P2 表示故事优先级，不表示可从生产版本省略安全项。

## Phase 8: Polish & Cross-Cutting — 制品与发布证据

**Purpose**: 汇总五个故事，排除跨模块/版本回归。T053/T054 可并行，后续共享制品/集群验证串行。
- [X] T053 [P] 完成 `docs/content/docs/connectors/pipeline-connectors/dws.md` 英文使用说明：AUTO/至少一次、完整配置表与默认变化、单表多 writer 条件、typed 撤回的整体制品部署、大小写范围、缓冲与超时边界；保留 T039 迁移章节并核对可运行 YAML。
- [X] T054 [P] 完成 `docs/content.zh/docs/connectors/pipeline-connectors/dws.md` 同等中文说明，逐项对齐英文及 contracts，不新增“exactly-once”“所有大小写均修复”“内存硬上限”等过度承诺，保留 T039 迁移章节。
- [X] T055 复核 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/pom.xml`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/main/resources/META-INF/services/org.apache.flink.cdc.common.factories.Factory` 与仓库 `NOTICE` 的制品/授权边界，按实际打包规则调整必要 notice；保存最终 shade/SPI/驱动/客户端版本与无旧协议引用证据至 `specs/001-dws-client-auto/validation/artifact.md`（新增）。
- [ ] T056 按 `specs/001-dws-client-auto/quickstart.md` 执行两代 clean 全部目标单元/harness、common/runtime/composer 回归和最终 JAR 小集群加载；核对新加 Test/ITCase 确实被选择，更新选择器并把测试数/有效依赖/类加载结果写入 `specs/001-dws-client-auto/validation/runtime-matrix.md`（新增），不接受零测试绿色。
- [ ] T057 在最终集成配置和制品上重跑关键串联场景：变键+DDL+故障+恢复+内存限速及迁移/回退核对，复用 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsNativePipelineITCase.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsRecoveryITCase.java`、`flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/src/test/java/org/apache/flink/cdc/connectors/dws/DwsMigrationITCase.java`，将跨故事证据记录到 `specs/001-dws-client-auto/validation/integrated.md`（新增）；必要授权/环境缺失标 BLOCKED。
- [X] T058 在 `specs/001-dws-client-auto/validation/release-readiness.md`（新增）逐项映射 FR-001～018/SC-001～008 到实际证据与 PASS/FAIL/BLOCKED，检查全部关键回归和性能回退说明，更新 `specs/001-dws-client-auto/quickstart.md` 的实际执行状态；任何缺失保持未就绪，只交付 reviewable 结果，不自动发布/提交/推送。

## Dependencies & Execution Order

### 阶段依赖图

```text
T001 基线 → {T002 oracle ∥ T003 fixture} → T004 旧状态样本
    → Foundation T005～T010
    → US1 T011～T024（隔离 MVP）
    → US2 T025～T031（恢复）
    → US3 T032～T037（schema）
    → US5 T038～T042（迁移流程）
    → US4 T043～T052（完整调优/预算）
    → T053～T058（文档、整套制品与发布证据）
```

阶段按规格优先级列出，编号是推荐拓扑顺序，不要求每个相邻任务都有语义依赖。下表是实际依赖；不得把故事独立验收误解为零共享代码依赖。

| 范围 | 最小前置与合流规则 |
| --- | --- |
| T002/T003 | T001；二者文件独立 |
| T004 | T001～T003，旧协议删除前完成 |
| T005/T006 | Setup 完成；测试编写可并行 |
| T007→T008→T009→T010 | T005/T006 后串行，T010 合流验证 |
| T011/T012/T013 | T010；三个测试文件组独立 |
| T014→T015→T016 | US1 测试批完成，依次建立公共操作/序列化/splitter |
| T017/T018/T019 | T014～T016；各一个不同 pre-partition 文件 |
| T020→T021→T022→T023→T024 | 三拓扑合流后串行；生产切换和旧协议清理同一阶段验收 |
| T025/T026 | T024；单测与故障 fixture 文件独立 |
| T027→T028→T029→T030→T031 | US2 测试批后串行，共享 writer/恢复测试 |
| T032/T033 | T031；connector schema test 与 runtime test 文件独立 |
| T034→T035→T036→T037 | US3 测试批后串行 |
| T038/T039 | T031、T037；迁移测试与文档可并行 |
| T040→T041→T042 | T038/T039 后串行，复用状态与隔离集群 |
| T043/T044/T045 | T042；配置/缓冲/指标测试文件独立 |
| T046→T047→T048→T049→T050→T051→T052 | US4 测试批后串行，共享 options/factory/writer |
| T053/T054 | T052；两个语言文档可并行 |
| T055→T056→T057→T058 | 两文档合流后串行，制品与发布证据归一 |

**Story dependencies**: US1 是公共事件与 native writer 基础；US2 基于 US1；US3 复用 US2 flush/失败契约；US5 复用 US2 state 及 US3 schema 并可先验证小规模迁移；US4 增量完善基础安全配置，最终 T057 重验迁移和恢复。共享 DwsWriter 意味着故事实现不宜无条件并行。

所有真实环境任务还依赖可用的已授权隔离 DWS；T041/T042 额外依赖旧制品与可重放源。外部条件缺失不阻止其他不依赖该条件的只读/单测工作，但不允许跳过该验收后声明故事或发布完成。

## Parallel Examples by Story

以下为任务分配示例，不自动启动代理，也不允许同一 fixture/集群并发制造相互干扰。

| 故事 | 可并行例子 | 必须合流的位置 |
| --- | --- | --- |
| US1 | T011 serializer 测试 ∥ T012 三拓扑测试 ∥ T013 writer event 测试；T016 后 T017 ∥ T018 ∥ T019 | 公共实现 T014；composer 透传 T020 |
| US2 | T025 lifecycle 单测 ∥ T026 故障/恢复 fixture 编写 | T027 state 实现；数据库执行仍串行 |
| US3 | T032 connector schema 单测 ∥ T033 runtime schema 测试 | T034～T037 |
| US5 | T038 旧状态兼容测试 ∥ T039 双语迁移文档 | T040 状态诊断与迁移演练 |
| US4 | T043 配置测试 ∥ T044 缓冲测试 ∥ T045 指标/脱敏测试 | T046 后生产实现串行 |

另外可并行：Setup T002/T003、Foundation T005/T006、最终双语文档 T053/T054。共 21 个 [P] 任务，分为 9 个明确批次；不是 21 个任意任务可同时开始。

## Requirement and Acceptance Traceability

| 要求 | 主要实现/验证任务 |
| --- | --- |
| FR-001 AUTO/实际模式 | T006、T009、T021、T023、T024、T048 |
| FR-002 主键 CDC | T005、T007、T012～T021、T023、T024 |
| FR-003 变键/独立删除 | T012～T023 |
| FR-004 至少一次语义 | T025～T031、T039、T053、T054 |
| FR-005 恢复最终一致 | T002、T026、T029～T031 |
| FR-006 flush/恢复边界 | T025、T028、T029、T031、T033 |
| FR-007 有限等待/重试 | T006、T009、T028、T030、T031、T043、T047 |
| FR-008 同键顺序 | T012、T016～T023、T026、T031 |
| FR-009 大小写 AUTO/MERGE | T032、T034、T035、T037 |
| FR-010 insensitive/标识符 | T032、T034、T037 |
| FR-011 schema 次序 | T033、T035～T037 |
| FR-012 刷新/预算/背压 | T006、T044、T046、T048、T049、T051、T052 |
| FR-013 配置映射 | T009、T043、T046、T047、T053、T054 |
| FR-014 诊断/隐私 | T003、T045、T050、T052 |
| FR-015 迁移/回退 | T004、T038～T042、T057 |
| FR-016 无自研提交/安全清理 | T022、T039～T042、T055 |
| FR-017 类型/数据不静默损失 | T005、T007、T009、T023、T037、T048、T049 |
| FR-018 两代环境 | T008、T010、T031、T037、T056、T057 |
| SC-001 事件最终状态 | T024、T031、T037、T057 |
| SC-002 四组大小写/模式 | T037、T057 |
| SC-003 五窗口故障 | T026、T030、T031、T057 |
| SC-004 每参数生效/拒绝 | T043、T046～T048、T052 |
| SC-005 30分钟限速 | T044、T049、T051、T052 |
| SC-006 稀疏/批量各三轮 | T051、T052 |
| SC-007 在途迁移/回退 | T041、T042、T057 |
| SC-008 schema/两代兼容 | T033、T037、T056、T057 |

## Implementation Strategy

1. **隔离 MVP**：T001～T024，先证明默认 AUTO、分区前变键、主键-only delete、单表多 writer 和 opt-out 兼容。即使此时通过，也不发布或迁移生产。
2. **可靠性增量**：US2 与 US3 逐个完成；依赖错误、schema、恢复都必须有反例与通过证据。
3. **迁移与运维增量**：US5 提供可验证迁移，US4 完成全部参数/预算/性能；最终集成回归消除分阶段实现差异。
4. **生产就绪**：仅 T058 全门槛通过后提供发布建议；执行发布、切换、清理或推送仍需用户授权。P2 的内存安全不能省略。

**运行命令维护**：沿用 quickstart 的 Maven reactor/JDK/profile 方式；新增真实测试类后，将明确选择器更新为 `-Dtest='DwsNativePipelineITCase,DwsRecoveryITCase,DwsMigrationITCase,DwsBufferITCase'`。这些类仅在外部 DWS 测试路径选择，缺配置必须失败；默认无数据库单测不应因它们误启动集群。不要用 -Dit.test，因为仓库 ITCase 使用 Surefire。每轮检查报告中具体类名和测试数，避免 failIfNoSpecifiedTests=false 掩盖目标类漏选。

**完成判定**：只在实现及该任务约定验证真实完成后勾选；报告中的 PASS/FAIL/BLOCKED 与证据一致。遇超时/内存/迁移反例先修复，不能将新 native 默认或未证明恢复假设写成可发布保证。

## Generation Validation

- 共 58 项：Setup 4、Foundation 6、US1 14、US2 7、US3 6、US5 5、US4 10、Cross-cutting 6。
- 所有任务采用未勾选 checkbox、顺序唯一 T001～T058、适用 [P]/[USn] 和明确文件路径；没有编号占位符或无归属故事任务。
- 已覆盖 5 个故事、18 项 FR、8 项 SC、三拓扑、两代 Flink 及真实 DWS/迁移证据；没有把计划中的新测试标为已经通过。
- setup-tasks.sh 按 feature.json 定位并列出全部设计输入；目标仓库无 constitution/extensions，before_tasks/after_tasks 钩子按规则跳过，不套用 helper 项目钩子。
