# Implementation Plan: DWS 官方客户端 AUTO 写入升级

**Branch**: `FLINK-39327` | **Date**: 2026-10-10 | **Spec**: [spec.md](spec.md)

**Input**: `specs/001-dws-client-auto/spec.md`；用户补充确认：保留单表多 writer，允许扩展公共分区层处理主键变更。

## Summary

保留 Pipeline SinkV2，直接依赖 `com.huaweicloud.dws:dws-client:2.1.0.6`，每个 writer 一个官方客户端；默认 AUTO，移除自研 staging/committer 提交协议及未使用的 SinkFunction 路线。语义为至少一次；正确性依赖同键有序路由、flush 后确认 checkpoint 和源端可重放，不宣称 exactly-once 或 checkpoint 事务可见性。

DWS 显式启用公共 pre-partition 主键更新拆分：变键 UPDATE 在分区前同步拆为旧键 UPDATE_BEFORE 和新键 REPLACE，分别按旧/新主键分区，保留单表多 writer；其他 connector 默认不启用。每个 client 内固定单表一个 native partition，避免第二次按 distribution key 分区引入重排。

本轮仅交付设计，不修改 Java/POM、不运行数据库任务。运行验收见 [quickstart.md](quickstart.md)。

## Technical Context

**Language/Version**: Java 11 基线、Flink 1.20.3；Flink 2.2.0 采用既有 flink2/compat 路线和 JDK 17。DWS 当前缺少 flink2 版本覆盖，实施必须补齐并检查有效 POM，不能只加 -Pflink2 就宣称已测 2.x。

**Primary Dependencies**: dws-client 2.1.0.6、其 JDBC 8.6.1-200、Druid 1.2.23、metrics-core 4.2.25；移除 dws-connector-flink 和 DwsConnectionOptions 类型依赖，检查 shade/NOTICE、排除不必要 Lombok 和旧 Flink API。

**Storage**: DWS 业务表及官方客户端管理的临时表；无自研提交表。Flink writer state 仅保存新协议版本标记，schema 恢复复用已有 SchemaRegistry。

**Testing**: JUnit 5/AssertJ、operator harness、MiniCluster、Maven Surefire。现有 openGauss 3.0.3 容器仅兼容快测；真实 DWS 是 AUTO/COPY、大小写、故障和性能发布门槛。

**Target Platform**: 现有部署平台及两代 Flink 的最终 shaded JAR 类加载；默认 CN，不启用 DirectDN，不因制品包含 .so 就要求 DN 网络配置。

**Project Type**: Connector 升级及 opt-in 公共事件/分区契约扩展。

**Performance Goals**: 保留单表多 writer；稀疏/批量新旧各三次同条件比较吞吐、p95 延迟、峰值内存和 checkpoint 耗时。无未经实测的提升百分比承诺。

**Constraints**: 不静默丢旧状态；异步失败传播；参数生效或明确拒绝；每 writer 显式缓冲预算，不能逐 client 采用 JVM 堆 40% 默认值。

**Scale/Scope**: DWS、common 事件能力、runtime 三类 pre-partition、composer 能力透传、Flink2 构建及测试、中英文文档。保留现有七类 schema 事件，不新增表重命名能力。

## Constitution Check

目标没有 `.specify/memory/constitution.md`，不继承 helper 的宪章。以下是已确认需求的设计检查，不是新设宪章。

| Gate | 研究前 | 设计后 |
| --- | --- | --- |
| 用户范围/并行度 | 主键变更顺序需要决策 | 通过：用户授权公共分区扩展，保留单表多 writer |
| 正确性 | 旧键删除可能被其他 writer 延迟写覆盖 | 设计通过：分区前拆分、单 native partition；真实故障验证待实施 |
| 生命周期/大小写 | 版本升级不等于所有缓存路径正确 | 设计通过：flush barrier、remove + reload，避开丢失 TableConfig 的刷新路径 |
| 公共兼容 | 不得全局改变其他 sinks | 设计通过：default false、append-only 操作、旧事件黄金序列化与默认路径回归 |
| 状态/回退 | 旧 committer 状态不兼容 | 设计通过：拒绝旧 state，首版空目标重同步，不允许忽略状态绕过 |
| 最小结构 | 复用 conversion/DDL/SchemaRegistry | 通过：无双引擎、第二提交池、schema 持久副本或通用插件框架 |
| 验证/隐私 | 尚无真实 DWS 证据 | 设计通过：明确发布门槛，禁止输出凭据和业务行 |

无待用户澄清的架构选择；真实运行门槛尚未通过。实施出现门槛失败时必须修复或阻断发布，不能把设计审查结果当作运行认证。

## Project Structure

### Documentation (this feature)

```text
specs/001-dws-client-auto/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── tasks.md
├── contracts/
│   ├── configuration.md
│   └── events-and-lifecycle.md
└── checklists/requirements.md
```

[tasks.md](tasks.md) 已由 /speckit-tasks 生成为 58 项依赖有序的待办，尚未实施。根 AGENTS.md 仅提供有范围限制的计划入口。

### Source Code (repository root)

以下 DWS 指 `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dws/`，Java 路径位于各模块既有 package 中。

| 边界 | 拟变更 |
| --- | --- |
| DWS pom.xml | 直接 client、依赖收敛/shade、flink2 profile |
| DWS factory/DwsDataSinkFactory、sink/DwsDataSinkOptions | 白名单、单位/冲突校验及实际配置传递 |
| DWS sink/DwsDataSink | 开启变键拆分能力，传可序列化配置，保留 MetadataApplier |
| DWS sink/v2/DwsSink、DwsWriter | StatefulSink/SinkWriter + client，不再 TwoPhaseCommittingSink；刷新、首错、cache、close |
| DWS sink/v2/DwsWriterState、DwsWriterStateSerializer | protocol marker v2，显式拒绝旧 v1 |
| DWS sink/DwsSinkFunction、utils/DwsUtils | 复用类型转换，必要时小型 event converter；删除 legacy SinkFunction |
| DWS sink/v2/DwsCommitter、DwsCommittable、DwsCommittableSerializer、DwsSqlUtils | 移除旧提交协议；仍供 DDL 使用的工具先归位，不误删 |
| flink-cdc-common 的 DataSink、OperationType、DataChangeEvent、默认 hash | default-false 能力、尾部 UPDATE_BEFORE、factory/hash/opTypeString |
| flink-cdc-runtime 的三类 PrePartitionOperator、DataChangeEventSerializer | schema/PK getter 同步更新、拆分再 hash、新事件序列化 |
| flink-cdc-composer 的 FlinkPipelineComposer、PartitioningTranslator | 能力透传，旧调用重载默认 false，不影响其他 sinks |
| 各模块 tests、docs/content/docs/connectors/pipeline-connectors/dws.md 与 docs/content.zh/docs/connectors/pipeline-connectors/dws.md | 公共兼容、参数、类型、恢复、真实 DWS、迁移文档 |

**Structure Decision**: 一个 opt-in 能力和 typed 撤回事件，不占用用户 metadata，不引入任意事件插件。客户端只在 writer 初始化，日志不打印完整配置。

## Phase 0 — Research

[research.md](research.md) 给出官方依据和源码决策：直接依赖、AUTO/大小写、错误/缓存、公共变键拆分、native 顺序、配置、迁移和测试。2.1.0.6 的发布依赖以精确制品为证，不编造官方版本记录尚未披露的特有收益。

## Phase 1 — Design

1. factory 解析白名单与兼容键，输出确定生效值，见 [配置契约](contracts/configuration.md)。
2. pre-partition 将变键 UPDATE 拆为 UPDATE_BEFORE(old) + REPLACE(new)，同 callback 发出，再分别 hash；正常 UPDATE 不拆。
3. writer 将撤回无条件 native delete，独立 DELETE 遵循开关；delete 只设置主键，write 设置完整 after，不再次跨键拆分。
4. checkpoint/schema/end 同步 flush；onError 记录并重抛，周期 mailbox 检查空闲失败；不把 task timeout 当整个 flush 截止时间。用仅数字的保守字节计数补 native 对 binary 的低估，预算将超限时先 flush，不新增行队列。
5. 复用 FlushEvent→coordinator DDL→schema 通知；removeTableSchema 后按正确 TableConfig reload，再更新 getter。恢复复用 SchemaRegistry 补 Create。
6. marker v2 拒绝旧状态；首版独立空目标全量+增量重同步，不自动丢状态、切换或清理资源。
7. 两代 Flink 的 reactor/序列化/链接及真实 DWS 各自留证。

实体见 [data-model.md](data-model.md)，详细顺序见 [事件/生命周期](contracts/events-and-lifecycle.md)。

## Phase 2 — Implementation Handoff

| 阶段 | 交付与退出条件 | 需求 |
| --- | --- | --- |
| 1 | 配置与依赖；有效 POM、shade、单位/冲突测试通过 | FR-001、012～014、018 |
| 2 | typed 撤回、三拓扑拆分；并行 2/4 旧键乱序反例消失；其他 sink/旧事件兼容 | FR-002、003、005、008、018 |
| 3 | native SinkV2、主键-only delete、缓冲/首错、marker/cache barrier；删除旧提交路径 | FR-004、006、007、009～012、017 |
| 4 | 七类 schema success/failure、两代恢复、真实大小写/AUTO/MERGE | FR-006、009～011、017、018 |
| 5 | 网络黑洞/取消、在途迁移回退、同条件性能/内存报告与文档 | FR-004、007、012～016、018 |

先为旧键复活、大小写 DDL、空闲异步失败写可复现失败用例，再实现。不用忽略状态或 mock 全绿替代后续发布门槛。

## Complexity Tracking

无宪章豁免。公共扩展是用户要求单表多 writer 的直接结果，按表单 writer 已被否决；最小 marker 用于识别危险旧状态，不保留自研事务协议。

## Workflow Notes

已运行目标仓库 setup-plan.sh --json；脚本按 feature.json 定位，返回 BRANCH 为空，实际分支经 git 核实为 FLINK-39327。脚本/模板来自安装的 Spec Kit 默认文件。目标没有 extensions.yml，before/after hooks 不适用，Ponytail hook 按规则跳过。没有 agent 更新脚本，按技能约定直接更新 AGENTS 的 SPECKIT 标记区。

两项只读研究和独立交叉审查已完成，采纳了 rescale 空状态、大小写元数据刷新、关闭自动刷新后的 force、UPSERT 构造器覆盖、NUL 默认删除、binary 预算低估等修正。文件结构、链接、占位符、脚本语法和 setup 重入检查通过；数据库与业务代码测试未运行。
