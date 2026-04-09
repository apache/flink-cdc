# Decimal Runtime-Null / Cached-Schema 问题设计文档

## 一、摘要

本文完整总结本次 `Zero length BigInteger` 生产故障的历史背景、产生根源、触发路径、修复方案、验证结果以及后续增强建议。

本次线上可见故障的典型异常如下：

```text
Caused by: java.lang.NumberFormatException: Zero length BigInteger
    at java.base/java.math.BigInteger.<init>(BigInteger.java:310)
    at org.apache.flink.cdc.common.data.DecimalData.fromUnscaledBytes(DecimalData.java:207)
    at org.apache.flink.cdc.runtime.operators.transform.PreTransformProcessor.processFillDataField(...)
```

业务层面的直接结果：

- Flink 作业在 `PreTransformOperator` 失败
- 因为使用了 `NoRestartBackoffTimeStrategy`，任务没有自动恢复
- 已观测到的出问题表为 `trade_18.ma_order_search_0092`

最终结论：

- 让这个问题在 3.6 中变成线上可见故障的直接触发提交，是 `da07654f6cd869cd57f8dcc1c4181026b1a3b86c`
- 但这次崩溃之所以会发生，不是单点问题，而是两个更早就存在的潜伏缺陷与这个新触发器叠加造成
- 因此，修复不能只是“拦住异常”，而必须分层修复整条故障链

## 二、问题定义

表面上看，这次故障是 decimal 解码失败；但从第一性原理看，真实问题更大，涉及三类错误假设：

- cached schema 被当成了绝对真相，即使它已经与当前 Debezium record schema 发生漂移
- schema 上的 `NOT NULL` 被错误地认为一定强于 runtime row 中的 null-bit
- 无效 decimal 字节载荷最终以 JVM 级别的 `BigInteger` 异常形式暴露，而不是以有语义的诊断暴露

从系统约束看，这次故障本质上违反了三条基础不变量：

1. 运行时 row 内容才是 nullability 的最终真相。
2. cached schema 只能是 hint，不能是绝对 authority。
3. 即使 schema change 不向下游输出，也必须保持 deserializer 内部 cache 同步。

## 三、历史背景

### 3.1 更早的潜伏缺陷一：`RecordData` 对 `NOT NULL` 的错误捷径

相关历史提交：

- `af5a0cef3f95f19ba91b31a0102d5ece6afd2e71`
- 提交主题：`[3.0][cdc-common] Add TypeSerializer implementations (#2683)`

这次事故中真正相关的点：

- `RecordData.createFieldGetter(...)` 在生成字段访问器时，默认认为只要 schema 声明了 `NOT NULL`，就可以跳过 runtime null 检查
- 这个假设在理想数据下成立，但在以下场景中不成立：
  - 历史脏数据
  - schema 漂移
  - 错误的 cached-schema 解码
  - 跨层语义不一致导致的 runtime null

它不一定立刻导致崩溃，但它把危险读路径提前埋进了公共层。

### 3.2 更早的潜伏缺陷二：`DecimalData` 对空字节数组无防御

相关历史提交：

- `4930698a9b429f3b0e3a0ddb0fe47debce50df9b`
- 提交主题：`[3.0][cdc-common] Introduce internal data structures and generic implementations (#2688)`

这次事故中真正相关的点：

- `DecimalData.fromUnscaledBytes(byte[], precision, scale)` 直接把原始字节传给 `new BigInteger(unscaledBytes)`
- 当传入 `byte[0]` 时，会触发 `NumberFormatException: Zero length BigInteger`

问题在于：

- `byte[0]` 在这里并不代表合法的 decimal zero
- 它更接近“null / binary row 已损坏 / schema 失配”这类语义

因此，这个实现让真正的语义错误以非常底层、非常难定位的方式暴露。

### 3.3 3.6 基线 `be5a529` 为什么重要

参考基线提交：

- `be5a529e185caed393b2ecdfed1a2cbd18c95908`
- 提交主题：`[3.6-port] Harden TableIdRouter replacement and regex routing (FLINK-38779)`

为什么要以它作为判断基线：

- 在这个时点之前，shared Debezium 数据路径对 data record 的解码，仍然更多依赖 value inference
- 上述两个 common 层潜伏缺陷虽然已经存在，但 stale cached-schema 的数据解码路径还没有被全面激活

也就是说：

- 问题机制已经潜伏
- 但触发条件还没有被系统性打开

### 3.4 3.6 中的直接触发提交

直接触发提交：

- `da07654f6cd869cd57f8dcc1c4181026b1a3b86c`
- 所在分支：`release-3.6`
- 提交主题：`Fix Oracle variable-scale NUMBER schema mismatch for Doris sink`

关于归因的说明：

- 从本地 `release-3.6` 分支历史看，这是一个直接提交，不是一个可追溯到 merge commit 的独立 PR merge
- 因此，本地代码层面最准确的说法是：本次故障触发器来自提交 `da07654f...` 本身

该提交做了什么：

- data record 解码开始优先使用 cached `CreateTableEvent` schema
- deserializer 内部 schema cache 成为更强的主导解码依据
- Oracle pipeline 路径围绕这套机制增加了 cache 同步逻辑

这个改动本身解决了一个真实问题：Oracle variable-scale NUMBER 到 Doris 的 schema/value mismatch。

但它同时引入了新的系统行为：

- stale cached schema 不再只是旁路信息，而是进入 data record 的主解码链路

于是，旧的潜伏缺陷被正式激活。

## 四、为什么 3.5 没有暴露这个问题

3.5 没暴露这个问题，并不代表底层实现是安全的。

真正原因是：

- 在 `da07654f` 之前，共享 Debezium 反序列化路径没有以同样强度优先使用 cached `CreateTableEvent` schema 来解 data record
- 因此，即使存在 runtime null 假设和 empty decimal bytes 问题，也较少进入这条 stale-cache 触发路径

所以，正确结论不是：

- 3.5 没有 bug

而是：

- 3.5 没有触发这一类 bug 的关键执行路径
- 3.6 打开了触发路径
- deeper crash mechanics 是更早就存在的

## 五、完整故障链路

结合栈和代码路径，本次故障链如下：

1. 某条 data record 到达，对应表存在 cached schema。
2. `da07654f` 之后，shared Debezium deserializer 优先使用 cached `CreateTableEvent` schema 解码该 record。
3. 当前 live record schema 与 cached schema 并不完全兼容。
4. 在这种失配下，某个 decimal 字段在内部 `BinaryRecordData` 中表现为 runtime null 或无效 empty bytes。
5. transform/runtime 层通过 schema 生成字段 getter，并去读取该 decimal 字段。
6. 旧实现由于字段声明为 `NOT NULL`，跳过了 `row.isNullAt(...)`。
7. `BinaryRecordData.getDecimal(...)` 继续读取 decimal。
8. `DecimalData.fromUnscaledBytes(byte[0], ...)` 调用 `new BigInteger(byte[0])`。
9. JVM 抛出 `NumberFormatException: Zero length BigInteger`。
10. `PreTransformOperator` 失败，Flink job 进入终态 `FAILED`。

这说明：

- 这不是单一的 decimal bug
- 而是 cached schema、runtime null、binary decimal 三层契约同时失效后的链式故障

## 六、根因拆解

### 6.1 根因 A：runtime nullability 被 schema nullability 压制

错误假设：

- schema 是 `NOT NULL`
- 因此 runtime 一定不可能是 null

为什么错误：

- schema 是声明
- row null-bit 才是运行时事实
- stale schema decode、脏数据、跨层不一致都可能让“声明不为空”的字段在运行时表现为 null

正确规则：

- 任何 typed getter 之前，都必须先检查 runtime null-bit

### 6.2 根因 B：cached schema 被当成 authority，而不是经过兼容性校验的 hint

错误假设：

- 只要 cached `CreateTableEvent` 存在，就应该直接按它解码

为什么错误：

- cached schema 可能已经过期
- schema change 可能没有下发到下游
- 即使字段名还在，字段类型也可能已经变了

正确规则：

- cached schema 只有在与当前 record schema 兼容时才能使用
- 一旦检测到 incompatibility，必须回退到 value inference

### 6.3 根因 C：`includeSchemaChanges=false` 时 deserializer cache 可能失同步

错误假设：

- 如果用户不想让 schema event 下发，那 schema record 也可以直接丢弃

为什么错误：

- 下游不需要看到 schema event，不代表 deserializer 自己不需要 schema 变化知识
- 如果 schema event 被静默丢弃，内部 cache 会逐步和真实 record schema 漂移

正确规则：

- 即使不向下游 emit schema event，也必须同步更新 deserializer cache

### 6.4 根因 D：empty decimal bytes 没有语义化处理

错误假设：

- 任意 byte[] 都可以用于 decimal 重建

为什么错误：

- `byte[0]` 在这个 binary-row 路径里不是合法 decimal zero 表示
- 它通常意味着 null、损坏、失配

正确规则：

- empty decimal bytes 应该 fail fast，并给出有语义的诊断

## 七、影响范围评估

### 7.1 直接受影响路径

- 所有在 shared Debezium cached-schema 解码之后，再从 `BinaryRecordData` 中读取 decimal 字段的链路
- 特别是 transform/runtime operator，例如：
  - `PreTransformOperator`
  - `PostTransformOperator`

### 7.2 同类风险涉及的 connector 范围

- Oracle pipeline connector
- MySQL CDC source connector
- 使用 shared Debezium deserialization 的 base incremental source connector
- Postgres pipeline connector 的 schema 推断/缓存同步路径

### 7.3 为什么不是单表问题

本次观测表包含：

- `id DECIMAL(20, 0) NOT NULL`

但这个问题族并不依赖具体表名，凡是满足以下条件的场景都可能受影响：

- 使用了 cached schema 解码
- 当前 record 与 cached schema 存在漂移
- 下游通过公共 getter/accessor 读取字段

因此，这不是单表问题，而是一类 shared abstraction 问题。

## 八、修复设计原则

最终修复遵循以下原则：

1. 不能把非法 decimal bytes 静默转换成 zero。
2. 不能用“catch 所有异常然后 fallback”的方式掩盖真实 bug。
3. 必须把 cache drift 变成显式、可判断的 compatibility 问题。
4. 必须保证 schema event 即使不下发，也会更新内部 cache。
5. 在 schema 兼容时，保留 cached-schema 路径的价值和性能收益。

## 九、最终修复方案

### 9.1 第一层：common/runtime null-safety 修复

修复内容：

- `RecordData.createFieldGetter(...)` 无论 schema 是否声明 `NOT NULL`，都会先执行 `row.isNullAt(fieldPos)`
- `BinaryRecordData.getDecimal(...)` 在 null-bit 为 true 时直接返回 `null`
- `BinaryArrayData.getDecimal(...)` 在 null-bit 为 true 时直接返回 `null`

修复效果：

- transform/runtime 层不再因为 schema 声明 `NOT NULL` 而错误跳过 runtime null

### 9.2 第二层：decimal fail-fast 语义修复

修复内容：

- `DecimalData.fromUnscaledBytes(...)` 对 `byte[0]` 显式抛出带语义的 `IllegalArgumentException`

修复效果：

- 不再出现难定位的 `Zero length BigInteger`
- 异常语义更接近真实问题：null / row 损坏 / schema mismatch

### 9.3 第三层：shared Debezium schema cache 硬化

修复内容：

- `DebeziumEventDeserializationSchema.deserialize(SourceRecord)` 会自动把解析出的 `SchemaChangeEvent` 回写到内部 cache
- data record 在使用 cached schema 解码时，只有遇到显式 `cached-schema incompatibility` 才会 fallback 到 value inference
- 当前已检测的 incompatibility 包括：
  - payload 不是 `STRUCT`
  - schema 不是 `STRUCT`
  - cached 字段在当前 record schema 中缺失
  - cached 字段还在，但当前字段类型已发生漂移
  - current record schema 比 cached schema 多出额外字段
  - nested `ROW` 的 current schema 比 cached schema 多出额外子字段

修复效果：

- cached schema 继续可用，但不再盲目信任
- stale cached schema 不会再因为“只校验单向兼容”而静默丢列

### 9.4 第四层：`includeSchemaChanges=false` 场景下的 cache 同步

修复内容：

- `IncrementalSourceRecordEmitter` 在 `includeSchemaChanges=false` 时，仍通过 no-op collector 把 schema record 送入 deserializer 做 cache 同步
- `MySqlRecordEmitter` 同样增加了这条路径
- MySQL / Oracle 的 schema-change deserializer 不再因为 `includeSchemaChanges=false` 而跳过解析
- Postgres pipeline emitter 在 hidden schema-change 模式下，不再在 schema event 路径做 JDBC 打开和重型 diff 推断
- Postgres pipeline emitter 在收到 hidden schema-change record 时，会先失效对应表的 cached `CreateTableEvent`
- Postgres pipeline emitter 在下一条 data record 到来时，再懒加载最新 `CreateTableEvent` 回填 cache
- 对已经发送过 `CreateTableEvent` 的表，Postgres 懒刷新只更新内部 cache，不会重复向下游补发 `CreateTableEvent`
- Postgres emitter 构造阶段原本存在一次无效的全表 schema 预取调用，现已移除，避免新增无意义 JDBC 失败面

修复效果：

- 即使 schema event 不向下游输出，deserializer 仍然知道 schema 已变化
- Postgres `includeSchemaChanges=false` 不再引入新的 hidden JDBC 推断失败路径
- 对 hidden schema-change 模式，行为更接近用户直觉：不输出 schema 事件，也不额外做高成本推断

## 十、关键设计决策

### 10.4 Postgres hidden schema-change 不做“隐式重推断”

被否决的方案：

- `includeSchemaChanges=false` 时，仍然在 Postgres schema-change 路径执行 `inferSchemaChangeEvent(...)`，只是最终不向下游输出

否决原因：

- 这会在用户明确关闭 schema event 输出的配置下，新增 JDBC 依赖和运行时失败面
- 对 Postgres 而言，schema diff 推断并不是纯内存操作，而是会打开 JDBC 连接并访问 type registry
- hidden 模式下真正需要的是“内部 cache 最终一致”，而不是“当场推断并丢弃结果”

采用方案：

- hidden schema-change 到达时，直接失效缓存
- 等下一条 data record 到来时再懒加载最新 schema

收益：

- 避免 schema-change 时刻的额外脆弱性
- 保证后续 data decode 仍能拿到最新 schema
- 不会重复下发 `CreateTableEvent`

### 10.1 不把 empty decimal bytes 当作 zero

被否决的方案：

- `byte[0]` 时直接返回 decimal zero

否决原因：

- zero 是合法业务值
- empty bytes 表示的是无效状态，不是 zero
- 静默转 zero 会造成 silent data corruption

### 10.2 不对所有 converter 异常统一 fallback

被否决的方案：

- cached-schema decode 过程中一旦出错就全部 fallback

否决原因：

- 会把真正的程序错误和 schema drift 混在一起
- 只应该对“明确的 cached-schema incompatibility”进行 fallback

### 10.3 cached/actual 类型比较不要求 nullability 完全一致

采用方案：

- 兼容性比较聚焦于危险的类型/结构漂移
- nullability 差异由 runtime null-bit 机制单独兜底

原因：

- 如果要求 nullability 完全一致，会过度触发 fallback
- 真正危险的是 stale type / stale shape，而不是 nullable 声明本身

## 十一、涉及文件

### 11.1 common/runtime 相关

- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/data/RecordData.java`
- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/data/DecimalData.java`
- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/data/binary/BinaryRecordData.java`
- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/data/binary/BinaryArrayData.java`

### 11.2 shared Debezium/schema cache 相关

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-debezium/src/main/java/org/apache/flink/cdc/debezium/event/DebeziumEventDeserializationSchema.java`

### 11.3 schema 同步相关

- `flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base/src/main/java/org/apache/flink/cdc/connectors/base/source/reader/IncrementalSourceRecordEmitter.java`
- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlRecordEmitter.java`
- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/source/MySqlEventDeserializer.java`
- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-oracle/src/main/java/org/apache/flink/cdc/connectors/oracle/source/OracleEventDeserializer.java`
- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres/src/main/java/org/apache/flink/cdc/connectors/postgres/source/reader/PostgresPipelineRecordEmitter.java`

## 十二、验证结果

### 12.1 新增或更新的回归测试

common 层：

- `flink-cdc-common/src/test/java/org/apache/flink/cdc/common/data/RecordDataTest.java`
- `flink-cdc-common/src/test/java/org/apache/flink/cdc/common/data/DecimalDataTest.java`

runtime 层：

- `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/typeutils/BinaryRecordDataExtractorTest.java`
- `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/operators/transform/PreTransformOperatorTest.java`
- `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/operators/transform/PostTransformOperatorRuntimeNullRegressionTest.java`

shared Debezium cache 层：

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-debezium/src/test/java/org/apache/flink/cdc/debezium/event/DebeziumEventDeserializationSchemaTest.java`

MySQL emitter/schema-sync 层：

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/test/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlRecordEmitterTest.java`

Postgres hidden schema-change 层：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres/src/test/java/org/apache/flink/cdc/connectors/postgres/source/reader/PostgresPipelineRecordEmitterTest.java`

### 12.2 已验证通过的测试

本轮已验证通过的关键测试：

- `RecordDataTest`
- `DecimalDataTest`
- `BinaryRecordDataExtractorTest`
- `PreTransformOperatorTest`
- `PostTransformOperatorRuntimeNullRegressionTest`
- `DebeziumEventDeserializationSchemaTest`
- `MySqlRecordEmitterTest`
- `PostgresPipelineRecordEmitterTest`

其中新增覆盖的关键回归点包括：

- cached schema 落后于实际 schema 且实际 schema 新增顶层字段时，能够 fallback 到 value inference
- cached schema 落后于实际 schema 且 nested `ROW` 新增子字段时，能够 fallback 到 value inference
- Postgres 在 `includeSchemaChanges=false` 时，不再执行 schema-change 路径的重型推断
- Postgres hidden schema-change 之后，会在下一条 data record 上懒刷新 cache，且不会重复补发 `CreateTableEvent`

### 12.3 代码风格验证

已验证：

- 本次修复涉及模块的 `spotless:check` 通过

## 十三、最终归因

### 13.1 `be5a529` 之后的直接触发改动是谁

最准确的结论：

- 直接触发提交是 `da07654f6cd869cd57f8dcc1c4181026b1a3b86c`

原因：

- 该提交让 shared Debezium data-record 解码优先走 cached `CreateTableEvent` schema
- stale cache 不再是次要信息，而变成主解码路径的一部分
- 这直接把旧的 latent defects 变成了线上可见故障

### 13.2 什么不是新引入的

以下问题并不是 `da07654f` 新引入的：

- `RecordData` 中基于 `NOT NULL` 跳过 runtime null 检查的危险假设，来自 `af5a0cef...` / PR `#2683`
- `DecimalData` 对 empty bytes 直接走 `BigInteger` 的危险实现，来自 `4930698a...` / PR `#2688`

所以正确归因应该分层表达：

- 触发器：`da07654f...`
- 崩溃机制的潜伏根因：`#2683` 与 `#2688`

## 十四、当前状态评估

### 14.1 改动范围

从最终落地结果看，本次与该 BUG 直接相关的改动属于“中等偏大，但结构化收敛”的修复，而不是无边界扩散修改。

改动大致覆盖以下层次：

- common data 层：
  - runtime null-bit 读取语义
  - decimal 空字节 fail-fast 语义
- shared Debezium deserializer 层：
  - cached schema 自动同步
  - cached/actual schema 双向兼容性校验
- source/emitter 层：
  - hidden schema-change 模式下的 cache 同步
  - Postgres hidden schema-change 的懒刷新策略
- 回归测试层：
  - common
  - runtime
  - Debezium
  - MySQL
  - Postgres
- 文档层：
  - 设计文档、归因、影响范围、上线建议

这意味着：

- 改动横跨多个模块，但每一层都围绕同一条故障链闭环
- 没有引入与该问题无关的大规模重构
- 风险集中在少数几个共享抽象，而不是分散到大量业务逻辑

### 14.2 影响评估

从影响性质看，本次修复同时覆盖了三类风险：

1. 线上直接崩溃风险

- `Zero length BigInteger`
- `NOT NULL` 跳过 runtime null 检查导致的错误访问

2. 数据正确性风险

- stale cached schema 仅做单向兼容检查时的静默丢列
- nested `ROW` 场景下的静默字段丢失

3. 运行时可用性风险

- Postgres 在 `includeSchemaChanges=false` 下新增的 hidden JDBC 推断失败面

因此，修复后的主要收益不是单点“防异常”，而是：

- 消除了主故障链
- 消除了 silent data loss 风险
- 消除了 hidden schema 模式下新的额外失败路径

### 14.3 当前剩余风险

当前剩余风险已经从“主故障未闭环”下降为“增强项未完全覆盖”：

- Oracle hidden schema-change 模式还缺同等级回归测试
- 线上观测指标仍然偏少，cache fallback 与 cache lazy refresh 的行为还不够可见

这两项会影响后续维护效率和可观测性，但不构成当前修复方案的 blocker。

### 14.4 最终验证补充说明

在最终收口阶段，新增的关键定向验证已再次执行通过：

- `DebeziumEventDeserializationSchemaTest`
- `PostgresPipelineRecordEmitterTest`

说明：

- 本地执行这两组验证时使用 JDK 17 环境
- 如果使用本机默认 JDK 11，部分模块会因为已有 Java 17 字节码而在 Maven 前置阶段失败
- 这属于本地验证环境差异，不属于本次修复的代码缺陷

当前工程结论：

- 本次问题的主故障机制已经闭环修复
- 二次 review 暴露出的两个剩余 P1 风险也已经收敛
- 修复不是局部 suppress，而是对 shared abstraction 进行了系统修正
- 按当前验证结果，可以进入正常发布/上线流程
