# MySQL CDC -> Doris JSON Column-Case Sync 设计与验证文档

## 一、文档目的

本文用于沉淀本次 `MySQL CDC -> Doris` 列名大小写同步问题的完整背景、异常现象、根因分析、代码改动和验证结果。

本文只覆盖本次实际修复的问题线：

- Doris sink 在 `format=json` 下，JSON key 与 Doris 物理列名大小写不一致
- `column-name-case=UPPER` 把 DDL 字段名改成大写后，数据事件没有同步成功
- `SchemaOperator regular -> Doris streaming sink` 在 `schema evolution + json + 晚到事件` 下，schema barrier 没有真正切换 stream load 批次，导致新增列值丢失
- transform `projection` 与 `pipeline.column-name-case` 叠加时，最终列名规则需要明确
- 典型现场表现为：`deploy_mode` 可以写入，但 `ID` 没有写入

此前的 `DECIMAL cached schema` 问题已经有单独文档，不混入本文。

## 二、问题背景

业务现场有两类直接反馈：

1. `format=json` 场景下，部分字段成功写入 Doris，部分字段丢失
2. 当 `pipeline.column-name-case=UPPER` 后，自动建表 DDL 的字段名已经变成大写，但数据仍然未同步

最典型的现场现象是：

- `deploy_mode` 已确认成功插入
- `ID` 字段没有插入值

这说明问题不是整行都没有落库，而是列级别的字段匹配出现了偏差。

## 三、最终需求语义确认

经过多轮确认，本次需求的最终语义不是“所有字段最终都再经过一次 `column-name-case` 统一改写”，而是：

1. `projection` 中显式写出的别名大小写优先，必须原样保留
2. `pipeline.column-name-case` 只作为兜底规则，处理 projection 没有显式改名的字段
3. 对于简单透传列，如果没有显式 alias，才应用 `UPPER / LOWER / AS_IS`

最关键的权威例子是：

```yaml
transform:
  - source-table: testdb.customer
    projection: id as my_id, NAME as name, age as AGE

pipeline:
  column-name-case: UPPER
```

最终下游列名必须是：

```text
my_id,name,AGE
```

解释如下：

- `id as my_id` 是显式 alias，保留 `my_id`
- `NAME as name` 是显式 alias，保留 `name`
- `age as AGE` 是显式 alias，保留 `AGE`
- `UPPER` 不会再把这三个显式 alias 二次改写成 `MY_ID, NAME, AGE`

这条语义直接决定了：

- runtime 层不要再对 projection alias 做二次大小写改写
- E2E 场景里 `projection + column-name-case: UPPER` 的期望应该是 `my_id,name,AGE`
- 如果出现 `MY_ID,NAME,AGE` 的断言，那是测试预期错误，不是当前实现错误

## 四、现场日志与现象解读

用户提供的 Flink 日志显示：

- MySQL snapshot 和 binlog reader 正常启动
- `streampark.t_flink_app` 快照数据成功导出 2 条
- Doris stream load 返回 `Status: "Success"`，`NumberLoadedRows: 2`

同时还出现了两类 warning：

- Flink: `web.port` 已废弃，应使用 `rest.port`
- Debezium/MySQL: `serverTimezone` 已废弃，应使用 `connectionTimeZone`

这两类 warning 与本次字段丢失没有直接关系，不是根因。

从症状反推，真正的问题是：

- Doris 整体 stream load 没有失败
- 但 JSON payload 中的部分 key 没能和 Doris 物理列正确对上

这与业务反馈 “`deploy_mode` 有值、`ID` 没值” 是一致的：

- `deploy_mode` 本身就是小写列名，误打误撞仍能匹配
- `ID` 是大写物理列名，如果 JSON key 被错误写成 `id`，就会丢失

## 五、第一性原理分析

### 5.1 问题不在 MySQL 读链路

从用户日志可以确认：

- Source split 正常分配
- snapshot 导出正常
- schema event 正常生成
- DataChangeEvent 正常流入下游

因此，这次线上反馈的主问题不是 “源端没采到数据”，而是 “下游 JSON 字段名和 Doris 物理列名没有对齐”。

### 5.2 问题也不在 `column-name-case=UPPER` 本身

`column-name-case=UPPER` 的职责是：

- 把 schema 中的列名格式化成大写
- 相应地让 DDL / schema event 也变成大写

它本身不应该导致数据事件丢失。

本次真正暴露出来的是另一个缺陷：

- Doris JSON serializer 并没有按照 Doris 物理 schema 来输出 JSON key
- 它沿用了“直接拿输入 schema 列名，或者直接统一 lower-case”的旧假设

所以：

- 当物理表列名是 `ID, deploy_mode` 时，如果 JSON 被写成 `id, deploy_mode`，只有后一列能匹配
- 当物理表列名被 `UPPER` 变换成 `ID, DEPLOY_MODE` 时，如果 JSON 仍被写成 `id, deploy_mode`，两列都无法稳定匹配

### 5.3 正确的不变量

对 Doris `format=json` 来说，正确约束不是：

- JSON key 等于 source schema 列名
- JSON key 一律转小写

而是：

- JSON key 必须与 Doris 当前物理列名一致

因此 serializer 必须以 Doris 物理 schema 为准，再把 Doris 物理列名映射回输入事件中的真实字段值。

## 六、根因

根因可以总结为一句话：

`DorisEventSerializer` 在 JSON 模式下没有基于 Doris 物理 schema 生成 payload key，而是错误依赖了输入 schema 名称 / lower-case 规则。`

这会带来两个直接错误场景：

### 6.1 mixed-case 物理列场景

例如 Doris 物理列为：

```text
ID
deploy_mode
```

如果 serializer 输出：

```json
{"id":1,"deploy_mode":2}
```

则表现为：

- `deploy_mode` 能写入
- `ID` 写不进去

### 6.2 `column-name-case=UPPER` 场景

如果上游变换后 Doris 物理列为：

```text
ID
DEPLOY_MODE
```

而 serializer 仍输出：

```json
{"id":1,"deploy_mode":2}
```

则表现为：

- DDL 看起来是对的
- 数据同步失败

这正是用户反馈的第二类问题。

除了上面的 Doris JSON key 根因，本次还需要同步纠正一个需求理解问题：

- 之前曾经错误尝试把 projection alias 继续交给 `column-name-case` 改写
- 在用户最终确认语义后，这个方向已经回退
- 当前 runtime 实现以“显式 alias 优先、pipeline case 兜底”为准

后续在补强 `schema evolution + json + 晚到事件` 覆盖时，又定位到了同一条链路上的第二个真实问题：

### 6.3 `SchemaOperator regular -> Doris streaming sink` 的 schema barrier 不完整

问题现象不是建表失败，也不是 serializer 不认识新增列，而是：

- `DorisEventSerializerTest#testJsonSerializationHandlesLateEventsAfterAddColumn` 能通过
- `DorisMetadataApplierITCase#testDorisJsonFormatHandlesLateEventsAfterAddColumn` 在 `batchMode=true` 能通过
- 同一条用例在 `batchMode=false` 的 streaming 路径里，`Bob.age` 会被写成 `null`

当时的真实断言现象是：

```text
Expected:
1 | Alice | null
2 | Bob   | 18
3 | Carol | null

Actual:
1 | Alice | null
2 | Bob   | null
3 | Carol | null
```

这说明：

- 晚到旧 schema 事件已经能正确补 `null`
- 但新增列已经存在的那条新 schema 事件，在 streaming sink 里仍然丢值

最终定位到的问题点是：

- `SchemaOperator regular` 在 schema change 前会先发 `FlushEvent`
- `DataSinkWriterOperator` 收到后会调用 `copySinkWriter.flush(false)`
- 但 Doris 原生 `DorisWriter.flush(false)` 在 JSON 模式下只 flush serializer，不会真正结束当前 active stream load

结果就是：

- schema change 前开启的那次 JSON stream load 会继续承接 schema change 之后的数据
- 下游 Doris 虽然已经完成 `ADD COLUMN`
- 但新 schema 的数据还在旧 stream load 会话里被提交，新增列值会被静默丢掉

这也是为什么：

- batch mode 正常，因为 `DorisBatchWriter.flush(...)` 会显式 `checkpointFlush()`
- streaming mode 异常，因为 JSON 路径缺少“在 schema barrier 上切批”这一步

## 七、代码改动

### 7.1 Doris JSON 字段映射修复

修改文件：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializer.java`

核心改动：

1. 新增 `JsonFieldMappingResolver`
2. 在 JSON 模式下通过 `RestService.getSchema(...)` 拉取 Doris 物理 schema
3. 以 Doris 物理字段名作为输出 JSON key
4. 通过 case-insensitive lookup，把 Doris 物理字段映射回输入 schema 中实际列名
5. 为每张表缓存 JSON 字段映射
6. 在 schema change / drop table 时清理缓存

修复后的行为变为：

- Doris 物理列名是 `ID`，JSON 就输出 `ID`
- Doris 物理列名是 `deploy_mode`，JSON 就输出 `deploy_mode`
- Doris 物理列名是 `DEPLOY_MODE`，JSON 就输出 `DEPLOY_MODE`

### 7.2 Doris streaming sink schema barrier 修复

最终修复位置：

- `/Users/benjobs/Github/doris-flink-connector/flink-doris-connector/src/main/java/org/apache/doris/flink/sink/writer/DorisWriter.java`
- `/Users/benjobs/Github/doris-flink-connector/flink-doris-connector/src/main/java/org/apache/doris/flink/sink/writer/DorisStreamLoad.java`
- `/Users/benjobs/Github/doris-flink-connector/flink-doris-connector/src/test/java/org/apache/doris/flink/sink/writer/TestDorisWriter.java`
- `/Users/benjobs/Github/doris-flink-connector/flink-doris-connector/src/test/java/org/apache/doris/flink/sink/writer/TestDorisStreamLoad.java`

CDC 侧回退文件：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisDataSink.java`

核心改动：

1. `DorisWriter.flush(false)` 在 JSON streaming 路径下也会真正结束 active stream load，而不再只 flush serializer
2. `DorisWriter.nextStreamLoadLabel(...)` 在所有 `2PC` 路径下统一启用 sequence label，而不是只对 CSV 生效
3. `DorisStreamLoad.abortPreCommit(...)` 同步按 sequence label 扫描，保证恢复 / abort 路径与新的 label 规则一致
4. Doris connector 内补了 JSON rollover 与 JSON abortPreCommit 的定向单测
5. Flink CDC 侧删除临时反射包装实现，非 batch 路径重新直接使用原生 `DorisSink`
6. 这样 `FlushEvent` 才真正成为 schema barrier：
   - schema change 前的数据在旧批次完成提交
   - schema change 后的数据进入新的 stream load 会话

实现说明：

- 第一阶段曾在 CDC 侧用 wrapper 快速验证根因
- 但那个实现依赖反射访问 Doris writer 私有字段与私有方法，工程稳定性一般
- 最终版本已经把真实修复下沉到 Doris connector，自身保证 stream load 边界与 label 规则
- CDC 重新恢复到直接接入原生 `DorisSink`

修复后的关键效果：

- `FlushEvent` 在 JSON streaming 路径下终于成为真实的 schema barrier
- `AddColumnEvent` 之后的新 schema 数据不会再沿用 schema change 前已经打开的旧 stream load 会话
- 同一个 checkpoint 内因为 barrier 或 flush 产生的多次 stream load，可以通过 sequence label 正确区分并恢复
- `SchemaOperator regular -> Doris streaming sink` 这条链路下，新增列值可以稳定写入

### 7.3 transform / column-name-case 语义收敛

修改文件：

- `flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/transform/SchemaColumnCaseFormatter.java`

关键调整：

1. 只有简单透传列才应用 `SchemaColumnCaseFormat`
2. 如果 projection 明确写了 alias，例如 `id AS my_id`，则 `my_id` 原样保留
3. primary key / partition key 的列名解析同步基于 projection 后结果进行映射
4. 已回退“对 projection alias 做二次大写化”的错误改动

收敛后的行为：

- `projection: id as MY_ID, NAME as name, age as AGE`
  - 无 `column-name-case` 时结果为：`MY_ID,name,AGE`
- `projection: id as my_id, NAME as name, age as AGE`
  - 配置 `column-name-case: UPPER` 时结果为：`my_id,name,AGE`

### 7.4 fallback 行为收敛

当没有 Doris 连接信息，无法解析物理 schema 时：

- 仍保留原有 fallback
- 只在没有 resolver 的场景下才做 lower-case 兼容

也就是说：

- 真实 sink 路径优先按 Doris 物理 schema 输出
- 本地纯单元测试 / 无 Doris options 场景才会走 fallback

### 7.5 回归测试补充

修改文件：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializerTest.java`
- `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/operators/transform/PostTransformOperatorProjectionKeysRegressionTest.java`
- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplierITCase.java`
- `flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/src/test/java/org/apache/flink/cdc/pipeline/tests/MySqlToDorisE2eITCase.java`
- `flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/src/test/resources/ddl/mysql_case_inventory.sql`

新增覆盖点：

- JSON 模式下 mixed-case 物理列
- JSON 模式下大写主键 + 小写普通列
- JSON 模式下全大写列
- JSON 模式下 `AddColumnEvent` 之后旧 schema 晚到事件仍可落库，新列补 `null`
- MySQL -> Doris JSON 真实链路下 mixed / upper / lower 三类表的 update/delete 同步
- runtime 层 `projection alias 优先，column-name-case 兜底` 的回归语义
- MySQL -> Doris Docker E2E 覆盖 mixed / upper / lower / projection / projection + upper 五类验收目标

## 八、端到端 IT 设计

本次真实 Docker E2E 主体放在：

- `flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/src/test/java/org/apache/flink/cdc/pipeline/tests/MySqlToDorisE2eITCase.java`

具体测试方法如下：

1. `testSyncMysqlColumnCaseTablesWithJsonFormat`
2. `testSyncMysqlColumnCaseTablesWithJsonFormatAndUpperColumnNameCase`
3. `testSyncMysqlProjectionTransformToDorisWithJsonFormat`
4. `testSyncMysqlProjectionTransformToDorisWithJsonFormatAndUpperColumnNameCase`

其中前两个测试方法每个都在一条 pipeline 内同时覆盖了三张 MySQL 源表：

- `mixed_case_customer`
- `upper_case_customer`
- `lower_case_customer`

其中，已经跑通过的主覆盖矩阵对应用户需求如下：

| 需求 | E2E 覆盖方式 |
| --- | --- |
| mixed-case 上游列名，自动建表并同步数据 | `testSyncMysqlColumnCaseTablesWithJsonFormat` |
| 全大写上游列名，自动建表并同步数据 | `testSyncMysqlColumnCaseTablesWithJsonFormat` |
| 全小写上游列名，自动建表并同步数据 | `testSyncMysqlColumnCaseTablesWithJsonFormat` |
| 以上三种情况 + `column-name-case: UPPER` | `testSyncMysqlColumnCaseTablesWithJsonFormatAndUpperColumnNameCase` |
| projection 映射，检查自动建表与同步数据 | `testSyncMysqlProjectionTransformToDorisWithJsonFormat` |
| projection 映射 + `column-name-case: UPPER` | `testSyncMysqlProjectionTransformToDorisWithJsonFormatAndUpperColumnNameCase` |

### 8.1 mixed / upper / lower 三类源表用例

这两个用例通过三张 MySQL 表一次性覆盖三种上游字段大小写：

```text
mixed_case_customer:   ID, Name, phone_Number
upper_case_customer:   ID, NAME, PHONE_NUMBER
lower_case_customer:   id, name, phone_number
```

验证点：

- 不配置 `column-name-case` 时，下游 Doris 自动建表字段名与上游保持一致
- 配置 `column-name-case: UPPER` 时，下游 Doris 自动建表字段统一转成大写
- 三张表都验证 snapshot + incremental 两阶段数据同步成功

### 8.2 projection 用例

无 `column-name-case` 的 projection 用例：

- projection: `id as MY_ID, NAME as name, age as AGE`
- 预期 Doris schema: `MY_ID, name, AGE`
- 验证 snapshot + incremental 都能正确写入

带 `column-name-case: UPPER` 的 projection 用例：

- projection: `id as my_id, NAME as name, age as AGE`
- pipeline: `column-name-case: UPPER`
- 预期 Doris schema: `my_id, name, AGE`
- 验证 snapshot + incremental 都能正确写入

这里特别强调：

- 这个场景下 `my_id` 和 `name` 不会被再改写成 `MY_ID` / `NAME`
- 这是本次需求语义的最终判定
- 本次 E2E 已按这个最终语义更新断言并跑通

## 九、本地执行与结果

本次本地验证统一使用 JDK 11。

### 9.1 runtime 回归测试

执行命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:$PATH \
mvn -pl flink-cdc-runtime \
  -Dtest=PostTransformOperatorProjectionKeysRegressionTest \
  test -DfailIfNoTests=false
```

执行结果：

```text
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

关键回归点：

- `testProjectionAliasesKeepAuthoredCaseWhenPipelineUpperCaseIsEnabled`
- 断言 schema 与 data event 都是：`my_id`, `name`, `AGE`

### 9.2 Doris connector 定向单测与本地安装

执行命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:$PATH \
mvn -Dtest=TestDorisWriter,TestDorisStreamLoad test -DfailIfNoTests=false
```

执行结果：

```text
Tests run: 30, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

安装 `Flink 1.20` 产物的命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:$PATH \
mvn -Drevision=25.1.0 -Dflink.major.version=1.20 \
  -Dtest=TestDorisWriter,TestDorisStreamLoad \
  -DfailIfNoTests=false install
```

执行结果：

```text
Tests run: 30, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

说明：

- 这一步把修复后的 `org.apache.doris:flink-doris-connector-1.20:25.1.0` 安装到了本地 Maven
- 后续 CDC 模块和 E2E 复制出来的 `doris-cdc-pipeline-connector.jar` 都会消费这次修复后的 Doris connector

### 9.3 MySQL -> Doris Docker E2E

执行命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:$PATH \
mvn -f flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/pom.xml \
  -Dmaven.plugin.download.cache.path=/tmp/flink-download-cache-20260409 \
  -DspecifiedFlinkVersion=1.20.3 \
  -Dtest=MySqlToDorisE2eITCase#testSyncMysqlColumnCaseTablesWithJsonFormat+testSyncMysqlColumnCaseTablesWithJsonFormatAndUpperColumnNameCase+testSyncMysqlProjectionTransformToDorisWithJsonFormat+testSyncMysqlProjectionTransformToDorisWithJsonFormatAndUpperColumnNameCase \
  integration-test -DfailIfNoTests=false
```

执行结果：

```text
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

补充说明：

- 这个模块的 Maven 生命周期里有两个 surefire 执行，所以上述 4 个测试方法会被模块重复执行一次
- 两轮执行最终都通过，最终 Maven 结果为 `BUILD SUCCESS`
- 总耗时约 `03:45 min`

### 9.4 `schema evolution + json + 晚到事件` streaming IT

执行命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:$PATH \
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierITCase#testDorisJsonFormatHandlesLateEventsAfterAddColumn \
  test -DfailIfNoTests=false
```

执行结果：

```text
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

说明：

- 这里的 2 次执行对应参数化的 `batchMode=true/false`
- 这里是在 CDC 侧已经删除 wrapper、重新接回原生 `DorisSink` 之后重新执行
- 也就是说，之前失败的 `streaming` 分支在最终形态下已经转绿
- 这是本次“SchemaOperator regular -> Doris streaming sink” 修复闭环的直接证据

### 9.5 MySQL -> Doris JSON E2E 定向复跑

执行命令：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:$PATH \
mvn -f flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/pom.xml \
  -DspecifiedFlinkVersion=1.20.3 \
  -Dflink.release.download.skip=true \
  -Dtest=MySqlToDorisE2eITCase#testSyncMysqlColumnCaseTablesWithJsonFormat+testSyncMysqlColumnCaseTablesWithJsonFormatAndUpperColumnNameCase \
  integration-test -DfailIfNoTests=false
```

执行结果：

```text
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

补充说明：

- 这轮定向 E2E 是在本次 streaming sink 修复之后重新执行的
- 这里已经使用“Doris connector 原生修复 + CDC 侧移除 wrapper”的最终代码形态
- 它验证了 mixed / upper / lower 三类表在 JSON 模式下的建表、snapshot、incremental、update/delete 断言仍然通过
- 说明这次 schema barrier 修复没有破坏已有 column-case 主链路能力
- 本次本地环境里 `target/flink-1.20.3-bin-scala_2.12.tgz` 缓存出现过损坏，因此使用了 `-Dflink.release.download.skip=true` 复用已解压的本地 Flink 目录

## 十、额外说明

### 10.1 为什么最终不保留 CDC 侧 wrapper

第一版 CDC wrapper 只是为了快速验证根因：

- 它证明了 schema barrier 确实需要真实切批
- 但实现方式依赖反射接入 Doris writer 私有实现
- Doris connector 后续升级时，字段名或方法签名变化都会让 wrapper 变得脆弱

因此最终版本已经：

- 把真实修复下沉到 Doris connector
- 删除 CDC 侧 `SchemaChangeAwareDorisSink / Writer`
- 让 CDC 恢复到直接接入原生 `DorisSink`

### 10.2 为什么还保留 Doris connector 侧 IT / Unit Test

本次问题并不是单一点修复，而是两层能力一起闭环：

- Doris connector 层负责保证 JSON key 与 Doris 物理 schema 一致
- runtime transform 层负责保证 projection / column-name-case 的最终 schema 语义正确

因此当前测试分层是合理的：

- Doris connector 单测 / IT：验证 JSON serializer 与 Doris 物理 schema 的对齐，同时补强 `schema evolution + json + late-event` 这种难以靠真实 MySQL 顺序 binlog 稳定复现的场景
- runtime 回归测试：验证 projection alias 优先级
- pipeline E2E：验证真实 MySQL -> Doris 端到端行为

### 10.3 为什么中间日志里会出现 “didn't get expected results”

MySqlToDorisE2eITCase 的 `waitAndVerify(...)` 本身是轮询机制。

因此在 snapshot 刚落完、增量数据尚未刷到 Doris 的短窗口里，会打印类似：

```text
Executing ... didn't get expected results.
```

这不是最终失败，而是“继续等待下一轮轮询”的中间日志。只要后续达到预期并最终 `BUILD SUCCESS`，就说明用例通过。

### 10.4 本次执行过程里的两个旁路问题

1. 从仓库根目录 `-pl ... -am` 跑 Doris 模块测试时，会被无关的 `flink-cdc-pipeline-udf-examples` 编译问题挡住
   - 这不是 Doris 修复引入的问题
   - 实际执行时改成直接进入目标模块目录运行即可
2. `flink-cdc-pipeline-e2e-tests/target/flink-1.20.3-bin-scala_2.12.tgz` 本地缓存曾出现损坏
   - 表现为 `Unexpected end of ZLIB input stream`
   - 实际复跑时通过 `-Dflink.release.download.skip=true` 复用已解压目录完成验证

### 10.5 当前仍需关注的剩余风险

本轮晚到事件修复的关键策略是：

- Doris 物理 schema 映射按 `tableId + inputSchema` 维度缓存
- 对旧 schema 晚到事件，允许新物理列输出为 `null`
- 通过 `recordData.getArity()` 在历史 schema 里回溯匹配

这已经能稳定覆盖本次最关键的 `AddColumnEvent` 晚到场景，但还要明确一个边界：

- 如果未来出现“列数不变但类型变化”的极端晚到事件，当前 serializer 缺少显式 schema version，只能按当前或最近一次同 arity schema 去解释 `RecordData`

这不影响本次已确认问题的修复范围，但在下一阶段如果要把“schema evolution + late-event”做成更强保证，建议继续评估：

- 是否要在数据事件里携带更强的 schema version / schema fingerprint
- 或者在 serializer 前就把 schema 解析结果固化下来，避免仅靠 arity 推断

## 十一、结论

本次改动已经把用户反馈的两条主问题和一条最终语义要求一起闭环掉：

1. `format=json` 下 mixed-case 物理列不再因为 JSON key 大小写错误而部分丢字段
2. `column-name-case=UPPER` 产出的全大写 schema/data，在 Doris 侧可以正确同步
3. `projection` 显式 alias 大小写优先，`pipeline.column-name-case` 仅对未显式改名的字段兜底

当前结论是：

- Doris JSON 字段丢失根因已经定位并修复在 `DorisEventSerializer`
- runtime 已明确并守住“projection alias 优先，column-name-case 兜底”的最终语义
- 早一批 MySQL -> Doris 真实 Docker E2E 已在 JDK 11 下跑通主覆盖矩阵
- `SchemaOperator regular -> Doris streaming sink` 在 `schema evolution + json + 晚到事件` 下的真实丢值根因也已经定位并修复
- 最终落地形态是“Doris connector 原生修复 + CDC 侧删除临时 wrapper”
- JDK 11 下的定向 connector IT 与 MySQL -> Doris Docker E2E 已重新执行并通过
- 当前最合理的下一步是：继续补剩余非 projection 方向的空白覆盖，例如更强的 schema evolution 组合场景和更完整的 update/delete 变体，然后整理提交信息、PR 描述和最终 review
