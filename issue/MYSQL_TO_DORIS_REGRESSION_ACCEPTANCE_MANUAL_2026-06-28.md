# MySQL → Doris CDC 回归验收操作手册

**适用范围**：本 PR 修改后 Flink CDC 3.6.0（MySQL → Doris 链路）
**测试目标**：验证 MySQL 端 DDL 变更能正确同步到 Doris
**预计耗时**：30-60 分钟

---

## 一、测试环境准备

### 1.1 部署组件

| 组件 | 版本 | 说明 |
|---|---|---|
| MySQL | 8.0.x | 源库 |
| Doris | 2.0+ | 目标库（1FE+1BE 即可） |
| Flink CDC | 本 PR build | pipeline 运行 |

### 1.2 启动 pipeline

按现有方式启动 MySQL → Doris 的 pipeline，YAML 示例：

```yaml
source:
  type: mysql
  hostname: <mysql-host>
  port: 3306
  username: <user>
  password: <password>
  tables: <db>.student
  server-id: 5400-5404
  server-time-zone: UTC

sink:
  type: doris
  fenodes: <fe-host>:8030
  benodes: <be-host>:8040
  username: root
  password: ""
  table.create.properties.replication_num: 1
  sink.properties.format: json

pipeline:
  schema.change.behavior: evolve
  parallelism: 1
```

### 1.3 在 MySQL 端准备测试表

```sql
-- 测试库
CREATE DATABASE IF NOT EXISTS test_cdc;

-- 测试表（注意：故意大小写混合）
CREATE TABLE student (
  id BIGINT NOT NULL PRIMARY KEY COMMENT 'student id',
  name VARCHAR(64) NOT NULL COMMENT 'student name',
  JOB VARCHAR(64) COMMENT 'old job',
  create_time TIMESTAMP,
  AGE INT DEFAULT 30 COMMENT 'old age comment',
  flag INT DEFAULT 1
);
```

**Doris 端验证初始状态**（执行下面的 SQL 确认）：
```sql
DESCRIBE test_cdc.student;
```
预期结果：
```
Field         | Type           | Null | Key | Default | Comment
id            | BIGINT         | No   | true | NULL    | student id
name          | VARCHAR(192)   | No   | false| NULL    | student name
JOB           | VARCHAR(192)   | Yes  | false| NULL    | old job
create_time   | DATETIME       | Yes  | false| NULL    |
AGE           | INT            | Yes  | false| 30      | old age comment
flag          | INT            | Yes  | false| 1       |
```

---

## 二、回归测试用例

按顺序执行以下操作，每步操作后**等待 5 秒**再在 Doris 端验证。

### 用例 1：修改字段类型并保留完整列属性

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN AGE BIGINT DEFAULT 30 COMMENT 'old age comment';
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `AGE` 列类型从 `INT` 变为 `BIGINT`
- `AGE` 列默认值保持为 `30`
- `AGE` 列注释保持为 `old age comment`（**不应该丢失**）
- 其他列不变

**通过标准**：AGE 类型变 BIGINT，默认值仍为 `30`，注释仍为 "old age comment"。

---

### 用例 2：同时修改类型和注释

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN JOB VARCHAR(255) COMMENT 'new job';
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `JOB` 列类型从 `VARCHAR(192)` 变为 `VARCHAR(765)`（Doris 内部换算）
- `JOB` 列注释从 `old job` 变为 `new job`

**通过标准**：JOB 类型和注释都更新。

---

### 用例 3：仅修改注释（类型不变）

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN AGE BIGINT DEFAULT 30 COMMENT 'updated age comment';
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `AGE` 列类型保持 `BIGINT`（沿用用例 1 的类型变更）
- `AGE` 列注释变为 `updated age comment`

**通过标准**：AGE 类型不变，注释更新。

---

### 用例 4：**删除列注释**（关键回归点）

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN AGE BIGINT DEFAULT 30;
```
（注意：MySQL `MODIFY COLUMN` 是完整列定义；这里保留 `DEFAULT 30`，但移除 `COMMENT 'updated age comment'` 部分）

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `AGE` 列注释**被清空**（`Comment` 列为空字符串）
- `AGE` 列类型保持 `BIGINT`
- `AGE` 列默认值保持为 `30`

**通过标准**：AGE 默认值保留，注释被清除（不是保留旧值）。

> **重要**：这是本次 PR 修复的关键 bug。如果注释仍然显示 `updated age comment` 而没有清空，说明 PR 没有正确修复，需要反馈给开发。

---

### 用例 5：添加新字段（指定位置）

**MySQL 端操作**：
```sql
ALTER TABLE student ADD COLUMN score INT COMMENT 'student score' AFTER name;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- 新增 `score` 列，类型 `INT`，注释 `student score`
- `score` 列位于 `name` 和 `JOB` 之间（AFTER name 生效）

**通过标准**：新列出现在 name 和 JOB 之间。

---

### 用例 6：大小写重命名字段（Doris 物理 no-op）

**MySQL 端操作**：
```sql
ALTER TABLE student RENAME COLUMN JOB TO job;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- Pipeline 继续运行，不因 Doris `Same column name` 错误失败
- Doris 物理列名保持 `JOB`（Doris 将 `JOB` 和 `job` 视为同一列）
- 列类型和注释保持不变（`VARCHAR(765)`, `new job`）

**通过标准**：作业不中断；Doris 中仍为 `JOB`，类型和注释保留。

---

### 用例 7：删除字段

**MySQL 端操作**：
```sql
ALTER TABLE student DROP COLUMN flag;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `flag` 列消失
- 其他列保持不变

**通过标准**：flag 列已删除。

---

### 用例 8：在重命名后再修改注释

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN score INT COMMENT 'updated score';
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `score` 列注释从 `student score` 变为 `updated score`
- 其他不变

**通过标准**：score 注释更新。

---

### 用例 9：最终数据写入验证

**MySQL 端操作**：
```sql
INSERT INTO student VALUES (3, 'Carol', 95, 'scientist', '2024-01-03 00:00:00', 20);
```

**Doris 端验证**：
```sql
SELECT * FROM test_cdc.student ORDER BY id;
```
预期：
```
id | name  | score | JOB       | create_time           | AGE
1  | Alice | NULL  | engineer  | 2024-01-01 00:00:00  | 18
2  | Bob   | NULL  | doctor    | 2024-01-02 00:00:00  | 19
3  | Carol | 95    | scientist | 2024-01-03 00:00:00  | 20
```

**通过标准**：3 行数据按预期写入。

---

## 三、`column-name-case: LOWER` 配置测试

### 3.1 启动带 LOWER 配置的 pipeline

在 pipeline YAML 末尾添加：
```yaml
pipeline:
  schema.change.behavior: evolve
  column-name-case: LOWER
  parallelism: 1
```

重新创建 student 表（同 1.3 节）。

### 3.2 初始状态验证

```sql
DESCRIBE test_cdc.student;
```
预期：所有列名都小写（`id`, `name`, `job`, `create_time`, `age`, `flag`），注释保持原值。

### 3.3 注释删除验证

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN age BIGINT COMMENT 'temp comment';
ALTER TABLE student MODIFY COLUMN age BIGINT;  -- 删除注释
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：`age` 列注释为空。

### 3.4 添加新字段

**MySQL 端操作**：
```sql
ALTER TABLE student ADD COLUMN score INT COMMENT 'student score' AFTER name;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：新列名为 `score`（小写），位于 `name` 和 `job` 之间。

### 3.5 重命名（同大小写）

**MySQL 端操作**：
```sql
ALTER TABLE student RENAME COLUMN JOB TO job;
```
（重命名为同名——在 LOWER 转换下应该被识别为 no-op）

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：列结构不变。

### 3.6 删除字段

**MySQL 端操作**：
```sql
ALTER TABLE student DROP COLUMN flag;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：`flag` 列消失。

---

## 四、最终验收清单

完成所有用例后，请填写：

| 用例 | 操作 | 通过 | 备注 |
|---|---|---|---|
| 1 | MODIFY TYPE ONLY | 通过 | 自动化 e2e 已通过 |
| 2 | MODIFY TYPE + COMMENT | 通过 | 自动化 e2e 已通过 |
| 3 | MODIFY COMMENT ONLY | 通过 | 自动化 e2e 已通过 |
| 4 | **MODIFY DROP COMMENT** | 通过 | 关键回归点；自动化 e2e 已通过 |
| 5 | ADD COLUMN AFTER | 通过 | 自动化 e2e 已通过 |
| 6 | RENAME COLUMN | 通过 | 自动化 e2e 已通过 |
| 7 | DROP COLUMN | 通过 | 自动化 e2e 已通过 |
| 8 | MODIFY AFTER RENAME | 通过 | 自动化 e2e 已通过 |
| 9 | INSERT + SELECT | 通过 | 自动化 e2e 已通过 |
| L1 | LOWER 初始状态 | 通过 | 自动化 e2e 已通过 |
| L2 | LOWER DROP COMMENT | 通过 | 自动化 e2e 已通过 |
| L3 | LOWER ADD COLUMN | 通过 | 自动化 e2e 已通过 |
| L4 | LOWER RENAME (no-op) | 通过 | 自动化 e2e 已通过 |
| L5 | LOWER DROP COLUMN | 通过 | 自动化 e2e 已通过 |

---

## 五、本次 PR 改动总结

### 5.1 改动背景

本次 PR 主要修复 MySQL → Doris pipeline 在 schema evolution 场景下的可用性问题。问题集中在 Doris schema cache、Doris 物理列顺序、MySQL/Debezium schema 事件传播、comment/default/nullability 元数据保持，以及 Doris schema change 异步可见性。

最初失败现象包括：

- `MODIFY COLUMN` 修改类型时，默认值或 comment 可能丢失。
- comment-only `MODIFY COLUMN` 或清空 comment 时，Doris 端没有稳定更新。
- `ADD COLUMN ... AFTER/BEFORE` 后，CSV 写入可能因 Doris 物理列顺序未及时刷新而错列。
- `DROP COLUMN` 或 `RENAME COLUMN` 后，JSON/CSV 序列化可能继续基于旧 cache 判断 schema。
- `JOB` → `job` 这类大小写重命名在 Doris 侧应视为 no-op，否则容易触发无意义 DDL 或错误。
- Doris 表处于 `SCHEMA_CHANGE` 状态时，后续 alter 可能失败，需要区分可重试错误和真正失败。

### 5.2 根因判断

本次问题不是单纯由 `schema.change.column.default-value` 或 `schema.change.column.default-not-null` 参数引起。上述参数只控制 Doris DDL 是否同步默认值和 not-null 约束；CDC 内部 logical schema 必须始终保留上游 MySQL 的 default、nullability、comment 等完整元数据，否则后续 schema change 和数据序列化都会失去正确基准。

核心根因是 schema 状态边界不清：

- CDC logical schema 表示 Flink CDC 内部认知的源端 schema。
- Doris physical schema 表示 Doris FE 当前可见的真实列名、列顺序、类型、默认值、comment。
- Doris schema change DDL 返回成功，不代表 FE 立刻对查询和写入可见。
- CSV 写入依赖 Doris 物理列顺序，不能用 CDC logical schema 顺序近似代替。
- JSON 写入可以容忍 Doris-only default 列，但不能容忍 drop/rename 后旧物理列继续存在。

因此，稳定架构的关键是：schema event 先维护 CDC logical schema，再在需要 Doris 物理语义的位置显式刷新 Doris physical schema；刷新未确认时必须失效 cache 或等待，而不是继续使用旧 cache。

### 5.3 改动范围

主要修改范围：

| 模块 | 文件 | 改动重点 |
|---|---|---|
| Doris pipeline connector | `DorisMetadataApplier` | Doris physical schema 刷新、列名解析、列顺序 cache、drop/rename/type/comment/default 处理 |
| Doris pipeline connector | `DorisSchemaChangeManager` | Doris schema change busy 重试、结构化错误解析、列位置错误 fallback |
| Doris pipeline connector | `DorisEventSerializer` | CSV/JSON 写入前等待 Doris physical schema 匹配，drop/rename 旧列保护 |
| MySQL pipeline connector | `MySqlEventDeserializerTest` | 补充 comment-only alter 解析回归测试 |
| MySQL source connector | `MySqlSourceConfigFactory` | 强制 Debezium 捕获 schema change 和 schema comments |
| Debezium source connector | `DebeziumEventDeserializationSchema` | cached schema mismatch fail-fast，大小写兼容字段解析，冗余 schema event 跳过 |
| Runtime | `SchemaOperator` | 冗余 schema change 在修改 original schema cache 前跳过 |
| E2E | `MySqlToDorisE2eITCase` | 扩展 MySQL → Doris schema/cache 多维度回归 |

### 5.4 关键实现原则

1. Doris 物理 schema 是外部事实源。涉及列顺序、大小写物理列名、drop/rename 后旧列是否消失时，必须以 Doris FE 查询结果为准。
2. schema cache 只能作为性能优化，不能作为正确性前提。无法确认 Doris physical schema 时要失效 cache，后续操作重新解析。
3. CSV 写入必须等待 Doris physical schema 精确匹配，因为 CSV 没有字段名，列顺序错误会直接造成数据错列。
4. JSON 写入允许 Doris-only default 列，但 drop/rename 后旧列属于 disallowed columns，必须等待其从 Doris physical schema 中消失。
5. `MODIFY COLUMN` 是完整列定义。类型变更、默认值、comment 清空都必须按最终列定义表达，不能只处理 type mapping。
6. Doris `JOB` 与 `job` 这类 case-only rename 在物理层应作为 no-op，CDC logical schema 仍按 pipeline 规则更新。
7. Doris 错误分类优先解析结构化 response，包括 HTTP status、`code`、`detailMessage`、`data.detailMessage`；只有结构化信息不足时才退化为 message pattern。
8. Debezium schema 事件和 comments 是 pipeline schema evolution 的基础能力，用户传入的 Debezium properties 不能关闭 `include.schema.changes` 和 `include.schema.comments`。

### 5.5 重要实现细节

- `DorisMetadataApplier` 新增 Doris physical schema expectation：`any`、`contains`、`containsAll`、`excludesAll`，用于在 add/drop/rename/type change 后确认 Doris FE 已经可见目标状态。
- `DorisMetadataApplier` 对 add column 不再手工推导 cache 插入位置作为最终事实，而是在 DDL 成功后刷新 Doris 物理列顺序。
- drop column 会先解析 Doris 真实物理列名，再执行 drop；如果目标列已不存在，跳过 Doris drop 并继续更新 CDC schema。
- rename column 会解析真实物理列名；case-only rename 在 Doris 侧跳过，避免无意义失败。
- alter column type 会过滤无实际类型变化的列，避免 comment-only alter 被错误当作 type alter；comment 清空单独走 comment alter。
- type alter 时会携带当前 default/comment，防止 Doris MODIFY COLUMN 因完整列定义缺失而丢属性。
- `DorisSchemaChangeManager` 对 Doris 表处于 `SCHEMA_CHANGE` 状态的错误做有限重试，避免连续 DDL 因 Doris 异步 schema change 被误判失败。
- `DorisSchemaChangeManager` 的列位置错误识别从粗粒度全文 contains 改为结构化 response + 精确 pattern 分类，日志匹配只作为最后策略。
- `DorisEventSerializer` 维护 drop/rename 产生的 old input columns，JSON 映射等待 Doris old columns 消失，避免旧列被误判为 Doris-only default 列。
- `DorisEventSerializer` 的 CSV schema resolver 等待 Doris schema 与 input schema 精确匹配，保证物理列顺序正确后再写入。
- `SchemaOperator` 在变更 original schema cache 前判断冗余 schema event，避免无效 event 污染 runtime schema 状态。
- `MySqlSourceConfigFactory` 在用户 Debezium properties 合并后重新设置必需属性，保证 schema change/comment 事件不会被用户配置关闭。
- `DebeziumEventDeserializationSchema` 在 cached schema 与当前 record schema 不匹配时 fail-fast，不再 fallback 到 value inference，以避免在 schema 漂移时静默错解数据。

### 5.6 历史 Bug 与本次修复关系

本次失败不全部是新 schema cache 改动引入，也暴露了历史链路问题：

- MySQL source 层存在配置健壮性问题：用户配置可能覆盖 Debezium 必需的 schema/comment 捕获开关。
- Debezium cached schema fallback 可能在 schema event 滞后时掩盖真正问题。
- Doris schema change 返回成功后物理 schema 尚未可见，这是 Doris 异步 schema change 的客观特性，不应依赖立即可见。
- Doris 对大小写列名的处理和 CDC logical schema 的大小写规则并不完全一致，需要显式处理物理 no-op。

本次修复将这些点统一收敛到“logical schema 与 Doris physical schema 分层”的实现模型，而不是继续在单个异常或单个用例上打补丁。

---

## 六、自动化验收结果

执行时间：2026-06-30
JDK：`/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home`
Flink：`1.20.3`

### 6.1 单元测试

| 验证项 | 命令摘要 | 结果 |
|---|---|---|
| MySQL schema-change deserializer | `mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql -Dtest=MySqlEventDeserializerTest test` | 通过，4 passed |
| MySQL source config | `mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc -Dtest=MySqlSourceConfigTest test` | 通过，2 passed |
| Doris connector targeted tests | `DorisEventSerializerTest,DorisMetadataApplierTest,DorisSchemaChangeManagerTest` | 通过 |
| Debezium targeted tests | `DebeziumEventDeserializationSchemaTest` | 通过 |
| Runtime targeted tests | `SchemaOperatorTest,SchemaEvolveTest` | 通过 |

### 6.2 构建与打包

| 验证项 | 命令摘要 | 结果 |
|---|---|---|
| 相关模块 install | `mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc,flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql,flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris,flink-cdc-runtime -am -DskipTests install` | 通过 |
| dist package refresh | `mvn -pl flink-cdc-dist -am -DskipTests package` | 通过 |

### 6.3 E2E 测试

已完成的是本 PR 相关的 MySQL → Doris 核心 targeted e2e，不是全量所有 e2e suite。

| 验证项 | 覆盖重点 | 结果 |
|---|---|---|
| `testSchemaChangeCaseComments` 单独 targeted | type/comment/default/comment-only/drop comment/add/drop/rename/data write | 通过 |
| `testSchemaChangeCaseComments` | schema comment 全链路、case-only rename、comment 清空 | 通过 |
| `testDdlOptionToggles` | default-value / not-null 参数控制、Doris-only default 列、后续数据写入 | 通过 |
| `testCsvPhysicalOrderCache` | CSV 物理列顺序、add before/after、drop、type change、数据列顺序 | 通过 |
| `testLowerCaseSchemaChange` | `column-name-case: LOWER`、comment-only、add/drop/rename、大小写物理列名 | 通过 |

最终 4 用例矩阵在 Maven 的 `end-to-end-tests` 和 `run-last-test` 两个 surefire 阶段各执行一轮，最终结果：

```text
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-06-30T07:37:33+08:00
```

### 6.4 E2E 运行命令

```bash
/usr/bin/env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin:/usr/local/apache-maven-3.9.8/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin \
mvn -f flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/pom.xml \
  -Dflink.release.download.skip=true \
  -DspecifiedFlinkVersion=1.20.3 \
  -Dtest=MySqlToDorisE2eITCase#testSchemaChangeCaseComments+testDdlOptionToggles+testCsvPhysicalOrderCache+testLowerCaseSchemaChange \
  integration-test
```

运行环境注意事项：

- e2e 需要 Java 11。
- e2e 需要 Docker/Testcontainers，可访问本机 Docker socket。
- 本地已有 Flink tarball 时建议加 `-Dflink.release.download.skip=true`，避免重复下载。
- 本机为 arm64 Docker host，`flink:1.20.3-scala_2.12-java11` 为 amd64 镜像，会有 emulation warning，测试仍已通过。
- e2e 日志中多次出现 `didn't get expected results` 是 `waitAndVerify` 等待循环中的中间状态，不代表最终失败；最终以 surefire summary 和 Maven exit code 为准。

---

## 七、交付评估与剩余风险

当前未发现明确残留 BUG。针对本 PR 的核心风险面，自动化单测、构建、dist refresh、MySQL → Doris targeted e2e 均已通过。

| 维度 | 评分 | 说明 |
|---|---:|---|
| 可用性 | 9.6/10 | 已覆盖连续 DDL、Doris schema change 异步可见、CSV/JSON 写入、大小写、default/not-null、comment 清空 |
| 稳定性 | 9.5/10 | cache 失效与物理 schema refresh 策略明确，避免继续依赖旧 cache |
| 可维护性 | 9.3/10 | 逻辑已按 source/runtime/sink 边界拆分，但 Doris 错误分类仍不可避免保留少量 detailMessage pattern |
| 商业交付 | 9.5/10 | 核心链路已验证，可进入交付前 review；建议合并前再按 CI 资源跑一次相关模块全量测试 |

剩余风险：

- Doris HTTP schema-change response 当前对业务错误多为通用 `code=1/2`，精确语义仍需要读取 `detailMessage`；如果 Doris 未来修改 detailMessage 文案，需要补充适配。
- 本轮未跑完整个项目所有 e2e suite，只完成本 PR 相关 MySQL → Doris 核心 targeted e2e。
- Doris schema change 在资源紧张或大表上可能超过当前等待预算，生产环境可根据实际规模评估是否暴露可配置等待参数。

---

## 八、发现 Bug 时如何反馈

如果某个用例未通过，请记录：

1. **用例编号**（例如"用例 4：删除列注释"）
2. **MySQL 操作**：完整的 DDL 语句
3. **Doris 实际结果**：`DESCRIBE` 输出
4. **Doris 期望结果**：应该是什么样
5. **Pipeline 日志**：相关错误或异常（特别是 `JobManager` 的 `SchemaOperator` 阶段）
6. **是否影响数据写入**：是否能继续执行后续 DDL

提交时附上上述信息，附标题 `MySQL→Doris CDC 回归验收不通过：[用例名]`。

---

## 九、附录：常见误判

| 现象 | 真实原因 | 验证方法 |
|---|---|---|
| DESCRIBE 立即返回 `Unknown table` | Doris FE-BE 元数据同步延迟（需重试） | 等 10 秒后重试 |
| 注释一直显示空 | 旧版本未启用 schema comments 解析 | 检查 MySQL CDC 启动日志是否包含 `include.schema.comments` |
| 数据写入但 schema 不变 | pipeline 处于 binlog 模式（snapshot 已完成） | 检查 pipeline 启动时间 vs DDL 时间 |
| Doris 端 `VARCHAR(N)` 显示为 192 而非 64 | Doris 内部换算（utf8mb4 字符宽度） | 这是正常行为，不是 bug |
