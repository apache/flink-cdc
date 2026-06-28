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
create_time   | DATETIMEV2(0)  | Yes  | false| NULL    |
AGE           | INT            | Yes  | false| 30      | old age comment
flag          | INT            | Yes  | false| 1       |
```

---

## 二、回归测试用例

按顺序执行以下操作，每步操作后**等待 5 秒**再在 Doris 端验证。

### 用例 1：仅修改字段类型（类型变更，注释保持不变）

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN AGE BIGINT;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `AGE` 列类型从 `INT` 变为 `BIGINT`
- `AGE` 列注释保持为 `old age comment`（**不应该丢失**）
- 其他列不变

**通过标准**：AGE 类型变 BIGINT，注释仍为 "old age comment"。

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
- `JOB` 列类型从 `VARCHAR(192)` 变为 `VARCHAR(768)`（Doris 内部换算）
- `JOB` 列注释从 `old job` 变为 `new job`

**通过标准**：JOB 类型和注释都更新。

---

### 用例 3：仅修改注释（类型不变）

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN AGE INT COMMENT 'updated age comment';
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `AGE` 列类型保持 `INT`（注意是 `INT` 不是 `BIGINT`，是上一次变回）
- `AGE` 列注释变为 `updated age comment`

**通过标准**：AGE 类型不变，注释更新。

---

### 用例 4：**删除列注释**（关键回归点）

**MySQL 端操作**：
```sql
ALTER TABLE student MODIFY COLUMN AGE INT;
```
（注意：与用例 3 相比，移除了 `COMMENT 'updated age comment'` 部分）

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- `AGE` 列注释**被清空**（`Comment` 列为空字符串）
- `AGE` 列类型保持 `INT`

**通过标准**：AGE 注释被清除（不是保留旧值）。

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

### 用例 6：重命名字段（大小写变化）

**MySQL 端操作**：
```sql
ALTER TABLE student RENAME COLUMN JOB TO job;
```

**Doris 端验证**：
```sql
DESCRIBE test_cdc.student;
```
预期：
- 列名从 `JOB` 变为 `job`（小写）
- 列类型和注释保持不变（`VARCHAR(768)`, `new job`）

**通过标准**：列名变为 `job`，类型和注释保留。

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
id | name  | score | job       | create_time           | AGE
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
ALTER TABLE student RENAME COLUMN job TO job;
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
| 1 | MODIFY TYPE ONLY | ☐ | |
| 2 | MODIFY TYPE + COMMENT | ☐ | |
| 3 | MODIFY COMMENT ONLY | ☐ | |
| 4 | **MODIFY DROP COMMENT** | ☐ | 关键回归点 |
| 5 | ADD COLUMN AFTER | ☐ | |
| 6 | RENAME COLUMN | ☐ | |
| 7 | DROP COLUMN | ☐ | |
| 8 | MODIFY AFTER RENAME | ☐ | |
| 9 | INSERT + SELECT | ☐ | |
| L1 | LOWER 初始状态 | ☐ | |
| L2 | LOWER DROP COMMENT | ☐ | |
| L3 | LOWER ADD COLUMN | ☐ | |
| L4 | LOWER RENAME (no-op) | ☐ | |
| L5 | LOWER DROP COLUMN | ☐ | |

---

## 五、发现 Bug 时如何反馈

如果某个用例未通过，请记录：

1. **用例编号**（例如"用例 4：删除列注释"）
2. **MySQL 操作**：完整的 DDL 语句
3. **Doris 实际结果**：`DESCRIBE` 输出
4. **Doris 期望结果**：应该是什么样
5. **Pipeline 日志**：相关错误或异常（特别是 `JobManager` 的 `SchemaOperator` 阶段）
6. **是否影响数据写入**：是否能继续执行后续 DDL

提交时附上上述信息，附标题 `MySQL→Doris CDC 回归验收不通过：[用例名]`。

---

## 六、附录：常见误判

| 现象 | 真实原因 | 验证方法 |
|---|---|---|
| DESCRIBE 立即返回 `Unknown table` | Doris FE-BE 元数据同步延迟（需重试） | 等 10 秒后重试 |
| 注释一直显示空 | 旧版本未启用 schema comments 解析 | 检查 MySQL CDC 启动日志是否包含 `include.schema.comments` |
| 数据写入但 schema 不变 | pipeline 处于 binlog 模式（snapshot 已完成） | 检查 pipeline 启动时间 vs DDL 时间 |
| Doris 端 `VARCHAR(N)` 显示为 192 而非 64 | Doris 内部换算（utf8mb4 字符宽度） | 这是正常行为，不是 bug |
