# Oracle CDC ROWID 元数据列实现方案

## 一、背景与需求

在使用 Flink CDC 采集 Oracle 数据时，业务需要获取每一行数据对应的 Oracle **ROWID 伪列**（唯一物理地址），用于：

- 精确定位数据源的物理行
- 支持数据溯源、去重、幂等写入
- 兼容基于 ROWID 的下游处理逻辑

**目标**：让用户通过 `METADATA FROM 'row_id' VIRTUAL` 语法，即可在 Flink SQL 中获取到 Oracle 表的 ROWID。

**测试环境**：Oracle Database 12c Standard Edition Release 12.1.0.2.0

**目标产物**：基于 Flink 1.16.3 编译的 `flink-sql-connector-oracle-cdc` fat jar。

---

## 二、最终使用方式

```sql
CREATE TABLE o_tab1_source (
    ID INT,
    NAME STRING,
    row_id STRING METADATA FROM 'row_id' VIRTUAL   -- 声明 ROWID 元数据列
) WITH (
    'connector' = 'oracle-cdc',
    'hostname' = '129.211.212.115',
    'port' = '11521',
    'username' = 'cptuser01',
    'password' = 'Tbds12345',
    'database-name' = 'XE',
    'schema-name' = 'CPTUSER01',
    'table-name' = 'O_TAB1',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial'
);

INSERT INTO print_sink SELECT * FROM o_tab1_source;
```

**输出结果**：

```
+I[2, bbb, AABDuHAAGAAAAF3AAA]
+I[3, ccc, AABDuHAAGAAAAF0AAA]
+I[4, ddd, AABDuHAAGAAAAF0AAB]
-D[2, bbb, AABDuHAAGAAAAF3AAA]
```

---

## 三、Oracle CDC 采集机制回顾

Oracle CDC 分为两个阶段，ROWID 来源不同：

| 阶段 | 类 | ROWID 来源 |
|------|-----|-----------|
| **快照阶段（Snapshot）** | `OracleScanFetchTask` | JDBC ResultSet 的 ROWID 伪列 |
| **流式阶段（Streaming）** | `LogMinerStreamingChangeEventSource` | `V$LOGMNR_CONTENTS.ROW_ID` 列 |

流式阶段的 Debezium `LogMinerChangeRecordEmitter` **已经原生将 ROWID 写入 SourceRecord 的 headers**。所以我们只需要：

1. **快照阶段**：手动提取 ROWID 并注入 headers（新增逻辑）
2. **读取端**：在 `OracleReadableMetaData` 新增 `ROW_ID` 元数据枚举（新增逻辑）
3. **修复中间管道 bug**：Flink CDC 在快照阶段重构 SourceRecord 时丢弃了 headers（关键 bug 修复）

---

## 四、方案实施（4 大关键修改）

### 4.1 新增 ROW_ID 元数据枚举

**文件**：`flink-connector-oracle-cdc/src/main/java/com/ververica/cdc/connectors/oracle/table/OracleReadableMetaData.java`

在 enum 中新增 `ROW_ID` 常量，从 SourceRecord 的 headers 中读取 `"ROWID"` header：

```java
ROW_ID(
    "row_id",
    DataTypes.STRING(),
    new MetadataConverter() {
        @Override
        public Object read(SourceRecord record) {
            ConnectHeaders headers = (ConnectHeaders) record.headers();
            if (headers != null) {
                for (Header header : headers) {
                    if (oracle.sql.ROWID.class.getSimpleName().equals(header.key())) {
                        return StringData.fromString(header.value().toString());
                    }
                }
            }
            return null;
        }
    });
```

### 4.2 快照阶段 SQL 重写 + ROWID 提取

**文件**：`flink-connector-oracle-cdc/src/main/java/com/ververica/cdc/connectors/oracle/source/reader/fetch/OracleScanFetchTask.java`

**核心难点**：Oracle 的 `SELECT *` **不包含 ROWID 伪列**，必须显式列出。但 Debezium 的 `ColumnUtils.toArray(rs, table)` 会严格校验 ResultSet 每一列都必须在 `Table` schema 中存在，加入 ROWID 会校验失败。

**解决方案**：

1. **SQL 重写**：将 `SELECT * FROM tab` 改写为 `SELECT T0.*, ROWID FROM tab T0`
   - ROWID 放在末尾，让物理列位置保持 1..N 不变
2. **绕过 ColumnUtils 严格校验**：手动构造 `ColumnArray`，只包含表原有列
3. **独立提取 ROWID**：从 ResultSet 最后一列（位置 N+1）用 `((OracleResultSet) rs).getROWID(N+1)` 读取
4. **注入 headers**：覆写匿名 `SnapshotChangeRecordEmitter.getEmitConnectHeaders()`

关键代码：

```java
// 1. SQL 重写
final String rowIdSelectSql = selectSql.replaceFirst(
    "SELECT \\* FROM " + Pattern.quote(tableRef),
    "SELECT T0.*, ROWID FROM " + tableRef + " T0");

// 2. 手动构造 ColumnArray 绕过校验
final int rowIdColumnIndex = rs.getMetaData().getColumnCount();
io.debezium.relational.Column[] tableColumns =
    table.columns().toArray(new io.debezium.relational.Column[0]);
int greatestPosition = /* ... */;
ColumnUtils.ColumnArray columnArray =
    new ColumnUtils.ColumnArray(tableColumns, greatestPosition);

// 3. 循环读行 + 提取 ROWID
while (rs.next()) {
    Object[] row = jdbcConnection.rowToArray(table, databaseSchema, rs, columnArray);
    String rowId = null;
    try {
        oracle.sql.ROWID oracleRowId =
            ((OracleResultSet) rs).getROWID(rowIdColumnIndex);
        if (oracleRowId != null) {
            rowId = oracleRowId.stringValue();
        }
    } catch (Exception e) { /* ... */ }
    dispatcher.dispatchSnapshotEvent(
        snapshotContext.partition, table.id(),
        getChangeRecordEmitter(snapshotContext, table.id(), row, rowId),
        snapshotReceiver);
}

// 4. 覆写 getEmitConnectHeaders 注入 ROWID
return new SnapshotChangeRecordEmitter<OraclePartition>(
        snapshotContext.partition, snapshotContext.offset, row, clock) {
    @Override
    protected Optional<ConnectHeaders> getEmitConnectHeaders() {
        if (rowId != null) {
            ConnectHeaders headers = new ConnectHeaders();
            headers.add(oracle.sql.ROWID.class.getSimpleName(),
                new SchemaAndValue(null, rowId));
            return Optional.of(headers);
        }
        return Optional.empty();
    }
};
```

### 4.3 修复 Flink CDC 中间管道丢弃 headers 的 bug ⭐

**这是本方案最关键的发现！**

**文件**：`flink-cdc-base/src/main/java/com/ververica/cdc/connectors/base/source/reader/external/JdbcSourceFetchTaskContext.java`

**Bug 描述**：`JdbcSourceFetchTaskContext.formatMessageTimestamp()` 在快照阶段重构 SourceRecord 时，使用了 **8 参数**的 SourceRecord 构造函数，**没有传入 headers**，导致我们注入的 ROWID headers 被完全丢弃。

**修复前（bug）**：

```java
SourceRecord sourceRecord = new SourceRecord(
    record.sourcePartition(),
    record.sourceOffset(),
    record.topic(),
    record.kafkaPartition(),
    record.keySchema(),
    record.key(),
    record.valueSchema(),
    envelope.read(updateAfter, source, fetchTs));  // ❌ 只有 8 参数
```

**修复后**：

```java
SourceRecord sourceRecord = new SourceRecord(
    record.sourcePartition(),
    record.sourceOffset(),
    record.topic(),
    record.kafkaPartition(),
    record.keySchema(),
    record.key(),
    record.valueSchema(),
    envelope.read(updateAfter, source, fetchTs),
    record.timestamp(),        // ✅ 保留 timestamp
    record.headers());          // ✅ 保留 headers（含 ROWID）
```

**影响范围**：此 bug 不只影响 Oracle row_id，凡是**依赖 headers 传递数据**的元数据列，在增量快照模式下都会丢失。修复后所有基于 headers 的元数据机制都能正确工作。

### 4.4 SQL 参数要求

必须启用增量快照模式，非增量模式走 Debezium 原生 `RelationalSnapshotChangeEventSource`，不经过我们修改的 `OracleScanFetchTask`：

```sql
'scan.incremental.snapshot.enabled' = 'true',
'scan.startup.mode' = 'initial'
```

---

## 五、完整数据流

```
┌─────────────────────────────────────────────────────────────────────┐
│  快照阶段：OracleScanFetchTask                                       │
│                                                                     │
│  SQL: SELECT T0.*, ROWID FROM "CPTUSER01"."O_TAB1" T0               │
│    ↓                                                                │
│  ResultSet 列布局：[ID(1), NAME(2), ROWID(3)]                       │
│    ↓                                                                │
│  rowToArray(手动构造 ColumnArray 只含 [ID, NAME])                   │
│  rs.getROWID(3) → "AABDuHAAGAAAAF1AAA"                              │
│    ↓                                                                │
│  匿名 SnapshotChangeRecordEmitter.getEmitConnectHeaders()           │
│    → Optional.of(ConnectHeaders{ROWID=AABDuHAAGAAAAF1AAA})          │
└─────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Debezium 事件管道                                                  │
│                                                                     │
│  RelationalChangeRecordEmitter.emitReadRecord()                     │
│    → receiver.changeRecord(..., headers)                            │
│      → EventDispatcher$1 → BufferingSnapshotChangeRecordReceiver    │
│        → new SourceRecord(..., headers)                             │
│          → ChangeEventQueue.enqueue(DataChangeEvent)                │
└─────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Flink CDC 增量快照读取                                              │
│                                                                     │
│  IncrementalSourceScanFetcher.pollSplitRecords()                    │
│    → outputBuffer.put(key, record)                                  │
│    → JdbcSourceFetchTaskContext.formatMessageTimestamp() ⭐         │
│      → new SourceRecord(..., record.headers())                      │
│        ✅ 修复后：headers 得以保留                                   │
└─────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  下游反序列化                                                        │
│                                                                     │
│  IncrementalSourceRecordEmitter.emitElement()                       │
│    → debeziumDeserializationSchema.deserialize(record, collector)   │
│      → AppendMetadataCollector.collect(physicalRow)                 │
│        → OracleReadableMetaData.ROW_ID.read(record)                 │
│          → 遍历 record.headers() 查找 key="ROWID"                   │
│            → 找到 → StringData.fromString("AABDuHAAGAAAAF1AAA")     │
│              → JoinedRowData([id, name] + [rowid])                  │
│                → 输出 +I[1, a, AABDuHAAGAAAAF1AAA]                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 六、修改文件清单

| 序号 | 文件 | 修改说明 | 类型 |
|-----|------|---------|------|
| 1 | `OracleReadableMetaData.java` | 新增 `ROW_ID` 枚举常量 | 新增元数据 |
| 2 | `OracleScanFetchTask.java` | SQL 重写 + 手动 ColumnArray + ROWID 提取 + 覆写 headers | 快照阶段实现 |
| 3 | `JdbcSourceFetchTaskContext.java` | 修复 formatMessageTimestamp 丢弃 headers | **关键 bug 修复** |

---

## 七、问题排查历程

| 阶段 | 问题现象 | 根因 | 解决 |
|-----|---------|-----|------|
| 1 | 未启用增量快照时 row_id 为 null | 走 Debezium 原生路径，未经过我们的代码 | 要求配置 `scan.incremental.snapshot.enabled=true` |
| 2 | 增量模式下 `Column 'ROWID' not found` | `ColumnUtils.toArray` 严格校验 ResultSet 列 | 手动构造 ColumnArray 绕过校验 |
| 3 | SQL 通过但 row_id 依然 null | Flink CDC 重构 SourceRecord 丢弃 headers | 修复 `formatMessageTimestamp()` 传入 headers |

---

## 八、编译与部署

### 编译命令

```bash
cd /path/to/flink-cdc
mvn clean package -pl flink-sql-connector-oracle-cdc -am \
    -Dflink.version=1.16.3 \
    -Dmaven.test.skip=true \
    -Dcheckstyle.skip=true \
    -Dspotless.check.skip=true \
    -T 4
```

### 产物路径

```
flink-sql-connector-oracle-cdc/target/flink-sql-connector-oracle-cdc-2.4-SNAPSHOT.jar
```

大小约 27MB，可直接放入 Flink 的 `lib/` 目录使用。

---

## 九、限制与注意事项

1. **仅支持增量快照模式**：`scan.incremental.snapshot.enabled` 必须为 `true`，非增量模式（Debezium 原生）不支持 row_id。
2. **补充日志要求**：为了让流式阶段能正确解析 ROWID，需要开启表的补充日志：
   ```sql
   ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
   ```
3. **数据库权限**：账户必须能读取表和 `V$LOGMNR_CONTENTS` 视图。
4. **Oracle 特定实现**：依赖 Oracle JDBC 的 `OracleResultSet.getROWID(int)` API 和 `oracle.sql.ROWID` 类。

---

## 十、扩展性

修复的 `JdbcSourceFetchTaskContext.formatMessageTimestamp()` 是 Flink CDC 通用逻辑，也惠及 **MySQL、PostgreSQL、SQL Server、Db2** 等所有基于 JDBC 的 Flink CDC 连接器 —— 它们的增量快照模式下现在都能正确保留 SourceRecord 的 headers，为后续实现其他基于 headers 的元数据列（如 SCN、Position 等）打好了基础。
