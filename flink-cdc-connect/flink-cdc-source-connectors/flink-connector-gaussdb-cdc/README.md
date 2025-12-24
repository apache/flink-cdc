# Flink CDC Connector for GaussDB

GaussDB CDC Connector 是一个 Flink CDC 连接器，用于从华为 GaussDB 数据库捕获全量和增量变更数据。它深度适配了 GaussDB 的 `mppdb_decoding` 插件，确保了在复杂数据类型和高并发场景下的稳定性。

## 🎯 快速开始

### 1. 编译打包
在项目根目录下执行 Maven 命令，编译并生成 Fat JAR（包含所有依赖）：
```bash
mvn clean install -DskipTests
```
编译完成后，你可以在以下目录找到对应的 JAR 包：
- **SQL Connector**: `flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar`
- **Source Connector**: `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/target/flink-connector-gaussdb-cdc-3.6-SNAPSHOT.jar`

### 2. 集成到 Flink 集群
将生成的 `flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar` 拷贝到 Flink 集群所有节点的 `lib/` 目录下：
```bash
cp flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar $FLINK_HOME/lib/
```
> [!IMPORTANT]
> 集群中如果存在版本冲突的 `gaussdbjdbc.jar` 或 `postgresql.jar`，建议先清理，以免引起类加载冲突。

### 3. 重启 Flink 集群
拷贝完成后，重启 Flink JobManager 和 TaskManager：
```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

### 4. 提交第一个 Flink SQL 作业
使用 Flink SQL Client 或作业提交工具运行以下 DDL 和查询：
```sql
CREATE TABLE orders_source (
    order_id INT,
    order_date TIMESTAMP(3),
    customer_name STRING,
    price DECIMAL(10, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'your_gaussdb_host',
    'port' = '8000',
    'username' = 'your_username',
    'password' = 'your_password',
    'database-name' = 'your_db',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_slot',
    'decoding.plugin.name' = 'mppdb_decoding'
);

SELECT * FROM orders_source;
```

---

## 🌟 功能特性

- **并行全量同步**: 支持分片读取，极大提高海量历史数据的同步速度。
- **实时增量捕获**: 基于 `mppdb_decoding` 插件，秒级捕获 I/U/D 变更。
- **Exactly-Once**: 结合 Flink Checkpoint 机制，确保数据不丢失、不重复。
- **完善的中文支持**: 原生支持 UTF-8，彻底解决中文同步乱码问题。
- **鲁棒验证框架**: 内置一键式 E2E 测试脚本 (`test_gaussdb_cdc.sh`)。

## 📖 核心配置

| 参数 | 必选 | 默认值 | 说明 |
|------|------|--------|------|
| `hostname` | 是 | - | GaussDB 节点 IP |
| `port` | 否 | 8000 | 数据库端口 |
| `username` | 是 | - | 具备 REPLICATION 权限的用户 |
| `password` | 是 | - | 用户密码 |
| `database-name` | 是 | - | 逻辑数据库名 |
| `table-name` | 是 | - | 监听的表名 (支持正则表达式) |
| `slot.name` | 是 | - | 逻辑复制槽名称 |
| `decoding.plugin.name`| 否 | mppdb_decoding | 解码插件名 |

## 🛠️ 自动化测试

项目根目录提供了完善的自动化脚本，建议在正式部署前运行验证：
```bash
./test_gaussdb_cdc.sh
```
该脚本会自动完成：**自动打包 -> 模拟集群部署 -> 自动制造 I/U/D 数据 -> 验证 Sink 端一致性**。

## 📑 故障排查

1. **无法捕获更新/删除?**
   - 检查表是否定义了主键。
   - 确认 `REPLICA IDENTITY` 是否设置为 `FULL` 或 `DEFAULT`。
2. **连接超时?**
   - 检查 `wal_level` 是否设为 `logical`。
   - 检查防火墙是否开放了 8000 端口。

## 📄 License

Apache License 2.0
