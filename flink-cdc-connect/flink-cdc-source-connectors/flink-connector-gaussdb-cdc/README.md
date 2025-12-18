# Flink CDC Connector for GaussDB

GaussDB CDC Connector 是一个 Flink CDC 连接器，用于从华为 GaussDB 数据库捕获变更数据。

## 功能特性

- 支持全量快照读取和增量变更捕获
- 基于 GaussDB 逻辑复制 (mppdb_decoding 插件)
- 支持 DataStream API 和 Table API (Flink SQL)
- 支持 Exactly-Once 语义
- 支持并行快照读取
- 支持 Checkpoint 和故障恢复

## 前置条件

### GaussDB 配置要求

1. **GaussDB 版本**: 5.x+ (已验证 Kernel 505.2.1.SPC0800)

2. **启用逻辑复制**:
```sql
-- 检查 wal_level
SHOW wal_level;  -- 应为 'logical'

-- 如需修改，执行：
ALTER SYSTEM SET wal_level = 'logical';
-- 重启数据库生效
```

3. **安装 mppdb_decoding 插件**:
```sql
-- 检查插件是否可用
SELECT * FROM pg_available_extensions WHERE name = 'mppdb_decoding';
```

4. **创建复制槽** (可选，连接器会自动创建):
```sql
SELECT * FROM pg_create_logical_replication_slot('flink_cdc_slot', 'mppdb_decoding');
```

5. **用户权限**:
```sql
-- 授予 REPLICATION 权限
ALTER USER your_user REPLICATION;
```

## 配置选项

### 必需参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `hostname` | String | GaussDB 服务器地址 |
| `port` | Integer | GaussDB 端口 (默认: 8000) |
| `username` | String | 数据库用户名 |
| `password` | String | 数据库密码 |
| `database-name` | String | 数据库名称 |
| `schema-name` | String | Schema 名称 (默认: public) |
| `table-name` | String | 表名 (支持正则表达式) |
| `slot.name` | String | 复制槽名称 |

### 可选参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `decoding.plugin.name` | String | mppdb_decoding | 解码插件名称 |
| `scan.startup.mode` | String | initial | 启动模式: initial, latest-offset |
| `scan.incremental.snapshot.chunk.size` | Integer | 8096 | 快照分片大小 |
| `connect.timeout` | Duration | 30s | 连接超时时间 |
| `connect.max-retries` | Integer | 3 | 最大重试次数 |
| `connection.pool.size` | Integer | 20 | 连接池大小 |
| `heartbeat.interval` | Duration | 30s | 心跳间隔 |

## 使用示例

### DataStream API

```java
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GaussDBCDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        GaussDBSource<String> source = GaussDBSource.<String>builder()
                .hostname("localhost")
                .port(8000)
                .database("testdb")
                .schemaList("public")
                .tableList("public.orders")
                .username("gaussdb")
                .password("password")
                .slotName("flink_cdc_slot")
                .decodingPluginName("mppdb_decoding")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "GaussDB CDC Source")
                .print();

        env.execute("GaussDB CDC Job");
    }
}
```

### Table API (Flink SQL)

```sql
-- 创建 GaussDB CDC 源表
CREATE TABLE orders_source (
    order_id INT,
    order_date TIMESTAMP(3),
    customer_name STRING,
    price DECIMAL(10, 2),
    product_id INT,
    order_status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = 'localhost',
    'port' = '8000',
    'username' = 'gaussdb',
    'password' = 'password',
    'database-name' = 'testdb',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_slot'
);

-- 查询变更数据
SELECT * FROM orders_source;

-- 写入到下游
INSERT INTO sink_table SELECT * FROM orders_source;
```

### 元数据列

支持以下元数据列：

```sql
CREATE TABLE orders_source (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    schema_name STRING METADATA FROM 'schema_name' VIRTUAL,
    table_name STRING METADATA FROM 'table_name' VIRTUAL,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    order_id INT,
    order_date TIMESTAMP(3),
    customer_name STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    ...
);
```

## 数据类型映射

| GaussDB 类型 | Flink SQL 类型 |
|--------------|----------------|
| SMALLINT | SMALLINT |
| INTEGER | INT |
| BIGINT | BIGINT |
| REAL | FLOAT |
| DOUBLE PRECISION | DOUBLE |
| NUMERIC(p, s) | DECIMAL(p, s) |
| BOOLEAN | BOOLEAN |
| CHAR(n) | CHAR(n) |
| VARCHAR(n) | VARCHAR(n) |
| TEXT | STRING |
| DATE | DATE |
| TIME | TIME(0) |
| TIMESTAMP | TIMESTAMP(6) |
| TIMESTAMPTZ | TIMESTAMP_LTZ(6) |
| BYTEA | BYTES |
| JSON/JSONB | STRING |
| UUID | STRING |
| ARRAY | ARRAY |

## 故障排查

### 常见问题

1. **连接失败**
   - 检查 hostname、port、username、password 是否正确
   - 确认 GaussDB 服务正在运行
   - 检查防火墙设置

2. **复制槽创建失败**
   - 确认用户具有 REPLICATION 权限
   - 检查 wal_level 是否为 'logical'
   - 确认 mppdb_decoding 插件已安装

3. **无法捕获变更**
   - 确认表有主键
   - 检查复制槽是否正常工作
   - 查看 GaussDB 日志

4. **Checkpoint 失败**
   - 增加 checkpoint 超时时间
   - 检查网络连接稳定性
   - 确认 GaussDB 负载正常

### 日志级别调整

```properties
# log4j.properties
log4j.logger.org.apache.flink.cdc.connectors.gaussdb=DEBUG
log4j.logger.io.debezium.connector.gaussdb=DEBUG
```

## 限制说明

- 不支持无主键表的增量快照
- 不支持 DDL 变更事件捕获 (v1.0)
- 不支持 SSL 连接 (v1.0)

## 版本兼容性

| Connector 版本 | Flink 版本 | GaussDB 版本 |
|----------------|------------|--------------|
| 3.6-SNAPSHOT | 1.20.x | 5.x+ |

## License

Apache License 2.0
