# Doris JSON 默认值列被写入 NULL 问题复盘与修复说明

## 一、结论

客户现场的 `confluent__last_updated` 写入失败，不是 transform projection 生成了该字段，也不是 Doris 默认值能力失效，而是 Doris JSON serializer 在构造 Stream Load JSON payload 时错误地把 Doris 目标表独有字段补进了输出，并写成了显式 `null`。

最终修复原则：

1. CDC 输出 schema 决定本次事件实际输出哪些字段。
2. Doris 物理 schema 只用于确定已有 CDC 字段在 Doris 侧应使用的物理列名大小写。
3. Doris-only 字段不得由 serializer 合成出来。
4. 默认值列、`ON UPDATE` 列、审计列等目标端维护字段，必须通过“payload 中省略字段”交给 Doris 自己维护，不能显式写 `null`。

## 二、客户现场影响

客户 MySQL 源表只有业务字段，例如：

```sql
CREATE TABLE `student` (
  `id` bigint NULL COMMENT '主键ID',
  `name` varchar(192) NOT NULL COMMENT '姓名',
  `create_time` datetime(3) NOT NULL COMMENT '创建时间'
);
```

Doris 目标表是预建表，除业务字段和 CDC 元数据字段外，还包含 Doris 自维护字段：

```sql
`confluent__last_updated` DATETIME(3) NOT NULL
    DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
```

pipeline projection 未声明该字段：

```yaml
transform:
  - source-table: test.student
    projection: |
      id AS _id_,
      create_time AS _create_time_,
      __schema_name__ AS _db_,
      __table_name__ AS _tb_,
      __data_event_type__ AS _op_,
      *
```

正确行为是 JSON payload 不包含 `confluent__last_updated`，由 Doris 根据默认值和 `ON UPDATE` 规则维护。

错误行为是 JSON payload 包含：

```json
{"confluent__last_updated": null}
```

该字段在 Doris 中是 `NOT NULL`，显式写入 `null` 会绕过默认值语义并触发写入失败。

## 三、根因

问题发生在：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializer.java
```

当前分支为支持 Doris JSON 写入时的物理列名大小写，加入了 Doris 物理 schema 到 CDC input schema 的字段映射逻辑。这个方向本身是必要的，因为 Doris JSON Stream Load 的 key 必须匹配 Doris 物理列名，否则 mixed-case、upper-case 和预建表大小写不一致场景会出现字段写不进去。

问题在于旧逻辑把“Doris 物理 schema 用于决定 JSON key 名称”扩大成了“Doris 物理 schema 是 JSON 输出字段全集”。

当 Doris 物理表存在字段，但 CDC input schema 中没有对应字段时，旧逻辑仍然保留该 Doris 字段，并把输入字段名记录为 `null`。随后 JSON 投影阶段将它输出为显式 `null`。

这会把目标端维护字段错误地变成 CDC 写入字段。

## 四、正确语义

这里必须区分两个集合：

```text
CDC input schema:
  当前事件真正拥有的数据字段集合

Doris physical schema:
  Doris 目标表当前物理字段集合，用于确认字段存在和确定 JSON key 的物理大小写
```

正确关系是：

```text
JSON 输出字段 = CDC input schema 与 Doris physical schema 的交集
JSON key      = Doris physical schema 中匹配到的物理字段名
JSON value    = CDC input schema 中对应字段的值
```

不在 CDC input schema 中的 Doris 字段，即使存在于 Doris physical schema，也不能进入 JSON payload。

## 五、修复方案

修复点分为两层。

第一层是在 JSON 字段映射构造阶段跳过 Doris-only 字段：

```java
Column inputColumn =
        DorisSchemaUtils.getColumnCaseInsensitive(inputSchema, field.getName()).orElse(null);
if (inputColumn != null) {
    mapping.put(field.getName(), inputColumn.getName());
}
```

第二层是在 JSON 投影阶段增加防御：即使外部或测试注入的 mapping 中存在 `null` input field，也不输出该字段：

```java
String inputFieldName = entry.getValue();
if (inputFieldName == null) {
    continue;
}
outputRecord.put(entry.getKey(), valueMap.get(inputFieldName));
```

这样可以同时保证：

1. mixed-case / upper-case Doris 物理列仍按 Doris 列名输出。
2. projection alias 后的字段仍能正确写入 Doris。
3. Doris-only 默认值列不会被写成 `null`。
4. 旧的异常 mapping 不会继续污染 JSON payload。

## 六、为什么不是 projection 的问题

本次问题不在 transform projection。

projection 的输入是 Flink CDC transform 前的 schema 和 CDC metadata columns，`*` 只展开 transform 输入 schema 中已有字段。Doris 目标表的额外字段不会被 projection 展开。

因此 `confluent__last_updated` 不是 projection 生成的，而是在 Doris sink serializer 阶段按 Doris 物理 schema 做 JSON key 映射时被错误带入的。

## 七、验证覆盖

已补充的关键回归测试：

```text
DorisEventSerializerTest#testJsonSerializationSkipsDorisOnlyDefaultColumns
DorisEventSerializerTest#testJsonSerializationSkipsNullInputFieldMappings
```

覆盖点：

1. Doris 表存在 `confluent__last_updated`，CDC input schema 不存在该字段时，JSON payload 不包含该字段。
2. mapping 中历史兼容性地出现 `null` input field 时，serializer 也不会输出该字段。
3. 原有 upper-case、mixed-case、projection alias 场景继续保留。

建议验证命令：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -DskipITs -DskipE2e -Dcheckstyle.skip -Drat.skip \
  -Dspotless.check.skip=true -Dtest=DorisEventSerializerTest test
```

## 八、客户侧解释口径

这是 Doris sink JSON 序列化阶段的字段全集判断错误。系统本应只写 CDC 事件中存在的字段，但旧逻辑在兼容 Doris 物理列名大小写时误把 Doris 目标表独有字段也放进了 JSON payload。

修复后，CDC 不再向 Doris 写入目标端维护字段，Doris 默认值和 `ON UPDATE` 语义恢复正常。该修复不会取消 mixed-case / upper-case 字段名兼容能力。

