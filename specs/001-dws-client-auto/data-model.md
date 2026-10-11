# Data Model: DWS AUTO 写入

运行期模型与事件契约，不新增业务库表。见 [plan](plan.md) 和 [事件契约](contracts/events-and-lifecycle.md)。

## Resolved configuration

连接三元组、zoneId、schema、caseSensitive、enableDelete、DDL distribution 参数，以及白名单原生配置。显式值与默认来源区分，规范化后同义值才允许共存。配置为可序列化快照，client/连接/回调不是传输字段，日志不输出密码或完整 URL。

无需通用配置框架，可复用 Configuration 的防御性快照与基础类型。

## PartitionContext（每表、临时缓存）

字段：TableId、当前 Schema、PK field getters、既有 hash function、split capability。

有效主键和非空 before/after 是变键 UPDATE 的前置条件；比较原始值、binary 深等值，不以 hash 判断变更。Regular 复用 evolved schema registry，Distributed 复用 schemaMap，Batch 复用 Create schema；schema 变化时 hash 与 getters 一起刷新。无单独持久状态，不修改输入事件或 meta。

## 归一化事件

| 输入 | 分区前输出 | writer 行为 |
| --- | --- | --- |
| INSERT/REPLACE | 原事件 | 完整 after → native write |
| 非变键 UPDATE | 原事件 | 完整 after → native write |
| 变键 UPDATE | UPDATE_BEFORE(before)，REPLACE(after) | 旧键 native delete，新键 native write |
| DELETE | 原事件 | enableDelete=true 才 native delete |

UPDATE_BEFORE 只承载 before，after=null，保留 tableId/meta；它是 enum 尾部新增的 typed 操作，不是可配置 metadata 标志。撤回不受独立 DELETE 开关影响。DWS writer 若收到尚未拆分的变键 UPDATE 则失败，防止绕过正确分区。

两半没有额外事务 ID/持久配对状态，不承诺原子可见；同一次 callback 发出，checkpoint 不切开回调，恢复后仍按同规则重放。

## DwsWriter runtime

一个 DwsClient、Map<TableId, TableInfo>、首个异步异常引用、mailbox 健康检查 timer、关闭标记、计数/耗时；另保存 writer 总计与分表的保守待确认字节数字，不存额外记录队列。

TableInfo 保存规范化目标名、CDC Schema、getter、PK 索引、已确认的目标 metadata；DDL/恢复时重新构造。操作必须使用当前 schema，不长期持有失效 TableSchema。

```text
NEW → RUNNING → FLUSHING → RUNNING
         │          │
         ├──────────┴──→ FAILED → CLOSED
         └─ end/close → FLUSHING → CLOSED
```

正常 API 由任务/mailbox 线程调用，native 线程只更新首错/指标。FAILED 不再推进 checkpoint；close 保留根因，关闭异常作为 suppressed，不覆盖写入失败。

## 原生缓冲与完成边界

native 拥有记录/分表缓存/action，connector 不保存第二份 queue。每 writer 显式 all/table/partition 预算，partition 数固定 1。native 不计 byte[]，connector 在 commit 前补计 binary/PK/容器开销；总计和分表数字表示自最近成功显式 flush 以来已接受工作，异步成功不扣减，因而保守。

下一条将使总预算或表预算（单 partition 时取 table/partition 较小值）超限时，先同步 client.flush，成功才清零并提交；单条本身超限直接拒绝，commit 不被调用。估算不是 heap 硬限，转换/在途开销仍由压力测试认证。schema/end/checkpoint 的成功显式 flush 也可清零；任何失败不归零后继续写。

commit 仅表示异步进入 native 流程；flush 成功且无首错才形成可确认边界。失败时运行缓存不可当作完成，由未成功 checkpoint 的上游重放恢复。

## WriterProtocolState

- serializer version=2，payload 为固定 marker `dws-client-auto-at-least-once-v1`。
- 不保存 schema、行、连接、jobId 或 committable。
- snapshot 前 flush；restore 拒绝已分配的旧 v1/未知 payload；rescale 可合并多个相同 marker，逐一验证。
- 新任务和合法 scale-up 新分配 writer 都可能拿到空集合，不能据此单独判定来源。空分配结合 restored-checkpoint 上下文、全任务制品/state 来源清单和未映射 committer 检查处理；协议 marker 不是全局来源证明。移除 committer 的旧任务不能用允许丢状态开关伪装新建。
- 新事件不能由旧 common/runtime 读取；部署与回退按整套制品处理。

## MigrationEvidence（操作记录）

字段：旧/新制品摘要、旧 state URI、最终提交核对、源快照/日志保留边界、独立空目标、行级比对、切换批准、回退条件、遗留资源所有者。

状态：PRECHECK → OLD_DRAINED → NEW_BOOTSTRAPPED → CAUGHT_UP → VERIFIED → SWITCHED。任一步失败保留证据与旧资源，不自动清理；没有“忽略旧 state 后直接成功”的状态。
