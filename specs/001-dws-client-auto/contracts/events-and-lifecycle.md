# Contract: 事件、生命周期、兼容与迁移

## C1 — opt-in 公共能力

拟新增 `DataSink.requiresPrimaryKeyUpdateSplit()`，默认 false，DWS true。composer→PartitioningTranslator→Regular/Distributed/Batch pre-partition 透传；保留旧构造器/方法重载并默认 false。不开启能力时事件数量、内容、分区及 schema/flush 广播完全不变。

仅主键实际变化的 UPDATE 被拆：先 UPDATE_BEFORE(before)，后 REPLACE(after)。同一个 callback 同步发出两半；相同 hash 也仍需比较实际 PK 后正确拆分，不能用 hash 相等假设主键相等。两半使用各自 before/after 按主键 hash，不在 writer 内发送跨键删除。

无主键、主键为空、before/after 缺失或 getter 与 schema 不一致时写入前失败。不能把未知主键更新当普通 UPDATE。

## C2 — typed 撤回与序列化

- `OperationType.UPDATE_BEFORE` 追加在 INSERT/UPDATE/REPLACE/DELETE 后，不重排已有枚举。
- `DataChangeEvent.updateBeforeEvent(tableId, before, meta)`；after=null；`opTypeString` 返回 -U；默认 hash 对 DELETE/UPDATE_BEFORE 取 before。
- DataChangeEventSerializer 的 copy/serialize/deserialize 增加撤回分支，布局与 before-only 事件一致；旧四种操作保持逐字节兼容，有 golden fixtures。
- 审计所有沿途 operation switch，特别是 distributed schema 派生/投影、type coercion、日志和测试 utilities。route/project 保留 op/meta，不能把撤回误当 INSERT。
- 不修改输入记录或 meta。object reuse 开启时派生事件/记录的所有权必须安全；跨输出和 channel 的深拷贝行为要有测试。
- 只在分区后链路出现新操作；普通 source/transform 及未 opt-in 的其他 sinks 不会收到它。不能将新 runtime 与旧事件 consumer 混部署。

## C3 — 顺序承诺

同一个源因果链 I(A)→U(A,B)→I(A) 在旧键 A 的 writer 观察为 I→撤回→I，新键 B 观察为 REPLACE。下游延迟、失败重放、checkpoint 前后和并行度 2/4 均需验证。两个键不要求原子可见，不能为此加入全局事务。

Flink 同键路由与有序输入是前提；多个独立来源对同目标键的冲突没有全局顺序保证。Distributed 的分区在 route/schema 演进前：目标合表要求主键域不重叠，且 schema coercion 不把不同源 hash 键映射为冲突目标键；部署检查/文档明确该边界，不能声称本次拆分解决全部多源覆盖。

每 writer 内 client 使用单表一个 native partition，避免按分布列二次分区重排。DELETE 与 write 必须通过同一 client 的同一表顺序链路，不能再建独立 delete JDBC 通道。

## C4 — writer 生命周期

| 入口 | 必须行为 | 失败行为 |
| --- | --- | --- |
| create/restore | 校验 protocol marker，构造 client/首错回调/timer | 初始化失败释放已创建资源，不暴露凭据 |
| write(CreateTableEvent) | 校验 PK、建立本地 getter、加载匹配目标 metadata | 无效 schema 不写业务数据 |
| write(data) | 检查首错；完整 after write，PK-only delete；撤回无条件，独立 delete 遵循开关；setObject 后 commit 前做保守字节预检，必要时先 flush | 超限单条 commit 不调用；不吞异常，不打印完整 Event |
| flush(false) | client.flush 等待之前接受的全部工作；检查首错 | 抛出任务失败，不确认 checkpoint/schema barrier |
| flush(true) | 同上，保证 bounded input 尾部写出 | 不以 close 替代成功判断 |
| snapshotState | 在成功 flush 的边界输出 protocol marker，防御性再检查首错 | 无新成功 state |
| timer | 1s 周期向 mailbox 投递首错检查，正常模式不另做一套批量调度 | 空闲时也触发任务失败 |
| close | 停 timer、检查/保留首错、释放 client；正常路径显式完成 flush | close 可能继续写出，不是 abort；根因保留，关闭异常 suppressed |

客户端 onError 回调必须重新抛出，不可仅记录后 return。不得在 native 工作线程直接执行 Flink operator 生命周期。客户端 close 没有 abort 保证；故障后仍可能可见更多数据，依赖重放最终一致，绝不将 close 计为 checkpoint 成功。

网络/SQL/重试有限配置和 Task cancellation 实测是共同保障。若底层发送阻塞不能结束，发布失败；不预建无界线程池，也不把 Future 超时当数据库连接已取消。

## C5 — schema barrier 与缓存

1. 既有 runtime 广播 FlushEvent，所有 writer 成功完成 native flush 后才回报。
2. coordinator 的 MetadataApplier 执行 DDL；失败停止推进。
3. writer 收到相应 schema 通知，清 native 表缓存并按正确 case-sensitive config reload，更新本地 getter/PK 索引，然后接受新行。
4. 禁用 `updateTableSchema` 的不带配置刷新路径，使用 removeTableSchema + getTableSchema；确认 native collector 在新 schema 下切换，不保留旧 Record/TableSchema 引用。
5. CREATE/ADD_COLUMN/ALTER_COLUMN_TYPE/DROP_COLUMN/RENAME_COLUMN/TRUNCATE/DROP 是现有支持集合。DROP 不重新加载已删表；TRUNCATE 清旧工作并保持新结构有效；不新增 RENAME_TABLE。
6. 恢复由已有 SchemaRegistry 补发 Create，writer 不持久复制 schema。DDL 成功但 checkpoint 未完成后的恢复/重放要实测，不能假设事件和数据库 DDL 同事务。

未通过大小写 DDL 后首次 AUTO/COPY_MERGE 写入回归，不得认定大小写问题整体解决。

## C6 — 状态兼容与首次迁移

新 writer state serializer v2 只接受固定新语义 marker；旧 v1 jobId 或未知版本显式报不兼容，且发布工具默认禁止允许未恢复状态。删除 committer 不意味着可丢旧 pending committable。不能依靠用户配置不同 UID 来规避迁移检查；上线指南要求来源版本和 state 清单核对。

首版支持流程：

1. 记录旧制品、source 日志保留期限、恢复点、目标及遗留资源归属，确认可重同步。
2. 使用旧版本完成在途提交并停止；检查 pending/staging，不删除尚未证明已提交的资源。无法确认时保留旧任务/证据并停止迁移。
3. 新任务不直接加载旧 savepoint；向单独空目标启动源支持的一致快照+增量同步。若要复用目标，清空/重建必须另外授权且先备份，不能作为自动步骤。
4. 追平后逐键/字段核对，确认删除、变键、数量及 schema；取得切换批准后切换消费端。
5. 旧目标、状态和必要日志保留至回退窗口结束，遗留清理单独按已确认归属执行。

回退不能直接加载新 state 到旧版本。使用保留的旧任务恢复边界前必须证明日志完整、旧提交资源仍可用且对当前目标重放能收敛；不能证明则独立空目标重同步。对已有脏目标只重跑快照会遗留源端已删键，不列为支持路径。

## C7 — 验收关联

FR-001～003/008 → C1～C3；FR-004～007 → C3/C4/C6；FR-009～011/017 → C5 与类型回归；FR-012～014 → 配置合同及 C4；FR-015/016 → C6；FR-018 → 两代 runtime 的所有合同测试。实际执行矩阵见 [quickstart](../quickstart.md)。
