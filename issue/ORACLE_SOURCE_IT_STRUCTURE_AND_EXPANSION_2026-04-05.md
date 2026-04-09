# Oracle Source IT Structure And Expansion 2026-04-05

## Goal

Continue the Oracle container regression expansion from the already validated pipeline-side batch into the source connector side, while also organizing the source IT suite into a clear execution structure.

## Oracle Source IT Suite Structure

### OracleSourceITCase

Current test count: 18

Logical groups:

1. Basic read-path validation
   - testReadSingleTableWithSingleParallelism
   - testReadSingleTableWithMultipleParallelism
   - testReadSingleTableWithSingleParallelismAndSkipBackfill

2. Failover validation
   - testTaskManagerFailoverInSnapshotPhase
   - testTaskManagerFailoverInRedoLogPhase
   - testJobManagerFailoverInSnapshotPhase
   - testJobManagerFailoverInRedoLogPhase
   - testTaskManagerFailoverSingleParallelism
   - testJobManagerFailoverSingleParallelism

3. Snapshot and backfill semantics
   - testSnapshotOnlyModeWithDMLPostHighWaterMark
   - testSnapshotOnlyModeWithDMLPreHighWaterMark
   - testEnableBackfillWithDMLPreHighWaterMark
   - testEnableBackfillWithDMLPostLowWaterMark
   - testEnableBackfillWithDMLPostHighWaterMark
   - testSkipBackfillWithDMLPreHighWaterMark
   - testSkipBackfillWithDMLPostLowWaterMark

4. Special table coverage
   - testTableWithChunkColumnOfNoPrimaryKey
   - testTableWithSpecificOffset

### NewlyAddedTableITCase

Current test count: 20

Logical groups:

1. Newly added table without failover
   - testNewlyAddedTableForExistsPipelineOnce
   - testNewlyAddedTableForExistsPipelineOnceWithAheadRedoLog
   - testNewlyAddedTableForExistsPipelineTwice
   - testNewlyAddedTableForExistsPipelineTwiceWithAheadRedoLog
   - testNewlyAddedTableForExistsPipelineTwiceWithAheadRedoLogAndAutoCloseReader
   - testNewlyAddedTableForExistsPipelineThrice
   - testNewlyAddedTableForExistsPipelineThriceWithAheadRedoLog
   - testNewlyAddedTableForExistsPipelineSingleParallelism
   - testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadRedoLog

2. Newly added table with failover
   - testJobManagerFailoverForNewlyAddedTable
   - testJobManagerFailoverForNewlyAddedTableWithAheadRedoLog
   - testTaskManagerFailoverForNewlyAddedTable
   - testTaskManagerFailoverForNewlyAddedTableWithAheadRedoLog

3. Remove-table flows
   - testJobManagerFailoverForRemoveTableSingleParallelism
   - testJobManagerFailoverForRemoveTable
   - testTaskManagerFailoverForRemoveTableSingleParallelism
   - testTaskManagerFailoverForRemoveTable
   - testRemoveTableSingleParallelism
   - testRemoveTable

4. Remove/add churn flow
   - testRemoveAndAddTablesOneByOne

## Current Expansion Result

### Stable current-turn verification

Verified with fresh surefire report update:

- OracleSourceITCase#testReadSingleTableWithSingleParallelism
- Result: Tests run 1, Failures 0, Errors 0, Skipped 0
- Report elapsed time: 36.818 s

This gives one clean, current-turn source-side baseline pass for the basic read path.

### Historical point results already present in module reports

Existing source-module reports also show prior targeted passes for:

- OracleSourceITCase#testTaskManagerFailoverInSnapshotPhase
- NewlyAddedTableITCase#testJobManagerFailoverForNewlyAddedTable

These are useful as historical evidence, but they are not the newly expanded batch from this round.

## Current Blocker

Heavier source-side targeted reruns are not yet yielding reliable fresh surefire conclusions in this environment.

Observed behavior:

- OracleSourceITCase#testEnableBackfillWithDMLPreHighWaterMark did not produce a fresh method-specific report conclusion.
- NewlyAddedTableITCase#testNewlyAddedTableForExistsPipelineOnce also did not refresh the class report with the new method identity.
- A fresh dumpstream file appeared during one source rerun, indicating surefire child-process instability rather than a normal assertion failure.
- Retrying with `-Dflink.forkCount=0` confirmed the no-fork mode was applied, but still did not yield a fresh heavier-source report.

Most relevant artifact:

- target/surefire-reports/2026-04-05T02-51-38_615-jvmRun1.dumpstream

Interpretation:

- The current blocker is process/report stability for heavier source IT reruns.
- It is not yet valid to classify those heavier source scenarios as pass or fail based on the current evidence.

## Recommended Expansion Order

Use this order for the next source-side expansion attempts:

1. OracleSourceITCase basic read path
   - testReadSingleTableWithMultipleParallelism
   - testReadSingleTableWithSingleParallelismAndSkipBackfill

2. OracleSourceITCase semantic path
   - testSnapshotOnlyModeWithDMLPostHighWaterMark
   - testEnableBackfillWithDMLPreHighWaterMark
   - testSkipBackfillWithDMLPreHighWaterMark

3. NewlyAddedTableITCase lightweight path
   - testNewlyAddedTableForExistsPipelineOnce
   - testNewlyAddedTableForExistsPipelineTwice
   - testRemoveTableSingleParallelism

4. Failover last
   - OracleSourceITCase failover group
   - NewlyAddedTableITCase failover group

## Operational Notes

- For source-side Oracle IT, prefer one-method-per-invocation when collecting trustworthy evidence.
- Always verify the exact method identity from the surefire XML report, not just the class-level txt summary.
- If a new dumpstream appears but the class report still points to an older method, treat the run as inconclusive rather than passed.