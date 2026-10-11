# US1 validation: native AUTO and primary-key update ordering

Date: 2026-10-10

## Local contract and harness results

| Scope | Result | Evidence |
| --- | --- | --- |
| Legacy event bytes and `UPDATE_BEFORE` serializer | PASS | JDK 11 reactor; `DataChangeEventSerializerTest`: 20 tests, 0 failures. Log: session `t011-green.log`. The legacy operation tags remain 0/1/2/3 and `UPDATE_BEFORE` is appended as 4. |
| Three pre-partition topologies | PASS | JDK 11 reactor; `PrePartitionOperatorTest`: 10 tests, 0 failures. Log: session `t012-green-attempt2.log`. Covers changed/unchanged keys, hash collision, binary deep equality, schema getter refresh, invalid input and default opt-out. |
| Composer capability propagation | PASS | JDK 11 reactor; `FlinkPipelineComposerTest`: 7 tests, 0 failures. Log: session `t020-composer.log`. Non-opt-in sinks remain false; DWS opts in. |
| Native writer event mapping | PASS | `DwsWriterEventTest`: 4 tests, 0 failures. Log: session `t013-green-attempt3.log`. Full-row writes, PK-only deletes, unconditional retractions, delete option, unsplit update rejection and no-PK rejection are covered. |
| DWS connector local regression | PASS | 19 selected tests, 0 failures. Log: session `t021-t022-green2.log`. The official client is lazy, one per writer, and uses the connector-owned fixed configuration. |
| Legacy staging protocol removal | PASS | Production `DwsCommitter`, `DwsCommittable`, serializer and staging SQL helper are removed. The decoded legacy committable fixture remains frozen at SHA-256 `a0be294ddeff2aa535c9307333fac7cd0af5536d6de8403fdcbe423861a872f6`. |

The local tests use independent expected maps and inspect emitted events/native calls rather than
only row counts. Existing sinks retain the original single-event path unless they explicitly opt in.

## External DWS result

Status: **BLOCKED**.

The required `DWS_TEST_JDBC_URL`, `DWS_TEST_USERNAME`, `DWS_TEST_PASSWORD`, and
`DWS_TEST_SCHEMA` settings were not present in this execution environment. Consequently no claim is
made for the real AUTO UPSERT/COPY_UPSERT branch, parallelism 2/4, delayed old-key writer scenario,
or final row/field equality on a DWS cluster. `DwsNativePipelineITCase` and T023 remain incomplete;
the unit/harness results above do not substitute for that database acceptance gate.

## Remaining limits

- Foundation T010 remains blocked by the unavailable JDK 17 installation and the pre-existing
  Flink 2 reactor dependency-copy ordering failure documented in `foundation.md`.
- US1 is locally reviewable but is not a production-release claim. Fault recovery, schema reload,
  migration and budget/performance gates belong to later phases.
