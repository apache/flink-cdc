# Runtime and test matrix

Validation date: 2026-10-11. Branch: `FLINK-39327`.

| Runtime / scope | Result | Executed evidence |
| --- | --- | --- |
| Flink 1.20.3, JDK 11, DWS module clean test | PASS | 61 tests, 0 failures/errors/skips; `dws-full-with-metrics.log` |
| Flink 1.20.3, JDK 11, clean reactor common | PASS | 79 tests, 0 failures/errors/skips; `t056-flink1-core-reactor.log` |
| Flink 1.20.3, JDK 11, clean reactor runtime | PASS | 882 tests, 0 failures/errors, 1 skipped; includes `PrePartitionOperatorTest` 10, `DataSinkOperatorWithSchemaEvolveTest` 5, and `SchemaDerivatorTest` 9 |
| Flink 1.20.3, JDK 11, clean reactor values dependency | PASS | 5 tests, 0 failures/errors/skips |
| Flink 1.20.3, JDK 11, clean reactor composer | PASS | 18 tests, 0 failures/errors/skips; includes `FlinkPipelineComposerTest` 7 |
| Final shaded JAR static load boundary | PASS | Factory/JDBC SPI and official client classes present; old protocol classes absent; see `artifact.md` |
| Flink 1 small-cluster JAR load | BLOCKED | No authorized/available test cluster in this execution environment |
| Flink 2.2.0 clean reactor and cluster load | BLOCKED | No real JDK 17; existing `copy-flink2-extra-libs` reactor ordering fails with MDEP-187 before DWS; see `foundation.md` |

The first attempt without `-am` failed because locally installed Flink 2 snapshot dependencies were
selected for Flink 1 runtime sources. The accepted run used `-am clean test`, rebuilt the compatible
reactor, and executed non-zero test counts shown above. This invocation error is retained in
`t056-flink1-core.log` and is not counted as a product-test failure.

T056 remains incomplete because its contract requires both generations and real small-cluster JAR
loading. The table deliberately records BLOCKED rather than treating the successful Flink 1 matrix
as full compatibility evidence.
