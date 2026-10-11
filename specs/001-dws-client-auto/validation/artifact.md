# DWS connector artifact validation

Validation date: 2026-10-11. Branch: `FLINK-39327`.

## Result

The JDK 11 module package completed successfully after the local unit gate. The generated artifact
is `flink-cdc-pipeline-connector-dws-3.7-SNAPSHOT.jar`, with SHA-256
`8c18c58d7bc9760d36e2336e311ea8eef24d3a63099fd778b65c0351ef6b42ce` for this working-tree
build. The hash is evidence for this build only and will change with subsequent edits.

The package log records the exact shaded dependencies:

| Component | Version | Packaging result |
| --- | --- | --- |
| `com.huaweicloud.dws:dws-client` | 2.1.0.6 | included |
| `com.huaweicloud.dws:huaweicloud-dws-jdbc` | 8.6.1-200 | included |
| `io.dropwizard.metrics:metrics-core` | 4.2.25 | included and relocated |
| `com.alibaba:druid` | 1.2.23 | included and relocated |
| Flink APIs | 1.20.3 profile | provided, not shaded |

Raw build evidence: session artifact `t055-package-final.log`.

## SPI and class boundary

- `META-INF/services/org.apache.flink.cdc.common.factories.Factory` is present in the final JAR and
  contains `org.apache.flink.cdc.connectors.dws.factory.DwsDataSinkFactory`.
- The shade service transformer preserves the JDBC driver service, and the final JAR contains the
  Huawei driver and official `DwsClient` classes.
- The final JAR contains `DwsSink`, `DwsWriter`, `DwsWriterMetrics`, the v2 marker state/serializer,
  and the official-client facade.
- The final JAR does not contain `DwsCommitter`, `DwsCommittable`,
  `DwsCommittableSerializer`, `DwsSinkFunction`, or `DwsSqlUtils`.
- Remaining source mentions of “staging” are compatibility-constructor documentation and the
  deliberate legacy-state rejection message, not an executable staging protocol.

## NOTICE boundary

The module already shaded Huawei DWS, Dropwizard Metrics, and Druid artifacts before this change;
the upgrade replaces the former DWS Flink adapter with the official client and JDBC artifact while
retaining the same shade categories. The inspected dependency JARs contain no `META-INF/NOTICE*`
resource to merge, and the generated artifact retains the repository-generated Apache NOTICE and
LICENSE. No speculative third-party notice text was added. Release review must still use the
project's normal dependency-license audit; Maven metadata for the two Huawei artifacts labels the
license only as “Huawei Cloud”, which this implementation does not reinterpret.

## Limits

This proves local packaging and static class/resource composition. Final JAR loading on real Flink
1 and Flink 2 clusters remains part of T056; the Flink 2/JDK 17 gate is still blocked as documented
in `foundation.md`.
