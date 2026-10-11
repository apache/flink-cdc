# Foundation validation

Validation date: 2026-10-10. Branch: `FLINK-39327`. Baseline commit: `3e88f65854cf3f96276456c58698fd09532bf0a0`.

## Result

Foundation implementation tasks T005–T009 are complete. T010 remains **BLOCKED** because this host has JDK 11 and JDK 21, but no JDK 17 required by the Flink 2 validation contract. The isolated Flink 2 module probe is useful compilation evidence but is not accepted as the required second runtime gate.

## Dependency and removal evidence

- Direct client: `com.huaweicloud.dws:dws-client:2.1.0.6`.
- JDBC: `com.huaweicloud.dws:huaweicloud-dws-jdbc:8.6.1-200`.
- Metrics: `io.dropwizard.metrics:metrics-core:4.2.25`.
- Druid: `com.alibaba:druid:1.2.23`.
- The module dependency tree contains no `dws-connector-flink` and source search contains no `DwsConnectionOptions`.
- `DwsSinkFunction` and its test were removed. The existing SinkV2 remains the only production engine until US1 switches its writer implementation.
- The native configuration fixes AUTO, one CN/DYNAMIC native partition, finite flush/retry/timeouts, 128/64/32 MiB typed budgets, NUL replacement off, and compatible-illegal-chars off. UPSERT force flush and disabled-auto-flush finite protection are covered by tests.

The configured Maven mirror on this host returned HTTP 403 for the new artifacts. Verification used a session-local settings file that maps the same mirror id to Maven Central; no user or repository Maven configuration was changed.

## Flink 1 gate

Environment: Microsoft OpenJDK 11.0.32, Flink 1.20.3 profile.

Command shape: clean reactor build through the DWS module with T005/T006 selected, RAT and Spotless skipped only for the already-recorded repository/toolchain baseline blockers.

Result: **PASS**. `DwsClientDefaultsTest` ran 3 tests and `DwsRecordConverterTest` ran 2 tests; total 5, zero failures/errors/skips. Reactor build succeeded.

Raw log: `/Users/guandata/Documents/work_space/codex-agent-helper/.local-state/sessions/users-guandata-documents-...-39/foundation-flink1-jdk11.log`.

## Flink 2 probes and blocker

- Effective dependency probe resolves `flink-streaming-java:2.2.0` together with the exact DWS dependency set above.
- An isolated DWS module clean test under `-Pflink2` passed the same 5 tests on JDK 11 after locally installing reactor prerequisites. This is only a profile/API probe because JDK 11 is not the contracted Flink 2 environment.
- The same isolated module probe also passed on JDK 21 with Java 17 target compilation. JDK 21 is not substituted for the missing JDK 17 because the repository baseline already showed Scala compiler-bridge incompatibility on a full JDK 21 reactor.
- The clean Flink 2 reactor probe stopped before the DWS module in `flink-cdc-connect`: its `copy-flink2-extra-libs` execution tried to copy the reactor `flink-cdc-flink2-compat` artifact during `process-test-resources` before that artifact had been packaged (`MDEP-187`). This is a separate pre-existing reactor-profile ordering blocker.

Raw logs:

- `/Users/guandata/Documents/work_space/codex-agent-helper/.local-state/sessions/users-guandata-documents-...-39/foundation-flink2-jdk11-probe.log`
- `/Users/guandata/Documents/work_space/codex-agent-helper/.local-state/sessions/users-guandata-documents-...-39/foundation-flink2-module-jdk11.log`
- `/Users/guandata/Documents/work_space/codex-agent-helper/.local-state/sessions/users-guandata-documents-...-39/foundation-flink2-module-jdk21-probe.log`
- `/Users/guandata/Documents/work_space/codex-agent-helper/.local-state/sessions/users-guandata-documents-...-39/foundation-flink2-dependency-tree.log`

T010 must remain unchecked until a real JDK 17 environment runs the clean Flink 2 reactor gate and the reactor copy-order issue is either corrected or the documented build sequence is adjusted without weakening test selection.
