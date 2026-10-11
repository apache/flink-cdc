# DWS AUTO upgrade baseline

## Source and workspace

- Captured: 2026-10-10 (Asia/Shanghai)
- Branch: `FLINK-39327`
- Commit: `3e88f65854cf3f96276456c58698fd09532bf0a0`
- Remote: `origin/FLINK-39327`
- The only untracked inputs at capture time were this feature's `.specify/`, `AGENTS.md`,
  `specs/`, and the implementation-required `.dockerignore`. No tracked source change was
  reset or hidden.
- Local `master` was 111 commits ahead of the merge base and the feature branch had 12
  commits not in `master`; implementation must not describe this baseline as rebased onto the
  current local `master`.

## Toolchain

- Maven: 3.9.12
- JDK 11: Microsoft OpenJDK 11.0.32
- Default JDK: Microsoft OpenJDK 21.0.12
- A real JDK 17 installation was not discoverable: `/usr/libexec/java_home -v 17` selected the
  installed JDK 21. The Flink 2/JDK 17 validation gate is therefore `BLOCKED` until JDK 17 is
  installed or an approved path is supplied.
- Project versions at the baseline: `revision=3.7-SNAPSHOT`, Flink 1.20.3 by default, and
  Flink 2.2.0 under the `flink2` profile.

## Legacy artifacts

The following files existed before the first clean build and were treated as local legacy build
evidence. They were removed by the subsequent Maven `clean`, so their hashes are provenance
records, not a durable artifact locator.

| Artifact | SHA-256 |
| --- | --- |
| `target/flink-cdc-pipeline-connector-dws-3.8-SNAPSHOT.jar` | `ffcfd4f82895582eec0a419e7cc02192b1d374772681420861986e97b6c2439e` |
| `target/original-flink-cdc-pipeline-connector-dws-3.8-SNAPSHOT.jar` | `3441258658faeb1dbce41fbf0d114419e4b208da23f26784ae0cab4218a0f0a6` |

The artifact version (`3.8-SNAPSHOT`) differs from the checked-out POM (`3.7-SNAPSHOT`), so these
files cannot be used for migration execution without an additional source/build provenance check.

### Legacy serializer fixtures

The checked-out v1 serializers at commit `3e88f658` generated deterministic, credential-free
fixtures before the staging classes are removed:

| Fixture | Source value | Decoded-byte SHA-256 |
| --- | --- | --- |
| `legacy-writer-state-v1.hex` | writer job id `legacy-job-FLINK-39327` | `9c37fa4e9884d2a9b4cf8008efc34339c4474903d76d23bdeac2a644217a875b` |
| `legacy-committable-v1.hex` | checkpoint 42, subtask 2, `ods.orders`, staging table `flink_cdc_stage_legacy_42_2_0`, columns `id/payload/updated_at`, PK `id` | `a0be294ddeff2aa535c9307333fac7cd0af5536d6de8403fdcbe423861a872f6` |

`DwsLegacySerializerFixtureTest` reserializes the baseline objects and compares the exact bytes.
The committable represents an old task stopped with checkpoint 42 pending against its owned staging
table; migration tests must reject silent state loss and separately prove how that pending table is
accounted for.

## Build and dependency observations

1. Building the DWS module without `-am` fails because the reactor-local 3.7-SNAPSHOT composer,
   common, runtime, test-util, and compat artifacts are unavailable. This is an invocation issue,
   not a DWS test result.
2. The first `-am` run without `clean` compiled Flink 1 code against stale Flink 2 compat classes.
   The resulting serializer/operator errors confirm that profile switches require `clean`.
3. A clean JDK 11 reactor build passes through common/runtime but fails at the DWS module's
   Spotless check because its google-java-format runtime references `Modifier.SEALED`, which does
   not exist on JDK 11.
4. A clean JDK 21 reactor build fails earlier in the Scala 2.12.16 compiler bridge with
   `bad constant pool index`. This matches the documented reason not to use JDK 21 as the
   substitute for the missing JDK 17.
5. The normal RAT gate currently reports six unapproved files: five planning/test inputs plus the
   pre-existing DWS `log4j2-test.properties`. RAT was skipped only to expose the deeper baseline
   toolchain failures; this is not a release-ready build result.

Raw logs are retained in the active implementation session:

- `dws-baseline-dependency-tree.log`
- `dws-baseline-package.log`
- `dws-baseline-reactor-install.log`
- `dws-baseline-reactor-install-rat-skip.log`
- `dws-baseline-clean-reactor-install.log`
- `dws-baseline-clean-reactor-install-jdk21.log`

## Baseline status

`T001 COMPLETE`: the baseline is reproducibly identified and its blockers are recorded. No DWS
behavioral, database, Flink 2, or migration test is claimed as passing.
