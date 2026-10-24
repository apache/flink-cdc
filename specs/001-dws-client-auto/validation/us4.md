# US4 configuration, buffer, and observability validation

## Local result

Status: **PASS for local unit scope**.

- JDK 11 DWS clean module test: 61 tests, 0 failures, 0 errors, 0 skipped; Spotless and Checkstyle
  passed. Raw log: session artifact `dws-full-with-metrics.log`.
- Configuration tests cover defaults, explicit values, normalized aliases and conflicts, typed byte
  units/hierarchy, URL query preservation and second/millisecond conversion, DN distribution
  combinations, fixed native partitioning, and explicit rejection of unsupported options.
- Buffer tests cover the native `byte[]` undercount, PK/container overhead, single-record rejection
  before client submission, accumulated all/table/partition pre-flush, disabled timer with finite
  force protection, and counters clearing only after successful flush.
- Metrics tests distinguish accepted submissions from synchronous-flush-confirmed records, count
  only definite synchronous record failures, retain pending counts on failed flush, expose the first
  asynchronous failure without inventing a failed-record count, and verify the safe configuration
  summary excludes test URL/username/password/record sentinels.

`conservativeBufferedBytes` is connector accounting, not a native byte count or JVM heap gauge.
“Written” means native `flush()` returned; it is not checkpoint-transaction visibility. Native
retry/action/SQL-route metrics without a public runtime value remain unavailable and are not
synthesized.

## External result

Status: **BLOCKED**.

No authorized DWS endpoint or performance environment variables are available on this host.
Consequently the 30-minute limited-downstream run, heap/backpressure evidence, actual AUTO
UPSERT/COPY path observation, oversized-record target query, and three-run legacy/new sparse/batch
comparison have not executed. T051 and T052 remain incomplete. The frozen procedure is recorded in
`performance-protocol.md`; local unit evidence does not satisfy SC-005 or SC-006.
