# DWS AUTO performance and buffer protocol

This protocol is frozen before the external run. Results may explain a miss, but must not change
the workload or tolerance after observing results.

## Environment

- One TaskManager, two sink-writer slots, 2 GiB TaskManager JVM heap.
- One isolated DWS test schema with fixture-owned tables and no unrelated writers.
- Connector budgets per writer: all tables 128 MiB, table 64 MiB, partition 32 MiB; native
  partition min/max 1; write thread 1; AUTO mode unless a scenario explicitly says otherwise.
- Fixed JDBC connect/socket and statement timeouts, retry values, DWS version, connector JAR hash,
  Flink version, JVM version, CPU/memory limits, network placement, and source replay seed recorded
  before each run.
- Fixed seed event log targeting at least two tables. One narrow-row stream is approximately 1 KiB
  per row and one wide/binary stream approximately 64 KiB per row.

## Workloads

1. Downstream-limited run: constrain DWS to about 50% of the intended source rate for at least 30
   minutes. Record the actual backpressured source rate rather than assuming the offered rate was
   accepted.
2. Repeat with `enable-auto-flush=false`; finite force, checkpoint, and connector-budget flushes
   must remain observable.
3. Submit a binary record whose conservative estimate exceeds the partition budget. It must fail
   before native commit and must not appear in the target.
4. Submit individually legal binary records until accumulated table/all accounting crosses its
   budget. A synchronous flush/backpressure or documented controlled failure must occur.
5. Run sparse and sustained-batch workloads against the frozen legacy staging artifact and new AUTO
   artifact, three measured repetitions per route and workload after an identical warm-up.

## Measurements and pass criteria

- Independent per-key/per-field target reconciliation is mandatory before accepting performance
  numbers. Record deletes, primary-key changes, and duplicates/extra keys.
- Sample throughput, end-to-end p50/p95 latency, checkpoint duration, connector conservative bytes,
  native public buffer estimate when available, inflight actions, JVM used heap, post-GC retained
  heap, GC time, CPU, network, flush count/duration, backpressure, and observed SQL/action route.
- Native buffer estimate, connector conservative bytes, and JVM heap are separate series. An absent
  native metric is `UNAVAILABLE`, never zero or a synthesized value.
- No OOM. After the first five-minute warm-up, the median post-GC retained heap in the final ten
  minutes must not exceed the first ten-minute median by more than 128 MiB. Connector budget
  overshoot tolerance is one maximum legal record; it does not apply to whole-JVM heap.
- Observe backpressure or the configured controlled failure. A run that silently grows memory,
  hangs beyond fixed timeouts, or mutates an oversized record is a failure.
- Report all three comparison runs. Do not claim improvement from the best single result. Any
  throughput, latency, heap, or checkpoint regression is retained and explained for release review,
  including the staging-versus-at-least-once semantic difference.

## Safety and evidence

Credentials and business rows are excluded from logs. The fixture registers only its generated
table prefix and may clean only those tables after explicit test selection. Store the raw command,
artifact hashes, effective configuration without secrets, metrics export, DWS query evidence, and
reconciliation output with the final `us4.md` result.
