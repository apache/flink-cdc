# DWS AUTO release readiness

Status: **NOT READY — external DWS, recovery, performance, migration, and Flink 2 gates remain
blocked.** This document is an evidence map, not release authorization.

## Functional requirements

| Requirement | Status | Evidence / gap |
| --- | --- | --- |
| FR-001–003 | PARTIAL | AUTO configuration, native writer mapping, PK/no-PK checks, typed retraction, serializer and three partition topologies pass locally (`foundation.md`, `us1.md`); actual AUTO SQL route and real parallel DWS result blocked. |
| FR-004–008 | PARTIAL | At-least-once contract, flush/snapshot/close, first async failure and v2 marker tests pass (`us2.md`); five real fault windows, rescale, and final DWS convergence blocked. |
| FR-009–011 | PARTIAL | Shared identifier rules, schema-cache reload atomicity, restore Create ordering, and UPDATE_BEFORE coercion pass locally; four real case/mode groups and seven DDL success/failure paths blocked. |
| FR-012–014 | PARTIAL | Complete configuration, conservative budgets and safe metrics pass locally (`us4.md`); 30-minute heap/backpressure, retry/action route, and performance comparison blocked. |
| FR-015–016 | PARTIAL | Legacy state rejection, preflight, migration and rollback guide pass locally (`migration-preflight.md`); authorized in-flight migration/rollback rehearsal blocked. |
| FR-017 | PARTIAL | Existing converter coverage and explicit no-silent-NUL configuration pass locally; real DWS type/NUL behavior blocked. |
| FR-018 | BLOCKED | Flink 1 local matrix passes; Flink 1 cluster and Flink 2/JDK 17 matrix do not. See `runtime-matrix.md`. |

## Success criteria

| Criterion | Status | Evidence / release blocker |
| --- | --- | --- |
| SC-001 | BLOCKED | Local event/partition oracle passes; real normal and recovered per-key/per-field comparison missing. |
| SC-002 | BLOCKED | Local mixed-case cache/DDL behavior passes; four real case-sensitive × mode combinations missing. |
| SC-003 | BLOCKED | Local lifecycle failures pass; five real fault-window recoveries missing. |
| SC-004 | PASS (local contract) | Factory/default/buffer/metrics tests and bilingual configuration documentation; 61-test DWS gate. |
| SC-005 | BLOCKED | Protocol frozen; 30-minute limited-downstream heap/backpressure run missing. |
| SC-006 | BLOCKED | Three sparse/batch repetitions for legacy and AUTO missing. |
| SC-007 | BLOCKED | State rejection and operator guide exist; in-flight migration and rollback rehearsals missing. |
| SC-008 | BLOCKED | Local schema/runtime checks partially pass; real DDL matrix and both cluster runtimes missing. |

## Required next execution

Provide an authorized isolated DWS endpoint, the four `DWS_TEST_*` settings, controllable failure
injection/proxy, frozen performance resources, the verified legacy artifact, a real JDK 17, and
Flink 1/2 test clusters. Run T023/T026/T030/T031/T037/T041/T042/T051/T052/T056/T057, retain raw
evidence, then update this table. Consumer switching, deleting legacy resources, commit, push, and
release are outside the current authorization.
