# US2 failure and recovery validation

## Local lifecycle and state checks

- Result: PASS (local unit scope)
- Runtime: JDK 11, DWS connector module, direct `dws-client:2.1.0.6`
- Tests: 39 run, 0 failures, 0 errors, 0 skipped
- Covered boundaries: first asynchronous failure, idle mailbox propagation, failure before and after flush, checkpoint marker suppression on failed flush, end-of-input flush, idempotent close with suppressed close failure, v2 fixed marker serialization, explicit v1 rejection, empty and multiple marker restore allocations.
- Raw log: session artifact `dws-full-us2.log`

The writer state is a protocol marker only. It is not evidence that every parallel writer completed a checkpoint, and the connector continues to provide at-least-once rather than checkpoint-transactional visibility.

## External recovery checks

- Result: BLOCKED
- Missing prerequisite: an authorized isolated DWS endpoint and the five controllable failure windows required by T026/T030.
- Not claimed: rescale 2→4→2 convergence, connection/socket black-hole cancellation, lock-wait termination, old execution-instance cleanup, or final row-level convergence against a real DWS service.

T026, T030, and T031 remain incomplete until those external checks run successfully.
