# DWS native-client migration preflight

The native-client writer protocol is intentionally incompatible with the removed staging/committer protocol. A v1 writer-state payload, an unknown serializer version, and a pending legacy committable must fail restoration; they must never be treated as an empty v2 state.

Before a migration, record and review all of the following without exposing credentials or business rows:

- deployed connector JAR SHA-256 and whether it contains the former `DwsCommitter`, `DwsCommittable`, or `DwsCommittableSerializer` classes;
- savepoint/checkpoint location, operator UID mapping, serializer version, and whether pending committable state exists;
- ownership of legacy staging tables and any cleanup process attached to the old job;
- replayable source offset/log retention covering the complete snapshot plus incremental cutover window;
- a new, empty, migration-owned target and an independent per-key/per-field reconciliation query;
- rollback artifact, old target, retained source position, and the people authorized to switch consumers or remove resources.

Do not use `allowNonRestoredState`, change the sink UID, or delete state to make a legacy savepoint appear compatible. An empty v2 marker allocation is legal during rescale, but a runtime cannot infer from one empty allocation that every old operator UID and pending committable was safely mapped. That remains a deployment-level preflight check.

The supported first migration is drain the old job, preserve its target and state, run a full snapshot plus incremental replay into an empty target, reconcile it, then perform an explicitly authorized consumer switch. Cleanup and production switching are outside this implementation task.
