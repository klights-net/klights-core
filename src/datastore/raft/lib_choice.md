# Raft Library Choice

T15 uses a narrow in-process N=1 Raft state-machine wrapper as the Phase 2 apply-path foundation, so single-leader writes already go through a propose-and-commit boundary before touching the datastore. Phase 3 multi-voter work should use `openraft` for the real transport, durable log, learner, snapshot, and online membership implementation because its state-machine API maps cleanly to the existing `DatastoreBackend` trait and avoids a bespoke consensus implementation.
