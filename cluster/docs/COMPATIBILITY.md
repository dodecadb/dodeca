# Compatibility

Each DozenDB build is pinned to exactly one Neo4j and DozerDB version. A node
refuses to join a cluster whose members run a different Neo4j version.

## Pinned versions

| Component | Version | Notes |
|---|---|---|
| Neo4j | 5.26.27 (Community) | The Neo4j version inside the `graphstack/dozerdb:5.26.27.0` image. |
| DozerDB source | `dozerdb-core` branch `neo4j-5.26.8`, commit `f76393e` | Built against Neo4j 5.26.27, see `UPSTREAM_TOUCHPOINTS.md`. |
| DozerDB image | `graphstack/dozerdb:5.26.27.0` | Test baseline and installation target. The DozenDB jar replaces `lib/dozerdb-core-5.26.27-1.1.0.jar`. |
| Apache Ratis | 3.3.1 | Uses the already shaded `ratis-thirdparty` 1.1.0 for gRPC, Netty and protobuf. |
| Kotlin | 2.4.21, kotlinx-coroutines 1.11.0 | Bundled and relocated to `org.dozendb.shaded`. |
| JDK | build 17, run 17 or 21 | Bytecode targets Java 17 like Neo4j 5.26. The DozerDB image runs Temurin 21. |

## Known differences from the DozerDB 5.26.27.0 release

The DozerDB 5.26.27.0 release jar was built from source that is not public.
Its bootstrap, edition module and Cypher classes match the public
`neo4j-5.26.8` source, but its constraint enforcement was rewritten. DozenDB
builds the public source, so it differs from the release in these ways:

- **Constraints (serious, with clustering disabled).** Creating a property
  existence constraint (`REQUIRE x.p IS NOT NULL`) fails the database: the
  transaction does not commit and the database must be restarted. The public
  source inherits Neo4j Community's `readConstraint`, which rejects existence,
  key and property type constraints when they are applied or loaded; the
  release overrides it to accept them. `DisabledParityIT` asserts this
  difference so that it is noticed when it changes.
- **Constraints with clustering enabled.** DozenDB's own constraint semantics
  fix three defects of the public source, without changing it: stores with
  existence, key and type constraints can be read; commit-time enforcement
  builds its checker from each constraint's schema (the public source casts the
  constraint itself, which fails every write once an existence constraint
  exists); and a relationship missing a required property fails with
  `Neo.ClientError.Schema.ConstraintValidationFailed`, also when the property
  is removed later (the public source reports an unknown error and checks only
  new relationships). Property existence constraints therefore work and are
  enforced.
  Still as in the public source: creating an existence constraint does not
  verify existing data, and property type, key, label existence and
  relationship endpoint label constraints are not enforced. The release
  enforces those.
- **Multi-database housekeeping.** The release contains small refactors of
  `MultiDatabaseManager`, `MultiDatabaseLifecycleService` and
  `SystemGraphTransactionEventListenerAdapter`. One of them changes behaviour:
  on shutdown the release no longer stops the default database twice.

## Clustering limitations

What clustering does not do yet.

- **Replicated databases.** Only the default database (`neo4j`) is
  replicated. `system` and databases created with `CREATE DATABASE` are local
  to each member, so administration commands apply to one member only.
- **Writes go to the leader.** A follower rejects writes with
  `Neo.ClientError.Cluster.NotALeader`. Drivers that route (`neo4j://`) get
  routing tables with the leader as writer and retry managed transactions on
  the new leader after a failover. Server-side routing
  (`dbms.routing.default_router=SERVER`) does not forward writes: a `bolt://`
  write sent to a follower is rejected. Local databases (`system`, and
  databases created with `CREATE DATABASE`) are routed to the member asked.
- **Linearizable reads** are requested per transaction with the metadata key
  `dozendb.consistency` set to `linearizable`, or for every transaction with
  `dbms.dozendb.reads.default_consistency`. They apply to Bolt clients
  (drivers, Cypher Shell) on replicated databases.
- **Transaction size.** A transaction can replicate at most 16 MiB of changes
  in Neo4j's command encoding. A larger one fails with
  `Neo.DatabaseError.Transaction.TransactionCommitFailed` and nothing is
  committed; split it, for example with `CALL { ... } IN TRANSACTIONS`.
- **Outcome unknown.** If the leader loses leadership or the majority while a
  commit is in flight, the transaction fails with
  `Neo.DatabaseError.Transaction.TransactionCommitFailed` and a message saying
  that it may or may not have committed. This is inherent to replication.
- **Starting databases.** All members must start from identical databases. A
  new cluster starts from the databases of at most one member, or of the member
  with the lowest id if none has any; the others copy them during formation.
  Members with databases of their own refuse to form a cluster together.
- **ID reuse.** IDs freed while a member is a follower are reused only after a
  restart, as in Neo4j's own clustering.
- **Durability (single fsync).** A commit to a replicated database is
  durable once a majority of members has forced it to their Raft log; Neo4j's
  transaction log is not forced at every commit. Before a member writes store
  pages or a checkpoint, it forces the transaction log they depend on, so the
  store never gets ahead of the log. After a power loss, recovery truncates a
  damaged log tail and the member replays the rest from the Raft log. For this,
  a clustered member sets `internal.dbms.tx_log.fail_on_corrupted_log_files`
  to `false` unless it is configured explicitly. The setting applies to every
  database, so damage to log bytes that were already forced (a failing disk)
  is truncated and logged instead of stopping the start. Set
  `server.dozendb.cluster.single_fsync=false` to force both logs at every
  commit, as Neo4j does on its own.
- **Raft log retention and catch-up.** Raft snapshots follow Neo4j
  checkpoints (`db.checkpoint.interval.*`): each member keeps its Raft log from
  its second-to-last checkpoint on. A member that was down for less than about
  one checkpoint interval catches up from the log. A member that was down
  longer stops its copy of the database, replaces it with a copy from the
  leader, and starts it again; the database is unavailable on that member
  meanwhile. `dozendb.cluster.status()` reports each group's latest snapshot
  as `snapshotIndex`.
- **Lost databases.** A member whose replicated database is missing at startup
  (for example after deleting `data/databases/neo4j` and
  `data/transactions/neo4j`) copies it from another member and keeps its own
  `system` database. Never delete the Raft directory (`data/raft`) of a member
  that keeps its identity (`data/server_id`): Raft would forget its votes.
- **Stopped databases.** A member whose replicated database stops, for example
  after a panic, keeps taking part in the database's Raft group but no longer
  applies entries, and hands leadership to another member.
