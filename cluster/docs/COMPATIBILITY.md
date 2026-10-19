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
- **`ALTER USER` with clustering enabled.** DozerDB, the release as well as
  the public source, fails every `ALTER USER` with an
  `UnsatisfiedDependencyException` for `UserSecurityGraphComponent`: its
  administration runtime resolves that component, but its edition module never
  registers it as a dependency. With clustering enabled DozenDB registers it,
  so `ALTER USER` works; with clustering disabled it fails as in DozerDB.
- **Multi-database housekeeping.** The release contains small refactors of
  `MultiDatabaseManager`, `MultiDatabaseLifecycleService` and
  `SystemGraphTransactionEventListenerAdapter`. One of them changes behaviour:
  on shutdown the release no longer stops the default database twice.

## Clustering limitations

What clustering does not do yet.

- **Replicated databases.** Every database is replicated, `system` included,
  each in its own Raft group. Administration commands (users,
  `CREATE DATABASE` and the like) run on the leader of `system` and reach every
  member; a follower rejects them with `Neo.ClientError.Cluster.NotALeader`.
  `CREATE`, `STOP`, `START` and `DROP DATABASE` apply on every member, and a
  member that was down applies the ones it missed when it returns.
- **New databases.** The system leader creates the store of a new database and
  the other members copy it, which takes a moment: until then the database is
  not available on them. Stores are copied only while a database is online, so
  a member that lacks a stopped database gets it once it is started again. If
  the creating member fails before it created the store, the others wait until
  it returns.
- **Default database.** As in DozerDB, `CREATE DATABASE` and `DROP DATABASE`
  neither create nor drop the default database (`initial_default_database`);
  `STOP DATABASE` and `START DATABASE` apply to it.
- **Writes go to the leader.** A follower rejects writes with
  `Neo.ClientError.Cluster.NotALeader`. Drivers that route (`neo4j://`) get
  routing tables with the leader as writer and retry managed transactions on
  the new leader after a failover. Server-side routing
  (`dbms.routing.default_router=SERVER`) does not forward writes: a `bolt://`
  write sent to a follower is rejected. A database that does not run on the
  member asked, such as a stopped one, is routed to that member only.
- **Linearizable reads** are requested per transaction with the metadata key
  `dozendb.consistency` set to `linearizable`, or for every transaction with
  `dbms.dozendb.reads.default_consistency`. They apply to Bolt clients
  (drivers, Cypher Shell).
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
  longer copies the database from the leader, or from another member, while
  its own copy keeps serving reads, which are stale meanwhile. Only then does
  it stop the database, move the copy into place and start it again: the
  database is unavailable on that member for well under a second (0.3 s in
  tests). This holds for `system` too: logins and administration commands on
  that member see the old users and databases until the copy is in place, and
  may fail with `Neo.TransientError.General.DatabaseUnavailable` while it is
  moved there. `dozendb.cluster.status()` reports each group's latest snapshot
  as `snapshotIndex`.
- **Slow links.** A member whose links to the others are slower than the
  cluster writes falls further and further behind. Once the others discard the
  log it still needs, it copies the database, falls behind again, and copies
  it again. Each copy logs a warning; one needed within an hour of the
  previous copy of that database also says that the member does not keep up.
- **Lost databases.** A member whose replicated database is missing at startup
  (for example after deleting `data/databases/neo4j` and
  `data/transactions/neo4j`) copies it from another member and keeps its own
  `system` database. Never delete the Raft directory (`data/raft`) of a member
  that keeps its identity (`data/server_id`): Raft would forget its votes.
- **Stopped databases.** A member whose replicated database stops, for example
  after a panic, keeps taking part in the database's Raft group but no longer
  applies entries, and hands leadership to another member.
