# Compatibility

Each DozenDB build is pinned to exactly one Neo4j version. A node refuses to
join a cluster whose members run a different Neo4j version.

## Pinned versions

| Component | Version | Notes |
|---|---|---|
| Neo4j | 5.26.27 (Community) | Unmodified; the DozenDB jar goes into its `lib/`. |
| Test image | `neo4j:5.26.27-community` | Neo4j's official image: test baseline and installation target. `StandaloneIT` compares DozenDB, with clustering disabled, against it. |
| Apache Ratis | 3.3.1 | Uses the already shaded `ratis-thirdparty` 1.1.0 for gRPC, Netty and protobuf. |
| Kotlin | 2.4.21, kotlinx-coroutines 1.11.0 | Bundled and relocated to `org.dozendb.shaded`. |
| JDK | build 17, run 17 or 21 | Bytecode targets Java 17 like Neo4j 5.26. The test image runs Temurin 21. |

## Differences from DozerDB 5.26.27.0

DozenDB contains its own port of the DozerDB code it used to build on, see
`UPSTREAM_TOUCHPOINTS.md`, and no longer needs DozerDB. It is not a drop-in
replacement for DozerDB: it declares only Neo4j's and its own settings, and
behaves differently in these ways, with clustering enabled or disabled. Each
difference was verified against the `graphstack/dozerdb:5.26.27.0` image when
DozenDB took over the code.

- **Constraints.** Stores with existence, key, property type and other
  Enterprise constraints can be read. A node or relationship that lacks a
  property that an existence or key constraint requires, or holds a property
  of a type that a property type constraint does not allow, fails its
  transaction with `Neo.ClientError.Schema.ConstraintValidationFailed`,
  whether it is created or its labels or properties change later. Creating
  such a constraint fails if existing data violates it. This is as in
  DozerDB's release. Label existence and relationship endpoint label
  constraints, which Cypher 5 cannot create, are refused as in Neo4j
  Community instead of being created and not enforced.
- **`ALTER USER`** works. DozerDB fails every `ALTER USER` with an
  `UnsatisfiedDependencyException` for `UserSecurityGraphComponent`.
- **User status and home database** work as in Neo4j Enterprise. A suspended
  user cannot log in, as with a wrong password, and the user's open sessions
  lose access at their next transaction. A user's home database is the
  database of the user's sessions that name none, also through routing.
  `SHOW USERS` and `SHOW CURRENT USER` show both. DozerDB stores both but acts
  on neither: a suspended user still logs in, and a user's home database is
  ignored. Neo4j Community refuses both. `SET AUTH` with another provider than
  native and `REMOVE AUTH` fail, since only native authentication exists, and
  `roles` is null, since there are no roles.
- **Database commands** fail with Neo4j's messages and status codes, such as
  `Neo.ClientError.Database.ExistingDatabaseFound` and
  `Neo.ClientError.Database.DatabaseNotFound`. Unlike in DozerDB:
  `START DATABASE` of a database that does not exist fails;
  `START DATABASE $name`, `STOP DATABASE $name` and the embedded
  `DatabaseManagementService` methods work; names with a dot, such as
  `film.archive`, work in every command; `CREATE OR REPLACE DATABASE` also
  creates a database that does not exist yet; `CREATE COMPOSITE DATABASE`,
  `CREATE DATABASE ... TOPOLOGY`, `CREATE DATABASE ... OPTIONS` with any
  option (such as `seedURI`, which would otherwise create an empty database)
  and `DROP DATABASE ... DUMP DATA` fail instead of being ignored or failing
  with an internal error; and `dbms.max_databases` counts every database
  including `system`, where DozerDB allows one fewer.
- **`WAIT`** on `CREATE`, `DROP`, `START` and `STOP DATABASE` works as in
  Neo4j Enterprise. The command commits its transaction, then returns once
  every server, with clustering every member, has the database running,
  stopped or dropped as asked, or failed to, or once the time limit passed:
  300 seconds for `WAIT`, n for `WAIT n SECONDS`. It returns a row per server
  with `address` (its Bolt address), `state` (`CaughtUp`, `Failed` or
  `TimedOut`), `message`, and `success`, true only if every server caught up.
  Without `WAIT` a command returns once committed: without clustering the
  database has then started or failed to, and `SHOW DATABASES` shows why in
  `statusMessage`; with clustering the members follow in the background.
- **The default database** cannot be dropped or replaced. DozerDB removes it
  from the system graph but keeps it running, until a restart adds it back;
  with clustering, a member could then no longer start.
- **Databases follow the system graph only after a command commits**, every
  database a transaction changes. DozerDB drops a database before the command
  commits, so a command that then fails has still dropped it. A database that
  failed to start is started again by `STOP DATABASE` and `START DATABASE`.
  A stopped default database starts with the server, like in DozerDB, but is
  then stopped again, where DozerDB leaves it running. On shutdown, `system`
  stops after every other database.
- **Startup banner.** DozenDB logs its version instead of DozerDB's banner.

## Edition

DozenDB reports the Enterprise edition, in `CALL dbms.components()` for
example, as DozerDB did, so that tools offer what DozenDB adds to Neo4j
Community, such as many databases. Of what Neo4j Enterprise adds, DozenDB
supports many databases, property existence, key and type constraints, user
status and home databases, and clustering of its own. It refuses, with an
error, the other Enterprise features that reach it, such as roles and
privileges, composite databases, aliases, `CREATE DATABASE` options and
topologies, and external authentication.

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
  not available on them. `CREATE DATABASE ... WAIT` returns once it is. Stores
  are copied only while a database is online, so a member that lacks a stopped
  database gets it once it is started again. If the creating member fails
  before it created the store, the others wait until it returns.
- **Default database.** The default database (`initial_default_database`)
  exists from the first start on and cannot be dropped or replaced;
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
- **Members.** Members are added with `dozendb.cluster.addMember` and removed
  with `dozendb.cluster.removeMember`, one at a time; every database's Raft
  group follows. See `OPERATOR_GUIDE.md`. A removed member cannot rejoin with
  its data: its data directory must be deleted first. A database that is
  offline while a member is added gets the member once it is started again.
- **Traffic between members.** A clustered member does not start until
  `dbms.ssl.policy.cluster.enabled` is set: `true` for mutual TLS, configured
  with Neo4j's SSL policy settings of the `cluster` scope as in Neo4j
  Enterprise, or `false` for plaintext. `client_auth=OPTIONAL` is refused.
  Certificates are read at start, and members accept any certificate from a
  trusted authority, whichever member presents it. See `OPERATOR_GUIDE.md`.
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
  that keeps its identity (`data/server_id`): Raft would forget its votes. To
  rebuild a member from nothing, remove it from the cluster, delete its whole
  data directory, and add it again as a new member.
- **Stopped databases.** A member whose replicated database stops, for example
  after a panic, keeps taking part in the database's Raft group but no longer
  applies entries, and hands leadership to another member.
