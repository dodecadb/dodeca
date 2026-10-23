# Compatibility

Each DozenDB build is pinned to exactly one Neo4j version. A member refuses to
form, join or rejoin a cluster whose members run a different Neo4j version.

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
  native and `REMOVE AUTH` fail, since only native authentication exists.
- **Roles and privileges** work as in Neo4j Enterprise, see "Roles and
  privileges" below. DozerDB and Neo4j Community refuse them, and every user
  can do everything.
- **Database commands** fail with Neo4j's messages and status codes, such as
  `Neo.ClientError.Database.ExistingDatabaseFound` and
  `Neo.ClientError.Database.DatabaseNotFound`. Unlike in DozerDB:
  `START DATABASE` of a database that does not exist fails;
  `START DATABASE $name`, `STOP DATABASE $name` and the embedded
  `DatabaseManagementService` methods work; names with a dot, such as
  `film.archive`, work in every command; `CREATE OR REPLACE DATABASE` also
  creates a database that does not exist yet; `CREATE COMPOSITE DATABASE`,
  `CREATE DATABASE ... TOPOLOGY`, `CREATE DATABASE ... OPTIONS` with an
  option other than `existingData`, `seedURI` and, with clustering,
  `existingDataSeedServer` (such as `seedCredentials`, `storeFormat` or a
  misspelt key), and `DROP DATABASE ... DUMP DATA` fail instead of being
  ignored or failing with an internal error; and `dbms.max_databases` counts
  every database including `system`, where DozerDB allows one fewer.
- **Creating a database from a seed**, `CREATE DATABASE ... OPTIONS
  {existingData: 'use', seedURI: '...'}`, works as in Neo4j Enterprise with a
  dump or a full backup as the seed. `dbms.databases.seed_from_uri_providers`
  enables `FileSeedProvider` (`file:`, the default) and
  `URLConnectionSeedProvider` (`http:`, `https:`, `ftp:`); there are no cloud
  providers, and other names fail the server's start. A URI whose scheme no
  enabled provider reads fails the command; a seed that cannot be read fails
  the database, which `WAIT` and `SHOW DATABASES` show, and the next start of
  the server tries it again. `existingData: 'use'` without a seed uses the
  files that `neo4j-admin database restore` put in place, and fails the
  command if there are none. A seed is never combined with `OR REPLACE`.
  With clustering, one member creates the database from the seed and the
  others copy it from that member: the system leader, which must then be able
  to read the seed, or the member that `existingDataSeedServer` names, which
  reads the seed, or without `seedURI` uses the database that `neo4j-admin
  database restore` prepared on it. `existingDataSeedServer` must name a
  member, and `existingData: 'use'` needs it or a `seedURI`. Neo4j 5.26 keeps
  the seeding server in the `seeding_servers` property of the `Database` node,
  as DozenDB does; `designated_seeder` is deprecated.
- **Backups and restores** use Neo4j Enterprise's commands, options and
  settings: `neo4j-admin database backup`, `restore` and
  `aggregate-backup`, `neo4j-admin backup inspect`, `neo4j-admin database
  check --from-path`, `server.backup.enabled`, `server.backup.listen_address`
  and `dbms.ssl.policy.backup.*`, and work alike on standalone servers and
  cluster members. A backup artifact is a file of its own format, which
  Neo4j Community's `neo4j-admin database load` also loads as a dump when it
  is a full backup. Exchanging backups with Neo4j Enterprise is not promised.
  The differences: there are no cloud locations (`s3:`, `gs:`, `azb:`);
  `--parallel-recovery` and `--inspect-path` are not supported; the script
  that `--include-metadata` keeps, and `restore` writes to
  `data/scripts/<database>/restore_metadata.cypher`, is DozenDB's own;
  `neo4j-admin backup inspect` without `--show-metadata` lists only the
  files; backup files are named in UTC; `--temp-path` of `backup`
  defaults to `--to-path`, and `restore` accepts `--temp-path` without using
  it; a backup is not restored with another Neo4j kernel version than the
  one it was taken with; and serving backups on an address that is not a
  loopback one needs `dbms.ssl.policy.backup.enabled` set explicitly, `true`
  or `false`, since the backup port has no other authentication. See
  `OPERATOR_GUIDE.md`.
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
- **Failed databases show as `offline`**, with why in `statusMessage`, as
  Neo4j documents a database that is not running because of a problem. Neo4j
  Community and DozerDB show a database that failed while running, such as
  after a panic on a full disk, as `online`, although it serves nothing. It
  shows as `online` again once restarted, with `STOP DATABASE` and
  `START DATABASE` or by restarting the server.
- **Startup banner.** DozenDB logs its version instead of DozerDB's banner.

## Edition

DozenDB reports the Enterprise edition, in `CALL dbms.components()` for
example, as DozerDB did, so that tools offer what DozenDB adds to Neo4j
Community, such as many databases. Of what Neo4j Enterprise adds, DozenDB
supports many databases, property existence, key and type constraints, user
status and home databases, roles and privileges, impersonation, and
clustering of its own. It refuses, with an error, the other Enterprise
features that reach it, such as composite databases, aliases,
`ALTER DATABASE`, the server commands (`SHOW SERVERS` and the like; the
`dozendb.cluster` procedures manage members instead), `CREATE DATABASE`
options other than seeds, topologies, and external authentication.

## Roles and privileges

DozenDB implements roles and privileges as Neo4j Enterprise's operations
manual describes them, from that documentation alone: the built-in roles
(`PUBLIC`, `reader`, `editor`, `publisher`, `architect`, `admin`), role
commands, `GRANT`, `DENY` and `REVOKE` of DBMS, database and graph
privileges, label, relationship type and property qualifiers, property-based
rules (`FOR (n:Label) WHERE n.p = value`), procedure, function, setting and
`LOAD` privileges, `IMMUTABLE` roles and privileges, impersonation
(`IMPERSONATE`, the drivers' impersonated user), `SHOW PRIVILEGES` with
`AS COMMANDS`, and `SHOW SUPPORTED PRIVILEGES`. Restrictions are enforced by
Neo4j's kernel for Cypher, procedures, counts and indexes alike. Messages
and status codes are Neo4j's where its documentation or code gives them.

- **The first start with roles** creates the built-in roles, gives `admin` to
  one user and leaves every other user with `PUBLIC` alone, as Neo4j
  Enterprise does: the user chosen with `neo4j-admin dbms set-default-admin`,
  else `neo4j`, else the only user. With none of them the server refuses to
  start with Neo4j's message. This applies to stores of Neo4j Community,
  DozerDB and earlier DozenDB builds alike; the log names the user that got
  `admin`. A cluster formed by an earlier DozenDB build gets its roles from
  its system leader, once; meanwhile logins are refused with "this server
  does not have the roles of the cluster yet".
- **Stores of Neo4j Enterprise with roles** are refused: DozenDB keeps roles
  and privileges in its own nodes of the system graph and cannot read
  Enterprise's. See `OPERATOR_GUIDE.md` for moving roles in either
  direction.
- **Auth disabled** allows everything, `IMMUTABLE` commands included, as in
  Neo4j; this is also how a locked-out admin recovers.
- **Where the documentation is silent**, DozenDB takes the stricter answer: a
  node without labels is found, created and deleted only through privileges
  on all labels (`NODES *`); creating or deleting a node, and setting its
  properties, needs the privilege on all labels or on each of the node's
  labels; a privilege DozenDB cannot read applies to nothing when granted and
  to everything in its scope when denied, and is logged; `SHOW SETTINGS`
  lists nothing without `SHOW SETTING`; any applying `DENY LOAD` blocks
  `LOAD CSV`; `GRANT` and `DENY` on a database that does not exist, or with a
  database parameter of `*`, fail, where `REVOKE` only warns; and
  impersonating oneself needs `IMPERSONATE` too.
- **Property-based rules** are evaluated when granted: `date()` is the date
  of the `GRANT`. A negated rule is kept as the opposite comparison
  (`NOT n.p > 1` as `n.p <= 1`), which differs only for NaN values. A `DENY`
  rule applies only to nodes whose value satisfies it, so it does not apply
  to nodes without the property, as documented. A null or NaN value, a list
  of mixed types and `IN` without a list are refused, parameters included.
  Rules cost a read of the property for each node: in a benchmark, scanning a
  million nodes took 2.5 times as long with a rule, 1.8 times with a label
  denied.
- **`SHOW PRIVILEGES`** names the home database graph `HOME`, a `LOAD`
  privilege's segment `ALL DATA` or `CIDR('range')`, and keeps `IMMUTABLE` in
  `AS COMMANDS`. `SHOW SUPPORTED PRIVILEGES` gives the documented rows
  verbatim and describes the others in DozenDB's words. `DROP ROLE` removes
  the role from its users.
- **With clustering**, roles and privileges reach every member through Raft;
  a follower applies a change a moment after the leader. Transactions keep
  the privileges they started with. Administration commands that change
  nothing, such as a `GRANT` of a privilege the role has, may answer on a
  follower from its copy instead of failing with `NotALeader`.
- **Refusals are logged in `security.log`**, as described under
  [Metrics and logs](#metrics-and-logs), except refused `LOAD CSV` URLs,
  which Neo4j Community's URL checks log in `debug.log`. Privileges that
  cannot be read are logged in `debug.log`.
- **What bypasses privileges in Neo4j Enterprise bypasses them here too**:
  procedures listed in `dbms.security.procedures.unrestricted` or run with
  `EXECUTE BOOSTED`, APOC triggers and other code that opens its own
  transactions, constraint violation messages that name existing values, and
  query plans estimated from counts.

## Metrics and logs

DozenDB reports metrics and writes the query log and the security log as
Neo4j Enterprise's operations manual describes them, from that documentation
alone; Neo4j Community declares the query log settings and ships the log
files' configuration, but writes neither log. `OPERATOR_GUIDE.md` describes
how to use them.

- **Exporters:** Prometheus and JMX (domain `neo4j.metrics`). The CSV and
  Graphite settings (`server.metrics.csv.*`, `server.metrics.graphite.*`) are
  not declared, so a configuration that names them is refused at startup
  with "Unrecognized setting" instead of being ignored. Neo4j Community
  refuses the `server.metrics.*` settings the same way.
- **Names** are Enterprise's, `<prefix>.dbms.<name>` and
  `<prefix>.database.<database>.<name>`, with its settings and default filter.
  Metrics Enterprise does not have are named under a `dozendb` segment, such
  as `dozendb.cluster.raft.follower_lag_max`. Enterprise's exact Prometheus
  output is not documented; DozenDB's replaces characters other than letters,
  digits and `_` with `_`, adds `_total` to counters, reports histograms as
  summaries (quantiles 0.5 to 0.999, `_sum`, `_count`), and starts each
  family's `# HELP` with the dotted name. Database names that differ only in
  such characters, such as `my-db` and `my_db`, give the same Prometheus
  names: the second family is left out and a warning logged. Prometheus's
  `promtool check metrics` reads the output and flags only gauges whose
  Enterprise names end like counters or summaries: `store.size.total`,
  `vm.file.descriptors.count` and `vm.memory.buffer.<pool>.count`.
- **Metrics without a source in Neo4j Community or DozenDB** are not
  reported: `server.threads.jetty.*`, `cluster.discovery.*` and
  `cluster.internal.discovery.*`, Enterprise's Raft internals
  (`cluster.raft.tx_retries`, `in_flight_cache.*`,
  `raft_log_entry_prefetch_buffer.*`, `message_processing_*`),
  `cluster.catchup.*` and `cluster.store_copy.*` of Enterprise's catch-up
  protocol (DozenDB's copies are counted under `dozendb.cluster.store_copy`),
  the pipelined and parallel runtimes' metrics, Cypher's `compiled` counts and
  `bolt.accumulated_queue_time`. Followers are reported together on the
  leader (`dozendb.cluster.raft.followers`, `follower_lag_max`), not one
  series each.
- **The query log is on by default**, at Neo4j's declared default
  `db.logs.query.enabled=VERBOSE`: every member logs the start and end of
  every query with its parameters, rotated by the shipped `server-logs.xml`
  (7 files of 20 MB). A server moving from Neo4j Community or DozerDB starts
  writing `query.log`, which costs up to 12% of the throughput of trivial
  queries (`OPERATOR_GUIDE.md`). Set `INFO` with a
  `db.logs.query.threshold`, or `OFF`, to log less.
- **Query log lines** follow the documented fields, their order and the JSON
  field names. Neo4j does not document the rest, which DozenDB defines:
  - `Query started: ` before start lines; `id:<n> - ` only with `VERBOSE`;
    then `transaction id:<n> - <ms> ms: (planning: <ms>[, cpu: <ms>],
    waiting: <ms>) - <bytes> B - <hits> page hits, <faults> page faults -
    <source>\t<database> - <user> - <query>[ - <parameters>] -
    runtime=<runtime> - <annotation data>[ - <failure>]`, with the plan
    description on the following lines when enabled.
  - Failures are logged at `ERROR` with the message on one line, or with
    `obfuscate_errors` only the status code and GQLSTATUS; the rest at
    `INFO`.
  - The user is the executing user, `<authenticated>:<executing>` when
    impersonating, as in the security log.
  - Parameters are a Cypher map (`{name: 'Tom', n: 1}`), each value cut to
    `max_parameter_length` characters followed by `...`.
  - A query that cannot be parsed is logged with its string literals masked,
    or without its text and parameters with `obfuscate_literals`, so that a
    mistyped command never logs a password.
  - Transaction lines: `Transaction started: transaction id:<n> - <database>
    - <user>`, `Transaction committed: transaction id:<n> - <ms> ms -
    <database> - <user>`, and `Transaction rolled back: ...` with the
    failure.
  - JSON lines list their fields in alphabetical order after `time` and
    `level`.
- **The security log** has logins (`dbms.security.log_successful_authentication`
  leaves out successful ones), failed logins with the reason, refused
  privileges and impersonations, and administration commands that changed
  something, with passwords hidden. A command is logged once its transaction
  commits, on the member where it committed: with clustering, the leader of
  `system`. A command that changed nothing, such as `CREATE ROLE ... IF NOT
  EXISTS` for a role that exists, is not logged.
- **`dbms.setConfigValue`** changes a dynamic setting as in Neo4j
  Enterprise: for administrators, on the member it runs on, until that
  member restarts. Neo4j Community has no such procedure.

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
  with the lowest id if none has any. Members with databases of their own
  refuse to form a cluster together. That member creates every database its
  `system` database lists, and the others copy them: `system` and the default
  database during formation, the others as they start. Its databases may come
  from another cluster, as backups restored with `neo4j-admin database
  restore`, a dump loaded with `neo4j-admin database load` or a copy of the
  database's files: on its first start, before the others copy them, the
  member ends their history in the other cluster's Raft groups with an empty
  transaction, and records itself in `system` as the member that creates
  them, in place of the other cluster's members.
- **Members.** Members are added with `dozendb.cluster.addMember` and removed
  with `dozendb.cluster.removeMember`, one at a time; every database's Raft
  group follows. See `OPERATOR_GUIDE.md`. A removed member cannot rejoin with
  its data: its data directory must be deleted first. A database that is
  offline while a member is added gets the member once it is started again.
- **Upgrades.** DozenDB is upgraded one member at a time, mixing at most two
  versions; formats that the previous version cannot read are used only once
  an administrator calls `dozendb.cluster.finishUpgrade()`, after which no
  member can return to it. Neo4j is upgraded with every member shut down. See
  `OPERATOR_GUIDE.md`.
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
- **Database files put in place by hand.** A member that finds a database's
  files applied up to a higher Raft index than its group reached there, as
  after copying another database's or cluster's files into place while it was
  down, checks the group's entry at that index against the files' last
  transaction, once the group's log reaches it. If they differ, the database
  fails on that member with a message saying how to recover: delete the files
  there and restart the member, which then copies the database. Files applied
  up to an index the group already reached on that member are not checked.
  Create databases from other files with a seed instead.
- **Stopped databases.** A member whose replicated database stops, for example
  after a panic, keeps taking part in the database's Raft group but no longer
  applies entries, and hands leadership to another member. Once a member's
  copy failed, routing tables, whichever member serves them, no longer list
  it as a writer or reader of that database, within about 10 seconds;
  it still serves routing tables, which send drivers to the others.
