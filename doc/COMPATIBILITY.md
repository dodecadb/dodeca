# Compatibility

DozenDB is Neo4j Community Edition with one jar added to its `lib/`, and adds
much of what Neo4j otherwise keeps for its Enterprise Edition. This document
compares DozenDB with Neo4j Enterprise 5.26, area by area: what works as in
Enterprise, where DozenDB differs, what it decided where Enterprise's
documentation says nothing, and what it does not support. DozenDB is built from
that documentation alone, Enterprise's operations manual and Cypher manual, so
"as in Enterprise" means as the documentation describes. "Neo4j Community"
means Neo4j Community Edition without DozenDB.

## Pinned versions

Each DozenDB build is pinned to exactly one Neo4j version. A member refuses to
form, join or rejoin a cluster whose members run a different Neo4j version.

| Component | Version | Notes |
|---|---|---|
| Neo4j | 5.26.27 (Community) | Unmodified; the DozenDB jar goes into its `lib/`. |
| Test image | `neo4j:5.26.27-community` | Neo4j's official image: test baseline and installation target. `StandaloneIT` compares DozenDB, with clustering disabled, against it. |
| Apache Ratis | 3.3.1 | Uses the already shaded `ratis-thirdparty` 1.1.0 for gRPC, Netty and protobuf. |
| Kotlin | 2.4.21, kotlinx-coroutines 1.11.0 | Bundled and relocated to `org.dozendb.shaded`. |
| JDK | build 17, run 17 or 21 | Bytecode targets Java 17 like Neo4j 5.26. The test image runs Temurin 21. |

## At a glance

*Same* means as Enterprise documents it, with DozenDB's answers where the
documentation is silent. *Same, with differences* means that the section
linked lists the differences. *Own design* means that DozenDB reaches the same
end its own way.

| Enterprise feature | DozenDB | Section |
|---|---|---|
| Many databases: `CREATE`, `DROP`, `START` and `STOP DATABASE`, `WAIT` | Same, with differences | [Databases](#databases) |
| Creating a database from a seed | Same, with differences | [Databases](#databases) |
| Composite databases | Not supported | [Not supported](#not-supported) |
| Property existence, key and type constraints | Same | [Constraints](#constraints) |
| User status and home databases | Same | [Users](#users) |
| Roles and privileges, impersonation | Same | [Roles and privileges](#roles-and-privileges) |
| LDAP, single sign-on and Kerberos | Not supported | [Not supported](#not-supported) |
| Local and remote database aliases | Same | [Database aliases](#database-aliases) |
| Clustering: replication, failover, routing | Own design, same for clients | [Clustering](#clustering) |
| Server-side routing | Same | [Clustering](#clustering) |
| Cluster status endpoints of the HTTP server | Same, with differences | [Clustering](#clustering) |
| Servers, topologies and secondaries | Same, with differences | [Servers](#servers) |
| Leadership balancing and priority tags | Same | [Leadership balancing](#leadership-balancing) |
| Online backup and restore | Same, with differences | [Backup and restore](#backup-and-restore) |
| Metrics | Same, with differences | [Metrics and logs](#metrics-and-logs) |
| Query log and security log | Same | [Metrics and logs](#metrics-and-logs) |
| Changing settings while running | Same | [Metrics and logs](#metrics-and-logs) |
| Changing the default database | Same | [Databases](#databases) |
| Quarantine, recreating databases and cluster status checks | Not supported | [Not supported](#not-supported) |
| Routing policies (the load balancing plugin) | Not supported | [Not supported](#not-supported) |
| Change data capture | Not supported | [Not supported](#not-supported) |
| `block` and `high_limit` store formats | Not supported | [Not supported](#not-supported) |
| Pipelined and parallel runtimes | Not supported | [Not supported](#not-supported) |
| Store copy and incremental import | Not supported | [Not supported](#not-supported) |
| Page cache warmup | Not supported | [Not supported](#not-supported) |
| Other procedures and settings only Enterprise has | Not supported | [Not supported](#not-supported) |

## Edition

DozenDB reports the Enterprise edition, in `CALL dbms.components()` for
example, so that tools offer what DozenDB adds to Neo4j Community, such as many
databases. [Not supported](#not-supported) says how DozenDB answers the
Enterprise features it lacks.

Neo4j's anonymous usage data (`dbms.usage_report.enabled`), which Neo4j sends
to udc.neo4j.com unless told not to, is off in DozenDB unless the operator
turns it on. Turned on, it reports the server as Neo4j Enterprise, as
`dbms.components()` does.

## Databases

`CREATE`, `CREATE OR REPLACE`, `DROP`, `START` and `STOP DATABASE` work as in
Enterprise, with `IF EXISTS` and `IF NOT EXISTS`, `DESTROY DATA`, parameters
such as `START DATABASE $name`, and names with a dot such as `film.archive`;
so do the embedded `DatabaseManagementService` methods. They fail with
Neo4j's messages and status codes, such as
`Neo.ClientError.Database.ExistingDatabaseFound` and
`Neo.ClientError.Database.DatabaseNotFound`. `CREATE DATABASE ... TOPOLOGY`
places the database on servers (see [Servers](#servers)), and
`ALTER DATABASE ... SET ACCESS` makes it read-only or writable again.

- **`storeFormat`** creates a new database's store in `aligned` or `standard`,
  as in Enterprise, with a deprecation warning for `standard`; `block` and
  `high_limit` fail, naming the formats there are. A database created from a
  seed or from files in place keeps their format. With clustering, the server
  that creates the store creates it in that format, and the others copy it.
- **`existingDataSeedInstance`**, the deprecated name of
  `existingDataSeedServer`, works as that option, with a deprecation warning.
  Given both, the command fails.
- **Not supported:** `CREATE COMPOSITE DATABASE`, `DROP DATABASE ... DUMP
  DATA`, `ALTER DATABASE ... SET OPTION`, and `CREATE DATABASE ... OPTIONS`
  with an option other than `existingData`, `seedURI`, `storeFormat` and,
  with clustering, `existingDataSeedServer` or `existingDataSeedInstance`:
  `seedConfig`, `seedCredentials`, `txLogEnrichment` or a misspelt key. Each
  fails with an error. To keep a database's data, back it up, or stop it and
  dump it with `neo4j-admin database dump`, before dropping it.
- **`dbms.max_databases`** counts every database, online or offline, as in
  Enterprise, and `system` too, which Enterprise's documentation leaves open.
- **`WAIT`** on `CREATE`, `DROP`, `START` and `STOP DATABASE` works as in
  Enterprise. The command commits its transaction, then returns once every
  server that hosts the database has it running, stopped or dropped as asked,
  or failed to, or once the time limit passed: 300 seconds for `WAIT`, n for
  `WAIT n SECONDS`. With clustering, a running copy has also joined the
  database's Raft group, and the member knows its leader, which then takes
  writes at once, through any server. It returns a row per server with
  `address` (its Bolt address), `state` (`CaughtUp`, `Failed` or `TimedOut`),
  `message`, and `success`, true only if every server caught up. Without
  `WAIT` a command returns once committed: without clustering the database has
  then started or failed to, and `SHOW DATABASES` shows why in
  `statusMessage`; with clustering the members follow in the background.
- **Creating a database from a seed**, `CREATE DATABASE ... OPTIONS
  {existingData: 'use', seedURI: '...'}`, works as in Enterprise with a dump or
  a full backup as the seed. `dbms.databases.seed_from_uri_providers` enables
  `FileSeedProvider` (`file:`) by default, where Enterprise enables its cloud
  providers, and also takes `URLConnectionSeedProvider` (`http:`, `https:`,
  `ftp:`). There are no cloud providers (`CloudSeedProvider`,
  `S3SeedProvider`), and other names fail the server's start. A URI whose
  scheme no enabled provider reads fails the command; a seed that cannot be
  read fails the database, which `WAIT` and `SHOW DATABASES` show, and the
  next start of the server tries it again.
  `existingData: 'use'` without a seed uses the files that `neo4j-admin
  database restore` put in place, and fails the command if there are none. A
  seed is never combined with `OR REPLACE`. With clustering, one member
  creates the database from the seed and the others copy it from that member:
  the system leader, which must then be able to read the seed, or the member
  that `existingDataSeedServer` names, which reads the seed, or without
  `seedURI` uses the database that `neo4j-admin database restore` prepared on
  it. `existingDataSeedServer` must name a member, and `existingData: 'use'`
  needs it or a `seedURI`. Neo4j 5.26 keeps the seeding server in the
  `seeding_servers` property of the `Database` node, as DozenDB does;
  `designated_seeder` is deprecated.
- **The default database** is the one `initial.dbms.default_database` names
  at the first start, as in Enterprise: changing the setting later changes
  nothing, where Neo4j Community makes its database the default again at
  every start. `CALL dbms.setDefaultDatabase('films')` on `system` makes
  another database the default once the default database is stopped, as in
  Enterprise; sessions that name no database, and users without a home
  database, then use it, and the old one is a database like any other,
  which may be started again or dropped. Where Enterprise's documentation is
  silent, DozenDB refuses aliases and `system`, and naming the default
  database changes nothing. The default database cannot be dropped or
  replaced. `STOP DATABASE` and `START DATABASE` apply to it; stopped, it
  stays stopped when the server restarts, after starting briefly with it
  without clustering.
- **Commands take effect once their transaction commits**, on every database
  the transaction changes, so a command that fails changes no database. A
  database that failed to start is started again by `STOP DATABASE` and
  `START DATABASE`. On shutdown, `system` stops after every other database.
- **Failed databases show as `offline`**, with why in `statusMessage`, as
  Enterprise documents a database that is not running because of a problem.
  This includes a database that failed while running, such as after a panic
  on a full disk, which Enterprise puts in quarantine (`quarantined`, until
  `dbms.quarantineDatabase` takes it out). DozenDB has no quarantine: the
  database shows as `online` again once restarted, with `STOP DATABASE` and
  `START DATABASE` or by restarting the server. Neo4j Community shows a
  database that failed while running as `online`, although it serves nothing.

## Constraints

Property existence, key and property type constraints work as in Enterprise. A
node or relationship that lacks a property that an existence or key constraint
requires, or holds a property of a type that a property type constraint does
not allow, fails its transaction with
`Neo.ClientError.Schema.ConstraintValidationFailed`, whether it is created or
its labels or properties change later. Creating such a constraint fails if
existing data violates it. Stores with these and other Enterprise constraints
can be read. Label existence and relationship endpoint label constraints, which
Cypher 5 cannot create, are refused as in Neo4j Community.

## Users

User status and home databases work as in Enterprise. A suspended user cannot
log in, as with a wrong password, and the user's open sessions lose access at
their next transaction. A user's home database is the database of the user's
sessions that name none, also through routing. `SHOW USERS` and
`SHOW CURRENT USER` show both; Neo4j Community refuses both. Only native
authentication exists: `SET AUTH` with another provider than native, and
`REMOVE AUTH`, fail.

## Roles and privileges

DozenDB implements roles and privileges as Enterprise's operations manual
describes them: the built-in roles (`PUBLIC`, `reader`, `editor`, `publisher`,
`architect`, `admin`), role commands, `GRANT`, `DENY` and `REVOKE` of DBMS,
database and graph privileges, label, relationship type and property
qualifiers, property-based rules (`FOR (n:Label) WHERE n.p = value`),
procedure, function, setting and `LOAD` privileges, `IMMUTABLE` roles and
privileges, impersonation (`IMPERSONATE`, the drivers' impersonated user),
`SHOW PRIVILEGES` with `AS COMMANDS`, and `SHOW SUPPORTED PRIVILEGES`.
Restrictions are enforced by Neo4j's kernel for Cypher, procedures, counts and
indexes alike. Messages and status codes are Neo4j's where its documentation
or code gives them.

- **The first start with roles** creates the built-in roles, gives `admin` to
  one user and leaves every other user with `PUBLIC` alone, as Enterprise
  does: the user chosen with `neo4j-admin dbms set-default-admin`, else
  `neo4j`, else the only user. With none of them the server refuses to start
  with Neo4j's message. This applies to every store without roles, such as
  Neo4j Community's; the log names the user that got `admin`. A cluster formed
  by an earlier DozenDB build gets its roles from its system leader, once;
  meanwhile logins are refused with "this server does not have the roles of
  the cluster yet".
- **Stores of Enterprise with roles** are refused: DozenDB keeps roles and
  privileges in its own nodes of the system graph and cannot read
  Enterprise's. See [From Neo4j Enterprise](#from-neo4j-enterprise).
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
- **What bypasses privileges in Enterprise bypasses them here too**:
  procedures listed in `dbms.security.procedures.unrestricted` or run with
  `EXECUTE BOOSTED`, APOC triggers and other code that opens its own
  transactions, constraint violation messages that name existing values, and
  query plans estimated from counts.

## Database aliases

DozenDB implements local and remote database aliases as Enterprise's
operations manual describes them: `CREATE`, `ALTER`, `DROP` and
`SHOW ALIASES` with `IF NOT EXISTS`, `IF EXISTS`, `OR REPLACE`, `PROPERTIES`
and parameters, the alias privileges (`CREATE`, `DROP`, `ALTER` and
`SHOW ALIAS`, `ALIAS MANAGEMENT`), and
`DROP DATABASE ... RESTRICT | CASCADE ALIASES`. Aliases are kept in the
system graph exactly as Neo4j keeps them, so Neo4j Community resolves their
names wherever a database name goes: driver sessions, `USE`, routing tables,
home databases, `SHOW DATABASES` and `SHOW DATABASE <alias>`.

- **Names.** Without composite databases, a dotted name such as
  `films.classic` is a name of its own; `SHOW ALIASES` shows `composite` as
  null. A local alias targets a standard database, never `system` or another
  alias.
- **Database commands given a local alias**, `START`, `STOP`,
  `DROP DATABASE` and `WAIT`, act on its database, as the documentation's
  "in all other Cypher commands in place of the target database" suggests;
  `CREATE OR REPLACE DATABASE` never follows an alias. `DROP DATABASE` of a
  database with local aliases fails naming them unless `CASCADE ALIASES`
  drops them too, and `CREATE OR REPLACE DATABASE` fails while they exist;
  remote aliases never block it.
- **Changes.** A transaction keeps the database its alias pointed at when it
  began. With clustering, a follower applies a change a moment after the
  leader; sessions with a bookmark of the change, and routing requests, find
  it at once.
- **A home database** set to a local alias is its database; one set to a
  remote alias is that alias, also for `ACCESS ON HOME DATABASE`.
- **`initial.dbms.default_database`** naming an alias stops the server from
  starting, with a message saying so: Neo4j does not support aliases there.
- **Privileges.** Access through a local alias is checked on its database,
  and `GRANT`, `DENY` and `REVOKE` on a local alias are refused naming the
  database. A remote alias has privileges of its own, kept by its name:
  `ALTER ALIAS` keeps them and `DROP ALIAS` removes them. Only `ACCESS`
  applies to a remote alias; other privileges on one are refused, since the
  other DBMS checks them for the user the alias names. These are DozenDB's
  answers where the documentation is silent.
- **Remote aliases** run their transactions on the DBMS at their URL, in the
  database they target, logged in as the user they name, on whichever server
  or member the client is connected to:
  - `neo4j://` URLs are routed, `bolt://` URLs direct. `+s` trusts the
    certificates that the JVM's trust store trusts and `+ssc` any
    certificate; as in Neo4j 5, there is no SSL policy for remote aliases.
    Insecure schemes need `DRIVER {ssl_enforced: false}`.
  - The alias's driver settings override `dbms.routing.driver.*`. Each version
    of an alias has its own connections: transactions that begin after
    `ALTER ALIAS` use the new URL, user or settings, while those running
    finish with the old ones. The connections of a version that changed or
    was dropped close when its transactions end, found within a minute.
  - Passwords are encrypted with a key from a keystore, under Enterprise's
    settings; see `OPERATOR_GUIDE.md`.
  - Failures there reach the client as remote execution failures,
    `Neo.ClientError.Statement.RemoteExecutionClientError`,
    `RemoteExecutionTransientError` or `RemoteExecutionFailed` with GQL
    status 50N06, 50N16 or 50N17, whose cause is the other DBMS's GQL error
    and whose message names its status, such as
    `Neo.ClientError.Statement.ArithmeticError`. A DBMS that cannot be
    reached gives 08N17, which drivers retry; an untrusted certificate
    08N16. The other DBMS's own status never reaches the client: a remote
    "not a leader" or "unauthorized" would mislead the client's driver about
    this server.
  - Queries are parsed here, as Neo4j's query router does, so a syntax error
    is this server's.
  - The other DBMS's bookmarks travel inside this one's.
  - Impersonation through a remote alias is refused, since its transactions
    run as the user it names. Administration commands cannot reach the other
    DBMS: a remote alias cannot target `system`.
  - Alias chains: routing requests for `neo4j://` URLs name the alias, so the
    other DBMS refuses to route to a remote alias of its own (08N14, inside
    a remote execution failure). `bolt://` URLs send no routing requests,
    and chains through them are not detected, as in Neo4j.
  - Their transactions do not show in `SHOW TRANSACTIONS` here, as
    documented.
- **Alias properties** are kept and shown as Enterprise does. Without
  composite databases, only `SHOW ALIASES` shows them:
  `graph.propertiesByName()` needs a composite session database.

## Clustering

DozenDB's clustering is its own design, on Apache Ratis, built to what
Enterprise documents that clients and operators see. Every database is
replicated, `system` included, each in its own Raft group of its primaries;
its secondaries pull its transactions instead. Clients see what an Enterprise
cluster shows them: routing tables with the leader as writer, failover,
bookmarks, server-side routing, and `SHOW DATABASES` with a row per member.
Members talk to each other in protocols of DozenDB's own (`FORMATS.md`), on
their Raft port (`server.dozendb.cluster.listen_address`, 7700) and their
routing connector (7688), where Enterprise uses ports 5000, 6000 and 7000, so
DozenDB and Enterprise servers cannot form a cluster together. A cluster forms
from `dbms.dozendb.cluster.initial_members`, and grows with
`dozendb.cluster.addMember`, instead of through Enterprise's discovery service;
see `OPERATOR_GUIDE.md`, "Cluster members". Enterprise's settings for its
discovery, Raft and catch-up internals (`dbms.cluster.discovery.*`,
`dbms.cluster.raft.*` and `db.cluster.raft.*` other than the leadership
settings, `dbms.cluster.network.*`, `server.cluster.listen_address` and the
other addresses, `server.directories.cluster_state`, `dbms.kubernetes.*`) are
not declared; DozenDB's own are named `*.dozendb.cluster.*`.

### What clients see

- **Writes go to the leader.** Drivers that route (`neo4j://`) get routing
  tables with the leader as writer and retry managed transactions on the new
  leader after a failover. As in Enterprise, the leader is a reader too only
  with `dbms.routing.reads_on_writers_enabled`, or while no other member can
  read the database. A new leader tells every server at once, so that
  routing tables from servers that do not host the database name it within
  moments too. A write transaction that reaches a follower runs on the leader
  when its client allows server-side routing, as in Enterprise: `neo4j://`
  drivers with `dbms.routing.default_router=SERVER`, and HTTP clients. This
  needs mutual TLS between members and their routing connector
  (`server.routing.listen_address`, port 7688) reachable from the others. A
  `bolt://` write is forwarded only with
  `dbms.dozendb.routing.forward_bolt_writes=true`, which Enterprise does not
  have. Otherwise the follower rejects the write with
  `Neo.ClientError.Cluster.NotALeader`, naming the leader's Bolt address. A
  database that does not run on the member asked, such as a stopped one, is
  routed to that member only. See `OPERATOR_GUIDE.md`, "Writes on any member".
- **Administration commands** (users, `CREATE DATABASE` and the like) run on
  the leader of `system` and reach every member; a follower forwards them
  there or rejects them, as it does writes. `CREATE`, `STOP`, `START` and
  `DROP DATABASE` apply on every server the database is placed on (see
  [Servers](#servers)), and one that was down applies the ones it missed when
  it returns.
- **`SHOW DATABASES`** lists, as Enterprise does, a row for each member a
  database runs on, on whichever member it is run: the member's `serverID`,
  Bolt `address`, `access`, `currentStatus` and `statusMessage`, the role
  `primary` or `secondary`, and `writer` true only for the leader, while the
  database is online and writable there. A member that does not answer within
  a second shows the role and status `unknown`, and no address, as in
  Enterprise. `lastCommittedTxn` and `replicationLag` are each copy's, the lag
  counted from the copy with the newest transaction; a member that does not
  answer shows neither. A standalone server shows one row, as Neo4j Community
  does.
- **Cluster status endpoints** of the HTTP server tell load balancers where
  to send writes and reads, as in Enterprise:
  `/db/<database>/cluster/writable` answers `true` with 200 OK on the member
  that leads the database, `read-only` on the other members that run it, and
  `available` on every member that runs it; otherwise each answers `false`
  with 404 Not Found. `/db/<database>/cluster/status` and
  `/dbms/cluster/status` give Enterprise's JSON, without
  `millisSinceLastLeaderMessage` and the deprecated `raftCommandsPerSecond`.
  On a secondary, `lastAppliedRaftIndex` is the Raft index of the last
  transaction it pulled. Enterprise's documentation names the health field of
  `/dbms/cluster/status` both `healthy` and `isHealthy`: DozenDB gives both.
  As in Enterprise, the endpoints need a login unless
  `dbms.security.cluster_status_auth_enabled` is `false`, and
  `server.http_enabled_modules` turns them off without
  `ENTERPRISE_MANAGEMENT_ENDPOINTS`. A server with clustering disabled has
  none: they answer 404. See `OPERATOR_GUIDE.md`, "Cluster status endpoints".
- **Linearizable reads**, DozenDB's own, are requested per transaction with
  the metadata key `dozendb.consistency` set to `linearizable`, or for every
  transaction with `dbms.dozendb.reads.default_consistency`. They apply to
  Bolt clients (drivers, Cypher Shell). The transaction waits on the member
  that runs it: the leader for a write that a follower forwards there. On a
  secondary, it asks a primary for the last transaction acknowledged before
  it, and pulls until it has it.
- **Transaction size.** A transaction can replicate at most 16 MiB of changes
  in Neo4j's command encoding. A larger one fails with
  `Neo.DatabaseError.Transaction.TransactionCommitFailed` and nothing is
  committed; split it, for example with `CALL { ... } IN TRANSACTIONS`.
- **Outcome unknown.** If the leader loses leadership or the majority while a
  commit is in flight, the transaction fails with
  `Neo.DatabaseError.Transaction.TransactionCommitFailed` and a message saying
  that it may or may not have committed. This is inherent to replication. A
  member that hands its leadership over on purpose, or stops a database it
  leads, first lets the commits under way finish.

### Running a cluster

- **Starting databases.** All members must start from identical databases. A
  new cluster starts from the databases of at most one member, or of the
  member with the lowest id if none has any. Members with databases of their
  own refuse to form a cluster together. That member creates every database
  its `system` database lists, and the others copy them: `system` and the
  default database during formation, the others as they start. Its databases
  may come from another cluster, as backups restored with `neo4j-admin
  database restore`, a dump loaded with `neo4j-admin database load` or a copy
  of the database's files: on its first start, before the others copy them,
  the member ends their history in the other cluster's Raft groups with an
  empty transaction, and records itself in `system` as the member that
  creates them, in place of the other cluster's members.
- **New databases.** One of the servers a new database is placed on creates
  its store, and the others copy it, which takes a moment: until then the
  database is not available on them. `CREATE DATABASE ... WAIT` returns once
  it is, and its leader takes writes. A new database's copy runs before it
  joins its Raft group; `SHOW DATABASES` shows it as `starting` until then.
  Stores are copied only while a database is online, so a server that lacks a
  stopped database gets it once it is started again. If the creating server
  fails before it created the store, the others wait until it returns, and
  its copies cannot move elsewhere meanwhile.
- **Members.** Members are added with `dozendb.cluster.addMember`, and
  dropped with `DEALLOCATE DATABASES FROM SERVER` and `DROP SERVER`, one at a
  time; the Raft group of each database follows the servers it is placed on.
  See `OPERATOR_GUIDE.md`. A dropped member cannot rejoin with its data: its
  data directory must be deleted first. A copy of a database that is offline
  is made once it is started again.
- **Upgrades.** DozenDB is upgraded one member at a time, mixing at most two
  versions; formats that the previous version cannot read are used only once
  an administrator calls `dozendb.cluster.finishUpgrade()`, after which no
  member can return to it. Neo4j is upgraded with every member shut down. See
  `OPERATOR_GUIDE.md`.
- **Traffic between members.** A clustered member does not start until
  `dbms.ssl.policy.cluster.enabled` is set: `true` for mutual TLS, configured
  with Neo4j's SSL policy settings of the `cluster` scope as in Enterprise, or
  `false` for plaintext. `client_auth=OPTIONAL` is refused. Certificates are
  read at start, and members accept any certificate from a trusted authority,
  whichever member presents it. See `OPERATOR_GUIDE.md`.
- **ID reuse.** IDs freed while a member is a follower are reused only after a
  restart, as in Enterprise's clustering.
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

## Servers

DozenDB shows and changes servers, and places databases on them, as Enterprise
documents: `SHOW SERVERS` with Enterprise's columns, `ENABLE SERVER`,
`ALTER SERVER`, `RENAME SERVER`, `[DRYRUN] DEALLOCATE DATABASES FROM
SERVER[S]`, `[DRYRUN] REALLOCATE DATABASES` and `DROP SERVER`; the states
`Free`, `Enabled`, `Cordoned`, `Deallocating`, `Deallocated` and `Dropped`;
the server options; `CREATE DATABASE ... TOPOLOGY` and
`ALTER DATABASE ... SET TOPOLOGY`, with a default of one primary and no
secondaries; secondaries, which pull transactions from upstreams; the settings
`initial.server.tags`, `initial.server.mode_constraint`,
`initial.server.allowed_databases`, `initial.server.denied_databases`,
`initial.dbms.default_primaries_count`,
`initial.dbms.default_secondaries_count`,
`initial.dbms.automatically_enable_free_servers`,
`server.cluster.system_database_mode`, `db.cluster.catchup.pull_interval`,
`server.cluster.catchup.upstream_strategy` with its strategies,
`server.cluster.catchup.connect_randomly_to_server_tags`,
`server.cluster.catchup.connect_randomly_to_server_group`,
`server.cluster.catchup.user_defined_upstream_strategy` and
`dbms.routing.reads_on_primaries_enabled`; and the procedures
`dbms.cluster.cordonServer`, `dbms.cluster.uncordonServer`,
`dbms.cluster.deallocateDatabaseFromServer(s)`,
`dbms.cluster.deallocateNumberOfDatabases`, `dbms.cluster.reallocateDatabase`,
`dbms.cluster.reallocateNumberOfDatabases`,
`dbms.cluster.setAutomaticallyEnableFreeServers`,
`dbms.setDefaultAllocationNumbers`, `dbms.showTopologyGraphConfig`,
`dbms.cluster.secondaryReplicationDisable` and its deprecated name
`dbms.cluster.readReplicaToggle`. See `OPERATOR_GUIDE.md`, "Servers",
"Placing databases" and "Secondaries". Where DozenDB differs, or decides what
Enterprise does not document:

- `ALTER DATABASE ... SET TOPOLOGY` without `SECONDARIES` keeps the number of
  secondaries; `CREATE DATABASE ... TOPOLOGY` without it takes the default
  number.
- A copy whose server loses its mode but may host the other keeps its store
  and changes its mode in place, both ways, primary to secondary once its
  replacement votes. A server changes how it hosts `system` when restarted
  with the other `server.cluster.system_database_mode` and added again with
  `dozendb.cluster.addMember`; until then it keeps its mode and logs why.
  Enterprise documents mode changes of `system` only through restarts and
  disaster recovery.
- Secondaries move as primaries do, the new copy first.
- A secondary that finds no upstream that answers retries every pull
  interval; `SHOW DATABASES` shows it online, its lag growing.
  `replicationLag` counts the transactions behind the copy with the newest, as
  a negative number, as Neo4j's listing page for 5.26 defines it.
- `dbms.cluster.secondaryReplicationDisable` returns `state`, `paused` or
  `running`. A pause ends when the database or the server starts again, so
  that a forgotten pause does not stop a secondary for good, and routing
  tables leave paused secondaries out, so that drivers get no data they
  stopped updating; `bolt://` connections still reach them.
- Strategies are also accepted as `connect-randomly-to-server-with-tag`, the
  name of Enterprise's settings page, and `user_defined`. A comma between the
  rules of `user_defined_upstream_strategy`, which one Enterprise example has,
  is refused, as its meaning is not documented. The deprecated group
  strategies log a warning at start.
- A server with `modeConstraint: 'SECONDARY'` cannot be the
  `existingDataSeedServer` of `CREATE DATABASE`: the seed server creates the
  database, which only a primary does.
- `dbms.cluster.minimum_initial_system_primaries_count` is not supported: a
  cluster forms from `dbms.dozendb.cluster.initial_members`, which must not
  name a server with `server.cluster.system_database_mode=SECONDARY`.
- Enterprise refuses to take a database with more than one primary down to
  one, and to deallocate a server that hosts a database's only primary.
  DozenDB does both, as every move makes the new copy before it removes the
  old one.
- Enterprise does not document where it places copies; DozenDB's placement is
  deterministic (see `OPERATOR_GUIDE.md`, "Where copies go").
  `dbms.showTopologyGraphConfig` shows `allocator` as null.
- `DROP SERVER` also refuses a deallocating server that still votes in a
  database's Raft group, as the leader of `system` knows it, also while the
  server is down, so that a database whose only copy is there is not lost.
- A server joins with `dozendb.cluster.addMember`, after which it is `Free`;
  a `Free` server votes in the Raft group of `system`, as every server does
  that hosts `system` as a primary. A server that hosts `system` as a
  secondary is added on the leader of `system`.
- A cluster recovered from backups places each database again on as many
  servers as its topology asks for, or on the default number if that is
  larger, as a standalone server records one primary for each of its
  databases.

## Leadership balancing

DozenDB balances the leaderships of databases over the members as Enterprise
documents, with its setting
`dbms.cluster.raft.leader_transfer.balancing_strategy`: `EQUAL_BALANCING`, the
default, moves leaders so that every member leads as many databases as the
others, and `NO_BALANCING` leaves them where elections put them. DozenDB makes
the setting dynamic. As in Enterprise, a member hands over the leadership of a
database that is read-only on it (`server.databases.read_only` and the like).
See `OPERATOR_GUIDE.md`, "Where databases are led".

Where Enterprise's documentation says nothing, DozenDB's choices:

- Balancing waits until the leaders have stayed put on every member for 10
  seconds, and moves one leadership at a time, from the member leading the
  most databases to the one leading the fewest, only between members that
  lead at least two apart. Every database counts the same, `system` too.
- A member that stops a database it leads, or stops itself, hands each
  leadership to the member leading the fewest databases, also with
  `NO_BALANCING`.
- A member that hands a leadership over on purpose lets the commits under way
  finish first; commits meanwhile fail with `NotALeader`, which drivers retry.
  No handover leaves a commit with an unknown outcome.
- Routing tables list no writer while a database is read-only on its leader. A
  database that is read-only on every member keeps its leader, whose writes
  fail.

A database's priority tag, a server tag that
`db.cluster.raft.leader_transfer.priority_tag` or
`db.cluster.raft.leader_transfer.priority_tag.<database>` names, takes its
leadership to a member with the tag, as in Enterprise, and excludes it from
balancing. Where Enterprise's documentation says nothing, DozenDB moves the
leadership to the member with the tag that leads the fewest databases, counts
a database with a priority tag when it balances the others, and leaves the
leadership where it is while no member with the tag can take it over. A
database whose name contains a dot cannot have a priority tag of its own:
Neo4j reads the setting's name only up to the dot, and ignores the setting.

## Backup and restore

Backups and restores use Enterprise's commands, options and settings:
`neo4j-admin database backup`, `restore` and `aggregate-backup`,
`neo4j-admin backup inspect`, `neo4j-admin database check --from-path`,
`server.backup.enabled`, `server.backup.listen_address` and
`dbms.ssl.policy.backup.*`. They work alike on standalone servers and cluster
members; see `OPERATOR_GUIDE.md`, "Backup and restore". A backup artifact is a
file of DozenDB's own format, which Neo4j Community's `neo4j-admin database
load` also loads as a dump when it is a full backup. Exchanging backups with
Enterprise is not promised. The differences:

- There are no cloud locations (`s3:`, `gs:`, `azb:`).
- `--parallel-recovery` and `--inspect-path` are not supported.
- The script that `--include-metadata` keeps, and `restore` writes to
  `data/scripts/<database>/restore_metadata.cypher`, is DozenDB's own.
- `neo4j-admin backup inspect` without `--show-metadata` lists only the files.
- Backup files are named in UTC.
- `--temp-path` of `backup` defaults to `--to-path`, and `restore` accepts
  `--temp-path` without using it.
- A backup is not restored with another Neo4j kernel version than the one it
  was taken with.
- Serving backups on an address that is not a loopback one needs
  `dbms.ssl.policy.backup.enabled` set explicitly, `true` or `false`, since
  the backup port has no other authentication.

## Metrics and logs

DozenDB reports metrics and writes the query log and the security log as
Enterprise's operations manual describes them; Neo4j Community declares the
query log settings and ships the log files' configuration, but writes neither
log. `OPERATOR_GUIDE.md` describes how to use them.

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
  (7 files of 20 MB). A server moving from Neo4j Community starts writing
  `query.log`, which costs up to 12% of the throughput of trivial queries
  (`OPERATOR_GUIDE.md`). Set `INFO` with a `db.logs.query.threshold`, or
  `OFF`, to log less.
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
- **`dbms.setConfigValue`** changes a dynamic setting as in Enterprise: for
  administrators, on the member it runs on, until that member restarts, and
  only a setting that `server.dynamic.setting.allowlist` names. Neo4j
  Community has neither.

## Not supported

DozenDB does not support these Enterprise features, checked against the
Enterprise-only features, procedures, `neo4j-admin` commands and settings of
Enterprise's 5.26 documentation. Unless an item says otherwise, their
settings are not declared, so a configuration that names one stops the server
at startup with "Unrecognized setting", as Neo4j does by default
(`server.config.strict_validation.enabled`); their commands fail with an
error, and their procedures do not exist.

- **Composite databases.** `CREATE COMPOSITE DATABASE` and the other commands
  for composite databases fail. A dotted name is a name of its own (see
  [Database aliases](#database-aliases)).
- **LDAP, single sign-on and Kerberos.** Users log in with native users and
  passwords only: `dbms.security.authentication_providers`,
  `authorization_providers`, `dbms.security.ldap.*`, `dbms.security.oidc.*`,
  the auth cache settings and `dbms.security.clearAuthCache`.
- **Change data capture**: `txLogEnrichment`, `db.cdc.*` and `cdc.*`.
- **The `block` and `high_limit` store formats.** As Neo4j Community, DozenDB
  reads and creates only the `aligned` and `standard` formats.
- **The pipelined and parallel runtimes.** A query that asks for one runs on
  Neo4j Community's slotted runtime with a notification, as in Neo4j
  Community, or fails with `dbms.cypher.hints_error=true`. Neo4j Community
  declares `server.cypher.parallel.worker_limit`, which has no effect.
- **Disaster recovery and cluster checks**: quarantine
  (`dbms.quarantineDatabase`), `dbms.cluster.recreateDatabase`,
  `dbms.cluster.statusCheck`, `dbms.cluster.checkConnectivity`,
  `dbms.cluster.protocols` and `neo4j-admin dbms unbind-system-db`.
  `neo4j-admin server unbind` removes only the server's id, as in Neo4j
  Community, and none of DozenDB's cluster state: the server then refuses to
  start, saying how to start it under its id again. `OPERATOR_GUIDE.md`
  describes how a DozenDB cluster recovers, and "Lost databases" under
  [Running a cluster](#running-a-cluster) how to rebuild a member.
- **Routing policies**: `dbms.routing.load_balancing.plugin`, its server
  policies and `dbms.routing.load_balancing.shuffle_enabled`.
- **Shutting the server down on a panic**
  (`server.panic.shutdown_on_panic`): a database that panics fails on its
  own, see [Databases](#databases).
- **Store copy and incremental import**: `neo4j-admin database copy`,
  `neo4j-admin database import incremental`, and resuming a stopped import.
  `neo4j-admin` lacks them, as in Neo4j Community.
- **Page cache warmup.** Neo4j Community declares
  `db.memory.pagecache.warmup.*`, which have no effect.
- **The `CONTINUOUS` and `VOLUMETRIC` checkpoint policies.** As in Neo4j
  Community, `db.checkpoint` with either logs a warning and checkpoints
  periodically.
- **Shared query caches**: `server.memory.query_cache.sharing_enabled` and
  `shared_cache_num_entries`.
- **Cloud storage** for seeds, backups and `LOAD CSV`
  (`dbms.integrations.cloud_storage.*`).
- **CSV and Graphite metrics** (`server.metrics.csv.*`,
  `server.metrics.graphite.*`, `server.directories.metrics`).
- **Monitoring procedures**: `dbms.listPools`, `db.listLocks`,
  `dbms.listActiveLocks`, `dbms.scheduler.jobs`, `groups` and `failedJobs`,
  `db.checkpoint` and `dbms.checkConfigValue`.
- **Settings of features DozenDB has in its own way**: `server.max_databases`,
  `initial.dbms.database_allocator` and `dbms.setDatabaseAllocator`, the
  deprecated `server.groups` and
  `db.cluster.raft.leader_transfer.priority_group`,
  `server.backup.exec_connector.*` and
  `server.backup.store_copy_max_retry_time_per_request`.
- **Neo4j Browser's Enterprise settings**: `browser.allow_outgoing_connections`,
  `credential_timeout`, `retain_connection_credentials` and
  `retain_editor_history`.
- **FIPS 140 compliance.**
- **Neo4j's separately licensed products**, such as Graph Data Science
  Enterprise Edition and Neo4j Operations Manager, are not tested with
  DozenDB.

## Moving to DozenDB

### From Neo4j Community

Neo4j Community of the pinned version, with the DozenDB jar in its `lib/`,
starts on Neo4j Community's data directory as it is. Its first start creates
the built-in roles and gives `admin` to one user (see
[Roles and privileges](#roles-and-privileges)). Other changes the server
shows: it writes the query log, at Neo4j's declared default `VERBOSE` (see
[Metrics and logs](#metrics-and-logs)); failed databases show as `offline`;
`dbms.components()` reports the Enterprise edition; and the default database
changes with `dbms.setDefaultDatabase`, no longer with
`initial.dbms.default_database`.

### From Neo4j Enterprise

- **Databases** move as dumps (`neo4j-admin database dump`), created on
  DozenDB from those (`OPERATOR_GUIDE.md`, "Creating a database from a backup
  or a dump"). Migrate a database in another store format than `aligned` or
  `standard` first, with `neo4j-admin database migrate --to-format=aligned`.
- **Roles and privileges** move as commands: DozenDB refuses to start on a
  `system` database that holds Enterprise's roles. `OPERATOR_GUIDE.md`,
  "Moving roles from or to Neo4j Enterprise", has the steps, both ways.
- **Backups** are not promised to be exchangeable.
- **Settings** of the features DozenDB does not support, Enterprise's cluster
  discovery settings among them, stop the server at startup: remove them.
- **Clusters** are formed anew: DozenDB and Enterprise servers cannot form a
  cluster together.

### From DozerDB

DozenDB began as an add-on to [DozerDB](https://dozerdb.org) and contains a
port of the DozerDB code it built on (`UPSTREAM_TOUCHPOINTS.md`). Neo4j
Community with the DozenDB jar starts on the data directory of DozerDB
5.26.27.0 as it is: databases, users and constraints carry over, and the
first start gives `admin` to one user as it does on Neo4j Community's.
Compared with DozerDB 5.26.27.0, as verified against the
`graphstack/dozerdb:5.26.27.0` image:

- `ALTER USER` works; DozerDB fails every `ALTER USER` with an
  `UnsatisfiedDependencyException` for `UserSecurityGraphComponent`.
- User status and home databases take effect; DozerDB stores both but acts
  on neither, so a suspended user still logs in.
- Roles and privileges work; DozerDB refuses them, and every user can do
  everything.
- `START DATABASE` of a database that does not exist fails, where DozerDB
  succeeds. Parameters such as `START DATABASE $name`, the embedded
  `DatabaseManagementService` methods, names with a dot, and
  `CREATE OR REPLACE DATABASE` of a database that does not exist yet work,
  where DozerDB fails on them.
- `WAIT` works, and composite databases, `DUMP DATA` and options DozenDB does
  not support fail with an error, where DozerDB ignores them or fails with an
  internal error.
- `dbms.max_databases` counts every database, where DozerDB allows one fewer.
- The default database cannot be dropped or replaced. DozerDB removes it from
  the system graph but keeps it running, until a restart adds it back. A
  stopped default database stays stopped when the server restarts, where
  DozerDB leaves it running.
- A command takes effect once it commits. DozerDB drops a database before the
  command commits, so a command that then fails has still dropped it.
- Failed databases show as `offline`, where DozerDB shows a database that
  failed while running as `online`.
- Label existence and relationship endpoint label constraints are refused,
  where DozerDB creates them and does not enforce them.
- DozenDB writes the query log (see [Metrics and logs](#metrics-and-logs)),
  and logs its version at startup instead of DozerDB's banner.
