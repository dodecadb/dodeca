# Compatibility

Dodeca is a jar for the `lib/` of Neo4j Community Edition that adds much of
what Neo4j otherwise keeps for its Enterprise Edition. It is an independent
project, not affiliated with or endorsed by Neo4j, Inc. (see
[Trademarks](../README.md#trademarks)). This document
compares Dodeca with Neo4j Enterprise 2026.10, area by area: what works as in
Enterprise, where Dodeca differs, what it decided where Enterprise's
documentation says nothing, and what it does not support. Dodeca is built from
that documentation alone, Enterprise's operations manual and Cypher manual, so
"as in Enterprise" means as the documentation describes. "Neo4j Community"
means Neo4j Community Edition of the pinned version without Dodeca.

Neo4j 2026 runs queries in Cypher 5 or Cypher 25, as a `CYPHER 5` or `CYPHER 25`
prefix chooses, or else the database's default version (see
[Databases](#databases)). Where Enterprise answers
differently in the two, the sections below say what Dodeca does.

## Pinned versions

Each Dodeca build is pinned to exactly one Neo4j version. A member refuses to
form, join or rejoin a cluster whose members run a different Neo4j version.

| Component | Version | Notes |
|---|---|---|
| Neo4j | 2026.10.2 (Community) | Unmodified; the Dodeca jar goes into its `lib/`. |
| Test image | `neo4j:2026.10.2-community` | Neo4j's official image: test baseline and installation target. `StandaloneIT` compares Dodeca, with clustering disabled, against it. |
| Apache Ratis | 3.3.1 | Uses the already shaded `ratis-thirdparty` 1.1.0 for gRPC, Netty and protobuf. |
| Kotlin | 2.4.21, kotlinx-coroutines 1.11.0 | Bundled and relocated to `dev.dodeca.shaded`. |
| Dropwizard Metrics | 4.2.40 | `metrics-core` and `metrics-jmx`, for the metrics and their exporters. Bundled and relocated. |
| AWS SDK for Java | 2.58.0 | S3, with Apache HttpClient 5. Bundled and relocated, as every cloud SDK and the libraries they use. |
| Google Cloud Storage | 2.78.0 (`libraries-bom` 26.93.0) | Over HTTP: without its gRPC transport. |
| Azure SDK | `azure-sdk-bom` 1.3.10: Storage Blob 12.37.2, Identity 1.19.0 | With the JDK's HTTP client. Uses the JNA that Neo4j ships. |
| JDK | build 21, run 21 or 25 | Bytecode targets Java 21 like Neo4j 2026, which runs on Java 21 and 25. |

## At a glance

*Same* means as Enterprise documents it, with Dodeca's answers where the
documentation is silent. *Same, with differences* means that the section
linked lists the differences. *Own design* means that Dodeca reaches the same
end its own way.

| Enterprise feature | Dodeca | Section |
|---|---|---|
| Many databases: `CREATE`, `DROP`, `START` and `STOP DATABASE`, `WAIT` | Same, with differences | [Databases](#databases) |
| Creating a database from a seed | Same, with differences | [Databases](#databases) |
| Default Cypher versions of databases and aliases (`DEFAULT LANGUAGE`) | Same | [Databases](#databases) |
| Composite databases | Not supported | [Not supported](#not-supported) |
| Replica databases (cross-cluster database replication) | Not supported | [Not supported](#not-supported) |
| Sharded property databases | Not supported | [Not supported](#not-supported) |
| Property existence, key and type constraints | Same | [Constraints](#constraints) |
| Graph types | Not supported | [Not supported](#not-supported) |
| User status and home databases | Same | [Users](#users) |
| Users and roles as commands (`SHOW USERS AS COMMANDS`, `SHOW ROLES AS COMMANDS`) | Same | [Users](#users) |
| Roles and privileges, impersonation | Same, with differences | [Roles and privileges](#roles-and-privileges) |
| User tags (`SET TAGS`, `ADD TAGS`, `ALTER USERS`) | Same | [Users](#users) |
| Attribute-based access control: auth rules, user tags in property rules | Same, with native user tags | [Attribute-based access control](#attribute-based-access-control) |
| LDAP, single sign-on and Kerberos | Not supported | [Not supported](#not-supported) |
| Local and remote database aliases | Same, with differences | [Database aliases](#database-aliases) |
| Clustering: replication, failover, routing | Own design, same for clients | [Clustering](#clustering) |
| Server-side routing | Same | [Clustering](#clustering) |
| Routing policies (the load balancing plugin) | Same, with differences | [Routing policies](#routing-policies) |
| Cluster status endpoints of the HTTP server | Same, with differences | [Clustering](#clustering) |
| Checking connectivity between members | Same, with differences | [Clustering](#clustering) |
| Protocols between members (`dbms.cluster.protocols`) | Same, with differences | [Clustering](#clustering) |
| Replication status checks | Same, with differences | [Clustering](#clustering) |
| Servers, topologies and secondaries | Same, with differences | [Servers](#servers) |
| Leadership balancing and priority tags | Same | [Leadership balancing](#leadership-balancing) |
| Online backup and restore | Same, with differences | [Backup and restore](#backup-and-restore) |
| Cloud storage: Amazon S3, Google Cloud Storage, Azure Blob Storage | Same, with differences | [Cloud storage](#cloud-storage) |
| Metrics: CSV files, Graphite, Prometheus, JMX | Same, with differences | [Metrics and logs](#metrics-and-logs) |
| Checkpoint policies and the checkpoint I/O limits | Same, with differences | [Checkpoints](#checkpoints) |
| Page cache warmup | Same, with differences | [Page cache warmup](#page-cache-warmup) |
| Query log and security log | Same | [Metrics and logs](#metrics-and-logs) |
| Monitoring procedures: locks, memory pools, job scheduler, checkpoints | Same | [Metrics and logs](#metrics-and-logs) |
| Changing settings while running | Same | [Metrics and logs](#metrics-and-logs) |
| Reloading TLS certificates while running | Same, with differences | [Running a cluster](#running-a-cluster) |
| Changing the default database | Same | [Databases](#databases) |
| Disaster recovery: `unbind-system-db`, recreating databases, quarantine | Same, with differences | [Disaster recovery](#disaster-recovery) |
| Change data capture | Not supported | [Not supported](#not-supported) |
| `block` and `high_limit` store formats | Not supported | [Not supported](#not-supported) |
| Pipelined and parallel runtimes | Not supported | [Not supported](#not-supported) |
| Incremental import (`neo4j-admin database import incremental`) | Same, with differences | [Import and copy](#import-and-copy) |
| Resuming a stopped full import | Same, with differences | [Import and copy](#import-and-copy) |
| Store copy (`neo4j-admin database copy`) | Same, with differences | [Import and copy](#import-and-copy) |
| Other procedures and settings only Enterprise has | Not supported | [Not supported](#not-supported) |

## Edition

Dodeca reports the Enterprise edition, in `CALL dbms.components()` for
example, so that tools offer what Dodeca adds to Neo4j Community, such as many
databases. [Not supported](#not-supported) says how Dodeca answers the
Enterprise features it lacks.

Neo4j's anonymous usage data (`dbms.usage_report.enabled`), which Neo4j sends
to udc.neo4j.com unless told not to, is off in Dodeca unless the operator
turns it on. Turned on, it reports the server as Neo4j Enterprise, as
`dbms.components()` does.

Neo4j 2026's fleet discovery (`server.fleet_discovery.enabled`), which
announces each server on the local network every 30 seconds, with its version
and edition, is off in Dodeca for the same reason unless the operator turns it
on. Neo4j's Fleet Manager (`dbms.fleet_manager.enabled`) sends nothing until an
operator registers a token with `fleetManagement.registerToken`; once it does,
it reports the server as Neo4j Enterprise too.

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
- **Not supported:** `CREATE COMPOSITE DATABASE`, `CREATE REPLICA DATABASE`,
  `ALTER DATABASE ... SET REPLICAS`, `DROP DATABASE ... DUMP DATA`,
  `ALTER DATABASE ... SET OPTION`, and `CREATE DATABASE ... OPTIONS`
  with an option other than `existingData`, `seedURI`, `seedRestoreUntil`,
  `seedSourceDatabase` (Cypher 25 only, as in Enterprise), `storeFormat`
  and, with clustering, `existingDataSeedServer` or
  `existingDataSeedInstance`: `seedConfig`, `seedCredentials`,
  `txLogEnrichment` or a misspelt key. Each fails with an error. To keep a
  database's data, back it up, or stop it and dump it with `neo4j-admin
  database dump`, before dropping it.
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
  In Cypher 25 each server also gets a notification, as in Enterprise:
  `03N85` caught up, `01N80` failed, `01N81` still catching up when the time
  limit passed, and `01N82` not available, for a server that could not be
  asked. They name the server as `SHOW SERVERS` does.
- **The default Cypher version** of a database is the version its queries run
  in without a `CYPHER 5` or `CYPHER 25` prefix, as in Enterprise:
  `CREATE [OR REPLACE] DATABASE ... DEFAULT LANGUAGE CYPHER 25` (`SET DEFAULT
  LANGUAGE` in Cypher 25) gives it, and `ALTER DATABASE ... SET DEFAULT
  LANGUAGE`, on `system` too, changes it. A database created without one
  keeps `db.query.default_language` as it was then: changing the setting
  changes no database that exists. `SHOW DATABASES ... YIELD defaultLanguage`
  shows it. Every server runs queries in the new version a moment after it
  has the change, as Enterprise documents ("expect a short delay").
- **Creating a database from a seed**, `CREATE DATABASE ... OPTIONS
  {existingData: 'use', seedURI: '...'}`, works as in Enterprise with a dump or
  a full backup as the seed, from a file, a URL or cloud storage (see [Cloud
  storage](#cloud-storage)). `dbms.databases.seed_from_uri_providers` enables
  `FileSeedProvider` (`file:`), `CloudSeedProvider` (`s3:`, `gs:`, `azb:`) and
  `ServerSeedProvider` (`server:`) by default, where Enterprise enables
  `CloudSeedProvider` alone since 2025.01, and also takes `S3SeedProvider` and
  `URLConnectionSeedProvider` (`http:`, `https:`, `ftp:`); other names fail
  the server's start. `server://<server id or name>/<artifact>` seeds from a
  dump or backup in `server.directories.seeds` of that server, and
  `server://<server id or name>/` from the latest chain there of the database
  of the same name, or of `seedSourceDatabase`, as in Enterprise 2026.04; the
  server named reads it and creates the database, which the others copy, in
  `CREATE DATABASE` and `dbms.recreateDatabase`. Dodeca keeps the URI with
  the server's id, so that `RENAME SERVER` does not change the seed. As a Dodeca
  extension, a seed read by `FileSeedProvider` or `CloudSeedProvider` may also
  be a differential backup, restored with the backups it continues in its
  folder, or a folder of backups, whose latest chain is restored, as Enterprise
  documents for cloud seeds. `seedRestoreUntil`, a transaction id or a
  datetime, restores the seed up to that point, with `FileSeedProvider` and
  `CloudSeedProvider` only, as in Enterprise; with another provider the
  command fails. `seedSourceDatabase`, in Cypher 25 only, picks the backups of
  that database in a folder of several databases' backups, as in Enterprise;
  a backup file of another database then fails the database, and a dump,
  which names no database, is taken as it is. A URI whose scheme no enabled
  provider reads fails the command; a seed that cannot be read fails the
  database, which `WAIT` and `SHOW DATABASES` show, and the next start of the
  server tries it again.
  `existingData: 'use'` without a seed uses the files that `neo4j-admin database
  restore` put in place, and fails the command if there are none. `CREATE OR
  REPLACE DATABASE` takes a seed, as Enterprise does since 2025.01: the
  database is dropped, its files too, and created from the seed. As in
  Enterprise, it takes no `existingDataSeedServer`, and `existingData: 'use'`
  needs a `seedURI` there. With clustering, one member creates the
  database from the seed and the others copy it from that member: the system
  leader, which must then be able to read the seed, or the member that
  `existingDataSeedServer` names, which reads the seed, or without `seedURI`
  uses the database that `neo4j-admin database restore` prepared on it.
  `existingDataSeedServer` must name a member, and `existingData: 'use'` needs
  it or a `seedURI`. Neo4j keeps the seeding server in the
  `seeding_servers` property of the `Database` node, as Dodeca does;
  `designated_seeder` is deprecated.
- **The default database** is the one `initial.dbms.default_database` names
  at the first start, as in Enterprise: changing the setting later changes
  nothing, where Neo4j Community makes its database the default again at
  every start. `CALL dbms.setDefaultDatabase('films')` on `system` makes
  another database the default once the default database is stopped, as in
  Enterprise; sessions that name no database, and users without a home
  database, then use it, and the old one is a database like any other,
  which may be started again or dropped. Dodeca refuses an alias, which
  Enterprise takes as the default database since 2025.04, and, where
  Enterprise's documentation is silent, `system`; naming the default
  database changes nothing. The default database cannot be dropped or
  replaced, but `dbms.recreateDatabase` recreates it under a new id
  as any other database (see [Disaster recovery](#disaster-recovery)).
  `STOP DATABASE` and `START DATABASE` apply to it; stopped, it
  stays stopped when the server restarts, after starting briefly with it
  without clustering.
- **Commands take effect once their transaction commits**, on every database
  the transaction changes, so a command that fails changes no database. A
  database that failed to start is started again by `START DATABASE` once
  what failed it is gone, as Enterprise documents for retrying a start; other
  commands leave it as it is. Dodeca starts it as a new database object, as
  Neo4j 2026 cannot start one again whose start failed. In a cluster, a member
  whose copy failed to start hands the leadership of the database's group
  over, so that the others keep taking writes. On shutdown, `system` stops
  after every other database.
- **Failed databases show as `offline`**, with why in `statusMessage`, as
  Enterprise documents a database that is not running because of a problem,
  such as one that failed to start. A database that fails while running, such
  as after a panic on a full disk, is put in quarantine, as in Enterprise:
  `quarantined`, until `dbms.unquarantineDatabase` lifts it (see
  [Disaster recovery](#disaster-recovery)). Neo4j Community shows a database
  that failed while running as `online`, although it serves nothing.
- **`server.panic.shutdown_on_panic`** ends the server's process, with status
  1, once a database panics, as in Enterprise, so that a process manager
  starts it again. It is true by default, as in Enterprise since 2025.01: set
  it to `false` to keep the server running, the database in quarantine.

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
`SHOW CURRENT USER` show both, and the user's roles; Neo4j Community refuses
to set either, and its `SHOW CURRENT USER` shows null for them and for the
roles. Only native
authentication exists: `SET AUTH` with another provider than native, and
`REMOVE AUTH`, fail (see [Not supported](#not-supported)).

User tags work as in Enterprise 2026.06: `SET TAGS` replaces a user's tags,
`ADD TAGS` adds to them, and `REMOVE TAGS` and `REMOVE ALL TAGS` remove them,
in `CREATE USER`, `ALTER USER`, and `ALTER USERS`, which alters several users
at once and with `IF EXISTS` leaves out those that do not exist. Changing
tags takes `SET USER METADATA`. `SHOW USERS`, `SHOW CURRENT USER` and `SHOW
USERS AS COMMANDS` show them only with `SHOW USER METADATA`, and null
otherwise. `USER METADATA MANAGEMENT` includes both, and `ALL DBMS
PRIVILEGES`, so `admin`, includes it; `USER MANAGEMENT` does not. Where the
docs say nothing: tags are Cypher 25's only, as Neo4j's parser has them; each
tag is kept once, in the order it was first set or added; `ALTER USERS`
changes all its users or, if one does not exist, none; a parameter must be a
non-empty string or a list of them, as literals must; and backups with
`--include-metadata` keep the tags of users.

In Cypher 25, `SHOW USERS [WITH AUTH] AS COMMANDS` gives the `CREATE USER`
commands that make the users again, with their encrypted passwords (`SET
ENCRYPTED PASSWORD`), status and home database, as Enterprise 2026.09 does.
It takes `SHOW USER` and `SHOW USER CREDENTIALS`. `USER MANAGEMENT`, and so
`ALL DBMS PRIVILEGES` and `admin`, include `SHOW USER CREDENTIALS`, as Neo4j's
kernel has it; the docs do not say. Cypher 5 has no `AS COMMANDS` for users
or roles.

## Roles and privileges

Dodeca implements roles and privileges as Enterprise's operations manual
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
  by an earlier Dodeca build gets its roles from its system leader, once;
  meanwhile logins are refused with "this server does not have the roles of
  the cluster yet".
- **Stores of Enterprise with roles** are refused: Dodeca keeps roles and
  privileges in its own nodes of the system graph and cannot read
  Enterprise's. See [From Neo4j Enterprise](#from-neo4j-enterprise).
- **Auth disabled** allows everything, `IMMUTABLE` commands included, as in
  Neo4j; this is also how a locked-out admin recovers.
- **Where the documentation is silent**, Dodeca takes the stricter answer: a
  node without labels is found, created and deleted only through privileges
  on all labels (`NODES *`); creating or deleting a node, and setting its
  properties, needs the privilege on all labels or on each of the node's
  labels; a privilege Dodeca cannot read applies to nothing when granted and
  to everything in its scope when denied, and is logged; `SHOW SETTINGS`
  lists nothing without `SHOW SETTING`; any applying `DENY LOAD` blocks
  `LOAD CSV`; `GRANT` and `DENY` on a database that does not exist, or with a
  database parameter of `*`, fail, where `REVOKE` only warns in Cypher 5; and
  impersonating oneself needs `IMPERSONATE` too.
- **`REVOKE` of what cannot exist** (a role, user or database that does not
  exist, or a database parameter of `*`) fails in Cypher 25, as Enterprise
  documents, with Neo4j's errors (`42N10`, `42N09`, `42N00` and `42N86`), and
  changes nothing. Cypher 5 warns, with `01N70`. Revoking what a role or user
  does not have is a notification in both, `00N71`.
- **Property-based rules** are evaluated when granted: `date()` is the date
  of the `GRANT`. A negated rule is kept as the opposite comparison
  (`NOT n.p > 1` as `n.p <= 1`), which differs only for NaN values. A `DENY`
  rule applies only to nodes whose value satisfies it, so it does not apply
  to nodes without the property, as documented. A null or NaN value, a list
  of mixed types and `IN` without a list are refused, parameters included.
  Rules cost a read of the property for each node: in a benchmark, scanning a
  million nodes took 2.5 times as long with a rule, 1.8 times with a label
  denied. Rules on relationships (`FOR ()-[r:T]-() WHERE r.p > 1`), as
  Enterprise has them since 2025.04, work as those on nodes, with the type in
  place of the labels. In Cypher 25 the property may come second, as in `1 <
  n.p`, which Neo4j turns around (`n.p > 1`), and a rule may look for a value
  in a list property, as in `'EU' IN n.regions` or `NOT 'EU' IN n.regions`,
  both as Enterprise does since 2026.08. `NOT` applies only to elements whose
  property is a list, as Neo4j's operator has it. Rules with the tags of
  users (2026.10) are described under
  [Attribute-based access control](#attribute-based-access-control).
- **`SHOW PRIVILEGES`** names the home database graph `HOME`, a `LOAD`
  privilege's segment `ALL DATA` or `CIDR('range')`, and keeps `IMMUTABLE` in
  `AS COMMANDS`. `SHOW SUPPORTED PRIVILEGES` gives the documented rows
  verbatim and describes the others in Dodeca's words. It lists `show user
  credentials`, which the docs' example of 2026.10 lacks, as it lists every
  privilege `GRANT` takes. `DROP ROLE` removes the role from its users.
- **`ALTER DATABASE`, `SET DATABASE ACCESS` and `SET DATABASE DEFAULT
  LANGUAGE`** are granted on the DBMS, and in Cypher 25 also on databases and
  the home database, as in Enterprise 2025.08; `ALL DATABASE PRIVILEGES` does
  not include them. Granted on a database, `ALTER DATABASE` and `SET DATABASE
  ACCESS` show it in `SHOW DATABASES`, as on the DBMS they show every
  database; `SET DATABASE DEFAULT LANGUAGE` shows none. Neo4j keeps them on
  the DBMS as on every database, so `AS COMMANDS` gives them `ON DBMS` in
  Cypher 5 and `ON DATABASE *` in Cypher 25.
- **`SHOW ... PRIVILEGES AS [REVOKE] COMMANDS`** in Cypher 25 has
  Enterprise's columns `command`, `access`, `action`, `roles` and `immutable`,
  and `users` for the privileges of users: each command once, with the roles
  that hold it, and for users and auth rules with `$role` in place of the
  role and the users or rules that have it (`users`, `authRules`). Cypher 5
  shows `command` and `immutable` only.
- **`SHOW ROLES [WITH USERS] AS COMMANDS`** in Cypher 25 gives a `CREATE
  ROLE` command per role, and with users a `GRANT ROLE ... TO USER ...`
  command per role and user, as in Enterprise 2026.09; with auth rules, a
  `GRANT ROLE ... TO AUTH RULE ...` command per role and rule. `PUBLIC` never
  appears, and roles without users have no `GRANT ROLE` command. Where the
  docs say nothing: an immutable role's command is `CREATE IMMUTABLE ROLE`,
  and the rows come in the docs' example order, `admin`, `architect`,
  `publisher`, `editor`, `reader`, then the other roles by name.
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

## Attribute-based access control

Auth rules give users roles by their attributes, as Enterprise 2026.10 has
them with its native provider, whose attributes are the tags of users (see
[Users](#users)). In Cypher 25: `CREATE [OR REPLACE] AUTH RULE name [IF NOT
EXISTS] SET CONDITION ... [SET ENABLED ...]`, `ALTER`, `RENAME` and `DROP AUTH
RULE`, `GRANT` and `REVOKE ROLE ... TO` or `FROM AUTH RULE`, `SHOW AUTH RULES
[AS COMMANDS]`, `SHOW ROLES WITH AUTH RULES [AS COMMANDS]` and `SHOW AUTH RULE
... PRIVILEGES [AS COMMANDS]`, with the privileges `AUTH RULE MANAGEMENT`,
`CREATE`, `RENAME`, `ALTER`, `DROP` and `SHOW AUTH RULE`; `GRANT` and `REVOKE
ROLE` take `ASSIGN ROLE` and `REMOVE ROLE`. A condition is a Cypher expression
of `abac.native.user_tags()` and the functions Neo4j allows there, such as
`'finance' IN abac.native.user_tags() AND time.transaction('UTC').hour < 18`.
Property rules compare with the tags as well:
`GRANT READ {*} ON GRAPH * FOR (n:Document) WHERE n.classification IN
abac.native.user_tags() TO regularUsers`.

- **`dbms.security.abac.authorization_providers=native`** turns it on, as in
  Enterprise, where the setting is empty by default. Without it, auth rules
  give no roles, and property rules with tags are left out, which `debug.log`
  says once. Dodeca takes no other provider, as it has no OIDC; Enterprise's
  `dbms.security.authorization_providers`, which must name `native` too, does
  not exist in Dodeca, whose only provider is native.
- **Each transaction** starts with the roles granted to the user, and those
  of the enabled rules whose conditions hold for the user's tags at that
  moment; `time.transaction()` and the like give that moment. So changes to
  tags and rules apply from the next transaction, as documented. A role given
  both ways stays, and suspended users get none.
- **`abac.oidc.user_attribute()`** is accepted in conditions, as in
  Enterprise without an OIDC provider, but a rule that calls it never holds;
  `debug.log` says so.
- **Roles that rules give have no denied privileges**, as documented: `GRANT
  ROLE ... TO AUTH RULE` of a role with a `DENY` fails (`22ND2`), and so does
  a `DENY` to a role a rule gives (`22ND1`). Property rules with tags are
  `GRANT` only and cannot be negated, as Neo4j's parser has them.
- **Where the docs say nothing**: `SHOW USERS`, `SHOW CURRENT USER` and `SHOW
  USER PRIVILEGES` show the roles granted to users, not those rules give,
  while `dbms.showCurrentUser()` shows every role of its transaction. `SHOW
  AUTH RULES` lists rules by name, with `roles` null without `SHOW ROLE`, and
  conditions as Neo4j prints them, which is how they are kept and read again.
  `SHOW POPULATED ROLES WITH AUTH RULES` shows the roles with users or rules.
  A condition that fails when evaluated gives no roles and is logged once.
  Backups with `--include-metadata` leave auth rules out.

## Database aliases

Dodeca implements local and remote database aliases as Enterprise's
operations manual describes them: `CREATE`, `ALTER`, `DROP` and
`SHOW ALIASES` with `IF NOT EXISTS`, `IF EXISTS`, `OR REPLACE`, `PROPERTIES`
and parameters, the alias privileges (`CREATE`, `DROP`, `ALTER` and
`SHOW ALIAS`, `ALIAS MANAGEMENT`), and
`DROP DATABASE ... RESTRICT | CASCADE ALIASES`. Aliases are kept in the
system graph exactly as Neo4j keeps them, so Neo4j Community resolves their
names wherever a database name goes: driver sessions, `USE`, routing tables,
home databases, `SHOW DATABASES` and `SHOW DATABASE <alias>`.

- **Names.** Without composite databases, a dotted name such as
  `films.classic` is a name of its own, as Enterprise reads a dot when no
  composite database has the name before it; `SHOW ALIASES` shows `composite`
  as null. Cypher 25 takes a name with several dots as one name too; Cypher 5
  refuses more than one dot in a local alias's name, as Neo4j does. A local
  alias targets a standard database, never `system` or another alias.
- **`SHOW ALIASES`** in Cypher 25 has Enterprise's `credentials` column:
  `STORED NATIVE CREDENTIALS` for a remote alias and null for a local one.
  Dodeca creates no aliases with `OIDC CREDENTIAL FORWARDING`, which fails
  (see [Not supported](#not-supported)); it shows that value only for such an
  alias in a store from Enterprise.
- **`DEFAULT LANGUAGE`** of `CREATE ALIAS ... AT` and `ALTER ALIAS ... SET
  DATABASE DEFAULT LANGUAGE` give a remote alias its default Cypher version,
  as for databases (see [Databases](#databases)): queries through the alias
  are parsed in it here and sent to the other DBMS with it.
  `SHOW ALIASES ... YIELD defaultLanguage` shows it. A local alias has none:
  its queries run in its database's.
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
  starting, with a message saying so, and `dbms.setDefaultDatabase` refuses
  an alias (see [Databases](#databases)). Enterprise takes an alias as the
  default database since 2025.04.
- **Privileges.** Access through a local alias is checked on its database,
  and `GRANT`, `DENY` and `REVOKE` on a local alias are refused naming the
  database. A remote alias has privileges of its own, kept by its name:
  `ALTER ALIAS` keeps them and `DROP ALIAS` removes them. Only `ACCESS`
  applies to a remote alias; other privileges on one are refused, since the
  other DBMS checks them for the user the alias names. These are Dodeca's
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
    settings; see
    [`OPERATOR_GUIDE.md`](OPERATOR_GUIDE.md#keystore-for-remote-aliases).
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

Dodeca's clustering is its own design, on Apache Ratis, built to what
Enterprise documents that clients and operators see. Every database is
replicated, `system` included, each in its own Raft group of its primaries; its
secondaries pull its transactions instead. Clients see what an Enterprise
cluster shows them: routing tables with the leader as writer, failover,
bookmarks, server-side routing, and `SHOW DATABASES` with a row per member.
Members talk to each other in protocols of Dodeca's own
([`FORMATS.md`](FORMATS.md)), on their Raft port
(`server.dodeca.cluster.listen_address`, 7700) and their routing connector
(7688), where Enterprise uses ports 5000, 6000 and 7000, so Dodeca and
Enterprise servers cannot form a cluster together. A cluster forms from
`dbms.dodeca.cluster.initial_members`, and grows with
`dodeca.cluster.addMember`, instead of through Enterprise's discovery service;
see [`OPERATOR_GUIDE.md`, "Cluster members"](OPERATOR_GUIDE.md#cluster-members).
Enterprise's settings for its discovery, Raft and catch-up internals
(`dbms.cluster.discovery.*`, `dbms.cluster.raft.*` and `db.cluster.raft.*` other
than the leadership settings, `dbms.cluster.network.*`,
`server.cluster.listen_address` and the other addresses,
`server.directories.cluster_state`, `dbms.kubernetes.*`) are not declared;
Dodeca's own are named `*.dodeca.cluster.*`.

### What clients see

- **Writes go to the leader.** Drivers that route (`neo4j://`) get routing
  tables with the leader as writer and retry managed transactions on the new
  leader after a failover. As in Enterprise, the leader is a reader too only
  with `dbms.routing.reads_on_writers_enabled`, or while no other member can
  read the database. A new leader tells every server at once, so that routing
  tables from servers that do not host the database name it within moments too.
  A write transaction that reaches a follower runs on the leader when its client
  allows server-side routing, as in Enterprise: `neo4j://` drivers with
  `dbms.routing.default_router=SERVER`, and HTTP clients. This needs mutual TLS
  between members and their routing connector (`server.routing.listen_address`,
  port 7688) reachable from the others. A `bolt://` write is forwarded only with
  `dbms.dodeca.routing.forward_bolt_writes=true`, which Enterprise does not
  have. Otherwise the follower rejects the write with
  `Neo.ClientError.Cluster.NotALeader`, naming the leader's Bolt address. A
  database that does not run on the member asked, such as a stopped one, is
  routed to that member only. See [`OPERATOR_GUIDE.md`, "Writes on any
  member"](OPERATOR_GUIDE.md#writes-on-any-member).
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
- **Cluster status endpoints** of the HTTP server tell load balancers where to
  send writes and reads, as in Enterprise: `/db/<database>/cluster/writable`
  answers `true` with 200 OK on the member that leads the database, `read-only`
  on the other members that run it, and `available` on every member that runs
  it; otherwise each answers `false` with 404 Not Found.
  `/db/<database>/cluster/status` and `/dbms/cluster/status` give Enterprise's
  JSON, without the deprecated `raftCommandsPerSecond`. On a secondary,
  `lastAppliedRaftIndex` is the Raft index of the last transaction it pulled.
  Enterprise's documentation names the health field of `/dbms/cluster/status`
  both `healthy` and `isHealthy`: Dodeca gives both. As in Enterprise, the
  endpoints need a login unless `dbms.security.cluster_status_auth_enabled` is
  `false`, and `server.http_enabled_modules` turns them off without
  `ENTERPRISE_MANAGEMENT_ENDPOINTS`. A server with clustering disabled has none,
  as in Neo4j Community: they answer 404. Enterprise has no such mode, as a
  single Enterprise server is a cluster of one; a Dodeca server answers them as
  a cluster of one too. See [`OPERATOR_GUIDE.md`, "Cluster status
  endpoints"](OPERATOR_GUIDE.md#cluster-status-endpoints).
- **Linearizable reads**, Dodeca's own, are requested per transaction with
  the metadata key `dodeca.consistency` set to `linearizable`, or for every
  transaction with `dbms.dodeca.reads.default_consistency`. They apply to
  Bolt clients (drivers, Cypher Shell). The transaction waits on the member
  that runs it: the leader for a write that a follower forwards there. On a
  secondary, it asks a primary for the last transaction acknowledged before
  it, and pulls until it has it. See [`OPERATOR_GUIDE.md`, "Linearizable
  reads"](OPERATOR_GUIDE.md#linearizable-reads).
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
- **Members.** Members are added with `dodeca.cluster.addMember`, and dropped
  with `DEALLOCATE DATABASES FROM SERVER` and `DROP SERVER`, one at a time; the
  Raft group of each database follows the servers it is placed on. See
  [`OPERATOR_GUIDE.md`, "Cluster members"](OPERATOR_GUIDE.md#cluster-members). A
  dropped member cannot rejoin with its data: its data directory must be deleted
  first. A copy of a database that is offline is made once it is started again.
- **Upgrades.** Dodeca is upgraded one member at a time, mixing at most two
  versions; formats that the previous version cannot read are used only once an
  administrator calls `dodeca.cluster.finishUpgrade()`, after which no member
  can return to it. Neo4j is upgraded with every member shut down. See
  [`OPERATOR_GUIDE.md`, "Upgrading
  Dodeca"](OPERATOR_GUIDE.md#upgrading-dodeca).
- **Traffic between members.** A clustered member does not start until
  `dbms.ssl.policy.cluster.enabled` is set: `true` for mutual TLS, configured
  with Neo4j's SSL policy settings of the `cluster` scope as in Enterprise, or
  `false` for plaintext. `client_auth=OPTIONAL` is refused. With
  `verify_hostname`, true by default in Neo4j 2026 as in Enterprise since
  2025.01, a member checks that the certificate of the member it connects to
  names the host it dials; otherwise members accept any certificate from a
  trusted authority, whichever member presents it. See [`OPERATOR_GUIDE.md`,
  "TLS between members"](OPERATOR_GUIDE.md#tls-between-members).
- **Reloading TLS.** With `dbms.security.tls_reload_enabled=true`, an
  administrator reloads a server's certificates with
  `CALL dbms.security.reloadTLS()`, on each server, as in Enterprise since
  2025.03. Connections made afterwards use the new certificates; open ones
  keep theirs. Bolt, HTTPS and the routing connector, which are Neo4j's own,
  take every change of their policies. Between members and on the backup
  server, the certificate and the trusted authorities change, revocation lists
  included, while the `tls_versions`, `ciphers`, `client_auth` and
  `verify_hostname` of the `cluster` and `backup` policies keep their values
  until the server restarts: a reload that changes them says so in
  `neo4j.log`. A reload whose files cannot be read fails and keeps the
  certificates in use. Without the setting, the procedure fails and says how
  to turn it on.
- **Cipher suites.** Members use the protocols and cipher suites of the
  policy, and with the default `tls_versions` negotiate TLS 1.3 with each
  other. Enterprise's documentation says that CBC suites are not supported
  since 2025.01, and that 2025.10 took four of them out of the defaults.
  Without `ciphers`, members and backups use the JDK's default suites without
  the CBC ones and those with static DH or ECDH key exchange. Neo4j 2026.10.2
  means to leave these out of every policy's defaults, but its filter matches
  none, so Bolt and HTTPS, which are Neo4j's own, still enable CBC suites:
  set `ciphers` in their policies to restrict them.
- **Checking connectivity.** `dbms.cluster.checkConnectivity` checks, as in
  Enterprise, whether the member it runs on reaches each other member at its
  ports. Dodeca carries the traffic of Enterprise's cluster and Raft ports on
  one port, the member's Raft address, so its `CLUSTER` and `RAFT` rows share
  one check: whether the member there answers as itself. `INTRA_BOLT` is the
  routing connector, which takes forwarded writes, so only members with TLS
  between them check it, logging in as the user who calls the procedure.
- **Status checks.** `dbms.cluster.statusCheck` replicates a status check
  entry in each database, as in Enterprise, and gives each primary's part in
  it. The entry is a transaction without changes, which every member skips:
  nothing is committed. A primary is `APPLYING` once it applied the entry
  within the timeout, `REPLICATING` when its log holds the entry but its
  database did not apply it, as while the database is stopped or failed on
  it, and `UNAVAILABLE` otherwise. `replicationSuccessful` tells whether a
  majority committed the entry within the timeout. The signature shows the
  timeout's default as `1000`, where Enterprise's shows `null`: both wait
  1000 milliseconds, also when given `null`. Each other member asked may take
  as long again to answer.
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
  `server.dodeca.cluster.single_fsync=false` to force both logs at every
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
  moved there. `dodeca.cluster.status()` reports each group's latest snapshot
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
  files applied up to a higher Raft index than its group reached there, as after
  copying another database's or cluster's files into place while it was down,
  checks the group's entry at that index against the files' last transaction,
  once the group's log reaches it. If they differ, the copy on that member is
  put in quarantine with a message saying how to recover: delete the files there
  and restart the member, which then copies the database, and lift the
  quarantine. Files applied up to an index the group already reached on that
  member are not checked. Create databases from other files with a seed instead.
- **Stopped databases.** A member whose replicated database stops, for example
  after a panic, keeps taking part in the database's Raft group but no longer
  applies entries, and hands leadership to another member. Once a member's
  copy failed, routing tables, whichever member serves them, no longer list
  it as a writer or reader of that database, within about 10 seconds;
  it still serves routing tables, which send drivers to the others.

## Routing policies

Neo4j Community has no routing policies. Dodeca has Enterprise's
`server_policies` load balancing plugin, its policies
(`dbms.routing.load_balancing.config.server_policies.<name>`, in the rules of
`server.cluster.catchup.user_defined_upstream_strategy`) and
`dbms.routing.load_balancing.shuffle_enabled`. [`OPERATOR_GUIDE.md`, "Routing
policies"](OPERATOR_GUIDE.md#routing-policies), describes how to use them.
`dbms.routing.load_balancing.plugin`, which Enterprise deprecated in 2025.05,
takes only `server_policies`, its default. `groups()`, Enterprise's
deprecated name of `tags()`, is taken for it.

- **Not supported:** Enterprise's built-in policies of 2025.12
  (`builtin_default`, `builtin_all`, `builtin_prefer_secondaries` and
  `builtin_add_writer`): a driver that names one fails as with any unknown
  policy. The `roles()` filter of 2025.12 fails the server's start.

Where Enterprise's documentation says nothing:

- **A policy chooses readers and routers.** The writer is the leader whatever
  the policy says. A policy that passes no reader gives a table without
  readers, so that the driver fails reads and writes still work; one that
  passes no router leaves the leader as the only router, or while there is no
  leader, the routers the table would have without the policy.
- **The servers a policy chooses from** are those that host the database and
  answered the latest probe of the server asked, so that `min(n)` counts the
  servers that run. Routing tables leave out a server that stops answering
  without a policy too.
- **An unknown policy** fails the routing request with Neo4j's own error for
  it, `Neo.ClientError.Routing.RoutingFailed` (GQL status `08N15`), "Policy
  definition for '<name>' could not be found.", followed by the server's
  policies.
- **Servers with different policies.** Enterprise says only that the result is
  unpredictable. Each server reports a digest of its policies to the others,
  and one that hears a different digest names the server in a warning in
  `neo4j.log`, once until it changes. Policies that differ only in their
  spelling, or in an `all()` that changes nothing, count as the same.
- **A comma between rules**, which one of Enterprise's upstream examples has, is
  refused: its meaning is not documented.
- **Shuffling** orders the readers and routers of each table at random; without
  it, in the order of the servers' ids.
- **Policies are read at start.**
- **Tables that take no policy**: server-side routing's, a server's with
  clustering disabled, and a one-server cluster's, which Neo4j Community makes
  and which name only the server asked; these do not check the policy name.

## Servers

Dodeca shows and changes servers, and places databases on them, as Enterprise
documents: `SHOW SERVERS` with Enterprise's columns, `ENABLE SERVER`, `ALTER
SERVER`, `RENAME SERVER`, `[DRYRUN] DEALLOCATE DATABASES FROM SERVER[S]`,
`[DRYRUN] REALLOCATE DATABASES` and `DROP SERVER`; the states `Free`, `Enabled`,
`Cordoned`, `Deallocating`, `Deallocated` and `Dropped`; the server options;
`CREATE DATABASE ... TOPOLOGY` and `ALTER DATABASE ... SET TOPOLOGY`, with a
default of one primary and no secondaries; secondaries, which pull transactions
from upstreams; the settings `initial.server.tags`,
`initial.server.mode_constraint`, `initial.server.allowed_databases`,
`initial.server.denied_databases`, `initial.dbms.default_primaries_count`,
`initial.dbms.default_secondaries_count`,
`initial.dbms.automatically_enable_free_servers`,
`server.cluster.system_database_mode`, `db.cluster.catchup.pull_interval`,
`server.cluster.catchup.upstream_strategy` with its strategies,
`server.cluster.catchup.connect_randomly_to_server_tags`,
`server.cluster.catchup.user_defined_upstream_strategy` and
`dbms.routing.reads_on_primaries_enabled`; and the procedures
`dbms.cluster.cordonServer`,
`dbms.cluster.deallocateDatabaseFromServer(s)`,
`dbms.cluster.deallocateNumberOfDatabases`, `dbms.cluster.reallocateDatabase`,
`dbms.cluster.reallocateNumberOfDatabases`,
`dbms.cluster.setAutomaticallyEnableFreeServers`,
`dbms.setDefaultAllocationNumbers`, `dbms.showTopologyGraphConfig` and
`dbms.cluster.secondaryReplicationDisable`; and `neo4j-admin server unbind`. See
`OPERATOR_GUIDE.md`, "[Servers](OPERATOR_GUIDE.md#servers)", "[Placing
databases](OPERATOR_GUIDE.md#placing-databases)",
"[Secondaries](OPERATOR_GUIDE.md#secondaries)" and "[Unbinding a
server](OPERATOR_GUIDE.md#unbinding-a-server)". Where Dodeca differs, or
decides what Enterprise does not document:

- `ALTER DATABASE ... SET TOPOLOGY` without `SECONDARIES` keeps the number of
  secondaries; `CREATE DATABASE ... TOPOLOGY` without it takes the default
  number.
- The allowed and denied databases of a server, its `allowedDatabases` and
  `deniedDatabases` options and the `initial.server.*` settings, are database
  names, compared regardless of case. Enterprise also takes name patterns
  there since 2025.12.
- The server groups that Enterprise removed in 2025.01 stop the server at
  startup, as in Enterprise, but the message names what replaced each one:
  `server.groups` (`initial.server.tags`),
  `server.cluster.catchup.connect_randomly_to_server_group`
  (`server.cluster.catchup.connect_randomly_to_server_tags`) and the
  strategies `connect-randomly-to-server-group` and
  `connect-randomly-within-server-group` (the `-tags` ones). Without strict
  validation (`server.config.strict_validation.enabled=false`), the settings
  are ignored with a warning; the strategies are refused either way, as any
  unknown strategy is.
- A copy whose server loses its mode but may host the other keeps its store
  and changes its mode in place, both ways, primary to secondary once its
  replacement votes. A server changes how it hosts `system` when restarted
  with the other `server.cluster.system_database_mode` and added again with
  `dodeca.cluster.addMember`; until then it keeps its mode and logs why.
  Enterprise documents mode changes of `system` only through restarts and
  disaster recovery.
- Secondaries move as primaries do, the new copy first.
- A secondary that finds no upstream that answers retries every pull
  interval; `SHOW DATABASES` shows it online, its lag growing.
  `replicationLag` counts the transactions behind the copy with the newest, as
  a negative number, as Enterprise's documentation of `SHOW DATABASES`
  defines it.
- `dbms.cluster.secondaryReplicationDisable` returns `state`, `paused` or
  `running`. A pause ends when the database or the server starts again, so
  that a forgotten pause does not stop a secondary for good, and routing
  tables leave paused secondaries out, so that drivers get no data they
  stopped updating; `bolt://` connections still reach them.
- Strategies are also accepted as `connect-randomly-to-server-with-tag`, the
  name of Enterprise's settings page, and `user_defined`. A comma between the
  rules of `user_defined_upstream_strategy`, which one Enterprise example has,
  is refused, as its meaning is not documented.
- A server with `modeConstraint: 'SECONDARY'` cannot be the
  `existingDataSeedServer` of `CREATE DATABASE`: the seed server creates the
  database, which only a primary does.
- `dbms.cluster.minimum_initial_system_primaries_count` is not supported: a
  cluster forms from `dbms.dodeca.cluster.initial_members`, which must not
  name a server with `server.cluster.system_database_mode=SECONDARY`.
- Enterprise refuses to take a database with more than one primary down to
  one, and to deallocate a server that hosts a database's only primary.
  Dodeca does both, as every move makes the new copy before it removes the
  old one.
- Enterprise does not document where it places copies; Dodeca's placement is
  deterministic (see [`OPERATOR_GUIDE.md`, "Where copies
  go"](OPERATOR_GUIDE.md#where-copies-go)). `dbms.showTopologyGraphConfig` shows
  `allocator` as null.
- `DROP SERVER` also refuses a deallocating server that still votes in a
  database's Raft group, as the leader of `system` knows it, also while the
  server is down, so that a database whose only copy is there is not lost.
- `neo4j-admin server unbind` removes the server's id and its cluster state,
  archived with `--archive-cluster-state=true`, which needs `--archive-path`.
  The files of the server's databases are kept, as in Enterprise, but moved to
  `unbound/<time>/` next to their directories: the server, which starts as a
  new one, copies the databases placed on it from the others and never serves
  or deletes the old files. A server whose `data/server_id` was removed or
  replaced otherwise, Neo4j Community's `neo4j-admin server unbind` included,
  refuses to start until `neo4j-admin server unbind` is run, as under a new id
  it would delete its databases.
- A server joins with `dodeca.cluster.addMember`, after which it is `Free`;
  a `Free` server votes in the Raft group of `system`, as every server does
  that hosts `system` as a primary. A server that hosts `system` as a
  secondary is added on the leader of `system`.
- A cluster recovered from backups places each database again on as many
  servers as its topology asks for, or on the default number if that is
  larger, as a standalone server records one primary for each of its
  databases.

## Leadership balancing

Dodeca balances the leaderships of databases over the members as Enterprise
documents, with its setting
`dbms.cluster.raft.leader_transfer.balancing_strategy`: `EQUAL_BALANCING`, the
default, moves leaders so that every member leads as many databases as the
others, and `NO_BALANCING` leaves them where elections put them. Dodeca makes
the setting dynamic. As in Enterprise, a member hands over the leadership of a
database that is read-only on it (`server.databases.read_only` and the like).
See [`OPERATOR_GUIDE.md`, "Where databases are
led"](OPERATOR_GUIDE.md#where-databases-are-led).

Where Enterprise's documentation says nothing, Dodeca's choices:

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
balancing. Where Enterprise's documentation says nothing, Dodeca moves the
leadership to the member with the tag that leads the fewest databases, counts
a database with a priority tag when it balances the others, and leaves the
leadership where it is while no member with the tag can take it over. A
database whose name contains a dot cannot have a priority tag of its own:
Neo4j reads the setting's name only up to the dot, and ignores the setting.
`db.cluster.raft.leader_transfer.priority_group` and
`db.cluster.raft.leader_transfer.priority_group.<database>`, which Enterprise
removed in 2025.01, stop the server at startup as in Enterprise, with a
message that names the `priority_tag` setting replacing them.

## Backup and restore

Backups and restores use Enterprise's commands, options and settings:
`neo4j-admin database backup` and `restore`, `neo4j-admin backup aggregate`
and `inspect`, `neo4j-admin database check --from-path`,
`server.backup.enabled`, `server.backup.listen_address`,
`server.backup.advertised_address` and `dbms.ssl.policy.backup.*`. They work
alike on standalone servers and cluster members; see [`OPERATOR_GUIDE.md`,
"Backup and restore"](OPERATOR_GUIDE.md#backup-and-restore). A backup artifact
is a file of Dodeca's own format, which Neo4j Community's `neo4j-admin
database load` also loads as a dump when it is a full backup. Exchanging
backups with Enterprise is not promised. The differences:

- Backups go to, and are restored, inspected, checked and aggregated from,
  cloud storage too (see [Cloud storage](#cloud-storage)). `restore` reads
  them where they are, as it restores them, and never needs `--temp-path`.
- `--split-archive-part-size` of `backup` and `backup aggregate`, and
  `server.split_archive.part_size`, split backups in Neo4j Community's format
  of split archives, whose `load` reads them. A split aggregated backup keeps
  the name it was written under, as its parts are named after it.
- `--parallel-download` of `backup` is accepted and changes nothing: each
  backup comes from one server, the first that serves it.
- `server.backup.advertised_address`, which Enterprise's backup pages do not
  describe, defaults to the host of `server.default_advertised_address` and
  the port of `server.backup.listen_address`.
- `--parallel-recovery` of `backup`, experimental in Enterprise, is accepted
  and changes nothing. `--inspect-path` lists the backups with their
  metadata, as `neo4j-admin backup inspect --show-metadata` does.
- `neo4j-admin database aggregate-backup`, the name of `neo4j-admin backup
  aggregate` before 2025.01, works as it, deprecated as in Enterprise.
  `--parallel-recovery`, experimental in Enterprise, is accepted and changes
  nothing: the aggregated backup is recovered as always.
- The script that `--include-metadata` keeps, and `restore` writes to
  `data/scripts/<database>/restore_metadata.cypher`, is Dodeca's own. It
  leaves out `ALTER DATABASE`, `SET DATABASE ACCESS` and `SET DATABASE
  DEFAULT LANGUAGE` on the DBMS, as other privileges on the DBMS, and starts
  those on the database, which only Cypher 25 grants, with `CYPHER 25`.
- `neo4j-admin backup inspect` without `--show-metadata` lists only the files,
  as Enterprise did before 2025.07; Enterprise now shows the metadata always
  and ignores `--show-metadata`. Its `RECOVERED` column, as Enterprise's since
  2025.06, is `false` for a full backup taken with `--skip-recovery`, and for
  every differential backup, which holds no store.
- A differential backup continues the latest backup only while the source's
  transaction logs still hold that backup's last transaction, with the same
  checksum; otherwise the command takes a full backup instead.
- Backup files are named in UTC.
- `--temp-path` of `backup` and `backup aggregate` defaults to the backups'
  folder, which must then be local; with a folder in cloud storage, it must
  be given. `restore` accepts `--temp-path` without using it.
- `restore` of a folder holding backups of several databases restores the
  latest chain of each, where Enterprise requires `--source-database` to pick
  one. Its value must be given, and a backup file of another database fails.
- A backup is not restored with another Neo4j kernel version than the one it
  was taken with.
- Serving backups on an address that is not a loopback one needs
  `dbms.ssl.policy.backup.enabled` set explicitly, `true` or `false`, since
  the backup port has no other authentication. With TLS, `neo4j-admin` checks
  that the server's certificate names the host it connects to, unless
  `dbms.ssl.policy.backup.verify_hostname=false`. The server takes reloaded
  certificates as members do (see [Running a cluster](#running-a-cluster)).

## Cloud storage

Amazon S3 (`s3://<bucket>/<path>`), Google Cloud Storage
(`gs://<bucket>/<path>`) and Azure Blob Storage (`azb://<storage
account>/<container>/<path>`) work as in Enterprise for `neo4j-admin database
dump`, `load`, `check` and `import full`, backups, restores, seeds and `LOAD
CSV`, with each SDK's own credentials and the settings
`dbms.integrations.cloud_storage.azb.blob_endpoint_suffix` and
`dbms.integrations.cloud_storage.azb.authority_endpoint`; see
[`OPERATOR_GUIDE.md`, "Cloud storage"](OPERATOR_GUIDE.md#cloud-storage). The
SDKs come in Dodeca's jar, relocated, so that they never meet other copies on
Neo4j's classpath. The differences:

- `S3SeedProvider` is a name for `CloudSeedProvider` on `s3:` URIs; its
  `seedConfig` and `seedCredentials` options fail `CREATE DATABASE`, which
  tells to give the servers the AWS SDK's own credentials instead.
- The default `dbms.databases.seed_from_uri_providers` is
  `FileSeedProvider,CloudSeedProvider`; see [Databases](#databases).
- `dbms.integrations.cloud_storage.s3.target_throughput_gbps` and
  `dbms.integrations.cloud_storage.gs.project_id` (Enterprise 2025.03) are not
  supported.

Where Enterprise's documentation says nothing, Dodeca's choices:

- With an S3 endpoint set (`AWS_ENDPOINT_URL_S3` and the like), as for an
  S3-compatible store, buckets are named in the path, and checksums are sent
  only where S3 requires them. Without a region, S3 requests start in
  us-east-1 and follow each bucket to its region.
- `AZURE_STORAGE_CONNECTION_STRING`, Azure's own variable, is used instead of
  the default Azure credentials when set, for the storage account it names.
- Without Google credentials, requests go anonymous, as Google's SDK does,
  which public buckets allow; refusals then say why there were none.
- `LOAD CSV` from cloud URIs needs what `file:` URLs need:
  `dbms.security.allow_csv_import_from_file_urls`, and `LOAD ON ALL DATA` for
  users whose LOAD privilege is limited, since no CIDR range covers them.
- A backup in cloud storage is written under a hidden name, then copied
  within the cloud to its own, as a local one is renamed; an object appears
  only once written whole. Restores and seeds read backups where they are.
- Objects are written in parts that grow as they do, up to 1 GiB: S3 objects
  of up to almost 4 TiB (S3 allows 5 TiB), Azure blobs of up to 19 TiB.
- A copy of a whole object fetches 16 ranges at once: Neo4j's interface says
  64, which takes up to 512 MiB of memory.
- Azure copies blobs within a storage account only.

## Disaster recovery

A cluster that lost a majority of its servers recovers as Enterprise's
disaster recovery guide shows, with the same commands; see
[`OPERATOR_GUIDE.md`, "Disaster recovery"](OPERATOR_GUIDE.md#disaster-recovery):

- **`neo4j-admin dbms unbind-system-db`**, with `--archive-cluster-state` and
  `--archive-path`, removes a stopped server's Raft state of `system` only and
  prints its last committed transaction. The servers left, unbound, re-form
  `system` from `dbms.dodeca.cluster.initial_members`, where Enterprise uses
  its discovery settings: Dodeca picks the copy with the latest transaction,
  so dumping and loading `system` is not needed, and there is no
  `dbms.cluster.minimum_initial_system_primaries_count`. The re-formed group
  has a new id, kept in each server's membership record. The lost servers stay
  servers, `Unavailable`, to cordon, deallocate and drop. One that comes back
  with its old state is refused once it reaches the recovered cluster; one
  that reaches only other lost servers cannot tell, so they must be kept from
  starting. A lost server rejoins as the same server once unbound from
  `system` and added with `dodeca.cluster.addMember`.
- **`dbms.recreateDatabase`** (Enterprise 2025.04) takes Enterprise's options
  `seedURI`, `seedingServers`, `clearData`, `storeFormat`, `primaries` and
  `secondaries`, and gives its status messages; a `server:` seed (2026.04)
  fails. `dbms.cluster.recreateDatabase`, which Enterprise deprecated for it
  and its Cypher 25 keeps, does the same with a deprecation warning.
  `clearData` without `storeFormat` creates the empty store in the format the
  database was created in, its `CREATE DATABASE` option `storeFormat` or else
  `db.format`, where Enterprise keeps the store's format: they differ for a
  store migrated to another format since, or for `db.format` changed since the
  database was created without the option. The recreated database gets a new
  id, as when it is dropped and created again: element ids change, and
  bookmarks of the old database are not waited for. Privileges on it are kept.
  A seeder that does not answer is waited for, with a status message naming
  it; a recreation that failed waits to be recreated again. Seeders whose
  stores fall behind the latest one pull the transactions they lack from it;
  one whose history differs is copied. Both procedures exist only on cluster
  members; Enterprise's documentation does not say whether a standalone server
  has them. On a standalone server, drop the database and create it again,
  from a seed or empty.
- **Quarantine.** `dbms.unquarantineDatabase(server, database, operation)`
  (Enterprise 2025.01) and copies that fail while running behave as in
  Enterprise 2026: the procedure lifts the quarantine of a copy on the server
  named, by its name or id, from any server, and fails with Enterprise's
  error 52N17. `replaceStateReplaceStore` deletes the copy and its Raft
  state, has the database's group remove the server, and copies the database
  anew, which the group adds; `replaceStateKeepStore` keeps the store, which
  is copied only if the group's log no longer holds what follows it.
  `dbms.quarantineDatabase` exists under
  Cypher 5 only, deprecated, as in Enterprise, whose Cypher 25 has no way to
  put a copy in quarantine. Quarantines are kept in `data/quarantine`, a file of Dodeca's own
  ([`FORMATS.md`](FORMATS.md)); that of `system` lasts until the server
  restarts.
- **`dbms.cluster.protocols`** lists Dodeca's protocols
  ([`FORMATS.md`](FORMATS.md)), not Enterprise's: `raft`, `probe` and `bulk`
  with the bulk transfers it carried. A leader lists its outbound Raft
  connections to the members it replicates to that answered lately.

## Metrics and logs

Dodeca reports metrics and writes the query log and the security log as
Enterprise's operations manual describes them; Neo4j Community declares the
query log settings and ships the log files' configuration, but writes neither
log. [`OPERATOR_GUIDE.md`](OPERATOR_GUIDE.md#monitoring) describes how to use
them. Dodeca logs its own messages in Neo4j's `debug.log`, which Neo4j writes
as JSON lines since 2025.01.

- **Exporters:** CSV files, Graphite, Prometheus and JMX (domain
  `neo4j.metrics`), with Enterprise's settings and defaults, but for the
  differences below. Neo4j Community refuses the `server.metrics.*` settings
  at startup with "Unrecognized setting".
- **Defaults are Enterprise's of 2026.10**:
  `server.metrics.csv.rotation.compression` is `ZIP`, as since 2025.01, and
  `server.metrics.filter` is the list of 2025.06, with the `neo4j.count.*`
  metrics. `server.metrics.deny_filter` (2025.10) leaves out metrics that the
  filter reports.
- **CSV files are on by default**, as in Enterprise: a server moving from
  Neo4j Community or from an earlier Dodeca starts writing a file per metric
  in `server.directories.metrics` (`metrics` under the home directory),
  growing by a line every `server.metrics.csv.interval` (30 s). Where
  Enterprise's documentation says nothing: the header is `t` and then `count`
  for counters, `value` for gauges, and for histograms Dropwizard's CSV
  reporter's columns (`count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999`);
  `t` is in seconds since the epoch; a gauge that has no value at a report
  gets no line; a database's files grow only while it runs. A file of
  `rotation.size` or more is renamed `<name>.csv.1`, or `<name>.csv.1.gz` or
  `.zip` as `rotation.compression` says, before its next line, the older ones
  shifting up to `rotation.keep_number`; a ZIP archive holds one entry named
  `<name>.csv`. A directory that cannot be written is logged once, until
  writing succeeds again. Neo4j's Docker entrypoint uses a `/metrics` mount
  only for Enterprise: set `server.directories.metrics=/metrics` with Dodeca.
- **Graphite** gets Graphite's plaintext protocol over a new TCP connection
  every `server.metrics.graphite.interval`: counters and gauges under their
  names, histograms as `<name>.count`, `.max`, `.mean`, `.min`, `.stddev`,
  `.p50`, `.p75`, `.p95`, `.p98`, `.p99` and `.p999`, `t` in seconds. Values
  that are not finite numbers are left out. Connecting and writing time out
  after 10 seconds; a server that cannot be reached is logged once, until
  sending succeeds again.
- **Names** are Enterprise's, `<prefix>.dbms.<name>` and
  `<prefix>.database.<database>.<name>`, with `store.size.full`, as
  Enterprise renamed `store.size.total` in 2025.01.
  Metrics Enterprise does not have are named under a `dodeca` segment, such
  as `dodeca.cluster.raft.follower_lag_max`. Enterprise's exact Prometheus
  output is not documented; Dodeca's replaces characters other than letters,
  digits and `_` with `_`, adds `_total` to counters, reports histograms as
  summaries (quantiles 0.5 to 0.999, `_sum`, `_count`), and starts each
  family's `# HELP` with the dotted name. Database names that differ only in
  such characters, such as `my-db` and `my_db`, give the same Prometheus
  names: the second family is left out and a warning logged. Prometheus's
  `promtool check metrics` reads the output and flags only gauges whose
  Enterprise names end like summaries: `vm.file.descriptors.count` and
  `vm.memory.buffer.<pool>.count`.
- **Metrics without a source in Neo4j Community or Dodeca** are not
  reported: `server.threads.jetty.*`, `cluster.discovery.*` and
  `cluster.internal.discovery.*`, Enterprise's Raft internals
  (`cluster.raft.tx_retries`, `in_flight_cache.*`,
  `raft_log_entry_prefetch_buffer.*`, `message_processing_*`),
  `cluster.catchup.*` and `cluster.store_copy.*` of Enterprise's catch-up
  protocol (Dodeca's copies are counted under `dodeca.cluster.store_copy`),
  the pipelined and parallel runtimes' metrics, Cypher's `compiled` counts and
  `bolt.accumulated_queue_time`. Followers are reported together on the
  leader (`dodeca.cluster.raft.followers`, `follower_lag_max`), not one
  series each. Nor does Dodeca report the metrics Enterprise added since
  2025.01: `vm.cpu_load.system` and `vm.cpu_load.process`,
  `vm.threads.virtual`, `transaction.lowest_available_committed_tx_id`,
  `transaction.rollbacks_deadlocked`, `page_cache.pages_snapshot_loaded` and
  `page_cache.async_*`, and the new metrics of its Raft internals, such as
  `cluster.raft.snapshot_*` and those of elections.
- **The query log is on by default**, at Neo4j's declared default
  `db.logs.query.enabled=VERBOSE`: every member logs the start and end of every
  query with its parameters, rotated by the shipped `server-logs.xml` (7 files
  of 20 MB). A server moving from Neo4j Community starts writing `query.log`,
  which costs up to 12% of the throughput of trivial queries
  ([`OPERATOR_GUIDE.md`](OPERATOR_GUIDE.md#query-log)). Set `INFO` with a
  `db.logs.query.threshold`, or `OFF`, to log less.
- **Query log lines** follow the documented fields, their order and the JSON
  field names. Neo4j does not document the rest, which Dodeca defines:
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
  only a setting that `server.dynamic.setting.allowlist` names.
  `dbms.checkConfigValue` tells whether a value is valid for a setting,
  dynamic or not, with an empty `message` for a valid one. Neo4j Community has
  neither.
- **Monitoring procedures**, as in Enterprise: `db.checkpoint` checkpoints the
  database it runs on, or waits for the checkpoint under way, on the member it
  runs on, without the checkpoint I/O limit (see [Checkpoints](#checkpoints)).
  `dbms.scheduler.jobs`, `dbms.scheduler.groups` and `dbms.scheduler.failedJobs`
  list the jobs of the member's internal job scheduler, the groups that run
  jobs, and the latest runs that failed. Where Enterprise's documentation says
  nothing, job ids are `job-<number>`, and times are shown in
  `db.temporal.timezone`, as Neo4j's other procedures show them.
  `dbms.listPools` lists the member's memory pools, and in them the pools of the
  databases the user may see in `SHOW DATABASES`; a pool without a limit has
  `Unlimited` free and in total. `db.listLocks` lists the locks held in the
  database it runs on, with their transactions as `SHOW TRANSACTIONS` names
  them, and `dbms.listActiveLocks` the locks of the transaction running a query,
  for a user who may see that transaction in `SHOW TRANSACTIONS`.

## Checkpoints

Neo4j Community accepts Enterprise's checkpoint settings, but runs the
`CONTINUOUS` and `VOLUMETRIC` policies as `PERIODIC`, with a warning, and
ignores `db.checkpoint.iops.limit` and `db.checkpoint.throughput.limit`.
Dodeca has both policies and both limits.
[`OPERATOR_GUIDE.md`](OPERATOR_GUIDE.md#checkpoints) describes how to use them.

- **`CONTINUOUS`** checks every 100 milliseconds and checkpoints whenever
  transactions were appended since the last checkpoint.
- **`VOLUMETRIC`** checks every 10 seconds and checkpoints when transactions
  were appended since the last checkpoint and moving the checkpoint on would
  let `db.tx_log.rotation.retention_policy` delete transaction logs. With a
  retention policy that keeps every log (`keep_all`), it never checkpoints on
  its own: only backups, `db.checkpoint()` and stopping the database do.
- **The I/O limits are per database**: databases that checkpoint at once may
  together write their limits added up. The IOPS limit counts I/Os as Neo4j's
  page cache counts them for the checkpoint log line. Both limits let a tenth
  of a second's worth go at once, and are followed to best effort, as
  Enterprise says. Reads and evictions are not counted.
- **Only background checkpoints are limited**, those the policy triggers.
  Checkpoints that someone waits for are not: recovery's, a new database's
  first, a stopping database's last, those of backups and store copies, and
  `db.checkpoint()`'s. When one of them waits for a background checkpoint under
  way, that checkpoint completes without the limit, as Enterprise's
  `db.checkpoint()` documents.
- **A limit too low for the write load** keeps checkpoints from completing:
  pages are dirtied faster than a checkpoint flushes them. Transaction logs, and
  on a cluster member the Raft log, are then not pruned. `check_point.duration`
  shows how long checkpoints take.
- **Each database counts its own checkpoints**: the checkpoint log line and the
  `check_point.*` metrics (pages, I/Os, the limit, pauses) count the flushes of
  that database alone. Neo4j Community counts those of databases that
  checkpoint at the same time together.
- **`db.checkpoint.throughput.limit`**, Enterprise's limit of the bytes
  checkpoints write a second since 2025.07, replaces the IOPS limit while it
  is set, as Neo4j documents, and goes back to it when unset. It counts
  the bytes of the pages flushed, 8 KiB each. Where Enterprise's
  documentation says nothing: the checkpoint log line gives it in bytes a
  second, and the `check_point.io_limit` metric in pages a second, both
  rounded down to whole pages.

## Page cache warmup

Neo4j Community declares `db.memory.pagecache.warmup.*` and does nothing with
them. Dodeca profiles and warms up the page cache as Enterprise does, on by
default, and preloads whole files with `db.memory.pagecache.warmup.preload`.
[`OPERATOR_GUIDE.md`, "Page cache warmup"](OPERATOR_GUIDE.md#page-cache-warmup),
describes how to use it. Where Enterprise's documentation says nothing:

- **Profiles** are Dodeca's own files, `profiles/<file>.cacheprof` in the
  database's directory, `<file>` the profiled file's path in it (see
  [`FORMATS.md`](FORMATS.md)). They are taken every interval once the warmup
  ended, so that a profile of a page cache still warming up never replaces a
  fuller one, and when the database stops; a profile is written only when it
  changed, and one of a file that is gone, such as a dropped index's, is
  deleted.
- **The warmup** loads the pages of each profile, up to the end of the file
  if it shrank, in the background once the database is available, and stops
  when the database stops. The pages it counts include those held already.
- **The preload allowlist** is a regular expression that must match the whole
  file name, such as `neostore\.nodestore\.db`.
- **Copies.** Store copies to other members, online backups, restores and
  seeds carry the profiles, as Enterprise says of its store copies and backups;
  `neo4j-admin database copy`, which writes a new store, does not.
- **Metrics** are Dodeca's own: `page_cache.warmup.pages_loaded` and
  `page_cache.warmup.duration` of each database.
- **`internal.db.memory.pagecache.warmup.blocking_enabled`**, an internal
  setting Neo4j Community declares, has no effect: the warmup never holds up
  a database's start.
- **`db.memory.pagecache.warmup.order`**, with which Enterprise orders the
  files it warms up since 2026.03, orders the profiles the warmup loads:
  `NONE`, the default, in the order the file system lists them;
  `ALPHABETIC` by the path of the profiled file in the database's directory;
  `PRIORITY` the ID files first, then the indexes (`schema/index/`), then the
  rest, each alphabetically. Enterprise's `PRIORITY` puts the `block`
  format's x1 store first, which Dodeca does not have. As the setting's
  description says, it orders the warmup from profiles: the preload, which
  takes no profiles, loads its files in the order the page cache has them.

## Import and copy

`neo4j-admin` in Neo4j Community lacks `database import incremental` and
`database copy`, and runs `import full` in one go. Dodeca adds both commands
with Enterprise's options, and resumes a stopped full import.
[`OPERATOR_GUIDE.md`](OPERATOR_GUIDE.md#importing-and-copying-databases)
describes how to use them. They work on the formats Dodeca has, `aligned` and
`standard`.

### Incremental import

- **The merge writes through Neo4j's transactions**, 100,000 entities at a
  time, on the stopped database, so that relationship chains, dense nodes,
  counts, indexes and constraints are kept as for any write. This is slower
  than a full import: about 300,000 nodes and 80,000 relationships a second
  on an SSD. Each batch is a transaction: a merge that stops leaves the
  database with the batches it committed, and continues after them when run
  again. A batch that fails, as on a constraint the increment breaks, keeps the
  batches before it, which the error message counts.
- **The increment** is kept in `<databases>/.<database>.increment/` between
  the stages: the keys `prepare` read, what `build` resolved, and the merge's
  journal. It needs about the space of the input, and goes once the merge is
  done.
- **Keys**: the ID column of each node file names the label and key property
  of its ID space, as `uuid:ID{label:Person}`, and that label and property
  need a uniqueness or node key constraint. Keys are strings or integers,
  compared as the group's `id-type` says. A node whose key the database has is
  a duplicate, failing the import or skipped with `--skip-duplicate-nodes`:
  existing nodes are not changed. A new node gets the label of its ID space if
  it lacks it, so that the constraint covers it. `--id-type=actual` is
  refused, and so are node files with several ID columns, which Enterprise
  imports in full imports only: `build` refuses them when it reads the nodes,
  before the merge writes anything. Full imports take them.
- **Memory and disk**: the merge reads and writes through a page cache of
  `--max-off-heap-memory`, which defaults to 90% of the free memory and is
  printed. It writes the store's transaction log, about 250 bytes per entity,
  which checkpoints prune as usual.
- **Clusters**: the merge keeps the store's Raft position. Enterprise's
  procedure, the database recreated with the importing server as its only
  primary, works as documented, and so does importing on a database's only
  primary while secondaries pull the import. A server refuses to start an
  imported store as a secondary, or as one of several primaries while it holds
  the database's Raft group, which the others' stores do not match: the
  database fails there, with a message naming `dbms.recreateDatabase`
  seeded from that server.
- **`--dry-run`** checks the input and prints Neo4j's estimates of it, as a
  full import's dry run does, and needs no `--force`. With `--stage=merge`,
  which reads no input, it is refused.
- **Not supported**: updating and deleting existing nodes and relationships,
  which Enterprise's incremental import does since 2025.03, and `--schema`,
  which is refused.

### Imports into backups

`import full` and `import incremental` take `--target-format=backup` with
`--target-location`, `--compress` and `--split-archive-part-size`, as
Enterprise's do since 2025.12, and write the imported database as a full
backup, uncompressed unless `--compress` is given. Where Enterprise's pages
are silent, Dodeca is strict:

- The database is imported into `.dodeca-import-<database>` in `--temp-path`,
  by default the local `--target-location`, and is not in the DBMS. A full
  import that stops keeps it there and resumes as one into a database does.
- An incremental import leaves the database as it is: a copy of the stopped
  database gets the import, with `--stage=all` only, since the stages would
  each need the copy.
- `--compress` and `--split-archive-part-size` without
  `--target-format=backup` are refused.

### Resuming a full import

- **How**: a full import that stopped, or failed, resumes when run again with
  the same input and options, without `--overwrite-destination`, which still
  deletes everything first and starts over. Enterprise documents the points,
  not how to resume.
- **From where**: from relationship linking, once all nodes and relationships
  are imported, or from post-processing, once relationships are linked.
  Post-processing is the last of the four stages the import's progress shows,
  which begins with the relationship groups of dense nodes. An import that
  stopped before its relationships were all imported starts over, with
  `--overwrite-destination`. A failed import says which point it resumes from.
- **The same input**: the same files, by their sizes and modification times,
  and the same options, but for those that do not change what the import
  writes, such as `--threads`, `--max-off-heap-memory` and `--report-file`.
  Other input is refused, naming what differs.
- **Its progress** is kept in `.dodeca-import.properties` in the database
  directory. The report of bad entries the stopped import wrote is kept: the
  stages it resumes from find none.

### Store copy

- **Formats**: `--to-format` takes `same`, `standard` and `aligned`;
  `high_limit` and `block` are refused. A store in a format of an earlier
  Neo4j version is refused until it is migrated.
- **Backups**: `--source-format=backup` with `--source-location`, and
  `--target-format=backup` with `--target-location`, `--compress` and
  `--split-archive-part-size`, copy from and to Dodeca's backups. Where
  Enterprise's pages are silent, Dodeca is strict: `--source-location` names
  a backup file, not a folder, which must be a backup of `<fromDatabase>`;
  `--compress` and `--split-archive-part-size` without `--target-format=backup`
  are refused, as are `--to-path-data` and `--to-path-txn` with it. The backup
  written is a full backup of `<toDatabase>` with a database id of its own,
  named as `neo4j-admin database backup` names them, and holds no users and
  roles. The stores in between are staged in `--temp-path`, by default the
  local `--target-location`, or else next to the target database.
- **Schema**: one statement per index and constraint, with its index
  configuration. Index providers are left out, so that the copy gets the
  current ones. Token lookup indexes are not in the statements: the copy
  builds its own. Graph type constraints on relationship endpoint labels are
  left out, and so is the schema of labels, types and properties the filters
  leave out; the command lists what it left out.
- **`--copy-schema`** creates the indexes and constraints on the copy and
  waits until they are built, where Enterprise builds them when the database
  first starts.
- **Records that cannot be read**, as in a store with inconsistencies, are
  left out of the copy, with the relationships of nodes left out. They are
  listed in `<to-database>-copy.report`, next to the schema file, one per line:
  `node 123: <error>` or `relationship 456: <error>`.
- **`--copy-only-node-properties`** on a node with several labels keeps only
  the properties listed for any of its labels that has a list: a node that is
  a `Person` and an `Actor`, with only `Person.name` listed, keeps only
  `name`. `--skip-node-properties` drops a property listed for any of its
  labels.
- **`--temp-path`**, where a copy with the source's name is prepared, defaults
  to a place next to the database, on the same file system, rather than the
  current directory. A copy prepared on another file system is copied into
  place.
- **`--force`** also copies a store that was not shut down cleanly, as it is,
  without the transactions recovery would apply, with a warning. Without it,
  such a store is refused.
- **Clusters**: on a clustered server, a copy to a new name is prepared where
  `CREATE DATABASE ... OPTIONS {existingData: 'use', existingDataSeedServer:
  '<id>'}` takes it, as a restore is, and the command prints that statement. A
  copy is a store of its own, without the Raft position of the database it
  came from: a copy with the source's name does not start on a server that
  hosts the database as a secondary, or as a primary while it holds the
  database's Raft group, until the database is recreated from that server's
  store with `dbms.recreateDatabase`.

## Not supported

Dodeca does not support these Enterprise features, checked against the
Enterprise-only features, procedures, `neo4j-admin` commands and settings of
Enterprise's 5.26 documentation and the changes its documentation lists up to
2026.10. The sections above list smaller gaps, such as options of commands.
Unless an item says otherwise, their settings are not declared, so a
configuration that names one stops the server at startup with "Unrecognized
setting", as Neo4j does by default
(`server.config.strict_validation.enabled`); their commands fail with an
error, and their procedures do not exist.

- **Composite databases.** `CREATE COMPOSITE DATABASE` and the other commands
  for composite databases fail. A dotted name is a name of its own (see
  [Database aliases](#database-aliases)).
- **Replica databases**, Enterprise's replication of databases between
  clusters: `CREATE REPLICA DATABASE` and `ALTER DATABASE ... SET REPLICAS`
  fail, and `dbms.promoteReplicaDatabase` does not exist.
- **Sharded property databases** (Infinigraph), and `neo4j-admin backup
  validate`, which checks their backups.
- **Graph types**, Cypher 25's `ALTER CURRENT GRAPH TYPE` and the commands
  around it. Dodeca adds nothing to Neo4j Community there.
- **LDAP, single sign-on and Kerberos.** Users log in with native users and
  passwords only: `dbms.security.authentication_providers`,
  `authorization_providers`, `dbms.security.ldap.*`, `dbms.security.oidc.*`,
  the auth cache settings and `dbms.security.clearAuthCache`. Remote aliases
  with `OIDC CREDENTIAL FORWARDING` fail.
- **Reloading procedures while running**: `dbms.reloadProcedures()`.
- **Change data capture**: `txLogEnrichment`, `db.cdc.*` and `cdc.*`.
- **The `block` and `high_limit` store formats.** As Neo4j Community, Dodeca
  reads and creates only the `aligned` and `standard` formats.
- **The pipelined and parallel runtimes.** A query that asks for one runs on
  Neo4j Community's slotted runtime with a notification, as in Neo4j
  Community, or fails with `dbms.cypher.hints_error=true`. Neo4j Community
  declares `server.cypher.parallel.worker_limit`, which has no effect.
- **Shared query caches**:
  `server.memory.query_cache.shared_cache_num_entries`. Neo4j Community
  declares `server.memory.query_cache.sharing_enabled`; Dodeca adds nothing
  to it.
- **Settings of features Dodeca has in its own way**:
  `server.backup.exec_connector.*` and
  `server.backup.store_copy_max_retry_time_per_request`.
- **Neo4j Browser's Enterprise settings**: `browser.allow_outgoing_connections`,
  `credential_timeout`, `retain_connection_credentials` and
  `retain_editor_history`.
- **FIPS 140 compliance.**
- **Neo4j's separately licensed products**, such as Graph Data Science
  Enterprise Edition and Neo4j Operations Manager, are not tested with
  Dodeca.

## Moving to Dodeca

### From Neo4j Community

Neo4j Community of the pinned version, with the Dodeca jar in its `lib/`,
starts on Neo4j Community's data directory as it is. Its first start creates
the built-in roles and gives `admin` to one user (see
[Roles and privileges](#roles-and-privileges)). Other changes the server
shows: it writes the query log, at Neo4j's declared default `VERBOSE`, and
metrics to CSV files in `metrics` under the home directory (see
[Metrics and logs](#metrics-and-logs)); failed databases show as `offline`;
`dbms.components()` reports the Enterprise edition; fleet discovery is off
unless turned on (see [Edition](#edition)); and the default database
changes with `dbms.setDefaultDatabase`, no longer with
`initial.dbms.default_database`.

### From Neo4j Enterprise

- **Databases** move as dumps (`neo4j-admin database dump`), created on Dodeca
  from those ([`OPERATOR_GUIDE.md`, "Creating a database from a backup or a
  dump"](OPERATOR_GUIDE.md#creating-a-database-from-a-backup-or-a-dump)).
  Migrate a database in another store format than `aligned` or `standard` first,
  with `neo4j-admin database migrate --to-format=aligned`.
- **Roles and privileges** move as commands: Dodeca refuses to start on a
  `system` database that holds Enterprise's roles. [`OPERATOR_GUIDE.md`, "Moving
  roles from or to Neo4j
  Enterprise"](OPERATOR_GUIDE.md#moving-roles-from-or-to-neo4j-enterprise), has
  the steps, both ways.
- **Backups** are not promised to be exchangeable.
- **Settings** of the features Dodeca does not support, Enterprise's cluster
  discovery settings among them, stop the server at startup: remove them.
- **Clusters** are formed anew: Dodeca and Enterprise servers cannot form a
  cluster together.

### From DozerDB

Dodeca began as an add-on to [DozerDB](https://dozerdb.org) and contains a port
of the DozerDB code it built on
([`UPSTREAM_TOUCHPOINTS.md`](UPSTREAM_TOUCHPOINTS.md)). Neo4j Community
2026.10.2 with the Dodeca jar starts as it is on the data directory of DozerDB
2026.10.2-1.2.1, which runs on the same Neo4j, and of DozerDB 5.26.27.0, which
runs on Neo4j 5.26.27: databases, users, data and constraints carry over,
writes work, and the first start gives `admin` to one user as it does on Neo4j
Community's. The other users have `PUBLIC`, which reaches the home database
only, where DozerDB lets every user do everything. Compared with DozerDB
2026.10.2-1.2.1, as verified against the `graphstack/dozerdb:2026.10.2-1.2.1`
image and its source:

- User status and home databases take effect; DozerDB accepts both but acts
  on neither, so a suspended user still logs in.
- Roles and privileges work; DozerDB refuses them, and every user can do
  everything, such as creating users and databases.
- `START DATABASE` of a database that does not exist fails, where DozerDB
  succeeds, and `CREATE OR REPLACE DATABASE` of a database that does not exist
  yet works, where DozerDB fails.
- `WAIT` works, where DozerDB refuses it. Composite databases, `DUMP DATA` and
  options Dodeca does not support fail with an error, where DozerDB creates a
  standard database instead of a composite one and ignores the others.
- `dbms.max_databases` counts every database, where DozerDB allows one fewer.
- The default database cannot be dropped or replaced. DozerDB removes it from
  the system graph, so that it cannot be reached, until a restart adds it
  back. A stopped default database stays stopped when the server restarts,
  where DozerDB starts it again.
- A command takes effect once it commits. DozerDB starts, stops and drops
  databases before the command commits, so a command that then fails has still
  taken effect.
- Failed databases show as `offline`, where DozerDB shows a database that
  failed while running as `online`.
- Label existence and relationship endpoint label constraints, which only
  Neo4j's Java API can create, are refused, where DozerDB creates and enforces
  them.
