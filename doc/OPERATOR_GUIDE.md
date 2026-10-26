# DozenDB operator guide

## Cluster members

The members of a cluster are the voting members of the Raft group of the
`system` database: every member hosts `system`. Every other database has its
own Raft group, whose members are the servers the database is placed on (see
[Placing databases](#placing-databases)): the leader of `system` adds members
to each database's group and removes them from it until they match.

Two procedures show the members, on any member:

- `CALL dozendb.cluster.members()` lists the cluster's members with their Raft
  and Bolt addresses, where each takes writes that other members forward
  (`routingAddress`, see [Writes on any member](#writes-on-any-member)), which
  one leads `system`, and in `databases` the databases whose groups each member
  votes in.
- `CALL dozendb.cluster.status()` lists, for every database running on the
  member asked, the member's role, the leader, Raft indexes, and in `members`
  the ids of the members that vote in the database's group.

Changing the members needs an administrator, and changes one member at a time.
A change needs a majority of the members before it and a majority after it: in
a cluster of three, one member may be down while another is added or dropped,
and in a cluster of two both must be up to drop one.

### Adding a member

1. Configure the new server like the others: the same Neo4j and DozenDB
   versions, `dbms.dozendb.cluster.enabled=true`, the same TLS settings with a
   certificate of its own (see [TLS between members](#tls-between-members)),
   its own `server.dozendb.cluster.advertised_address`, and
   `dbms.dozendb.cluster.initial_members` listing the Raft address of at least
   one current member. Its data directory must be empty.
2. Start it. It copies `system` from a member, starts it without taking part
   in the cluster yet, and logs:

   ```
   This member is not part of the cluster yet. To add it, run
   CALL dozendb.cluster.addMember('<its Raft address>') on a member of the cluster.
   ```

3. Run that call on any member. It returns once the new member votes in the
   group of `system`, which brings it up to date first. The new member is now
   a Free server: it hosts no other database.
4. Enable it: `ENABLE SERVER '<its id>'`, with the id `SHOW SERVERS` shows
   (see [Servers](#servers)). As in Neo4j Enterprise, this places no database
   on it: new databases, topologies that grow and `REALLOCATE DATABASES` do
   (see [Placing databases](#placing-databases)). For each database placed on
   it, it copies the database and joins the database's group; the database's
   leader brings it up to date before it votes. `dozendb.cluster.members()`
   shows each database it joined, and routing tables list it for a database
   once it votes there. Reads and writes go on throughout. With servers
   enabled as they join, this step is not needed.

`system`, copied by the new member before it is added, is not copied again
once it is added, unless the cluster discarded the Raft log since. A database
that is offline when it is placed on the server is copied there once it is
started again.

`addMember` refuses a server that is not reachable, runs another Neo4j
version, still starts, belongs to another cluster (its databases came from
elsewhere) or was dropped from this one. A server whose certificate the
members do not trust is not reachable: the error says why, for example
`unable to find valid certification path`.

### Dropping a member

```
DEALLOCATE DATABASES FROM SERVER '<name>'
DROP SERVER '<name>'
```

with the name or id `SHOW SERVERS` shows. `DEALLOCATE DATABASES FROM SERVER`
moves every copy the server hosts to other servers (see
[Moving copies](#moving-copies)); it fails, changing nothing, if a copy has
nowhere to go. Meanwhile the server is `Deallocating`, and once it holds no
copy, `Deallocated`. `DROP SERVER` then takes it out of the cluster: the leader
of `system` removes it from the group of `system`. A Free server hosts no
database, and is dropped at once. A server dropped while it is down learns it
when it starts again.

A dropped server stops its databases, logs an error saying that it was
removed, and takes no further part in the cluster; `SHOW SERVERS` lists it as
`Dropped` while it still answers. Shut it down. It cannot be added again as it
is: to bring the server back, delete its whole data directory,
`data/server_id` included, and start it; it then waits to be added as a new
member.

`DROP SERVER` refuses an Enabled or Cordoned server, a deallocating one that
still holds a copy, and the last server. A copy counts as held as long as the
server votes in the database's group, as far as the leader of `system` knows:
a server that is down counts with what it last reported, so that a database
whose only copy is on it is not lost.

### Replacing a failed member

Add the replacement and enable it, then deallocate the failed server: its
copies are made again on the other servers, from the copies they hold, and it
leaves each database's group. Once `SHOW SERVERS` shows it `Deallocated`, drop
it. Deallocating first works as well, if the other servers can take its
copies.

A database whose only copy is on the failed server cannot be copied: it stays
placed where `DEALLOCATE` moved it, without a copy, and the failed server stays
`Deallocating`. Start the failed server again, or restore the database from a
backup (see [Restoring a database](#restoring-a-database)). Databases with more
than one primary avoid this (see [Raising the default](#raising-the-default)).

### Changing an address

A member's Raft address is part of its membership. To change it, drop the
member, delete its data directory, and add it again with the new address. With
hostname verification, its certificate must name the new address.

### A database that failed on one member

A member that cannot apply a transaction of a database, for instance because
its disk failed, stops its copy of that database; the other members go on as
long as a majority of them hold theirs. `SHOW DATABASES` then lists the copy on
that member as `offline`, with the error as `statusMessage`, and drivers are
sent to the other members for that database. The member's `debug.log` has the
error, and the leader logs `Failed appendEntries` for that member, saying that
the group is closed on it, once every 5 seconds.

Once the cause is repaired, run `STOP DATABASE` and then `START DATABASE`: the
member starts its copy again from its local state, and catches up with the
others. Both commands act on every server the database is placed on, so the
database is unavailable in between. Restarting the member repairs the copy as
well, along with its other databases.

## Servers

Each member is a server, as in Neo4j Enterprise. `SHOW SERVERS` lists them on
any member, with Enterprise's columns: `name`, `address` (its Bolt address),
`state`, `health` and `hosting`, and with `YIELD *` also `serverId`,
`httpAddress`, `httpsAddress`, `requestedHosting`, `tags`, `allowedDatabases`,
`deniedDatabases`, `modeConstraint` and `version`. A server's name is its id
until `RENAME SERVER` gives it another; the server commands take either.

A server is in one of these states:

- **Enabled**: databases can be placed on it, and its options are recorded in
  the system graph. The members a cluster forms with are enabled at its first
  start, each with the options of its `initial.server.*` settings.
- **Free**: a member added to a running cluster. It votes in the group of
  `system` and hosts no other database until `ENABLE SERVER` enables it, so
  clients connected to it find no other database there.
- **Cordoned**: an enabled server that keeps the copies it hosts and takes no
  new ones.
- **Deallocating**: its copies move to other servers, and it takes no new
  ones.
- **Deallocated**: deallocating, and it holds no copy any more: no database is
  placed on it and it votes in no database's group. It can be dropped.
- **Dropped**: dropped from the cluster, and still running.

A Free server can lead `system`, as it votes in its group, and changes the
databases' groups as any leader of `system` does, through the members that
vote in them.

`health` is `Available` while the server answers the member asked, and
`Unavailable` otherwise; `hosting` lists the databases running on it, none
while it does not answer, and `requestedHosting` those it should host:
`system`, and the databases placed on it that are requested online. A
standalone server shows itself alone, enabled, and takes `ALTER SERVER` and
`RENAME SERVER` too; deallocating or dropping it fails, as no other server can
host its databases.

### Enabling a server

```
ENABLE SERVER '<name or id>' OPTIONS {tags: ['eu']}
```

enables a Free server, and a Cordoned one again. The options it is not given
come from the server's own `initial.server.tags`,
`initial.server.mode_constraint`, `initial.server.allowed_databases` and
`initial.server.denied_databases`, which the member asks it for: the server
must be running, unless every option is given. `ENABLE SERVER` on an enabled
server with the same options changes nothing, and with other options fails:
`ALTER SERVER` changes them. It places no database on the server.

With `initial.dbms.automatically_enable_free_servers=true` at the cluster's
first start, servers that join are enabled at once, with their initial
options. `CALL dbms.cluster.setAutomaticallyEnableFreeServers(true)` (or
`false`) changes this later, for the whole cluster.

### Changing a server

```
ALTER SERVER '<name>' SET OPTIONS {tags: ['eu', 'fast']}
RENAME SERVER '<name>' TO '<new name>'
```

`ALTER SERVER` replaces every option of an enabled server: options it is not
given take their defaults. Copies that the new options do not allow move to
other servers (see [Moving copies](#moving-copies)); if one has nowhere to go,
`ALTER SERVER` fails and changes nothing. `RENAME SERVER` gives an enabled
server a name that no other server has, and that is not another server's id.
Both refuse a Free server. The options:

- `tags`: strings without duplicates or commas, which name the server for the
  leader priority (see [Where databases are led](#where-databases-are-led)).
  Default: none.
- `modeConstraint`: `NONE`, the default, or `PRIMARY`: the server hosts
  primaries. With `SECONDARY` it hosts no primary, and as DozenDB has no
  secondaries yet, no database but `system`.
- `allowedDatabases`: if not empty, the only databases placed on the server.
  `deniedDatabases`: databases not placed on it. Only one of the two can be
  set. Default: none.

### Cordoning, deallocating and dropping

```
CALL dbms.cluster.cordonServer('<name>')
[DRYRUN] DEALLOCATE DATABASES FROM SERVER[S] '<name>'[, '<name>' ...]
DROP SERVER '<name>'
```

A cordoned server keeps its copies and takes no new ones until
`ENABLE SERVER` (or `dbms.cluster.uncordonServer`, which Enterprise
deprecates) enables it again. `DEALLOCATE DATABASES FROM SERVER` makes servers
`Deallocating` and moves all their copies away; `DRYRUN` lists the moves and
changes nothing, with Enterprise's columns `database`, `fromServerName`,
`fromServerId`, `toServerName`, `toServerId` and `mode`. These procedures move
copies off servers without changing their state, and return the same rows:

- `dbms.cluster.deallocateDatabaseFromServer(server, database)` and
  `dbms.cluster.deallocateDatabaseFromServers(servers, database)`: the copies
  of one database;
- `dbms.cluster.deallocateNumberOfDatabases(server, number)`: the copies of
  that many of the server's databases, by name.

Each takes `dryRun` as an optional last argument. Deallocating fails, changing
nothing, for a database requested offline, which cannot be copied (start it
first), for one with a majority of its copies on cordoned servers, and when no
other server can host a copy. See [Dropping a member](#dropping-a-member) for
`DROP SERVER`.

`SHOW SERVERS` needs the privilege `SHOW SERVERS`, and the other server
commands `SERVER MANAGEMENT`, which includes it; the `admin` role has both.
The procedures need an administrator. Like the other administration commands,
the server commands run on the leader of `system`.

## Placing databases

Each database but `system` is placed on some of the enabled servers, as in
Neo4j Enterprise: it has a copy, a primary, on each, and runs in a Raft group
of those servers. Every server hosts `system`.

```
CREATE DATABASE films TOPOLOGY 3 PRIMARIES
ALTER DATABASE films SET TOPOLOGY 2 PRIMARIES
```

Without `TOPOLOGY`, a new database gets the default number of primaries: 1,
as in Neo4j Enterprise, or `initial.dbms.default_primaries_count` (1 to 11) at
the cluster's first start. `CALL dbms.setDefaultAllocationNumbers(3, 0)`
changes it later, for new databases, and `CALL dbms.showTopologyGraphConfig()`
shows it. The second number, of secondaries, must be 0: DozenDB has no
secondaries yet. The default database of a new cluster gets the default number
of primaries, or as many as there are servers if fewer.

`ALTER DATABASE films SET ACCESS READ ONLY` (or `READ WRITE`) makes a database
read-only on every server, as in Neo4j Enterprise. `ALTER DATABASE ... SET
OPTION` is refused.

### Raising the default

A database with one primary has one copy: while its server is down, the
database is unavailable, and if the server's disk is lost, so is the database,
apart from its backups. A database with more primaries takes writes while a
majority of them run: 2 of 3, 3 of 5. In a cluster of three, give databases
three primaries: set `initial.dbms.default_primaries_count=3` on the initial
members, or call `dbms.setDefaultAllocationNumbers(3, 0)` before creating
databases, and give the default database three copies too:
`ALTER DATABASE neo4j SET TOPOLOGY 3 PRIMARIES`.

### Where copies go

A copy goes to an enabled server, not Cordoned or Deallocating, whose
`modeConstraint` allows primaries and whose `allowedDatabases` and
`deniedDatabases` let it host the database; a server hosts at most one copy of
a database. New copies go to servers that answer first, then to those that host
the fewest copies, then by id. When a topology shrinks, copies leave servers
that are cordoned first, then those that do not answer, then those that host
the most. A command that cannot place every copy fails and changes nothing,
such as `CREATE DATABASE` with more primaries than servers can host it.

### Moving copies

Copies move when a topology changes, when `ALTER SERVER` options no longer
allow a copy, and on `DEALLOCATE` and `REALLOCATE`. A move makes the new copy
first: the new server copies the database from a server that holds it, and
joins the database's group once it caught up. Only then does the old copy
leave the group, and is deleted. A copy that leads the database first hands
the leadership over to one that stays, waiting at most 30 seconds. Reads and
writes go on throughout. `SHOW DATABASES` lists each copy, with
`currentStatus`:

- `store copying` on a server that copies the database;
- `initial` on a server it is placed on that has no copy yet;
- `deallocating` on a server it moves off, which may still lead it.

`WAIT` waits for the servers a database is placed on, and for a dropped
database for every server that still holds it.

`REALLOCATE DATABASES` moves copies so that the servers host about as many
each: first the copies their servers may no longer host, then one at a time
from a server that hosts the most to one that hosts at least two fewer.
`DRYRUN REALLOCATE DATABASES` lists the moves and changes nothing.
`CALL dbms.cluster.reallocateDatabase('films')` evens out the copies of one
database, and `CALL dbms.cluster.reallocateNumberOfDatabases(5)` makes at most
5 moves; both take `dryRun` as an optional last argument and return the moves.
As enabling a server places nothing on it, reallocate to give it copies.

### Clients

Routing tables, from any server, list the servers a database is placed on, so
`neo4j://` drivers reach them wherever they connect. A transaction that reaches
a server that does not host its database runs on one that does, when its
client lets the server route it (see [Writes on any member](#writes-on-any-member)):
a write on the leader, a read on any server that runs the database. Otherwise
it fails, saying where to connect.

## TLS between members

Members talk to each other on their Raft port: Raft itself, membership
changes, and copies of databases all go through it. A clustered member does
not start until `dbms.ssl.policy.cluster.enabled` is set, on every member
alike:

- `true` (recommended): every connection between members uses TLS, and each
  end presents a certificate that the other end trusts (mutual TLS).
- `false`: plaintext. Any host that reaches a Raft port can read what members
  send and act as a member. Members log a warning at every start. Use it only
  on a network that nothing but the members can reach.

The settings are Neo4j's own SSL policy settings of the `cluster` scope, as in
Neo4j Enterprise. A typical member has:

```
dbms.ssl.policy.cluster.enabled=true
dbms.ssl.policy.cluster.verify_hostname=true
```

and these files in `certificates/cluster` under the Neo4j home (or the
directory `dbms.ssl.policy.cluster.base_directory` names):

| File | Content |
|---|---|
| `private.key` | The member's private key, in PEM. |
| `public.crt` | The member's certificate, in PEM, followed by any intermediate certificates. |
| `trusted/` | The certificates of the authorities whose certificates members accept, in PEM, one per file. |
| `revoked/` | Optional: certificate revocation lists of those authorities. |

Every member's certificate must come from an authority in every member's
`trusted/` directory, and serve both ends of a connection: if it lists
extended key usages, it needs both `serverAuth` and `clientAuth`.

- `verify_hostname=true` makes a member check that the certificate of the
  member it connects to names the address it dials: the host of that member's
  `server.dozendb.cluster.advertised_address`, as a DNS name or IP address
  among the certificate's subject alternative names. Neo4j's default is
  `false`, which only checks that a trusted authority issued the certificate.
- `client_auth` keeps its default, `REQUIRE`, for members to refuse
  connections without a trusted certificate. `NONE` leaves only the member
  connected to checked, and members log a warning; `OPTIONAL` is refused.
- `tls_versions` (default TLS 1.2 and 1.3) and `ciphers` restrict the protocol
  versions and cipher suites.
- A certificate is not tied to a member id: members accept every certificate
  from a trusted authority. Use an authority, or an intermediate authority,
  that issues certificates for the cluster alone.

Each member logs how it talks to the others when it starts, in `debug.log`,
for example `Raft member ... listening on 0.0.0.0:7700 (TLS, client
certificates required, hostnames verified)`.

For example, with OpenSSL 3, an authority and a certificate for a member that
others reach as `core1`:

```
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -days 3650 -subj "/CN=DozenDB cluster CA" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=core1" -keyout private.key -out core1.csr
printf "subjectAltName=DNS:core1\nextendedKeyUsage=serverAuth,clientAuth\n" \
  > core1.ext
openssl x509 -req -in core1.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 825 -extfile core1.ext -out public.crt
```

Copy `private.key` and `public.crt` to the member, `ca.crt` to `trusted/` on
every member, and keep `ca.key` elsewhere.

### Replacing certificates

Members read their certificates when they start. Replace a member's files and
restart it, one member at a time. To move to another authority, first add its
certificate to `trusted/` on every member, restarting each; then give each
member a certificate from the new authority, restarting each; then remove the
old authority from `trusted/`, restarting each.

### Switching between TLS and plaintext

Members with TLS and members without cannot reach each other. Shut down every
member, change `dbms.ssl.policy.cluster.enabled` on each, and start them all
again.

## Writes on any member

Each database takes writes on its leader alone. A write transaction that
reaches another member runs on the leader when its client lets the server
route it, as with Neo4j Enterprise's server-side routing:

- drivers that route (`neo4j://`), while the members set
  `dbms.routing.default_router=SERVER`. With the default, `CLIENT`, these
  drivers send writes to the leader themselves, as the routing tables say;
- HTTP clients, on the Query API and the transactional endpoint;
- Bolt clients connected to one member (`bolt://`, also Cypher Shell with
  `-a bolt://...`), only while that member sets
  `dbms.dozendb.routing.forward_bolt_writes=true`. The setting is off by
  default, as in Neo4j Enterprise, and dynamic. Without it, such a write fails
  with `Neo.ClientError.Cluster.NotALeader`, and the error names the leader's
  Bolt address.

The member forwards the whole transaction, explicit or auto-commit,
administration commands included: they run on the leader of `system`. On the
leader, the transaction runs as its user, with the same privileges,
impersonation, metadata and timeout; the results, notifications, errors and
bookmarks the client gets are the leader's. The leader's query log and
security log record it, with `dozendb-forwarding/<version>` as the client's
user agent, and `SHOW TRANSACTIONS` and `TERMINATE TRANSACTION` on the leader
see it. Read transactions run on the member they reach if it hosts the
database (see [Clients](#clients) otherwise), and linearizable reads
(`COMPATIBILITY.md`) wait where the transaction runs.

### Requirements

Members forward writes to each other's routing connector, which only members
use. On every member:

- Mutual TLS between members: `dbms.ssl.policy.cluster.enabled=true` with
  `client_auth` at its default, `REQUIRE` (see
  [TLS between members](#tls-between-members)). A member logs in on another's
  routing connector with its certificate, naming the user whose transaction it
  forwards, and the routing connector takes no other logins. It uses the
  cluster's SSL policy as Raft does: with `verify_hostname=true`, a member's
  certificate must also name the host of its routing address.
- `dbms.routing.enabled=true`, Neo4j's default, and Bolt enabled.
- The routing connector reachable from the other members:
  `server.routing.listen_address` (default `:7688`) and
  `server.routing.advertised_address`, the address other members connect to
  (default: the host of `server.default_advertised_address` and port 7688).
  Clients need not reach it.

`dozendb.cluster.members()` shows each member's `routingAddress`, or null for
a member that takes no forwarded writes. Such a member logs why when it
starts. A member that forwards no writes says why in the error that a write
on it gets.

### Failures

- **No leader.** The member waits for one up to
  `dbms.dozendb.cluster.election_timeout_max`, then fails the transaction with
  `Neo.ClientError.Cluster.NotALeader`. Drivers retry managed transactions
  (`executeWrite`) on that error.
- **The leader changes, or cannot be reached.** Open transactions fail with
  `Neo.TransientError.Transaction.LeaderSwitch` when another member leads the
  database or none has for twice the election timeout, and with
  `Neo.TransientError.General.DatabaseUnavailable` when the leader does not
  answer. Nothing of them was committed, and drivers retry managed
  transactions on the new leader.
- **The leader goes away during a commit.** If the member sent the commit and
  got no answer, the transaction fails with
  `Neo.DatabaseError.Transaction.TransactionCommitFailed`, saying that it may
  or may not have committed, as a commit on the leader itself does when the
  leader loses its majority.
- **The leader refuses the user**, for example one suspended meanwhile:
  `Neo.ClientError.Security.Forbidden`.

### Connections

Each member keeps a pool of connections to each leader it forwards to,
following Neo4j's settings for server-side routing:
`dbms.routing.driver.connection.pool.max_size` (default unlimited),
`dbms.routing.driver.connection.pool.acquisition_timeout`,
`dbms.routing.driver.connection.pool.idle_test`,
`dbms.routing.driver.connection.max_lifetime` and
`dbms.routing.driver.connection.connect_timeout`. What the connections log goes
to `debug.log`, at `dbms.routing.driver.logging.level`.

## Where databases are led

Each database has a leader of its own, which takes all its writes: member 1
can lead `system`, member 2 `neo4j` and member 3 `films`. The members share
the write load as far as they share the leaderships.

Leadership moves:

- **In an election**, when the leader stops answering, and at the first
  election of a new cluster's databases. Any member that votes may win.
- **When a member stops a database it leads**, or stops itself: it hands each
  leadership to the member leading the fewest databases among those where the
  database runs and is writable, those with the database's priority tag first
  (below), so that a stopping member spreads its leaderships over the others.
- **When a database is read-only on the member that leads it**, by
  `server.databases.read_only`, `server.databases.default_to_read_only` or
  `server.databases.writable`, which are dynamic and set per member: the
  member hands the leadership to the member where the database is writable
  that leads the fewest databases, those with the database's priority tag
  first, within a second or two. Meanwhile routing
  tables list no writer for the database, so routing drivers wait and retry.
  A database that is read-only on every member keeps its leader, its writes
  fail with `Neo.ClientError.General.WriteOnReadOnlyAccessDatabase`, and the
  leader logs a warning once.
- **To a member with the database's priority tag**, a server tag that
  `db.cluster.raft.leader_transfer.priority_tag` names for every database, or
  `db.cluster.raft.leader_transfer.priority_tag.<database>` for one, in place
  of the other: a member that leads such a database without the tag hands the
  leadership, within a second or two, to the member with the tag that leads
  the fewest databases, among those where the database runs and is writable.
  This follows tags that `ALTER SERVER` changes. While no member with the tag
  can take over, as while all are down, the leader keeps the database and
  logs that once; the leadership moves once one is back and has caught up.
  The settings are not dynamic; give every member the same. A database whose
  name contains a dot cannot have a priority tag of its own: Neo4j reads the
  setting's name only up to the dot, and ignores the setting.
- **To balance the leaderships**, with
  `dbms.cluster.raft.leader_transfer.balancing_strategy=EQUAL_BALANCING`, the
  default. Once the leaders have stayed put on every member for 10 seconds,
  the member that leads the most databases hands one over to the member that
  leads the fewest, if they lead at least two apart. One move at a time, until
  every member leads as many databases as the others, give or take one; every
  database counts the same, `system` too. Databases with a priority tag count
  but stay where the priority puts them. This is how a member that comes
  back from a restart gets its share again, once it has caught up with the
  writes it missed, and how a new database, which
  starts led by the member that created it (the leader of `system`), finds
  its place. `NO_BALANCING` leaves the leaders where elections and handovers
  put them. The setting is dynamic; give every member the same.

A member that hands a leadership over on purpose stops taking new commits of
the database, lets the commits under way finish, then hands over, which
usually takes a fraction of a second. Commits meanwhile fail with
`Neo.ClientError.Cluster.NotALeader`, as on a member that does not lead:
nothing of them was committed, and drivers retry managed transactions
(`executeWrite`) on the new leader. A transaction that the client keeps open
across the move fails the same way and needs a retry. No commit is left with
an unknown outcome by a handover, as it can be when a leader fails.

To see where databases are led: `CALL dozendb.cluster.status()` on any member
lists its role and the leader of every database; the metric
`cluster.raft.is_leader` is 1 on each database's leader. Every handover is
logged in `debug.log` with its reason, as in
`Transferred leadership of neo4j(...) to ...: the database is read-only on this
member`, and counted in `dozendb.cluster.raft.leadership_transfers` on the
member that handed over.

## Upgrading DozenDB

DozenDB is upgraded one member at a time while the cluster keeps serving.
Every member must keep running the same Neo4j version: members refuse to form
a cluster with, add, or rejoin a member that runs another one. To upgrade
Neo4j, shut down every member, upgrade them all, and start them again.

A cluster mixes at most two DozenDB versions, the one it ran and the next.
Each version supports a range of format levels: the formats members send each
other and keep, such as the entries of the Raft log. The cluster's format
level says which formats members write. A newer version can read the formats
of the cluster's level, so it can run next to the previous version; it writes
newer formats only once the upgrade is finished.

1. On one member at a time: shut it down, replace the DozenDB jar in Neo4j's
   `lib/`, and start it. Before the next member, wait until it has started
   (Neo4j logs `Started.`), `CALL dozendb.cluster.status()` on it shows the
   group of every database with the role `FOLLOWER` or `LEADER`, and it has
   caught up with the writes it missed: for each database, its `appliedIndex`
   is close to the `commitIndex` that `dozendb.cluster.status()` shows on the
   database's leader. With one member down at a time, a cluster of three or
   more members keeps a majority and goes on serving writes; a member still
   catching up counts towards that majority only once it has.
2. Check that every member runs the new version: `CALL
   dozendb.cluster.members()` shows each member's `dozendbVersion` and the
   `formatLevels` it supports.
3. Finish the upgrade, as an administrator, on the leader of the `system`
   database:

   ```
   CALL dozendb.cluster.finishUpgrade()
   ```

   It raises the cluster's format level to the newest level any member
   supports, and returns `previousFormatLevel` and `formatLevel`. It refuses
   while a member runs a version that does not support that level, naming the
   member, or does not answer.

Until the upgrade is finished, any member can return to the previous version
the same way. Afterwards none can: a member refuses to start with a version
that does not support the cluster's format level, and so does a new member
added to the cluster. `CALL dozendb.cluster.formatLevel()` shows the cluster's
format level and the levels the member asked supports. When a new version
supports no newer format level, `finishUpgrade` changes nothing; calling it
after every upgrade is harmless.

## Backup and restore

Backups are taken from running servers, standalone or clustered, with
`neo4j-admin database backup`, and kept as backup artifacts: one file per
backup. A full backup holds a copy of a database; a differential backup holds
the transactions committed since the backup before it. A full backup and the
differential backups after it form a chain, which restores the database as of
the last of them.

### Serving backups

Every server serves backups of its databases while `server.backup.enabled` is
`true`, the default, on `server.backup.listen_address`, by default
`127.0.0.1:6362`. Any member of a cluster can serve any database: before it
sends one, it waits until it holds every write that the cluster acknowledged
when the backup started, so a backup from a follower is as recent as one from
the leader.

The backup port has no user authentication: whoever reaches it can copy every
database. A server serves backups on an address other than a loopback one only
once `dbms.ssl.policy.backup.enabled` is set explicitly:

- `true` (recommended): TLS, with Neo4j's SSL policy settings of the `backup`
  scope, as in Neo4j Enterprise, and certificates in `certificates/backup`
  laid out as for [TLS between members](#tls-between-members).
  `dbms.ssl.policy.backup.client_auth` keeps its default, `REQUIRE`, so that
  only clients with a certificate from a trusted authority can take backups.
  `NONE` logs a warning; `OPTIONAL` is refused.
- `false`: plaintext. The server logs a warning at every start. Use it only on
  a network that nothing but the backup clients can reach.

`neo4j-admin` reads the same settings from the configuration of the server it
runs on to connect, so the client presents its own certificate and checks the
server's, its host name too with `dbms.ssl.policy.backup.verify_hostname=true`.

### Taking backups

```
neo4j-admin database backup --from=core1:6362,core2:6362 --to-path=/backups "*"
```

- The databases are names or patterns in double quotes; the default is
  `neo4j`. `"*"` backs up every database, `system` included.
- `--from` lists servers, tried in order until one serves the backup of a
  database: name several members of a cluster.
- `--type=AUTO`, the default, takes a differential backup if `--to-path`
  holds a backup of the database to continue, and a full one otherwise.
  `DIFF` and `AUTO` take a full backup instead when the server no longer holds
  the transactions after the latest backup, or when the latest backup is of
  another database of that name. `FULL` always takes a full backup.
- Files are named `<database>-<yyyy-MM-ddTHH-mm-ss>.backup`, the time in UTC.
  A file appears only once the backup is complete and synced to disk.
- A full backup's copy of the database is recovered and checked before it is
  kept, in `--temp-path` (default: `--to-path`) with a page cache of
  `--pagecache`. `--keep-failed=true` keeps the copy of a failed full backup
  there.
- `--compress` (default `true`) compresses the artifacts.
- `--include-metadata=all` (the default) keeps with each backup the Cypher that
  gives a DBMS back the users, roles and privileges of the database: `roles`
  the roles with privileges on it, on every database or on the home database,
  and those privileges; `users` the users who can access it, with their
  encrypted passwords, status, home database and roles among those; `all`
  both; `none` nothing. Backups of `system` keep none: `system` holds them all.
- The command exits with 1 if a database could not be backed up, naming it,
  or if a server could not be reached although another one served.

### Inspecting and checking backups

- `neo4j-admin backup inspect /backups` lists the backups there.
  `--show-metadata` adds each one's database and its id, time, whether it is
  full and compressed, and the range of transactions it holds (by append
  index). `--latest-backup` and `--latest-chain` narrow the list, `--database`
  picks a database, `--format=JSON` prints JSON, and `--empty` includes
  backups that hold no transactions.
- `neo4j-admin database check --from-path=/backups neo4j` restores the latest
  chain of `neo4j` in a temporary directory and checks its consistency.
- `neo4j-admin database aggregate-backup --from-path=/backups neo4j` turns the
  latest chain into one full backup; the backups after the chain still continue
  it. `--keep-old-backup=true` keeps the chain's files.

Each differential backup needs every backup of its chain. To keep backups for
a while, delete whole chains, oldest first, or aggregate them first.

### Restoring a database

With the database stopped (`STOP DATABASE films`), or not created yet:

```
neo4j-admin database restore --from-path=/backups/films-2026-10-21T13-52-10.backup
```

- `--from-path` names a backup, which restores the chain ending at it, or a
  directory, which restores the latest chain of each database in it. Several
  paths may be given, separated by commas.
- A name after the options restores a single backup under another name.
- `--restore-until=123` stops before transaction 123, and
  `--restore-until="2026-10-21 13:00:00"` restores the transactions committed
  before that time, in UTC.
- `--overwrite-destination=true` replaces an existing database.

Then run `START DATABASE films`, or `CREATE DATABASE films` for a database the
server does not have yet.

A backup taken with `--include-metadata` makes the restore write the users,
roles and privileges of the database to
`data/scripts/films/restore_metadata.cypher`, under the name it is restored
as. Once the database exists, run the script on `system`, as the restore
prints:

```
cat data/scripts/films/restore_metadata.cypher | bin/cypher-shell -d system --param "database => 'films'"
```

The script only adds what is missing: users that exist keep their passwords,
status and home database, and running it twice does no harm.

In a cluster, a restored database cannot be put in place on every server that
hosts it by hand. Restore it on one member, which prepares it there and prints
the command that creates the database from it:

```
CREATE DATABASE films OPTIONS {existingData: 'use', existingDataSeedServer: '<member id>'}
```

That member, which the database is placed on first, creates it from the
restored files, and the other servers it is placed on copy it from that
member. Drop a database before restoring it under its old name.

### Creating a database from a backup or a dump

`CREATE DATABASE` can create a database from a full backup, the last backup of
a chain, or a dump made with `neo4j-admin database dump`:

```
CREATE DATABASE films OPTIONS {existingData: 'use', seedURI: 'file:///backups/films-2026-10-21T13-52-10.backup'}
```

Without clustering the server reads the seed. In a cluster, one of the servers
the database is placed on reads it and creates the database, and the others
copy it from that server: the one that `existingDataSeedServer` names, which
the database is placed on first, or else one the cluster chooses. That server
must be able to read the seed. `dbms.databases.seed_from_uri_providers` lists
the kinds of seed URIs servers read: `FileSeedProvider` (`file:`, the default)
and `URLConnectionSeedProvider` (`http:`, `https:` and `ftp:`). A seed that
cannot be read fails the database on that server, and `SHOW DATABASES` and
`WAIT` say why.

### Recovering a whole cluster

When every member of a cluster is lost, recreate it from backups of every
database, `system` included, taken with `"*"`:

1. On one server with an empty data directory and clustering disabled, restore
   every database: `neo4j-admin database restore --from-path=/backups`.
2. Configure that server and the other members for a new cluster, each with an
   empty data directory but the first, and start them all. The server that
   holds the restored databases forms the cluster with the others and creates
   every database that its `system` database lists. Each database is placed
   again on as many servers as its topology asks for, or on the default number
   if that is larger (see [Placing databases](#placing-databases)), or on
   every server if there are fewer: that server first, and the other servers
   copy the databases placed on them.

Users, roles, privileges, the default numbers of copies and whether each
database is online come with `system`.

## Roles and privileges

Roles and privileges work as Neo4j Enterprise's operations manual describes,
with the differences listed in `COMPATIBILITY.md`. With clustering, their
commands run on the system leader: run them there, or through a routing
driver or Cypher Shell (`neo4j://`), which sends them there, or on any member
that forwards them (see [Writes on any member](#writes-on-any-member)).

### The first admin

The first start of a DozenDB version with roles, on a new or an existing
store, creates the built-in roles and gives `admin` to one user: the user
chosen with `neo4j-admin dbms set-default-admin <user>` before that start,
else `neo4j`, else the only user. Every other user has only `PUBLIC`: give
them roles before they need more, for example
`GRANT ROLE reader TO alice`. With several users, none of them called
`neo4j` and none chosen, the server refuses to start until one is chosen.
The log names the user that got `admin`.

### Recovering admin access

When no user who can still log in has `admin`, start every server with
authentication disabled, give the role back, and enable authentication
again:

1. Stop every member. Set `dbms.security.auth_enabled=false` on each, and
   make sure that only trusted clients reach Bolt meanwhile, for example with
   `server.bolt.listen_address=127.0.0.1:7687`.
2. Start every member. On the member where `CALL dozendb.cluster.status()`
   shows `system` with the role `LEADER`, give the role back; the other
   members refuse the command with `NotALeader`:
   `bin/cypher-shell -a bolt://localhost:7687 -d system "GRANT ROLE admin TO neo4j"`.
   With authentication disabled, every command is allowed, including those
   about `IMMUTABLE` roles and privileges. A user whose password is lost gets
   a new one the same way:
   `ALTER USER neo4j SET PASSWORD 'new-password' CHANGE REQUIRED`.
3. Stop every member, remove the setting, and start them again.

A server with clustering disabled is recovered the same way.

### Impersonation

`GRANT IMPERSONATE (bob) ON DBMS TO support` lets the users with the role
`support` run queries as `bob`, with bob's privileges and home database: in
Cypher Shell with `--impersonate bob`, in drivers with the session's
impersonated user. Impersonating users does not allow administration
commands that change anything.

### Procedures and plugins such as APOC

`PUBLIC` may execute every procedure and function (`EXECUTE PROCEDURE *` and
`EXECUTE FUNCTION *`), which then read and write only what the user may. As
in Neo4j Enterprise, some can do more: procedures a role may run with
`EXECUTE BOOSTED` run with full access, and procedures listed in
`dbms.security.procedures.unrestricted`, which plugins such as APOC ask for,
can reach Neo4j's internals, for example to open transactions of their own,
as APOC triggers do. Limit who may run such procedures by narrowing what
`PUBLIC` may execute, for example:

```
REVOKE EXECUTE PROCEDURE * ON DBMS FROM PUBLIC
GRANT EXECUTE PROCEDURE db.*, dbms.*, tx.* ON DBMS TO PUBLIC
GRANT EXECUTE PROCEDURE apoc.* ON DBMS TO developer
```

A `DENY` to `PUBLIC` would not do: it applies to every user, whatever their
other roles grant.

### Moving roles from or to Neo4j Enterprise

DozenDB keeps roles and privileges in the system graph in nodes of its own,
and refuses to start on a system database that holds Neo4j Enterprise's.
Move them as commands instead, in either direction:

1. On the server that has them, save the output of `SHOW ROLES WITH USERS`
   and `SHOW PRIVILEGES AS COMMANDS`, and the users from `SHOW USERS`.
2. Move the databases other than `system` as dumps
   (`neo4j-admin database dump`), and create them on the new servers from
   those ([Creating a database from a backup or a dump](#creating-a-database-from-a-backup-or-a-dump)).
   Neo4j Community, and so DozenDB, reads only the `aligned` and `standard`
   store formats: migrate a database in another format with
   `neo4j-admin database migrate --to-format=aligned` first.
3. On the new servers, create the users (`CREATE USER`, with new passwords:
   no command shows the old ones) and the roles (`CREATE ROLE`), give the
   users their roles (`GRANT ROLE`), and run the saved privilege commands.
   Commands about databases or features the new servers lack fail and can
   be left out.


## Database aliases

A local alias is another name of a database on this DBMS:

```cypher
CREATE ALIAS movies FOR DATABASE films
```

A remote alias names a database on another DBMS, which transactions in the
alias run on, logged in with the user and password it keeps:

```cypher
CREATE ALIAS `remote-films` FOR DATABASE films
  AT 'neo4j+s://films.example.com:7687'
  USER alice PASSWORD 'alice-secret'
  DRIVER {connection_timeout: duration('PT5S')}
```

`COMPATIBILITY.md` describes what aliases do and where DozenDB answered what
Neo4j's documentation leaves open.

### Keystore for remote aliases

DozenDB encrypts the passwords of remote aliases with an AES key from a
PKCS12 keystore, under Neo4j Enterprise's settings. Make the keystore with
the JDK's `keytool`, with the same password for the keystore and the key:

```sh
keytool -genseckey -keyalg aes -keysize 256 -storetype pkcs12 \
  -keystore /var/lib/neo4j/conf/aliases.p12 -alias aliases \
  -storepass "$KEYSTORE_PASSWORD"
```

```properties
dbms.security.keystore.path=/var/lib/neo4j/conf/aliases.p12
dbms.security.keystore.password=<the keystore password>
dbms.security.key.name=aliases
```

The settings are dynamic and read at each use. Without a readable key,
creating or using a remote alias fails naming the three settings. A new key
cannot read the passwords encrypted with the old one: set them again with
`ALTER ALIAS ... SET DATABASE PASSWORD`. With clustering, every member needs
the same keystore, since every member runs transactions in remote aliases
itself.

### TLS for remote aliases

The URL's scheme decides: `neo4j+s` and `bolt+s` trust the certificates that
the JVM's trust store trusts, `neo4j+ssc` and `bolt+ssc` any certificate. To
trust a private CA, add it to a copy of the JDK's `cacerts` and name the copy
for the JVM; the JVM's other TLS clients, such as `LOAD CSV` over HTTPS, then
trust it too:

```sh
cp "$JAVA_HOME/lib/security/cacerts" /var/lib/neo4j/conf/trust.jks
keytool -importcert -noprompt -alias films-ca -file films-ca.crt \
  -keystore /var/lib/neo4j/conf/trust.jks -storepass changeit
```

```properties
server.jvm.additional=-Djavax.net.ssl.trustStore=/var/lib/neo4j/conf/trust.jks
```

A trust store of the PKCS12 type also needs
`server.jvm.additional=-Djavax.net.ssl.trustStorePassword=<its password>`,
on a line of its own: without it, the JVM reads none of its certificates.

`neo4j` and `bolt` without TLS are refused unless the alias has
`DRIVER {ssl_enforced: false}`.

### Failures of remote aliases

Errors of the other DBMS reach the client as remote execution failures,
`Neo.ClientError.Statement.RemoteExecutionClientError` for instance, whose
message names the other DBMS's status, such as
`Neo.ClientError.Security.Unauthorized` for a wrong password or
`Neo.ClientError.Database.DatabaseNotFound` for a target that does not exist
there. A DBMS that cannot be reached gives
`Neo.TransientError.Statement.RemoteExecutionTransientError`. Connection
failures towards it are also in `debug.log`, at the level of
`dbms.routing.driver.logging.level` unless the alias's driver settings set
another.

## Monitoring

DozenDB reports metrics, and writes the query log and the security log, as
Neo4j Enterprise's operations manual describes them; `COMPATIBILITY.md` lists
the differences. With clustering, each member reports and logs what happens
on it: monitor every member.

### Metrics

Metrics are on by default and reported over JMX. For Prometheus, enable the
endpoint on every member, on an address Prometheus reaches (the default,
`localhost:2004`, serves only the member's own host):

```
server.metrics.prometheus.enabled=true
server.metrics.prometheus.endpoint=0.0.0.0:2004
```

In Neo4j's Docker image, set `NEO4J_server_metrics_prometheus_enabled=true`
and `NEO4J_server_metrics_prometheus_endpoint=0.0.0.0:2004`, and publish port
2004. Prometheus scrapes `http://<member>:2004/metrics`.

| Setting | Default | |
|---|---|---|
| `server.metrics.enabled` | `true` | `false` reports no metrics at all. |
| `server.metrics.prefix` | `neo4j` | The start of every name. |
| `server.metrics.filter` | Neo4j Enterprise's list | Globs of the names reported; `*` reports every metric. |
| `server.metrics.jmx.enabled` | `true` | MBeans in the domain `neo4j.metrics`, also read by `CALL dbms.queryJmx('neo4j.metrics:*')`. |
| `server.metrics.prometheus.enabled` | `false` | |
| `server.metrics.prometheus.endpoint` | `localhost:2004` | |

Names are Neo4j Enterprise's: `neo4j.dbms.<name>` for the server and
`neo4j.database.<database>.<name>` for each database, such as
`neo4j.database.neo4j.transaction.committed`. In Prometheus, characters other
than letters, digits and `_` become `_`, counters end in `_total`, and
histograms are summaries with quantiles, `_sum` and `_count`:
`neo4j_database_neo4j_transaction_committed_total`. A database's series
appear when it starts and go when it stops or is dropped. Metrics that only
DozenDB has carry `dozendb` in their names.

A filter replaces the default list rather than adding to it. To report more,
repeat the default, which
`SHOW SETTINGS YIELD name, defaultValue WHERE name = 'server.metrics.filter'`
shows, and append globs such as `*cluster.raft.*,*dozendb.cluster.*`.

With clustering, each member reports for each database, under
`neo4j.database.<database>.`:

| Metric | |
|---|---|
| `cluster.raft.is_leader` | 1 on the member that leads the database and takes its writes, else 0. The sum over the members should be 1; alert when it stays otherwise. |
| `cluster.raft.append_index`, `commit_index`, `applied_index`, `prune_index`, `term` | The member's Raft log: its last entry, the last entry known committed, the last applied to the database, and the first still kept. |
| `cluster.raft.last_leader_message` | Milliseconds since the member last heard from the leader; 0 on the leader. |
| `cluster.raft.replication_new`, `replication_attempt`, `replication_success`, `replication_fail`, `replication_maybe` | Transactions the member asked Raft to replicate, and their outcomes. `replication_maybe` counts transactions whose outcome the client did not learn, as when the leader changes during the commit. |
| `dozendb.cluster.replication.latency.millis` | Milliseconds from asking Raft to replicate a transaction until it was applied. |
| `dozendb.cluster.raft.followers`, `follower_lag_max` | On the leader only: the followers it sends to, and the most committed entries one of them does not have yet. A lag that keeps growing means a follower does not keep up. |
| `dozendb.cluster.raft.snapshots`, `leader_changes` | Raft snapshots taken and leaders learned of since the member started. |
| `dozendb.cluster.raft.leadership_transfers` | Leaderships the member handed over to another member on purpose since it started: when it stopped the database, when the database was read-only on it, or to balance leaders (see [Where databases are led](#where-databases-are-led)). |
| `dozendb.cluster.store_copy.received`, `served`, `repeated`, `received.millis` | Copies of the database the member installed, to join or to catch up; copies it sent to other members; copies it needed within an hour of the previous one, a sign that it does not keep up (see "Raft log retention and catch-up" in `COMPATIBILITY.md`); and how long catching up took. |

The server reports `neo4j.dbms.dozendb.cluster.members`, the voting members
as the member knows them.

### Query log

Every member writes `logs/query.log`, as `conf/server-logs.xml` configures it:
seven files of 20 MB. At the default level, `db.logs.query.enabled=VERBOSE`,
it logs the start and the end of every query, with its parameters. `INFO`
logs the end of queries that took at least `db.logs.query.threshold`
(default `0s`, every query), and `OFF` logs nothing. The other
`db.logs.query.*` settings work as Neo4j documents them, for example
`db.logs.query.parameter_logging_enabled=false` to leave parameters out, or
`db.logs.query.obfuscate_literals=true` to hide the literals in query text.
Passwords never appear: Neo4j hides them in administration commands, and a
query that cannot be parsed is logged with its string literals masked.

Logging costs time on every query. In a benchmark of trivial queries over
Bolt, which shows the cost the most, `VERBOSE` served 8% fewer queries per
second than `OFF` from one client and 12% fewer from eight, and raised the
99th percentile latency from 0.26 to 0.29 ms; `INFO`, with the default
threshold, served 2 to 3% fewer. Small write transactions, which wait for the
disk, lost less than 5%. Metrics cost less than the variation between runs,
and a scrape of every metric of a server with 102 databases, about 14,000
series, took 16 ms and 3 MB, or 180 kB compressed as Prometheus asks for it.
`MonitoringBenchmark` in `dozendb-core` measures this.

For JSON lines, use Neo4j's layout in the `QueryLog` appender of
`conf/server-logs.xml`, in place of its `PatternLayout`:

```
<JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/QueryLogJsonLayout.json"/>
```

### Security log

Every member writes `logs/security.log`:

- logins (`[alice]: logged in`), unless
  `dbms.security.log_successful_authentication=false`, and failed logins with
  the reason (`[alice]: failed to log in: invalid principal or credentials`);
- administration commands that changed something, with passwords hidden
  (`[neo4j]: CREATE USER bob SET PASSWORD '******' CHANGE REQUIRED`), on the
  member where they committed: with clustering, the leader of `system`;
- privileges refused and impersonation, with the user, as
  `[alice:bob]: impersonating user bob logged in` when `alice`
  impersonates `bob`.

For JSON lines, use
`<JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/StructuredJsonLayout.json"/>`
in the `SecurityLog` appender.

### Changing settings while running

`CALL dbms.setConfigValue('db.logs.query.enabled', 'INFO')` changes a
dynamic setting at once, until the member restarts; an empty value restores
the default. It needs the `admin` role and changes only the member it runs
on: with clustering, run it on each member, connecting to each directly
(`bolt://`), and change `neo4j.conf` too so that the change outlasts a
restart. A member with `dbms.dozendb.routing.forward_bolt_writes=true`
forwards write transactions to the leader, which the procedure would change
instead: run it in a read transaction there, as with Cypher Shell's
`--access-mode read`. `SHOW SETTINGS YIELD name, isDynamic WHERE isDynamic` lists the
dynamic settings.
