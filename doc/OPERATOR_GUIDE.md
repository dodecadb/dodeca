# Dodeca operator guide

How to install and run Dodeca, alone or as a cluster.
[`COMPATIBILITY.md`](COMPATIBILITY.md) says where it differs from Neo4j
Enterprise.

- [Installing Dodeca](#installing-dodeca)
- [Cluster members](#cluster-members)
- [Servers](#servers)
- [Placing databases](#placing-databases)
- [Secondaries](#secondaries)
- [Routing policies](#routing-policies)
- [TLS between members](#tls-between-members)
- [Writes on any member](#writes-on-any-member)
- [Linearizable reads](#linearizable-reads)
- [Where databases are led](#where-databases-are-led)
- [Changing the default database](#changing-the-default-database)
- [Moving databases to Cypher 25](#moving-databases-to-cypher-25)
- [Upgrading Dodeca](#upgrading-dodeca)
- [Backup and restore](#backup-and-restore)
- [Importing and copying databases](#importing-and-copying-databases)
- [Cloud storage](#cloud-storage)
- [Disaster recovery](#disaster-recovery)
- [Roles and privileges](#roles-and-privileges)
- [Database aliases](#database-aliases)
- [Checkpoints](#checkpoints)
- [Page cache warmup](#page-cache-warmup)
- [Monitoring](#monitoring)
- [Dodeca's settings](#dodecas-settings)

## Installing Dodeca

Dodeca is one jar added to Neo4j Community's `lib/`. It runs only on the Neo4j
version it was built for, 2026.10.2, as it builds on Neo4j's internal classes
([`COMPATIBILITY.md`, "Pinned versions"](COMPATIBILITY.md#pinned-versions)), on
Java 21 or 25.

### The jar

Each [release](https://github.com/dodecadb/dodeca/releases) has the jar,
`dodeca-<version>-2026.10.2.jar`, about 70 MB: it bundles what Dodeca
needs, and its name says which Neo4j version it runs on. To check that the
release workflow built it, from the tagged commit:

```sh
gh attestation verify dodeca-<version>-2026.10.2.jar --repo dodecadb/dodeca
```

Built from source as the README says, the jar is
`dodeca-plugin/target/dodeca-<version>-2026.10.2.jar`; the small
`dodeca-plugin-<version>.jar` next to it is not the one.

Copy it into the `lib/` directory of Neo4j Community 2026.10.2, not into
`plugins/`: it replaces how Neo4j starts. `neo4j-admin` finds Dodeca's commands
there too, such as `neo4j-admin database backup`. Without further settings, the
server runs alone, as Neo4j Community does, with what Dodeca adds; it starts on
a Neo4j Community data directory as it is (see [`COMPATIBILITY.md`, "From Neo4j
Community"](COMPATIBILITY.md#from-neo4j-community)). `neo4j.log` names the
version at each start: `Dodeca 0.1.0-SNAPSHOT: clustering is disabled`.

### Docker

Each release publishes an image, `ghcr.io/dodecadb/dodeca`, for `linux/amd64`
and `linux/arm64`: Neo4j's official Community image with the jar in its
`lib/`, which runs as Neo4j's image does. Its tags are the release's version,
such as `0.2.0`, the version with Neo4j's, `0.2.0-2026.10.2`, the minor
version, `0.2`, and `latest` (see [`RELEASING.md`, "What a release
publishes"](RELEASING.md#what-a-release-publishes)).

With a jar built from source, build such an image:

```dockerfile
FROM neo4j:2026.10.2-community
COPY dodeca-0.1.0-SNAPSHOT-2026.10.2.jar /var/lib/neo4j/lib/
```

Mounting the jar into the container read-only does not work: Neo4j's
entrypoint changes the owner of the files under `/var/lib/neo4j`, and the
container stops when it cannot. Settings are environment variables, as in
Neo4j's image: `NEO4J_` and the setting's name, with `_` for `.` and `__` for
`_`, such as `NEO4J_dbms_dodeca_cluster_initial__members` for
`dbms.dodeca.cluster.initial_members`.

### Forming a cluster

A cluster forms from its initial members, three for a cluster that bears the
loss of one, five for two. Give each initial member, in `neo4j.conf`:

```properties
server.default_listen_address=0.0.0.0
server.default_advertised_address=core1.example.com
dbms.dodeca.cluster.enabled=true
dbms.dodeca.cluster.initial_members=core1.example.com:7700,core2.example.com:7700,core3.example.com:7700
dbms.ssl.policy.cluster.enabled=true
dbms.ssl.policy.cluster.verify_hostname=true
initial.dbms.default_primaries_count=3
```

- `server.default_listen_address`: Neo4j listens on `localhost` only by
  default, which other servers cannot reach. Neo4j's Docker image sets
  `0.0.0.0` itself.
- `server.default_advertised_address`: the host name or address of this
  member that the others and clients reach, its own on each member. It gives
  the member's addresses their host: Bolt on port 7687, Raft on 7700
  (`server.dodeca.cluster.advertised_address`) and the routing connector on
  7688 (`server.routing.advertised_address`). Neo4j's default, `localhost`,
  does not work between servers.
- `dbms.dodeca.cluster.initial_members`: the Raft addresses of every initial
  member, this one included, the same list on each. Members read it only at
  their first start, when they have no Raft state yet.
- `dbms.ssl.policy.cluster.enabled`: a member does not start until it is set.
  `true` makes members talk over mutual TLS, with a certificate for each (see
  [TLS between members](#tls-between-members)); `false` in plaintext, only on
  a network that nothing but the members reach.
- `initial.dbms.default_primaries_count=3`: new databases, and the default
  database, get three primaries, one on each member, so that each takes
  writes while one member is down. Without it they get one, as in Neo4j
  Enterprise, and a database is unavailable while its only server is down
  (see [Raising the default](#raising-the-default)). It is read at the
  cluster's first start.

[Dodeca's settings](#dodecas-settings) lists the others of Dodeca's own.

Set the same initial password on every member, as with `neo4j-admin dbms
set-initial-password` or Docker's `NEO4J_AUTH`: one of them creates the
users, and which one is decided at the first start.

Start every initial member. Each waits until it reaches all of them, logging
in `neo4j.log` every 10 seconds which it does not reach yet:

```
Waiting for initial cluster members, 2 of 3 reachable. Unreachable: ...
```

Once they all answer, one member creates `system` and the default database,
and the others copy them:

```
All 3 initial cluster members are reachable: [...]. Databases come from 95371b19-... at core1:7700, which creates them.
```

Each member then logs `Started.`. From then on, a member that starts again
rejoins from its own Raft state, and `initial_members` matters again only to
[recover the system database](#recovering-the-system-database); new members
join as [Adding a member](#adding-a-member) describes. A cluster can
also start from the databases of one existing server: list it among the
initial members, with its data directory, and give the others empty ones (see
[`COMPATIBILITY.md`, "Running a cluster"](COMPATIBILITY.md#running-a-cluster)).

To check the cluster, as an administrator on any member:

```cypher
CALL dodeca.cluster.members()
SHOW SERVERS
SHOW DATABASES
```

`dodeca.cluster.members()` lists the three members, one of them `LEADER` of
`system`; `SHOW SERVERS` lists them `Enabled` and `Available`; and
`SHOW DATABASES` lists `system` and the default database `online` on each.
Drivers connect with `neo4j://` to any member, which routes them (see
[Clients](#clients)), for example
`neo4j://core1.example.com:7687`.

To try a cluster on one machine, this Docker Compose file starts three
members of the published image on Docker's network, in plaintext. To try a
jar built from source, add `build: .` under `image:`, next to the
`Dockerfile` above and the jar, and add `--build` to `docker compose up`.

```yaml
x-member: &member
  image: ghcr.io/dodecadb/dodeca
  environment: &config
    NEO4J_AUTH: neo4j/change-this-password
    NEO4J_dbms_dodeca_cluster_enabled: "true"
    NEO4J_dbms_dodeca_cluster_initial__members: core1:7700,core2:7700,core3:7700
    NEO4J_initial_dbms_default__primaries__count: "3"
    NEO4J_dbms_ssl_policy_cluster_enabled: "false"
    # Small, for one machine.
    NEO4J_server_memory_heap_max__size: 1g
    NEO4J_server_memory_pagecache_size: 256m

services:
  core1:
    <<: *member
    hostname: core1
    environment:
      <<: *config
      NEO4J_server_default__advertised__address: core1
  core2:
    <<: *member
    hostname: core2
    environment:
      <<: *config
      NEO4J_server_default__advertised__address: core2
  core3:
    <<: *member
    hostname: core3
    environment:
      <<: *config
      NEO4J_server_default__advertised__address: core3
```

```sh
docker compose up -d
docker compose exec core1 cypher-shell -u neo4j -p change-this-password \
  "CALL dodeca.cluster.members()"
```

### Ports

| Port | Setting | Reached by |
|---|---|---|
| 7687 | `server.bolt.listen_address` | Clients: drivers, Cypher Shell, and remote aliases of other DBMSs. |
| 7474 | `server.http.listen_address` | HTTP clients, Neo4j Browser, and load balancers asking the [cluster status endpoints](#cluster-status-endpoints). |
| 7473 | `server.https.listen_address` | The same, over HTTPS, when `server.https.enabled` is set. |
| 7700 | `server.dodeca.cluster.listen_address` | The other members: Raft, membership, probes, database copies, and secondaries' pulls. |
| 7688 | `server.routing.listen_address` | The other members, with TLS between them: writes they forward (see [Writes on any member](#writes-on-any-member)). |
| 6362 | `server.backup.listen_address` | `neo4j-admin database backup`, at `server.backup.advertised_address` with `--remote-address-resolution`; only from the server's own host by default (see [Serving backups](#serving-backups)). |
| 2004 | `server.metrics.prometheus.endpoint` | Prometheus, when enabled; only from the server's own host by default (see [Metrics](#metrics)). |

Members also connect out: to Graphite (2003) when it is enabled, to cloud
storage, and to the DBMSs of remote aliases.

### Stopping and starting a cluster

Members can be shut down and started in any order, one at a time or all
together, as for a Neo4j upgrade (see [Upgrading Dodeca](#upgrading-dodeca)).
A member keeps its Raft state and its databases, and starts from them
(`Rejoining the cluster from local Raft state`). It catches up with what the
others wrote meanwhile, from their Raft logs or with a copy (see
[`COMPATIBILITY.md`, "Raft log retention and
catch-up"](COMPATIBILITY.md#running-a-cluster)).

Each member says in `neo4j.log` when another stops answering, and why, within
the 10 seconds between its probes, and when it answers again:

```
Member 5f6e... at core3:7700 does not answer: probe of core3:7700 failed: UNAVAILABLE: Unable to resolve host core3 (java.net.UnknownHostException: core3)
Member 5f6e... at core3:7700 answers again
```

A database takes writes while a majority of its primaries run, once they
elected a leader, within seconds. A member that runs without that majority,
as the first member of a cluster to start, serves reads of what it holds, and
refuses writes: `No member leads database 'films' right now; writes succeed
once one is elected.` Start the others, and writes go on.

## Cluster members

The members of a cluster, its servers, are the voting members of the Raft
group of the `system` database, the primaries of `system`, and the servers
that host `system` as a secondary, which pull it instead (see
[`system` as a secondary](#system-as-a-secondary)): every member hosts
`system`. Every other database has its own Raft group, whose members are the
servers the database is placed on as a primary (see
[Placing databases](#placing-databases)): the leader of `system` adds members
to each database's group and removes them from it until they match.

Two procedures show the members, on any member:

- `CALL dodeca.cluster.members()` lists the cluster's members with their Raft
  and Bolt addresses, where each takes writes that other members forward
  (`routingAddress`, see [Writes on any member](#writes-on-any-member)), which
  one leads `system`, which host it as secondaries (`SECONDARY`), and in
  `databases` the databases whose groups each member votes in.
- `CALL dodeca.cluster.status()` lists, for every database running on the
  member asked, the member's role, the leader, Raft indexes, and in `members`
  the ids of the members that vote in the database's group.

Changing the members needs an administrator, and changes one member at a time.
A change of the primaries of `system` needs a majority of them before it and a
majority after it: with three, one may be down while another is added or
dropped, and with two both must be up to drop one.

### Adding a member

1. Configure the new server like the others (see
   [Forming a cluster](#forming-a-cluster)): the same Neo4j and Dodeca
   versions, `dbms.dodeca.cluster.enabled=true`, the same TLS settings with a
   certificate of its own (see [TLS between members](#tls-between-members)),
   its own `server.dodeca.cluster.advertised_address`, and
   `dbms.dodeca.cluster.initial_members` listing the Raft address of at least
   one current member. Its data directory must be empty.
2. Start it. It copies `system` from a member, starts it without taking part
   in the cluster yet, and logs:

   ```
   This member is not part of the cluster yet. To add it, run
   CALL dodeca.cluster.addMember('<its Raft address>') on a member of the cluster.
   ```

3. Run that call on any member. It returns once the new member votes in the
   group of `system`, which brings it up to date first. The new member is now
   a Free server: it hosts no other database. A server that hosts `system` as
   a secondary is added on the leader of `system` instead (see
   [`system` as a secondary](#system-as-a-secondary)).
4. Enable it: `ENABLE SERVER '<its id>'`, with the id `SHOW SERVERS` shows
   (see [Servers](#servers)). As in Neo4j Enterprise, this places no database
   on it: new databases, topologies that grow and `REALLOCATE DATABASES` do
   (see [Placing databases](#placing-databases)). For each database placed on
   it, it copies the database and joins the database's group; the database's
   leader brings it up to date before it votes. `dodeca.cluster.members()`
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
`unable to find valid certification path`. A server whose host name does not
resolve is probed again for up to 15 seconds before `addMember` fails with
`Unable to resolve host`: Java caches failed lookups for 10 seconds
(`networkaddress.cache.negative.ttl`), so the name of a server that was missing
from DNS until moments ago, as while its container was replaced, does not
resolve at once.

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
server votes in the database's group, as the members report it when the leader
of `system` asks them, as `SHOW SERVERS` does: a server that is down counts
with what it last reported, so that a database whose only copy is on it is not
lost.

### Replacing a failed member

Add the replacement and enable it, then deallocate the failed server: its copies
are made again on the other servers, from the copies they hold, and it leaves
each database's group. The replacement may have the failed server's address:
until the failed server is dropped, the others still count it, down, and the
replacement answers only for itself. Once `SHOW SERVERS` shows it `Deallocated`,
drop it. Deallocating first works as well, if the other servers can take its
copies.

A database whose only copy is on the failed server cannot be copied: it stays
placed where `DEALLOCATE` moved it, without a copy, and the failed server stays
`Deallocating`. Start the failed server again, or restore the database from a
backup (see [Restoring a database](#restoring-a-database)). Databases with more
than one primary avoid this (see [Raising the default](#raising-the-default)).

### Unbinding a server

`neo4j-admin server unbind` makes a server a new server of its cluster, as in
Neo4j Enterprise: for troubleshooting one server, when replacing it is not an
option. Until its old id is dropped, the cluster counts the old server as a
member that is down, and bears one failure fewer. On the stopped server:

```
neo4j-admin server unbind --archive-cluster-state=true --archive-path=<directory>
```

It removes the server's id (`data/server_id`) and its cluster state
(`data/raft`), which it first writes to
`unbound_cluster_state.<time>.zip` in `<directory>` when asked, and moves the
files of the server's databases to `data/unbound/<time>/databases` and
`data/unbound/<time>/transactions`, where they stay until deleted. It refuses
while the server runs, and prints the server's old id. Then:

1. Start the server. It starts under a new id, copies `system` and waits to be
   added, as a new member does (see [Adding a member](#adding-a-member)):
   `CALL dodeca.cluster.addMember('<its Raft address>')` on a member, then
   `ENABLE SERVER '<its new id>'`.
2. Drop the old server: `DEALLOCATE DATABASES FROM SERVER '<old id>'`, then,
   once `SHOW SERVERS` shows it `Deallocated`, `DROP SERVER '<old id>'`. Its
   copies are made again on the other servers, which may include the new one,
   from the copies they hold.

A database whose only copy was on the server cannot be copied (see [Replacing a
failed member](#replacing-a-failed-member)). Restore it from a backup, or from
its files in `data/unbound/<time>/`: put its two directories back under
`data/databases` and `data/transactions` of a stopped server without clustering,
dump it with `neo4j-admin database dump`, and create the database from the dump
(see [Creating a database from a backup or a
dump](#creating-a-database-from-a-backup-or-a-dump)).

A server whose `data/server_id` was removed or replaced otherwise, as Neo4j
Community's `neo4j-admin server unbind` does, refuses to start: under a new
id, the cluster would count neither its votes nor its databases, and it would
delete them. Put back `data/server_id` from a copy of the data directory to
start it as before, or unbind it as above.

### Changing an address

A member's Raft address is part of its membership. To change it, drop the
member, delete its data directory, and add it again with the new address. With
hostname verification, its certificate must name the new address.

### A database that failed on one member

A member that cannot apply a transaction of a database, for instance because
its disk failed, puts its copy of that database in quarantine, as Neo4j
Enterprise does; the other members go on as long as a majority of them hold
theirs. `SHOW DATABASES` then lists the copy on that member as `quarantined`,
with the error and the call that lifts the quarantine as `statusMessage`, and
drivers are sent to the other members for that database. The member's
`debug.log` has the error.

Once the cause is repaired, lift the quarantine, from any member:
`CALL dbms.unquarantineDatabase('<server>', '<database>')`, with the member's
name or id as `SHOW SERVERS` lists them. The member starts its copy again
from its local state, and catches up with the others. Neither a
restart nor `START DATABASE` starts a copy in quarantine (see
[Quarantine](#quarantine)).

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
- **Free**: a member added to a running cluster. It hosts `system`, as a
  primary or a secondary, and no other database until `ENABLE SERVER` enables
  it, so clients connected to it find no other database there.
- **Cordoned**: an enabled server that keeps the copies it hosts and takes no
  new ones.
- **Deallocating**: its copies move to other servers, and it takes no new
  ones.
- **Deallocated**: deallocating, and it holds no copy any more: no database is
  placed on it and it votes in no database's group. It can be dropped.
- **Dropped**: dropped from the cluster, and still running.

A Free server that hosts `system` as a primary can lead it, as it votes in its
group, and changes the databases' groups as any leader of `system` does,
through the members that vote in them.

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
- `modeConstraint`: `NONE`, the default: the server hosts primaries and
  secondaries; `PRIMARY`: only primaries; `SECONDARY`: only secondaries (see
  [Secondaries](#secondaries)). A copy whose mode the new constraint does not
  allow turns into the other mode where it may, without copying the database
  (see [Changing modes](#changing-modes)). How a server hosts `system` does not
  depend on it, but on `server.cluster.system_database_mode`.
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
`ENABLE SERVER` enables it again (or, in Cypher 5 only, the deprecated
`dbms.cluster.uncordonServer`). `DEALLOCATE DATABASES FROM SERVER` makes servers
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
Neo4j Enterprise: it has a copy on each, a primary or a secondary. Its
primaries run in a Raft group of those servers, and its secondaries pull from
them (see [Secondaries](#secondaries)). Every server hosts `system`.

```
CREATE DATABASE films TOPOLOGY 3 PRIMARIES 2 SECONDARIES
ALTER DATABASE films SET TOPOLOGY 2 PRIMARIES
```

Without `TOPOLOGY`, a new database gets the default numbers: 1 primary and 0
secondaries, as in Neo4j Enterprise, or `initial.dbms.default_primaries_count`
(1 to 11) and `initial.dbms.default_secondaries_count` (0 to 20) at the
cluster's first start, and a `TOPOLOGY` without `SECONDARIES` the default
number of secondaries. `CALL dbms.setDefaultAllocationNumbers(3, 1)` changes
both later, for new databases, and `CALL dbms.showTopologyGraphConfig()` shows
them. `ALTER DATABASE ... SET TOPOLOGY` without `SECONDARIES` keeps the number
of secondaries. The default database of a new cluster gets the default numbers,
or as many copies of each as the servers can host if fewer, its primaries
first.

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
`modeConstraint` allows its mode and whose `allowedDatabases` and
`deniedDatabases` let it host the database; a server hosts at most one copy of
a database, a primary or a secondary. Primaries go to servers whose
`modeConstraint` is `PRIMARY` before those with `NONE`, and secondaries to
those with `SECONDARY` before those with `NONE`, so that servers that take
either mode are used last. Within that, new copies go to servers that answer
first, then to those that host the fewest copies, of either mode, then by id.
When a topology shrinks, copies leave servers that are cordoned first, then
those that do not answer, then those that host the most. A command that cannot
place every copy fails and changes nothing, such as `CREATE DATABASE` with more
primaries than servers can host it.

### Moving copies

Copies move when a topology changes, when `ALTER SERVER` options no longer
allow a copy, and on `DEALLOCATE` and `REALLOCATE`, secondaries as primaries. A
move makes the new copy first: the new server copies the database from a server
that holds it, and joins the database's group once it caught up, or for a
secondary, runs it. Only then does the old copy leave the group, and is
deleted. A copy whose server may host it in the other mode only changes its
mode, keeping its store (see [Changing modes](#changing-modes)). A copy that
leads the database first hands the leadership over to one that stays, waiting
at most 30 seconds. Reads and writes go on throughout. `SHOW DATABASES` lists
each copy, with `currentStatus`:

- `store copying` on a server that copies the database;
- `initial` on a server it is placed on that has no copy yet;
- `starting` on a server where a new database runs but has yet to join its
  Raft group;
- `deallocating` on a server it moves off, which may still lead it.

`WAIT` waits for the servers a database is placed on, and for a dropped
database for every server that still holds it. A server has caught up with a
database that is to be online once its copy runs and it knows the database's
leader, so that the database takes writes through any server as soon as
`WAIT` returns, and can be backed up from any server that hosts it. `WAIT`
returns a row per server, with its `address`, `state` (`CaughtUp`, `Failed` or
`TimedOut`) and `message`, and in Cypher 25 also a notification per server:
`03N85` caught up, `01N80` failed, `01N81` still catching up, `01N82` not
available.

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
`neo4j://` drivers reach them wherever they connect; secondaries as readers (see
[Reading from secondaries](#reading-from-secondaries)). Servers that stop
answering drop out of the tables within about 10 seconds, and a driver can name
a routing policy that chooses among the others by their tags (see [Routing
policies](#routing-policies)). The leader is listed as the writer, and as a
reader too only with `dbms.routing.reads_on_writers_enabled=true` (dynamic, per
server), or while no other server can read the database. A server that does not
host the database answers from what the others last told it, and a server that
starts or stops leading a database tells the others at once, so that their
routing tables follow a new leader within moments. When what they told it names
no leader, or no server that runs the database, it asks them first. It waits up
to 1 second for each server's answer, and up to 3 seconds in all, so a server
that does not answer at all delays it by no more than that. A transaction that
reaches a server that does not host its database runs on one that does, when its
client lets the server route it (see [Writes on any
member](#writes-on-any-member)): a write on the leader, a read on any server
that runs the database. Otherwise it fails, saying where to connect.

## Secondaries

A database can have secondaries besides its primaries, as in Neo4j
Enterprise: read-only copies that pull committed transactions from another
server that hosts the database. They never vote in the database's Raft group
and never lead it, so they add servers to read from without slowing writes,
which wait for a majority of the primaries only.

```
CREATE DATABASE films TOPOLOGY 3 PRIMARIES 2 SECONDARIES
ALTER DATABASE films SET TOPOLOGY 3 PRIMARIES 4 SECONDARIES
```

A topology has 0 to 20 secondaries, placed as
[Where copies go](#where-copies-go) says: on servers whose `modeConstraint` is
`SECONDARY`, then on those with `NONE`. A new secondary copies the database
from a server that hosts it, then pulls from there on. `WAIT` waits for it as
for a primary, until it runs.

### Pulling

A secondary asks an upstream for the transactions it lacks every
`db.cluster.catchup.pull_interval` (1 second by default, not dynamic), at once
again while an answer brought as many as one pull carries (about 4 MiB), and
at once for a read that waits for a transaction it lacks (see
[Reading from secondaries](#reading-from-secondaries)). It applies them as a
primary applies the database's Raft entries, so that every copy holds the same
transactions with the same ids. It forces its transaction log once per pull;
after a crash it recovers what it forced, and pulls the rest again.

An upstream answers from its transaction logs. When they no longer hold the
next transaction a secondary needs, as after the secondary was down for longer
than its upstreams keep their logs (`db.tx_log.rotation.retention_policy`,
`2 days 2G` by default), the secondary copies the database from its upstreams
next to its own copy, which keeps serving reads meanwhile, and then puts the
new copy in place: the database is unavailable on that server for a moment,
and the server logs `Secondary <database> runs again, from a copy`. A
secondary whose store holds another history than its upstream's, as files put
in place by hand, fails with a message that says how to recover: delete the
database's files on that server and restart it, which copies the database.

A secondary that finds no upstream, or none that answers, logs a warning once
and retries every interval; `SHOW DATABASES` shows it online with a growing
lag meanwhile.

### Choosing upstreams

`server.cluster.catchup.upstream_strategy` lists strategies, asked in order
before every pull: the first that finds servers wins, and the secondary pulls
from one of them at random. Strategies find only servers that run the
database and answer. Without any, the strategy is
`typically-connect-to-random-secondary`; when no strategy finds a server, a
random primary is the last resort.
The group strategies that Enterprise removed in 2025.01,
`connect-randomly-to-server-group` and `connect-randomly-within-server-group`,
are refused at startup, with a message naming the tag strategy replacing each.

| Strategy | Upstream |
|---|---|
| `connect-to-random-primary-server` | A primary. |
| `typically-connect-to-random-secondary` | Another secondary; one pull in ten, a primary. |
| `connect-randomly-to-server-tags` | A secondary with a tag of `server.cluster.catchup.connect_randomly_to_server_tags` (dynamic). |
| `connect-randomly-within-server-tags` | A secondary that shares a tag with this server. |
| `leader-only` | The leader. |
| `user-defined` | As `server.cluster.catchup.user_defined_upstream_strategy` says (below). |

`user_defined_upstream_strategy` holds rules separated by `;`, asked in order:
the first that passes any server wins. A rule chains filters with `->`,
starting from every server that runs the database: `tags(a, b)` passes those
with either tag (`groups(...)` is its old name), `min(n)` passes the servers
it is given if there are at least `n`, else none, and `all()` passes every
server. `halt()` may end the last rule, which then passes none; otherwise
`all()` follows the rules. For example, to pull from a server in `eu`, else
from one in `us` if at least two run there, else from a random primary:

```
server.cluster.catchup.upstream_strategy=user-defined
server.cluster.catchup.user_defined_upstream_strategy=tags(eu); tags(us)->min(2); halt()
```

A server with an unknown strategy, or rules that do not parse, does not start.
A comma between rules, which one of Neo4j Enterprise's examples has, is
refused: its meaning is not documented. Routing policies are written in the
same rules (see [Routing policies](#routing-policies)).

### Reading from secondaries

Routing tables, from any server, list a database's secondaries as readers,
besides its primaries. With `dbms.routing.reads_on_primaries_enabled=false`
(not dynamic) they list only the secondaries as readers while any of them can
read, and the primaries when none can. Server-side routing reads where routing
tables say.

A secondary refuses writes as a follower does: with
`Neo.ClientError.Cluster.NotALeader`, naming the leader, or by running them on
the leader when the client lets the server route them (see
[Writes on any member](#writes-on-any-member)).

A read on a secondary sees what it pulled, about a pull interval behind the
primaries. A read whose bookmarks name a transaction the secondary lacks has it
pull from the leader at once, and waits for it as on any server
(`db.transaction.bookmark_ready_timeout`), so that a driver session reads its
own writes without waiting for the next pull. A linearizable read (see
[Linearizable reads](#linearizable-reads)) on a secondary first asks a primary
for the last transaction acknowledged before the read, then pulls until it has
it; it fails as on a follower when no primary answers. A backup taken from a
secondary does the same first, so that it holds every write acknowledged before
it started.

`SHOW DATABASES` lists each secondary with the role `secondary`, `writer`
false, its `lastCommittedTxn`, and as `replicationLag` how many transactions
it is behind the copy with the newest, as a negative number;
`currentSecondariesCount` and `requestedSecondariesCount` count them.
`SHOW SERVERS` includes them in `hosting` and `requestedHosting`.

### Pausing a secondary

```
CALL dbms.cluster.secondaryReplicationDisable('films', true)
CALL dbms.cluster.secondaryReplicationDisable('films', false)
```

pauses, then resumes, the secondary of `films` on the server connected to, for
example to read a fixed state over a `bolt://` connection, or to back it up as
it was. Each returns the secondary's `state`, `paused` or `running`. A paused
secondary pulls nothing, also not for reads that wait for a bookmark, which
time out. Routing tables leave it out, so that drivers get no data it stopped
updating, and `SHOW DATABASES` shows `Secondary replication is paused` as its
`statusMessage`. A pause ends when the database or the server starts again.
The procedure needs an administrator, and fails on a server where the database
does not run as a secondary. `dbms.cluster.readReplicaToggle` is its
deprecated name, in Cypher 5 only.

### Changing modes

A server that loses a primary of a database and may host a secondary of it,
or the reverse, keeps its copy and changes its mode, without copying the
database. This happens when `ALTER SERVER` changes a server's `modeConstraint`
(see [Changing a server](#changing-a-server)), and when a topology changes.

- A primary keeps voting until its replacement votes and the leader of
  `system` removes it from the database's group. It then starts the database
  again as a secondary, from its own store.
- A secondary stops pulling, asks the database's leader for the Raft term of
  its last transaction, and joins the database's group from its own store. If
  the leader's Raft log no longer holds that transaction, the server copies
  the database instead.

The database restarts on that server, which takes a moment; on the others,
reads and writes go on. For example, to let `read1`, which hosts only
secondaries, take over the primaries of `core3`, which is to host only
secondaries:

```
ALTER SERVER 'read1' SET OPTIONS {modeConstraint: 'NONE'}
ALTER SERVER 'core3' SET OPTIONS {modeConstraint: 'SECONDARY'}
```

In the other order, the second command fails and changes nothing while no
other server can host the primaries.

### `system` as a secondary

With `server.cluster.system_database_mode=SECONDARY` (not dynamic; `PRIMARY`
by default), a server hosts `system` as a secondary: it pulls `system` from
the servers that host it instead of voting in its Raft group. A cluster needs
few primaries of `system`, three, or five to survive the loss of two, and
administration commands wait for a majority of them only; the other servers
can be secondaries of `system`. Otherwise a system secondary takes part as any
server does: it is enabled, hosts databases in either mode, answers routing
requests, and takes administration commands, which it sends to the leader of
`system` as a follower does. Reads of `system` that must be current, such as
`WAIT`, ask a primary of `system` first, as linearizable reads do.

To add one, configure it as for [Adding a member](#adding-a-member), with
`server.cluster.system_database_mode=SECONDARY`. Its own address must not be
in `dbms.dodeca.cluster.initial_members`: only primaries of `system` form a
cluster, and the server refuses to start otherwise. Once the cluster has
formed, the server copies `system`, pulls it, and logs:

```
This server hosts the system database as a secondary and is not part of the
cluster yet. To add it, run CALL dodeca.cluster.addMember('<its Raft address>')
on the leader of the system database.
```

Run that call on the leader of `system`, which `dodeca.cluster.members()`
shows: a `neo4j://` connection in write mode, as Cypher Shell opens by
default, reaches it with `-d system`. The leader records the server, which
learns it when it next pulls and is then a Free server, listed by
`dodeca.cluster.members()` with the role `SECONDARY`. `DROP SERVER` drops it
as any other server: it learns it from the system graph it pulls, and stops
its databases.

To change how a server hosts `system`, restart it with the other
`system_database_mode`, then run `addMember` with its address on the leader of
`system` again. Until then it hosts `system` as before, and logs that it does.
A primary that turns secondary is recorded, then removed from the group of
`system`, which is refused if it is the group's only member; it pulls on from
its own store. A secondary that turns primary joins the group of `system`
from its own store, as in [Changing modes](#changing-modes); `addMember`
returns once it votes there. Its other databases stay as they are.

### Sizing

- **Primaries of `system`**: three, or five in a large cluster, with every
  other server a secondary of `system`. Administration commands commit with a
  majority of them, and only they take part in its elections.
- **Pull interval**: reads without bookmarks on a secondary lag by about
  `db.cluster.catchup.pull_interval`; reads with bookmarks do not wait for it.
  A longer interval sends upstreams fewer pulls.
- **Upstreams**: an upstream reads each pull's transactions from its
  transaction log and sends them. With many secondaries, let them pull from
  each other, as `typically-connect-to-random-secondary` does, or by tags, so
  that the primaries serve few pulls; at a site far from the primaries, let a
  few secondaries pull from the primaries and the others from them.
- **Transaction log retention**: a secondary that was down for longer than its
  upstreams keep their transaction logs copies the whole database. Keep them,
  on every server that serves pulls, for at least as long as a secondary may
  be down for maintenance.

## Routing policies

A driver that routes (`neo4j://`) can name a routing policy, which chooses the
servers of its routing tables by their server tags, as in Neo4j Enterprise:
clients in one data centre read from the servers there, and from others when
too few of those run. A policy is a setting, the same on every server:

```
dbms.routing.load_balancing.config.server_policies.<name>=<rules>
```

The rules are those of a secondary's upstreams (see [Choosing
upstreams](#choosing-upstreams)): rules separated by `;`, asked in order, the
first that passes any server wins; a rule chains filters with `->`, from every
server: `tags(a, b)` passes those with either tag, and two in a chain those
with a tag of each, `min(n)` passes the servers it is given if there are at
least `n`, else none, `all()` passes every server, and `halt()`, at the end of
the last rule, passes none. Unless the rules end with `halt()`, `all()` follows
them. For three data centres, two of them in Europe:

```
# Servers in eu-west, if at least two run; else none.
dbms.routing.load_balancing.config.server_policies.eu_west_only=tags(eu-west)->min(2); halt()
# Servers in eu-west, if at least two run; else every server.
dbms.routing.load_balancing.config.server_policies.eu_west=tags(eu-west)->min(2)
# Servers in eu-west or eu-central, if at least two run; else any in Europe; else every server.
dbms.routing.load_balancing.config.server_policies.eu=\
  tags(eu-west, eu-central)->min(2);\
  tags(europe)
```

The servers have their tags from `initial.server.tags` or `ALTER SERVER` (see
[Servers](#servers)); a server in `eu-west` above is tagged `eu-west,europe`. A
driver names the policy in its URI:

```
neo4j://db.example.com:7687?policy=eu_west
```

What a policy chooses:

- **Readers and routers**: among the servers that host the database and
  answered the latest probe of the server asked, as `min(n)` counts them; a
  server that stops drops out within about 10 seconds. The writer is the leader
  whatever the policy says, so writes work wherever the client is.
- **A policy that passes no reader** gives a table without readers: the
  driver fails reads, saying that it has no readers, and writes still work.
- **A policy that passes no router** leaves the leader as the only router, so
  that the driver can still refresh its table; while there is no leader, the
  servers it would route through without the policy.
- **A driver that names no policy** gets the policy named `default`, which is
  `all()` unless set.

Names have letters, digits and underscores, and are case-sensitive. A server
with a policy whose name has other characters, or whose rules do not parse, does
not start. A driver that names a policy the server does not have cannot use the
cluster: its routing requests fail with `Neo.ClientError.Routing.RoutingFailed`,
"Policy definition for 'eu' could not be found. The routing policies of this
server are default, eu_west, eu_west_only."

Every server must have the same policies, as Neo4j Enterprise says; otherwise
the routing table a driver gets depends on the server it asks. Servers compare
theirs, and a server whose policies differ from another's says so in
`neo4j.log`, once until they change:

```
Server 5f6e... at core3:7700 has other routing policies than this server, so that the routing tables of drivers that name a policy depend on the server they ask: set the same dbms.routing.load_balancing.config.server_policies.* on every server
```

Policies are read at start. Rename or remove one only once no client names it.

`dbms.routing.load_balancing.shuffle_enabled` (default `true`) lists the
readers and routers of each table in a new random order, so that drivers
spread their first connections; with `false`, in the order of the servers'
ids. `dbms.routing.load_balancing.plugin` takes only `server_policies`, its
default, as in Neo4j Enterprise.

Policies apply to routing tables, so to drivers that route on the client.
Server-side routing (`dbms.routing.default_router=SERVER`), a server with
clustering disabled, and a cluster of one server give Neo4j Community's
tables, which name only the server asked.

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
  `server.dodeca.cluster.advertised_address`, as a DNS name or IP address
  among the certificate's subject alternative names. It is Neo4j's default;
  `false` only checks that a trusted authority issued the certificate.
- `client_auth` keeps its default, `REQUIRE`, for members to refuse
  connections without a trusted certificate. `NONE` leaves only the member
  connected to checked, and members log a warning; `OPTIONAL` is refused.
- `tls_versions` (default TLS 1.2 and 1.3) and `ciphers` restrict the protocol
  versions and cipher suites. Without `ciphers`, members use the JDK's default
  suites without the CBC ones and those with static DH or ECDH key exchange.
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
  -days 3650 -subj "/CN=Dodeca cluster CA" -keyout ca.key -out ca.crt
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

Members read their certificates when they start. With
`dbms.security.tls_reload_enabled=true`, which itself takes a restart to
change, a member also reloads them while it runs:

1. Replace the files of the member's policies, such as `private.key` and
   `public.crt` in `certificates/cluster`.
2. Run `CALL dbms.security.reloadTLS()` on that member, as an administrator.
   It reloads every SSL policy of the server: Bolt, HTTPS, the cluster and
   backups.

Connections made afterwards use the new certificates; open ones keep theirs.
A reload whose files cannot be read fails and keeps the certificates in use.
Between members and for backups, a reload takes the certificate and the
trusted authorities (`trusted/`, `revoked/`); `tls_versions`, `ciphers`,
`client_auth` and `verify_hostname` of these two policies change only when the
member restarts, and `neo4j.log` says so when a reload finds them changed.

Without reloads, replace a member's files and restart it, one member at a
time.

To move to another authority, either way, first add its certificate to
`trusted/` on every member, reloading or restarting each; then give each member
a certificate from the new authority; then remove the old authority from
`trusted/`. Each step is done on every member before the next starts.

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
  `dbms.dodeca.routing.forward_bolt_writes=true`. The setting is off by
  default, as in Neo4j Enterprise, and dynamic. Without it, such a write fails
  with `Neo.ClientError.Cluster.NotALeader`, and the error names the leader's
  Bolt address.

The member forwards the whole transaction, explicit or auto-commit,
administration commands included: they run on the leader of `system`. On the
leader, the transaction runs as its user, with the same privileges,
impersonation, metadata and timeout; the results, notifications, errors and
bookmarks the client gets are the leader's. The leader's query log and
security log record it, with `dodeca-forwarding/<version>` as the client's
user agent, and `SHOW TRANSACTIONS` and `TERMINATE TRANSACTION` on the leader
see it. Read transactions run on the member they reach if it hosts the
database (see [Clients](#clients) otherwise), and linearizable reads
(see [Linearizable reads](#linearizable-reads)) wait where the transaction
runs.

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

`dodeca.cluster.members()` shows each member's `routingAddress`, or null for
a member that takes no forwarded writes. Such a member logs why when it
starts. A member that forwards no writes says why in the error that a write
on it gets.

### Failures

- **No leader.** The member waits for one up to
  `dbms.dodeca.cluster.election_timeout_max`, then fails the transaction with
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
- **The member stops**, for example in a rolling restart. It forwards no new
  transactions: they fail with `Neo.TransientError.General.DatabaseUnavailable`,
  and drivers retry managed transactions through another member. It waits for
  the transactions it already forwards to end, up to
  `db.shutdown_transaction_end_timeout` (default 10 seconds), as a database
  that stops waits for its own, and their clients can still commit them
  meanwhile. The member's debug log says how many it waits for. It then ends
  the others.

### Connections

Each member keeps a pool of connections to each leader it forwards to,
following Neo4j's settings for server-side routing:
`dbms.routing.driver.connection.pool.max_size` (default unlimited),
`dbms.routing.driver.connection.pool.acquisition_timeout`,
`dbms.routing.driver.connection.pool.idle_test`,
`dbms.routing.driver.connection.max_lifetime` and
`dbms.routing.driver.connection.connect_timeout`. What the connections log goes
to `debug.log`, at `dbms.routing.driver.logging.level`.

## Linearizable reads

A read sees the writes that its transaction's bookmarks name, as in Neo4j
Enterprise, so a driver session reads its own writes on any server. A write
it holds no bookmark of, such as another client's, may not be visible yet on a
server other than the leader, although it was acknowledged: the other servers
apply what the leader commits a moment later. A linearizable read, Dodeca's
own, sees every write acknowledged anywhere in the cluster before the read
began, wherever it runs.

A transaction asks for one with the metadata key `dodeca.consistency` set to
`linearizable`, in any case; `causal` is the usual read. In the Java driver:

```java
var linearizable = TransactionConfig.builder()
    .withMetadata(Map.of("dodeca.consistency", "linearizable"))
    .build();
long films = session.executeRead(
    tx -> tx.run("MATCH (f:Film) RETURN count(f) AS n").single().get("n").asLong(),
    linearizable);
```

Other drivers pass transaction metadata alike, such as Python's
`@unit_of_work(metadata={"dodeca.consistency": "linearizable"})`. Any other
value fails the transaction with `Neo.ClientError.Request.Invalid`.
`dbms.dodeca.reads.default_consistency=LINEARIZABLE` (not dynamic) makes
every transaction on a member that names no consistency linearizable, also
those of clients that cannot set metadata, such as Cypher Shell. Linearizable
reads apply to Bolt clients.

Before the transaction begins, the server it runs on makes sure that it
applied everything the database had committed when the read asked:

- A primary asks the leader for its commit index, a Raft ReadIndex: the leader
  first confirms with a majority of the primaries that it still leads. The
  primary then waits until it applied up to that index.
- A secondary asks a primary the same, then pulls until it holds it (see
  [Reading from secondaries](#reading-from-secondaries)).

Each linearizable transaction thus waits for a round trip to the leader and
from it to a majority, where a read with a bookmark waits only while the
server lacks the bookmarked write. Use bookmarks where the client has them,
and linearizable reads where it has none, as across clients. A write
transaction that a member forwards waits on the leader (see
[Writes on any member](#writes-on-any-member)).

A read that cannot be confirmed, as on a server that does not reach a
majority of the primaries, fails with
`Neo.TransientError.Cluster.ReplicationFailure`, "A linearizable read of films
could not be confirmed with the leader; retry later.", which drivers retry in
managed transactions (`executeRead`). Causal reads still run there.

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
  `server.databases.writable`, which are dynamic and set per member: the member
  hands the leadership to the member where the database is writable that leads
  the fewest databases, those with the database's priority tag first, within a
  second or two. Meanwhile routing tables list no writer for the database, so
  routing drivers wait and retry. A database that is read-only on every member
  keeps its leader, its writes fail with
  `Neo.ClientError.General.WriteOnReadOnlyAccessDatabase`, and the leader logs a
  warning once.
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
  default. Once the leaders have stayed put on every member for 10 seconds, the
  member that leads the most databases hands one over to the member that leads
  the fewest, if they lead at least two apart. One move at a time, until every
  member leads as many databases as the others, give or take one; every database
  counts the same, `system` too. Databases with a priority tag count but stay
  where the priority puts them. This is how a member that comes back from a
  restart gets its share again, once it has caught up with the writes it missed,
  and how a new database, which starts led by the member that created it (the
  leader of `system`), finds its place. While the leaders stay uneven although
  they stayed put, the member that leads the most databases logs why once in
  `debug.log`, as `Leaders stay uneven`: a database without a leader or whose
  voting members are changing, each member that leads fewer with why it cannot
  take a database over (it does not answer, does not run the database yet, or
  has it read-only), or a member that does not keep up with the database's log.
  `NO_BALANCING` leaves the leaders where elections and handovers put them. The
  setting is dynamic; give every member the same.

A member that hands a leadership over on purpose stops taking new commits of
the database, lets the commits under way finish, then hands over, which
usually takes a fraction of a second. Commits meanwhile fail with
`Neo.ClientError.Cluster.NotALeader`, as on a member that does not lead:
nothing of them was committed, and drivers retry managed transactions
(`executeWrite`) on the new leader. A transaction that the client keeps open
across the move fails the same way and needs a retry. No commit is left with
an unknown outcome by a handover, as it can be when a leader fails.

To see where databases are led: `CALL dodeca.cluster.status()` on any member
lists its role and the leader of every database; the metric
`cluster.raft.is_leader` is 1 on each database's leader. Every handover is
logged in `debug.log` with its reason, as in
`Transferred leadership of neo4j(...) to ...: the database is read-only on this
member`, and counted in `dodeca.cluster.raft.leadership_transfers` on the
member that handed over.

## Changing the default database

The default database is the one sessions that name no database use, and the
home database of users without one of their own. `initial.dbms.default_database`
names it at the first start only; afterwards, change it as an administrator,
on `system`:

1. `CREATE DATABASE films WAIT`, unless the new default exists.
2. `STOP DATABASE neo4j WAIT`, the default database; `SHOW DEFAULT DATABASE`
   names it.
3. `CALL dbms.setDefaultDatabase('films')`.
4. Optionally `START DATABASE neo4j`, now a database like any other, or
   `DROP DATABASE neo4j`.

With clustering, the call changes the system graph on the leader of `system`,
and every member follows it; on a follower it fails with
`Neo.ClientError.Cluster.NotALeader`, which routing drivers retry on the
leader.

## Moving databases to Cypher 25

Each database has a default Cypher version, which its queries run in without
a `CYPHER 5` or `CYPHER 25` prefix. A database gets `db.query.default_language`
when it is created, and keeps it: changing the setting later moves no
database. To move one, as an administrator on `system`:

```cypher
ALTER DATABASE films SET DEFAULT LANGUAGE CYPHER 25
```

`SHOW DATABASES YIELD name, defaultLanguage` shows each database's. Servers
pick the change up a moment later, and transactions that already ran a query
keep their version. A remote alias has its own, which `ALTER ALIAS ... SET
DATABASE DEFAULT LANGUAGE` changes; a local alias runs in its database's.
`GRANT SET DATABASE DEFAULT LANGUAGE ON DBMS` lets a role change databases'
versions without other database privileges.

## Upgrading Dodeca

Dodeca is upgraded one member at a time while the cluster keeps serving.
Every member must keep running the same Neo4j version: members refuse to form
a cluster with, add, or rejoin a member that runs another one. To upgrade
Neo4j, shut down every member, upgrade them all, and start them again.

A cluster mixes at most two Dodeca versions, the one it ran and the next.
Each version supports a range of format levels: the formats members send each
other and keep, such as the entries of the Raft log. The cluster's format
level says which formats members write. A newer version can read the formats
of the cluster's level, so it can run next to the previous version; it writes
newer formats only once the upgrade is finished.

1. Start once `CALL dodeca.cluster.status()` shows the group of every
   database on every member: a database created moments ago has its only copy
   on the member that created it until the others copied it. Then, on one
   member at a time: shut it down, replace the Dodeca jar in Neo4j's `lib/`,
   and start it. Before the next member, wait until it has started
   (Neo4j logs `Started.`), `CALL dodeca.cluster.status()` on it shows the
   group of every database with the role `FOLLOWER` or `LEADER`, and it has
   caught up with the writes it missed: for each database, its `appliedIndex`
   is close to the `commitIndex` that `dodeca.cluster.status()` shows on the
   database's leader. With one member down at a time, a cluster of three or
   more members keeps a majority and goes on serving writes; a member still
   catching up counts towards that majority only once it has.
2. Check that every member runs the new version: `CALL
   dodeca.cluster.members()` shows each member's `dodecaVersion` and the
   `formatLevels` it supports.
3. Finish the upgrade, as an administrator, on the leader of the `system`
   database:

   ```
   CALL dodeca.cluster.finishUpgrade()
   ```

   It raises the cluster's format level to the newest level any member
   supports, and returns `previousFormatLevel` and `formatLevel`. It refuses
   while a member runs a version that does not support that level, naming the
   member, or does not answer.

Until the upgrade is finished, any member can return to the previous version
the same way. Afterwards none can: a member refuses to start with a version
that does not support the cluster's format level, and so does a new member
added to the cluster. `CALL dodeca.cluster.formatLevel()` shows the cluster's
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

`server.backup.advertised_address` is the address that clients reach the
backup port on, which servers give `--remote-address-resolution` (see
[Taking backups](#taking-backups)). A port alone, such as `:6362`, takes the
host of `server.default_advertised_address`; the default takes that host and
the port the server listens on.

The backup port has no user authentication: whoever reaches it can copy every
database. A server serves backups on an address other than a loopback one only
once `dbms.ssl.policy.backup.enabled` is set explicitly:

- `true` (recommended): TLS, with Neo4j's SSL policy settings of the `backup`
  scope, as in Neo4j Enterprise, and certificates in `certificates/backup`
  laid out, and cipher suites by default, as for
  [TLS between members](#tls-between-members).
  `dbms.ssl.policy.backup.client_auth` keeps its default, `REQUIRE`, so that
  only clients with a certificate from a trusted authority can take backups.
  `NONE` logs a warning; `OPTIONAL` is refused.
- `false`: plaintext. The server logs a warning at every start. Use it only on
  a network that nothing but the backup clients can reach.

`neo4j-admin` reads the same settings from the configuration of the server it
runs on to connect, so the client presents its own certificate and checks the
server's, and its host name unless `dbms.ssl.policy.backup.verify_hostname=false`
(Neo4j's default is `true`).

### Taking backups

```
neo4j-admin database backup --from=core1:6362,core2:6362 --to-path=/backups "*"
```

- The databases are names or patterns in double quotes; the default is
  `neo4j`. `"*"` backs up every database, `system` included.
- `--from` lists servers, tried in order until one serves the backup of a
  database: name several members of a cluster.
- `--remote-address-resolution` backs up each database from the servers that
  host it instead, which the first server of `--from` that answers names by
  their `server.backup.advertised_address`: the database's secondaries first,
  then the primaries that do not lead it, then its leader, so that backups
  load the leader last. A database that no server hosting it serves backups
  of fails.
- `--type=AUTO`, the default, takes a differential backup if `--to-path`
  holds a backup of the database to continue, and a full one otherwise.
  `DIFF` and `AUTO` take a full backup instead when the server no longer holds
  the transactions after the latest backup, or when the latest backup is of
  another database of that name. `FULL` always takes a full backup.
- A differential backup continues the latest backup that holds transactions.
  `--prefer-diff-as-parent` continues the latest differential backup instead,
  even when a full backup was taken after it, so that a chain of differential
  backups goes on whatever full backups are taken beside it.
- A differential backup without new transactions is empty: it still marks the
  time and keeps the metadata. `--skip-empty-diffs` writes none, and needs
  `--include-metadata=none`.
- Files are named `<database>-<yyyy-MM-ddTHH-mm-ss>.backup`, the time in UTC.
  A file appears only once the backup is complete and synced to disk.
- `--to-path` may be a folder in cloud storage, such as
  `s3://backups/films/` (see [Cloud storage](#cloud-storage)). Then
  `--temp-path` must name a local directory, and the command lists the folder
  before it backs anything up, so that missing credentials or a missing bucket
  fail it at once.
- `--skip-recovery` keeps a full backup's copy of the database as the server
  sent it, which is faster; restoring, seeding, checking or aggregating the
  backup recovers it then.
- A full backup's copy of the database is otherwise recovered and checked
  before it is kept, in `--temp-path` (default: `--to-path`, which must be
  local then) with a page cache of `--pagecache`: it needs room for the
  database.
  `--keep-failed=true` keeps the copy of a failed full backup there.
- `--compress` (default `true`) compresses the artifacts.
- `--split-archive-part-size=4GiB` writes each backup in parts of at most that
  size, for file systems and object stores that limit the size of a file:
  `<name>.backup` then names the parts, `<name>.backup.1`, `<name>.backup.2`
  and so on, which hold the backup; it appears last, once they are complete.
  Every command that reads backups, and seeds, take the backup by
  `<name>.backup`, and need its parts beside it: keep and move them together.
  `server.split_archive.part_size` in the configuration of `neo4j-admin`
  gives the default. The minimum is 1 GiB; 0, the default, does not split.
  Neo4j Community's `neo4j-admin database load` reads split full backups as
  it does split dumps.
- `--include-metadata=all` (the default) keeps with each backup the Cypher that
  gives a DBMS back the users, roles and privileges of the database: `roles`
  the roles with privileges on it, on every database or on the home database,
  and those privileges; `users` the users who can access it, with their
  encrypted passwords, status, home database, tags and roles among those, and
  `users=alice,bob` only those of them; `all` both; `none` nothing. Backups of
  `system` keep none: `system` holds them all.
- `--inspect-path=/backups` lists the backups there with their metadata
  instead of backing up, as `neo4j-admin backup inspect /backups
  --show-metadata` does.
- The command exits with 1 if a database could not be backed up, naming it,
  or if a server could not be reached although another one served.

### Inspecting and checking backups

- `neo4j-admin backup inspect /backups` lists the backups there.
  `--show-metadata` adds each one's database and its id, time, whether it is
  full and compressed, the range of transactions it holds (by append index),
  and whether its store is recovered. `--latest-backup` and `--latest-chain`
  narrow the list, `--database` picks a database, `--format=JSON` prints
  JSON, and `--empty` includes backups that hold no transactions.
- `neo4j-admin database check --from-path=/backups neo4j` restores the latest
  chain of `neo4j` in a temporary directory and checks its consistency.
- `neo4j-admin backup aggregate --from-path=/backups neo4j` turns the latest
  chain into one full backup; the backups after the chain still continue it.
  `--keep-old-backup=true` keeps the chain's files. `--split-archive-part-size`
  splits the aggregated backup as `backup` does; it then keeps the name it was
  written under, which ends in `-2` when the chain's last backup had the same
  time. `neo4j-admin database aggregate-backup`, its name before Neo4j
  2025.01, still works, deprecated. For a folder in cloud storage,
  `--temp-path` must name a local directory with room for the database.

Each of these takes folders in cloud storage too, as `s3://backups/films/`;
`check` then needs a local `--temp-path`, as Neo4j's check of dumps does.

Each differential backup needs every backup of its chain. To keep backups for
a while, delete whole chains, oldest first, with the parts of split backups,
or aggregate them first.

### Restoring a database

With the database stopped (`STOP DATABASE films`), or not created yet:

```
neo4j-admin database restore --from-path=/backups/films-2026-10-21T13-52-10.backup
```

- `--from-path` names a backup, which restores the chain ending at it, or a
  directory, which restores the latest chain of each database in it. Several
  paths may be given, separated by commas. Each may be in cloud storage, as
  `s3://backups/films/`: the backups are read where they are, as they are
  restored, without a local copy.
- `--source-database=films` restores only the backups of `films`: the latest
  chain of its backups in a directory, and a backup file only if it is one of
  them.
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

`CREATE DATABASE` can create a database from a dump made with `neo4j-admin
database dump`, a full backup, a differential backup, with the backups before
it in its folder, or a folder of backups, whose latest chain it restores:

```
CREATE DATABASE films OPTIONS {existingData: 'use', seedURI: 'file:///backups/films-2026-10-21T13-52-10.backup'}
CREATE DATABASE films OPTIONS {existingData: 'use', seedURI: 's3://backups/films/'}
```

A split backup or dump is named by its first file, as `films.backup`; its
parts, `films.backup.1` and so on, are read beside it, and over `http:` or
`https:` at the URI with `.1` and so on appended.

A folder of backups of several databases seeds the database of the same name,
or, in Cypher 25, the database that `seedSourceDatabase` names, whose backups
seed it under the new name. A backup file must then be one of its backups.

`seedRestoreUntil` restores a seed read by `FileSeedProvider` or
`CloudSeedProvider` up to a point, as `neo4j-admin database restore
--restore-until` does: before a transaction, `seedRestoreUntil: 123`, or the
transactions committed before a time, `seedRestoreUntil:
datetime('2026-10-21T13:00:00Z')`. Of a folder, it takes the latest chain that
reaches that point; of a differential backup, the chain ending with it. A full
backup or a dump holds every transaction up to its last, and fails if that one
is not restored:

```
CYPHER 25 CREATE DATABASE films2 OPTIONS {seedURI: 's3://backups/', seedSourceDatabase: 'films', seedRestoreUntil: datetime('2026-10-21T13:00:00Z')}
```

Without clustering the server reads the seed. In a cluster, one of the servers
the database is placed on reads it and creates the database, and the others
copy it from that server: the one that `existingDataSeedServer` names, which
the database is placed on first, or else one the cluster chooses. That server
must be able to read the seed: for a seed in cloud storage, give every server
credentials. `dbms.databases.seed_from_uri_providers` lists the kinds of seed
URIs servers read, by default
`FileSeedProvider,CloudSeedProvider,ServerSeedProvider`:

- `FileSeedProvider`: `file:` URIs.
- `CloudSeedProvider`: `s3:`, `gs:` and `azb:` URIs (see
  [Cloud storage](#cloud-storage)).
- `S3SeedProvider`: `s3:` URIs, read as `CloudSeedProvider` reads them. It is
  Neo4j Enterprise's deprecated name, accepted so that its configurations
  load; its `seedConfig` and `seedCredentials` options are refused, since the
  AWS SDK's own credentials are used.
- `URLConnectionSeedProvider`: `http:`, `https:` and `ftp:` URIs, a dump or a
  full backup only, downloaded next to the database first.
- `ServerSeedProvider`: `server:` URIs, which name a server, by id or name,
  and a file in its seeds directory, `server.directories.seeds` (`seeds` in
  the server's home by default): `server://server-2/films.backup`. That
  server reads the seed and creates the database, and the others copy it, so
  the seed need only be on that server. A differential backup restores with
  the backups before it in that directory. `server://server-2/` restores the
  latest chain there of the database of the same name, or, in Cypher 25, of
  the one `seedSourceDatabase` names; a dump must be named. Delete the files
  once the database is created.

`CREATE OR REPLACE DATABASE films OPTIONS {seedURI: '...'}` drops `films`,
its files too, and creates it from the seed.

A seed that cannot be read fails the database on that server, and
`SHOW DATABASES` and `WAIT` say why.

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

## Importing and copying databases

`neo4j-admin database import full` creates a database from CSV files, as in
Neo4j. Dodeca adds `import incremental`, which adds CSV files to an existing
database, resumes a full import that stopped, and adds `neo4j-admin database
copy`. [`COMPATIBILITY.md`, "Import and
copy"](COMPATIBILITY.md#import-and-copy), lists where they differ from Neo4j
Enterprise.

Neither import takes `--schema`, which Neo4j applies only to its Enterprise
`block` format. Create the indexes, constraints and graph type once the
database is imported, for example with a file of statements:

```
CREATE DATABASE films WAIT
cat films-schema.cypher | bin/cypher-shell -d films
```

### Importing into an existing database

An incremental import adds nodes and relationships to a database that exists,
in three stages:

1. `prepare`, with the database stopped, reads the keys of its nodes.
2. `build` reads the CSV files and resolves each new relationship's nodes,
   new or existing, without touching the database, which may run read-only
   meanwhile.
3. `merge`, with the database stopped, writes the new nodes and relationships.

`--stage=all` runs the three on a stopped database:

```
neo4j-admin database import incremental --force --stage=all films \
  --nodes=Person=persons.csv --relationships=KNOWS=knows.csv
```

The database name goes before `--nodes` and `--relationships`, which take
several files each. Take a backup before, as Neo4j Enterprise advises.
`--dry-run` checks the files and prints estimates of what they hold, writing
nothing; it needs no `--force`.

- **Keys.** The ID column of each node file names the label and the property
  that identify its nodes, as `uuid:ID{label:Person}`, and that label and
  property need a uniqueness or node key constraint. A node file has one ID
  column: several, which a full import joins into one ID, work in full imports
  only, as in Neo4j Enterprise. Keys are compared as
  strings, or as integers with `--id-type=integer` or an `id-type` in the
  braces, as `"id:ID{label:Movie,id-type:long}"`, quoted for its comma.
  Relationship files refer to nodes by these keys, new ones and those the
  database has. A new node gets the label of its keys if it lacks it.
- **Duplicates.** A node whose key the database has, or another node of the
  increment has, fails the import, or is skipped with
  `--skip-duplicate-nodes`; existing nodes are not changed. A relationship
  whose node is missing fails it, or is skipped with
  `--skip-bad-relationships`. Both are listed in `--report-file`.
- **Reads during the build.** For the database to serve reads while the
  increment is built, run the stages one by one:

  ```
  STOP DATABASE films WAIT
  neo4j-admin database import incremental --force --stage=prepare films --nodes=...
  ALTER DATABASE films SET ACCESS READ ONLY
  START DATABASE films WAIT
  neo4j-admin database import incremental --force --stage=build films --nodes=...
  STOP DATABASE films WAIT
  neo4j-admin database import incremental --force --stage=merge films
  START DATABASE films WAIT
  ALTER DATABASE films SET ACCESS READ WRITE
  ```

  `merge` needs no files. The database must not change between `prepare` and
  `merge`: a merge into a database that changed is refused.
- **The merge** writes transactions of 100,000 nodes or relationships, which
  keep indexes, constraints and counts up to date: about 300,000 nodes and
  80,000 relationships a second on an SSD. It reads and writes through a page
  cache of `--max-off-heap-memory`, 90% of the free memory unless set, which
  it prints. If it stops, the database keeps the batches it committed: run
  `merge` again, and it continues after them. A batch that breaks a constraint
  fails the merge, and the error says how many nodes and relationships the
  database kept.
- **Disk.** Between the stages, the increment is kept in
  `data/databases/.films.increment/`, about the size of the input. The merge
  also writes the transaction log, about 250 bytes per node or relationship.

#### In a cluster

Import on one server, with the database's other copies out of the way, as
Neo4j Enterprise's procedure has it:

1. Recreate the database with that server as its only primary, from its store:

   ```
   CALL dbms.recreateDatabase('films', {seedingServers: ['<id>'], primaries: 1})
   ```

2. `STOP DATABASE films WAIT`, import on that server, and `START DATABASE
   films WAIT`. Secondaries pull what the import wrote.
3. Put the topology back, as `ALTER DATABASE films SET TOPOLOGY 3 PRIMARIES`:
   the other servers copy the database with the import.

A server refuses to start an imported database while other primaries host it
and it is still in their Raft group, since their copies lack the import, and
refuses an import on a secondary. `SHOW DATABASES` gives the reason on that
server, with the command that recreates the database from its store, which
also puts the import on every server.

### Resuming a full import

A full import that stops, or fails, after all its nodes and relationships are
imported resumes from relationship linking, or, once those are linked, from
post-processing, the last of the stages its progress shows. A failed import
prints which:

```
The import resumes from relationship linking: run the same command again, without --overwrite-destination, which starts over.
```

Run the same command again, with the same files and options, but without
`--overwrite-destination`, which deletes what the import wrote and starts over.
`--threads`, `--max-off-heap-memory`, `--high-parallel-io`, `--report-file`
and `--verbose` may change. Files that changed, by size or modification time,
and other options are refused, naming what differs. An import that stopped
earlier starts over with `--overwrite-destination`. The import keeps its
progress in `.dodeca-import.properties` in the database's directory, and the
report of bad entries of the run that stopped.

### Importing into a backup

`--target-format=backup --target-location=/backups` writes what `import full`
or `import incremental` imports as a full backup of the database in that
folder, local or in cloud storage, instead of into the DBMS. It is
uncompressed unless `--compress` is given, and split with
`--split-archive-part-size` as `neo4j-admin database backup` splits backups.
Seed or restore a database from it.

- The import is made in `.dodeca-import-<database>` in `--temp-path`, by
  default `--target-location`, which must be local then: it needs room for
  the database. A full import that stops keeps it there and resumes from it.
- An incremental import imports into a copy of the stopped database, with
  `--stage=all` only, and leaves the database as it is.

### Copying a database

`neo4j-admin database copy` copies a stopped database to a new one, leaving out
what its filters say, and writes its stores anew: relationships and properties
no longer take the space of deleted ones. Nodes keep their ids, so that the
node store keeps the gaps of deleted nodes, unless `--compact-node-store`
numbers them anew; relationships get new ids.

```
STOP DATABASE films WAIT
neo4j-admin database copy films films2 --ignore-nodes-with-labels=Draft --skip-properties=secret
CREATE DATABASE films2 WAIT
cat films2-schema.cypher | bin/cypher-shell -d films2
```

- **Filters.** `--copy-only-nodes-with-labels` or `--ignore-nodes-with-labels`
  choose the nodes, and with them their relationships;
  `--copy-only-relationships-with-types` or `--ignore-relationships-with-types`
  the relationships; `--skip-labels` drops labels; `--skip-properties`,
  `--skip-node-properties=Person.secret`, `--skip-relationship-properties`,
  `--copy-only-node-properties=Person.name` and
  `--copy-only-relationship-properties` drop properties. Lists are separated by
  commas; a name with a comma or dot in it goes in backticks, as
  ``--skip-labels=`My,label` ``. Options that contradict each other are
  refused.
- **Indexes, constraints and the graph type.** The statements that create
  them on the copy go to `<copy>-schema.cypher`, in `--to-path-schema` or the
  current directory, to run once the database exists. `--copy-schema` creates
  them on the copy instead, before the command ends, which waits until the
  indexes are built. Those on what the filters leave out are left out, and
  printed.
- **Format.** `--to-format` gives the copy another format, `standard` or
  `aligned`; it keeps the source's by default.
- **The same name** replaces the database with its copy, made first next to
  it, or in `--temp-path`, then moved into place: `START DATABASE films` then
  starts the copy.
- **Damaged stores.** Records that cannot be read are left out of the copy,
  which is consistent, and listed in `<copy>-copy.report` next to the schema
  file. A database that was not shut down cleanly is refused, unless `--force`
  copies its store as it is, without the transactions recovery would apply:
  start and stop it first to keep them.
- **Elsewhere.** `--from-path-data` and `--from-path-txn` read a database from
  other directories, `--to-path-data` and `--to-path-txn` write the copy to
  others. `--from-pagecache` (8 MiB) sizes the page cache that reads the
  source.
- **From and to backups.** `--source-format=backup
  --source-location=/backups/films-2026-10-21T13-52-10.backup` copies the
  database as that backup restores it, with the backups before it in its
  folder. `--target-format=backup --target-location=/backups` writes the copy
  as a full backup of `<toDatabase>` in that folder, uncompressed unless
  `--compress` is given, and split with `--split-archive-part-size` as
  `neo4j-admin database backup` splits backups. Either location may be in
  cloud storage, as `s3://backups/films/`; the stores in between are staged in
  `--temp-path`, by default the local `--target-location` or next to the
  target database, which needs room for them. Users and roles are not copied.

On a server of a cluster, a copy to a new name is prepared where
`CREATE DATABASE` takes a seed from, and the command prints the statement that
creates the database from it on every server it is placed on:

```
CREATE DATABASE films2 OPTIONS {existingData: 'use', existingDataSeedServer: '<id>'}
```

A copy that replaces a clustered database is a store of its own, which the
database's Raft group did not write: the server does not start it until the
database is recreated from its store, with
`CALL dbms.recreateDatabase('films', {seedingServers: ['<id>']})`,
which `SHOW DATABASES` names on that server.

## Cloud storage

Backups, restores, dumps, loads, seeds, imports and `LOAD CSV` read and write
Amazon S3, Google Cloud Storage and Azure Blob Storage, as in Neo4j Enterprise.
The clouds' SDKs come in Dodeca's jar.

### URIs

- `s3://<bucket>/<path>`
- `gs://<bucket>/<path>`
- `azb://<storage account>/<container>/<path>`

A path that ends with `/` names a folder. Folders are the prefixes of objects'
names up to a `/`: they always exist, so there is nothing to create, and a
folder named without its `/` works too where a folder is expected.

### What reads and writes them

- `neo4j-admin database dump --to-path`, `load --from-path`, `check
  --from-path` (with a local `--temp-path`) and the files of `import full`.
- `neo4j-admin database backup --to-path`, `restore --from-path`,
  `neo4j-admin backup aggregate --from-path` and `backup inspect`, as
  [Backup and restore](#backup-and-restore) describes.
- `CREATE DATABASE ... OPTIONS {seedURI: ...}`, with `CloudSeedProvider`
  enabled, as it is by default.
- `LOAD CSV FROM 's3://...'`, which needs what `file:` URLs need:
  `dbms.security.allow_csv_import_from_file_urls` (`true` by default), and for
  users whose LOAD privilege is limited, `LOAD ON ALL DATA`.

### Credentials

Dodeca takes no credentials of its own: each cloud's SDK finds them where it
always does, in the environment of the server and of `neo4j-admin`, or in the
files of the user they run as. Every server and every `neo4j-admin` that reads
or writes cloud storage needs them; in a cluster, every server that may read a
seed.

- **Amazon S3**: the AWS SDK's default chain: `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN`), the profiles of
  `~/.aws/credentials` and `~/.aws/config` (`AWS_PROFILE`, single sign-on),
  web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, as on EKS),
  and the role of the container or instance. The region comes from
  `AWS_REGION`, the profile or the instance; without one, requests start in
  us-east-1 and follow each bucket to its region.
- **Google Cloud Storage**: Google's application default credentials:
  `GOOGLE_APPLICATION_CREDENTIALS` naming a key file, `gcloud auth
  application-default login`, or the instance's service account. Without
  them, requests are anonymous, which only public buckets allow, and a refusal
  says why no credentials were found.
- **Azure Blob Storage**: the default Azure credentials: the environment
  (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and a secret or certificate), workload
  identity, managed identity, or the Azure CLI's sign-in. When
  `AZURE_STORAGE_CONNECTION_STRING` is set, it is used instead, for the
  storage account it names only.
  `dbms.integrations.cloud_storage.azb.blob_endpoint_suffix` (default
  `blob.core.windows.net`) and
  `dbms.integrations.cloud_storage.azb.authority_endpoint` (default: the
  SDK's, Azure's public cloud) select another Azure cloud, as
  `blob.core.usgovcloudapi.net` and `https://login.microsoftonline.us` for
  Azure Government.

With Docker, pass the variables with `-e`; for a service, set them in its
environment, such as systemd's `Environment=`.

### S3-compatible stores

For a store that speaks S3's protocol, such as MinIO or Ceph, set its endpoint
as the AWS SDK reads it: `AWS_ENDPOINT_URL_S3` (or `AWS_ENDPOINT_URL`, or
`endpoint_url` in the profile). With an endpoint set, Dodeca names the bucket
in the path (`https://<endpoint>/<bucket>/<key>`) and sends checksums only
where S3 requires them, as such stores expect.

For tests, `STORAGE_EMULATOR_HOST` points Google Cloud Storage at an emulator,
without credentials, and a connection string reaches Azure's emulator,
Azurite.

### Transfers

- Objects are read in ranges of 8 MiB, four ahead; the first 256 KiB come in
  one request, so that reading a backup's header costs little. A copy of a
  whole object fetches 16 ranges at once.
- Objects are written in parts as they fill, sent four at once: parts of
  8 MiB, twice as big every 1000 parts on S3 (every 5000 on Azure) up to 1 GiB,
  which makes objects of up to almost 4 TiB on S3 and 19 TiB on Azure;
  resumable uploads in chunks of 16 MiB on Google Cloud Storage, one at a
  time. A writer holds at most 64 MiB of parts being sent, or one part if
  bigger, and the part it fills.
- An object appears only once it is written whole: an upload that fails
  leaves none. Backups are written under a hidden name, then copied within
  the cloud to their own, as locally they are renamed.

## Disaster recovery

A cluster recovers by itself from the loss of a minority of its servers: the
others go on, and replacements copy what they need (see
[Replacing a failed member](#replacing-a-failed-member)). When a majority of
the servers is lost for good, the Raft groups that lost their majority take
no more writes: that of `system`, so that administration fails, and that of
every database with a majority of its primaries on the lost servers. Recover
as Neo4j Enterprise's disaster recovery guide does, in three steps: the
`system` database, then the servers, then the databases. When every server is
lost, recreate the cluster from backups instead (see
[Recovering a whole cluster](#recovering-a-whole-cluster)).

Keep the lost servers from starting until the recovery is done: a lost server
that comes back with its old state and reaches only other lost servers may
form a majority of an old group with them again. One that reaches a server of
the recovered cluster is refused (see [Lost servers](#lost-servers)).

### Recovering the system database

1. Stop every server left.
2. On each of them, remove its Raft state of `system`, keeping its store and
   its other databases:

   ```
   neo4j-admin dbms unbind-system-db --archive-cluster-state=true --archive-path=<directory>
   ```

   It writes the state it removes to
   `unbound_system_cluster_state.<time>.zip` in `<directory>` when asked,
   refuses while the server runs, and prints the server's id and the last
   transaction of its `system` store (`lastCommittedTransaction`). A run cut
   short can run again.
3. Set `dbms.dodeca.cluster.initial_members` on each server left to the
   servers left and any new servers that are to replace lost ones, and start
   them all, the new ones with empty data directories.

The servers re-form `system` once they reach each other: the server whose
`system` holds the latest transaction, then the one with the lowest id, starts a
new Raft group of `system` from its store, and the others copy `system` from it
and join that group. The new group has an id of its own, kept in each server's
membership record ([`FORMATS.md`](FORMATS.md)), so that lost servers cannot take
part in it. A server left that was not unbound, servers whose `system` comes
from another cluster, and servers without any refuse to re-form, with a message.
Users, roles, privileges, servers and databases come with `system`; what the
lost servers had committed to it that no server left holds is lost.

`SHOW SERVERS` then lists the lost servers as `Unavailable`, still hosting
what they hosted, and `CALL dbms.cluster.statusCheck([])` shows which
databases replicate.

### Lost servers

Cordon each lost server, so that nothing new is placed on it:
`CALL dbms.cluster.cordonServer('<id>')`. Recreate the databases that lost
their majority (see [Recreating a database](#recreating-a-database)), then
deallocate the lost servers, so that the databases that kept their majority
get their copies back on other servers (`DEALLOCATE DATABASES FROM SERVER
'<id>'`), and drop them once `SHOW SERVERS` shows them `Deallocated`:
`DROP SERVER '<id>'`.

A lost server that comes back with its old state is refused once it reaches a
server of the recovered cluster, with a message: its Raft group of `system` is
stale. To bring it back as the same server, with its databases, before it is
dropped: run `neo4j-admin dbms unbind-system-db` on it, set its
`dbms.dodeca.cluster.initial_members` to servers of the cluster, start it, and
add it with `CALL dodeca.cluster.addMember('<its Raft address>')` on the leader
of `system`. To bring it back as a new server, unbind it with `neo4j-admin
server unbind` instead (see [Unbinding a server](#unbinding-a-server)).

### Recreating a database

```
CALL dbms.recreateDatabase('<database>', {
  seedingServers: ['<id>', ...], seedURI: '<uri>', primaries: <n>, secondaries: <n>
})
CALL dbms.recreateDatabase('<database>', {clearData: true, storeFormat: '<format>'})
```

recreates a database, as one that lost a majority of its primaries, from a
seed, or empty, keeping its name, aliases, access, options, store format
unless `storeFormat` changes it, whether it is the default database, and
every privilege on it. It runs on the leader of `system`, needs the
privileges `CREATE DATABASE` and `DROP DATABASE`, and takes Neo4j
Enterprise's options:

- `seedingServers`: the servers whose stores the database is recreated from,
  by id; the latest of their stores seeds it. An empty list takes every
  enabled server that hosts a copy, primaries first, and is refused while a
  server that hosts a copy does not answer and is not cordoned. Each seeder
  hosts a copy of the recreated database, so there may be no more seeders than
  copies.
- `seedURI`: a backup or a dump to recreate the database from, read as `CREATE
  DATABASE` reads one (see [Creating a database from a backup or a
  dump](#creating-a-database-from-a-backup-or-a-dump)); a `server:` seed is
  read by the server it names, which then hosts a copy. With `seedingServers`,
  which must then be empty, it seeds the database only when none of the seeders
  holds a store.
- `clearData`: `true` recreates the database empty, without its data, indexes
  and constraints, and takes neither `seedingServers` nor `seedURI`.
- `storeFormat`: with `clearData`, the format of the empty store, `aligned` or
  `standard`. Without it the store gets the format the database was created
  in: its `CREATE DATABASE` option `storeFormat`, else `db.format`.
- `primaries` and `secondaries`: the new topology; `secondaries` needs
  `primaries`, and `primaries` alone means no secondaries. Without them the
  topology stays.

`dbms.cluster.recreateDatabase`, which Neo4j Enterprise deprecated in 2025.04
for `dbms.recreateDatabase`, takes the same options and warns that it is
deprecated.

It is refused without a seed or `clearData`, for `system` (see
[Recovering the system database](#recovering-the-system-database)), for an
alias, and when a seeder's copy is in quarantine. The database may be online
or offline; it is started.

The recreated database has a new id: Neo4j sees one database dropped and one
created under the same name, so element ids change, and bookmarks of the old
database are not waited for. Every server stops its old copy and removes its
Raft group, which needs no leader, and the database is placed anew, its
seeders first. A copy in quarantine on a server that is not a seeder goes with
the old database. Recreated empty or from a seed URI, the database is created
as a new one is, and its old stores are deleted.

From seeders, each seeder keeps its old store as a seed and reports it. Once
all of them reported, the leader of `system` names the creator: the seeder
whose last transaction has the latest append index, then the one with the
lowest id. The seeds must have the same store id, and each other seeder's
last transaction the same checksum as the creator's transaction at its append
index; one the creator's transaction logs no longer hold cannot be checked.
The creator starts the database from its store. The other seeders start
theirs as secondaries, pull the transactions they lack from it and then join
the database's group, or are copied if their history differs; the other
servers copy the database.

`SHOW DATABASES` shows, as the `statusMessage` of the copies that do not run,
what the recreation waits for, such as `Waiting for seeder <id> to report its
store.` while a seeder is down, and why it failed, with Neo4j Enterprise's
messages:

- `Seeders <id> and <id> have different checksums for transaction <id>. All
  seeders must have the same checksum for the same append index.`
- `Seeders <id> and <id> have incompatible storeIds. All seeders must have
  compatible storeIds.`
- `No store found on any of the seeders <id>, ...`

A recreation that failed stays as it is: recreate the database again, from
other seeders or from a seed URI, or drop it.

### Quarantine

```
CALL dbms.unquarantineDatabase('<server>', '<database>')
CALL dbms.unquarantineDatabase('<server>', '<database>', '<operation>')
CYPHER 5 CALL dbms.quarantineDatabase('<database>', true, '<reason>')
```

`dbms.unquarantineDatabase` lifts the quarantine of the copy of a database on
a server, named by its name or id as `SHOW SERVERS` lists them, from any
server of the cluster, as in Neo4j Enterprise. It needs an administrator and
returns nothing. The operation `keepStateKeepStore`, the default, leaves the
copy's store and cluster state as they are. `replaceStateReplaceStore` joins
the copy to its database as a new member, from a new copy of the database: the
server deletes its copy and its state of the database's Raft group, and asks
the group to remove it, which a majority of the group's voters must commit;
once lifted, the copy is copied from the others and the group adds it again.
`replaceStateKeepStore` does the same but keeps the store: the log entry of
its last transaction starts the server's new state of the group, and the
group sends it what follows; if the group's log no longer holds that entry,
the copy is copied instead. If the removal fails, the copy stays in
quarantine, without its old state: run the call again once a majority
answers. A secondary is copied again, or kept as it is. A database's only
primary, and `system`, cannot join as new; a server without clustering has
no cluster state to replace. The call fails with
Neo4j's error 52N17, `Setting/removing the quarantine marker failed`, when
the server or the database does not exist, the copy is in no quarantine
there, or the server does not answer.

`dbms.quarantineDatabase` puts the copy of a database on the server it runs
on in quarantine, or lifts the quarantine with `false`, and returns
`databaseName`, `quarantined` and `result`. Enterprise deprecated it in
2025.01, and its Cypher 25 does not have it, nor any other way to put a copy
in quarantine: call it with `CYPHER 5`. It needs an administrator and acts on
the server it runs on only: connect to that server with `bolt://`. A copy in quarantine
is stopped on that server and stays stopped: neither `START DATABASE` nor a
restart starts it until the quarantine is lifted, while the other servers
follow commands as usual. A primary in quarantine hands its leadership over
and keeps taking part in the database's Raft group. `SHOW DATABASES` shows the
copy `quarantined`, with the role `unknown` and the message
`By <user> at <time>: <reason>` (`No reason given` without one), and routing
leaves it out. Lifting the quarantine brings the copy to the state the system
graph asks of it: it starts and catches up with the others.

A copy that fails while running, as after a panic on a full disk, is put in
quarantine by the server (`By the server at ...`), with the failure and the
call that lifts it. The server's process then ends, with status 1, so that a
process manager starts it again, as Enterprise does since 2025.01; set
`server.panic.shutdown_on_panic=false` to keep it running instead. Quarantines are kept across restarts in `data/quarantine`,
a line per copy; deleting a line of the stopped server lifts that quarantine.
The quarantine of `system` lasts until the server restarts, as in Enterprise.

## Roles and privileges

Roles and privileges work as Neo4j Enterprise's operations manual describes,
with the differences listed in
[`COMPATIBILITY.md`](COMPATIBILITY.md#roles-and-privileges). With clustering,
their commands run on the system leader: run them there, or through a routing
driver or Cypher Shell (`neo4j://`), which sends them there, or on any member
that forwards them (see [Writes on any member](#writes-on-any-member)).

### The first admin

The first start of a Dodeca version with roles, on a new or an existing
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
2. Start every member. On the member where `CALL dodeca.cluster.status()`
   shows `system` with the role `LEADER`, give the role back; the other
   members refuse the command with `NotALeader`:

   ```sh
   bin/cypher-shell -a bolt://localhost:7687 -d system "GRANT ROLE admin TO neo4j"
   ```

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

### Roles by tags: auth rules

Auth rules give users roles by their tags, and property rules can compare
with the tags, as in Neo4j Enterprise 2026.10. Both need, on every member:

```properties
dbms.security.abac.authorization_providers=native
```

Then tag users, give roles to rules, and let rules decide who has them, all
in Cypher 25 on `system`:

```cypher
CYPHER 25 ALTER USER alice ADD TAGS ['finance', 'on-call']
CYPHER 25 CREATE AUTH RULE finance SET CONDITION 'finance' IN abac.native.user_tags()
CYPHER 25 GRANT ROLE analyst TO AUTH RULE finance
CYPHER 25 GRANT READ {*} ON GRAPH * FOR (n:Document) WHERE n.classification IN abac.native.user_tags() TO analyst
```

Each transaction of a user starts with the roles of the rules that hold for
the user then, so changes apply from the next transaction. `SHOW AUTH
RULES`, `SHOW ROLES WITH AUTH RULES` and `SHOW AUTH RULE finance PRIVILEGES`
show what the rules give, and `CALL dbms.showCurrentUser()` the roles a user
has in a transaction; `SHOW USERS` shows only the roles granted to users.
Roles that rules give cannot have `DENY` privileges. Without the setting,
rules give nothing, and property rules with tags are left out, which
`debug.log` says once.

### Moving roles from or to Neo4j Enterprise

Dodeca keeps roles and privileges in the system graph in nodes of its own,
and refuses to start on a system database that holds Neo4j Enterprise's.
Move them as commands instead, in either direction:

1. On the server that has them, save the commands these give, in this
   order, as a user with `SHOW USER CREDENTIALS` (which `admin` has):

   ```cypher
   CYPHER 25 SHOW ROLES AS COMMANDS
   CYPHER 25 SHOW USERS AS COMMANDS
   CYPHER 25 SHOW ROLES WITH USERS AS COMMANDS
   CYPHER 25 SHOW AUTH RULES AS COMMANDS
   CYPHER 25 SHOW ROLES WITH AUTH RULES AS COMMANDS
   CYPHER 25 SHOW PRIVILEGES AS COMMANDS
   ```

   The user commands keep the encrypted passwords, so that users log in
   with the passwords they have. Neo4j Enterprise before 2026.09 lists users
   and roles only as `SHOW USERS` and `SHOW ROLES WITH USERS`: write their
   `CREATE USER`, `CREATE ROLE` and `GRANT ROLE` commands from those, with
   new passwords, and those of auth rules from `SHOW AUTH RULES` and `SHOW
   ROLES WITH AUTH RULES` if it lacks their `AS COMMANDS`.
2. Move the databases other than `system` as dumps (`neo4j-admin database
   dump`), and create them on the new servers from those ([Creating a database
   from a backup or a dump](#creating-a-database-from-a-backup-or-a-dump)).
   Neo4j Community, and so Dodeca, reads only the `aligned` and `standard`
   store formats: migrate a database in another format with `neo4j-admin
   database migrate --to-format=aligned` first.
3. On the new servers, run the saved commands in the same order, each
   prefixed with `CYPHER 25`, on `system`. Leave out those of what the new
   servers have from the start: the `CREATE ROLE` commands of the built-in
   roles, and `CREATE USER neo4j`. Commands about databases or features the
   new servers lack fail and can be left out, such as users of single
   sign-on on Dodeca.


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

[`COMPATIBILITY.md`](COMPATIBILITY.md#database-aliases) describes what aliases
do and where Dodeca answered what Neo4j's documentation leaves open.

### Keystore for remote aliases

Dodeca encrypts the passwords of remote aliases with an AES key from a
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

## Checkpoints

A checkpoint writes the pages that transactions changed to the store files, so
that recovery after a crash starts from it and transaction logs before it can be
pruned. `db.checkpoint` chooses when a database checkpoints, on each member:

| Policy | Checkpoints |
|---|---|
| `PERIODIC` (default) | Every `db.checkpoint.interval.time` (15 minutes) or `db.checkpoint.interval.tx` transactions (100,000), whichever comes first. |
| `VOLUME` | Every `db.checkpoint.interval.volume` of transaction logs (250 MiB). |
| `CONTINUOUS` | Whenever transactions were committed since the last checkpoint, checked every 100 ms: recovery replays little, and checkpoints write a steady trickle instead of bursts. |
| `VOLUMETRIC` | When moving the checkpoint on would let `db.tx_log.rotation.retention_policy` delete logs, checked every 10 seconds: logs are kept as the retention policy says and no longer. With a policy that keeps every log, only backups, `db.checkpoint()` and stopping the database checkpoint. |

On a cluster member, checkpoints also let the member take Raft snapshots and
prune its Raft log (see ["Raft log retention and catch-up" in
`COMPATIBILITY.md`](COMPATIBILITY.md#running-a-cluster)).

`db.checkpoint.iops.limit` (default `600`, `-1` for none) caps the I/Os a
second of each database's background checkpoints, those the policy starts, so
that queries keep disk bandwidth for their reads. An I/O is mostly an 8 KiB
write: 600 is about 5 MB/s. Checkpoints someone waits for are not limited:
recovery's, a new database's first, a stopping database's last, those of
backups and store copies, and `CALL db.checkpoint()`, which runs a checkpoint
at once and waits for it. The limit is dynamic:

```cypher
CALL dbms.setConfigValue('db.checkpoint.iops.limit', '1200')
```

To limit bytes instead of I/Os, set `db.checkpoint.throughput.limit`, such as
`20MiB` for 20 MiB a second. It replaces the IOPS limit while it is set, limits
the same checkpoints, and is dynamic too; an empty value, its default, goes back
to the IOPS limit:

```cypher
CALL dbms.setConfigValue('db.checkpoint.throughput.limit', '20MiB')
CALL dbms.setConfigValue('db.checkpoint.throughput.limit', '')
```

Each checkpoint logs its numbers in `debug.log`:

```
Checkpoint triggered by "Scheduled checkpoint for every 15 minutes threshold" @ txId: 4567, append index: 4567 checkpoint completed in 4s 312ms. Checkpoint flushed 2614 pages (0% of total available pages), in 2101 IOs. Checkpoint performed with IO limit: 600, paused in total 38 times( 3604 millis).
```

With a throughput limit the line says `IO limit: 20.00MiB/s`, and the
`check_point.io_limit` metric gives the limit in 8 KiB pages a second.

The metrics `check_point.duration`, `check_point.io_performed`,
`check_point.limit_times` and `check_point.limit_millis` give the same for the
last checkpoint of each database. A limit that is too low for the write load
keeps checkpoints from completing, as pages are changed faster than they are
written, and transaction logs from being pruned: when checkpoints take longer
and longer, raise the limit or remove it.

## Page cache warmup

After a restart, a database's first queries read their pages from disk. As in
Neo4j Enterprise, each database records which pages of its files the page
cache holds, a profile, and loads those pages again when it starts, in the
background, so that the first queries find them in memory. It is on by
default.

| Setting | Default | |
|---|---|---|
| `db.memory.pagecache.warmup.enable` | `true` | Profiles and warmup. |
| `db.memory.pagecache.warmup.profile.interval` | `1m` | How often profiles are taken. |
| `db.memory.pagecache.warmup.preload` | `false` | Loads every page of the database's files instead, and takes no profiles: for a page cache larger than the store. |
| `db.memory.pagecache.warmup.preload.allowlist` | `.*` | The files the preload loads: a regular expression that matches the whole file name, such as `neostore\..*` for the store files without their indexes. |
| `db.memory.pagecache.warmup.order` | `NONE` | The order in which the warmup loads the files of the profiles: `NONE` as the file system lists the profiles, `ALPHABETIC` by file path, `PRIORITY` the ID files first, then the indexes, then the rest, so that the files every query needs are in memory first. A restart to change. |

- **Profiles** cover the store files and the indexes the page cache holds. They
  are taken every interval once the warmup ended, and when the database
  stops, and kept in `profiles/` in the database's directory, one file per
  store file; a profile is rewritten only when it changed. A database stopped
  before its warmup ended keeps the profiles it had. A profile costs a bit per
  8 KiB page, compressed: at most 16 KiB per GiB of store.
- **The warmup** starts once the database is available: queries run
  meanwhile, and share the disk with it. Pages that a profile names past the
  end of a file that shrank are left out. `debug.log` says what it did:

  ```
  [neo4j/4f3a1b2c] Page cache warmup loaded 51234 pages in 5341 ms
  ```

- **Copies carry the profiles**: a member that copies a database from another
  one, an online backup and the database restored or seeded from it warm up
  from the profiles of the copy's source. `neo4j-admin database copy` writes a
  new store, without them.
- **Metrics**: `<prefix>.database.<database>.page_cache.warmup.pages_loaded`,
  the pages the warmup or preload loaded since the database started, and
  `page_cache.warmup.duration`, the milliseconds it took (see
  [Metrics](#metrics)).

## Monitoring

Dodeca reports metrics, and writes the query log and the security log, as Neo4j
Enterprise's operations manual describes them;
[`COMPATIBILITY.md`](COMPATIBILITY.md#metrics-and-logs) lists the differences.
With clustering, each member reports and logs what happens on it: monitor every
member.

### Metrics

Metrics are on by default, written to CSV files and reported over JMX. For
Prometheus, enable the endpoint on every member, on an address Prometheus
reaches (the default, `localhost:2004`, serves only the member's own host):

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
| `server.metrics.deny_filter` | none | Globs of the names not reported although the filter reports them. |
| `server.metrics.jmx.enabled` | `true` | MBeans in the domain `neo4j.metrics`, also read by `CALL dbms.queryJmx('neo4j.metrics:*')`. |
| `server.metrics.prometheus.enabled` | `false` | |
| `server.metrics.prometheus.endpoint` | `localhost:2004` | |
| `server.metrics.csv.enabled` | `true` | See [CSV files](#csv-files). |
| `server.metrics.csv.interval` | `30s` | |
| `server.metrics.csv.rotation.size` | `10.00MiB` | `0` never rotates. |
| `server.metrics.csv.rotation.keep_number` | `7` | |
| `server.metrics.csv.rotation.compression` | `ZIP` | `GZ` or `ZIP` compress rotated files; `NONE` leaves them as they are. |
| `server.directories.metrics` | `metrics` | Relative to the home directory. |
| `server.metrics.graphite.enabled` | `false` | See [Graphite](#graphite). |
| `server.metrics.graphite.server` | `:2003` | A port without a host uses `server.default_listen_address`. |
| `server.metrics.graphite.interval` | `30s` | |

Names are Neo4j Enterprise's: `neo4j.dbms.<name>` for the server and
`neo4j.database.<database>.<name>` for each database, such as
`neo4j.database.neo4j.transaction.committed`. In Prometheus, characters other
than letters, digits and `_` become `_`, counters end in `_total`, and
histograms are summaries with quantiles, `_sum` and `_count`:
`neo4j_database_neo4j_transaction_committed_total`. A database's series
appear when it starts and go when it stops or is dropped. Metrics that only
Dodeca has carry `dodeca` in their names.

A filter replaces the default list rather than adding to it. To report more,
repeat the default, which
`SHOW SETTINGS YIELD name, defaultValue WHERE name = 'server.metrics.filter'`
shows, and append globs such as `*cluster.raft.*,*dodeca.cluster.*`.

With clustering, each member reports for each database, under
`neo4j.database.<database>.`:

| Metric | |
|---|---|
| `cluster.raft.is_leader` | 1 on the member that leads the database and takes its writes, else 0. The sum over the members should be 1; alert when it stays otherwise. |
| `cluster.raft.append_index`, `commit_index`, `applied_index`, `prune_index`, `term` | The member's Raft log: its last entry, the last entry known committed, the last applied to the database, and the first still kept. |
| `cluster.raft.last_leader_message` | Milliseconds since the member last heard from the leader; 0 on the leader. |
| `cluster.raft.replication_new`, `replication_attempt`, `replication_success`, `replication_fail`, `replication_maybe` | Transactions the member asked Raft to replicate, and their outcomes. `replication_maybe` counts transactions whose outcome the client did not learn, as when the leader changes during the commit. |
| `dodeca.cluster.replication.latency.millis` | Milliseconds from asking Raft to replicate a transaction until it was applied. |
| `dodeca.cluster.raft.followers`, `follower_lag_max` | On the leader only: the followers it sends to, and the most committed entries one of them does not have yet. A lag that keeps growing means a follower does not keep up. |
| `dodeca.cluster.raft.snapshots`, `leader_changes` | Raft snapshots taken and leaders learned of since the member started. |
| `dodeca.cluster.raft.leadership_transfers` | Leaderships the member handed over to another member on purpose since it started: when it stopped the database, when the database was read-only on it, or to balance leaders (see [Where databases are led](#where-databases-are-led)). |
| `dodeca.cluster.store_copy.received`, `served`, `repeated`, `received.millis` | Copies of the database the member installed, to join or to catch up; copies it sent to other members; copies it needed within an hour of the previous one, a sign that it does not keep up (see ["Raft log retention and catch-up" in `COMPATIBILITY.md`](COMPATIBILITY.md#running-a-cluster)); and how long catching up took. |
| `cluster.catchup.tx_pull_requests_received` | Pulls of transactions that secondaries sent the member. |
| `cluster.store_copy.pull_updates`, `pull_update_highest_tx_id_requested`, `pull_update_highest_tx_id_received` | On a secondary: the pulls it sent, the id of the last transaction it had when it last pulled, and the highest transaction id it pulled. Its lag shows in `SHOW DATABASES` (see [Reading from secondaries](#reading-from-secondaries)). |

The server reports `neo4j.dbms.dodeca.cluster.members`, the cluster's servers
as the member knows them: the primaries of `system` and the servers that host
it as a secondary.

#### CSV files

Every member writes a file per metric in `server.directories.metrics`, such as
`metrics/neo4j.database.neo4j.check_point.events.csv`, and adds a line every
`server.metrics.csv.interval`:

```
t,count
1762812000,14
1762812030,15
```

`t` is the time in seconds since 1970. Counters have a `count` column, gauges
`value`, and histograms `count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999`.
A database's files grow while it runs. With the default filter a server writes
some tens of files, each growing by about 100 bytes a line: at the default
interval, a file reaches `rotation.size` after months. It is then renamed
`<name>.csv.1` (compressed to `.gz` or `.zip` if configured), older ones become
`.2` and on up to `rotation.keep_number`, and the oldest is deleted.

In Neo4j's Docker image, files go to `/var/lib/neo4j/metrics` in the container.
To keep them, mount a volume and point the setting at it, which Neo4j's image
does on its own only for Enterprise:

```
docker run -v metrics:/metrics -e NEO4J_server_directories_metrics=/metrics ...
```

The directory must be writable by the `neo4j` user. A directory that cannot be
written is logged once in `debug.log`, until writing works again. Set
`server.metrics.csv.enabled=false` to write no files.

#### Graphite

```
server.metrics.graphite.enabled=true
server.metrics.graphite.server=graphite.example.com:2003
```

Every `server.metrics.graphite.interval`, each member connects to Graphite's
plaintext port and sends its metrics: counters and gauges under their names,
such as `neo4j.dbms.vm.heap.used`, and histograms as one series per column,
such as `neo4j.database.neo4j.db.query.execution.latency.millis.p99`. Each
member sends under the same names; to tell members apart, give each its own
`server.metrics.prefix`, such as `neo4j.core1`. A Graphite server that cannot
be reached is logged once in `debug.log`, until sending works again. The JVM
caches the address a host name resolves to as its `networkaddress.cache.ttl`
says.

### Cluster status endpoints

Load balancers, and health checks such as Kubernetes probes, ask a member's
HTTP server (`server.http.listen_address`, port 7474) about a database, as
they ask Neo4j Enterprise's:

| Path | Answers `true` with 200 OK, else `false` with 404 Not Found |
|---|---|
| `/db/<database>/cluster/writable` | On the member that leads the database, while it takes writes there. |
| `/db/<database>/cluster/read-only` | On the other members that run it, secondaries included. |
| `/db/<database>/cluster/available` | On every member that runs it. |

`/db/<database>/cluster/status` gives the member's view of the database as
JSON, and `/dbms/cluster/status` that of every database the member hosts, as
`databaseName`, `databaseUuid` and `databaseStatus`:

```json
{"core":true,"lastAppliedRaftIndex":42,"participatingInRaftGroup":true,
 "votingMembers":["0c9a3e44-6c1e-4f5e-9d2b-3b1f0a7e2c11","5f0d2a9b-8e47-4c39-a1d6-7e3b9c2f4d58","b7e14c62-3d95-4a08-8f2e-6a5c1d9b0e37"],
 "isHealthy":true,"memberId":"0c9a3e44-6c1e-4f5e-9d2b-3b1f0a7e2c11",
 "leader":"5f0d2a9b-8e47-4c39-a1d6-7e3b9c2f4d58","millisSinceLastLeaderMessage":84}
```

`core` is false on a secondary, which does not take part in the database's Raft
group (`participatingInRaftGroup`); its `lastAppliedRaftIndex` is the Raft index
of the last transaction it pulled. `leader` is missing while the member knows no
leader. `millisSinceLastLeaderMessage`, the milliseconds since the member last
heard from the leader, is 0 on the leader, and missing on a secondary or while
the member knows no leader. `isHealthy` is false once the database failed on the
member. A database the member does not host answers 404 on every path.

The endpoints need a login, as the rest of the HTTP API does, unless
`dbms.security.cluster_status_auth_enabled=false` is set, for load balancers
that cannot log in. They are off when `server.http_enabled_modules` does not
name `ENTERPRISE_MANAGEMENT_ENDPOINTS`.

A server with clustering disabled has no cluster status endpoints, as in
Neo4j Community: they answer 404. A single server whose load balancer or
health checks ask them, as they would a single Neo4j Enterprise server, runs as
a cluster of one instead, which leads every database:

```
dbms.dodeca.cluster.enabled=true
dbms.dodeca.cluster.initial_members=<its own Raft address>
dbms.ssl.policy.cluster.enabled=false
```

Its Raft address is `server.dodeca.cluster.advertised_address`: by default
`server.default_advertised_address` with port 7700, such as `solo:7700`.

Members added later make it a cluster of more (see
[Adding a member](#adding-a-member)), with TLS between members if they need it
(see [TLS between members](#tls-between-members)).

### Checking connectivity

An administrator checks whether a member reaches the others by calling, on
that member:

```cypher
CALL dbms.cluster.checkConnectivity()
```

It gives a row for each port of each other member, with `result` `OK`, or why
the member could not reach it. `CLUSTER` and `RAFT` are both the member's Raft
address, where it must answer as itself: a different member answering there,
as when one took over the address of a member that was replaced, fails the
check. `INTRA_BOLT` is the routing connector, which takes forwarded writes
(see [Writes on any member](#writes-on-any-member)); members without TLS
between them have no `INTRA_BOLT` rows. The check opens a new connection and
logs in as the calling user, as forwarded writes do.

`CALL dbms.cluster.checkConnectivity('RAFT', '<server id>')` checks one port
of one server. All checks run at once, each waiting up to its timeout for a
member that does not answer: 5 seconds for the Raft port, and
`dbms.routing.driver.connection.connect_timeout` for the routing connector.

### Checking replication

An administrator checks whether databases replicate, and which of their
primaries keep up, as in Neo4j Enterprise:

```cypher
CALL dbms.cluster.statusCheck([])
CALL dbms.cluster.statusCheck(['neo4j'], 500)
```

The leader of each database named, or of each database the server hosts as a
primary if the list is empty, replicates a status check entry, which every
member skips: nothing is committed. Each primary of the database then gets a
row:

| Column | Meaning |
|---|---|
| `replicationSuccessful` | Whether a majority committed the entry within the timeout, 1000 milliseconds unless given; the same in every row of the database. It is false without a leader or a majority. |
| `memberStatus` | `APPLYING` if the primary applied the entry within the timeout; `REPLICATING` if its log holds the entry but its database did not apply it, as while the database is stopped or failed there; else `UNAVAILABLE`, as when it did not answer or lacks the entry. Only `APPLYING` primaries add to the database's fault tolerance. |
| `recognisedLeader`, `recognisedLeaderTerm` | The leader and term the primary knows. When primaries know different leaders, the one with the highest term leads. |
| `requester` | True for the server that runs the procedure. |
| `error` | Why the entry was not replicated, why the primary did not answer, or that the server does not host a database named as a primary. |

`serverName` is the server's name, or its id if it has none, and `address` its
Bolt address. Each other member asked may take the timeout again to answer.

### Connections between members

```cypher
CALL dbms.cluster.protocols()
```

lists, as in Neo4j Enterprise, a row for each connection between the server it
runs on and another member that is open now, and for each protocol the
connection carried: `orientation` (`inbound` or `outbound`), `remoteAddress`,
`applicationProtocol`, `applicationProtocolVersion`, and `modifierProtocols`,
`TLS` for an encrypted connection and empty otherwise. The protocols are
Dodeca's own ([`FORMATS.md`](FORMATS.md)): `raft` for Raft's messages, at the
version of the replicated transactions; `probe`; and `bulk`, with a row for each
bulk transfer the connection carried, such as `dodeca.storeCopy`. A leader
lists its outbound Raft connections to the members it replicates to that
answered lately. It needs no administrator.

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
`MonitoringBenchmark` in `dodeca-core` measures this.

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

For JSON lines, use Neo4j's layout in the `SecurityLog` appender:

```
<JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/StructuredJsonLayout.json"/>
```

### Changing settings while running

`CALL dbms.setConfigValue('db.logs.query.enabled', 'INFO')` changes a dynamic
setting at once, until the member restarts; an empty value restores the default.
It needs the `admin` role and changes only the member it runs on: with
clustering, run it on each member, connecting to each directly (`bolt://`), and
change `neo4j.conf` too so that the change outlasts a restart. A member with
`dbms.dodeca.routing.forward_bolt_writes=true` forwards write transactions to
the leader, which the procedure would change instead: run it in a read
transaction there, as with Cypher Shell's `--access-mode read`. `SHOW SETTINGS
YIELD name, isDynamic WHERE isDynamic` lists the dynamic settings. `CALL
dbms.checkConfigValue('db.logs.query.enabled', 'INFO')` tells whether a value is
valid for a setting, dynamic or not, without changing it.

`server.dynamic.setting.allowlist` limits the settings the procedure may
change: setting names, comma separated, in which `*` stands for any text, such
as `db.logs.query.*,dbms.max_databases`. The default `*` lets every dynamic
setting change, and an empty value none. It is read at start.

## Dodeca's settings

Settings that Neo4j Enterprise has keep Enterprise's names and work as its
operations manual describes them; the sections above say where Dodeca differs,
and [`COMPATIBILITY.md`](COMPATIBILITY.md) lists them. Dodeca's own settings,
for what Enterprise does its own way or does not have, have `dodeca` in their
names:

| Setting | Default | Dynamic | |
|---|---|---|---|
| `dbms.dodeca.cluster.enabled` | `false` | No | Makes the server a cluster member; otherwise it runs alone. See [Forming a cluster](#forming-a-cluster). |
| `dbms.dodeca.cluster.initial_members` | none | No | The Raft addresses of the members that form the cluster, this one included. Read at a member's first start, and when servers re-form `system` (see [Recovering the system database](#recovering-the-system-database)). |
| `server.dodeca.cluster.listen_address` | `:7700` | No | Where the member's Raft server listens: Raft, membership, probes, copies and pulls. A port alone takes the host of `server.default_listen_address`. |
| `server.dodeca.cluster.advertised_address` | `:7700` | No | The member's Raft address, which the others reach, and which names it in `initial_members` and `dodeca.cluster.addMember`. A port alone takes the host of `server.default_advertised_address`. See [Changing an address](#changing-an-address). |
| `server.dodeca.cluster.raft_directory` | `raft` | No | Where the member keeps its Raft logs, one directory per database, and its membership record; relative to `server.directories.data`. Never delete it while the member keeps its id ([`COMPATIBILITY.md`, "Lost databases"](COMPATIBILITY.md#running-a-cluster)). |
| `server.dodeca.cluster.single_fsync` | `true` | No | Commits to replicated databases force only the Raft log to disk; `false` forces Neo4j's transaction log at every commit too. See [`COMPATIBILITY.md`, "Durability (single fsync)"](COMPATIBILITY.md#running-a-cluster). |
| `dbms.dodeca.cluster.election_timeout_min`, `election_timeout_max` | `1s`, `2s` | No | A primary that hears nothing from its database's leader for a random time between the two starts an election, so they bound how soon a failed leader is replaced. Shorter timeouts fail over sooner, and on a slow or busy network start elections that are not needed. At least 100 ms and 200 ms, the maximum above the minimum; give every member the same. |
| `dbms.dodeca.routing.forward_bolt_writes` | `false` | Yes | Forwards write transactions of `bolt://` connections to the leader. See [Writes on any member](#writes-on-any-member). |
| `dbms.dodeca.reads.default_consistency` | `CAUSAL` | No | `LINEARIZABLE` makes every transaction that names no consistency a linearizable read. See [Linearizable reads](#linearizable-reads). |

Dodeca's settings under `internal.`, such as
`internal.dbms.dodeca.cluster.raft_log_segment_size`, are for tests, and may
change or go in any version. On a running server,

```cypher
SHOW SETTINGS YIELD name, value, isDynamic, description WHERE name CONTAINS 'dodeca'
```

shows Dodeca's settings with their values.
