# DozenDB operator guide

## Cluster members

The members of a cluster are the voting members of the Raft group of the
`system` database. Every other database has its own Raft group, whose members
follow the cluster's: the leader of `system` adds members to each database's
group and removes them from it until they match.

Two procedures show the members, on any member:

- `CALL dozendb.cluster.members()` lists the cluster's members with their Raft
  and Bolt addresses, which one leads `system`, and in `databases` the
  databases whose groups each member votes in.
- `CALL dozendb.cluster.status()` lists, for every database running on the
  member asked, the member's role, the leader, Raft indexes, and in `members`
  the ids of the members that vote in the database's group.

Changing the members needs an administrator. The calls below work on any
member and change one member at a time. A change needs a majority of the
members before it and a majority after it: in a cluster of three, one member
may be down while another is added or removed, and in a cluster of two both
must be up to remove one.

### Adding a member

1. Configure the new server like the others: the same Neo4j and DozenDB
   versions, `dbms.dozendb.cluster.enabled=true`, the same TLS settings with a
   certificate of its own (see [TLS between members](#tls-between-members)),
   its own `server.dozendb.cluster.advertised_address`, and
   `dbms.dozendb.cluster.initial_members` listing the Raft address of at least
   one current member. Its data directory must be empty.
2. Start it. It copies `system` and the default database from a member, starts
   them without taking part in the cluster yet, and logs:

   ```
   This member is not part of the cluster yet. To add it, run
   CALL dozendb.cluster.addMember('<its Raft address>') on a member of the cluster.
   ```

3. Run that call on any member. It returns once the new member votes in the
   group of `system`, which brings it up to date first. The new member then
   copies every other database and joins the database's group; the database's
   leader brings it up to date before it votes. `dozendb.cluster.members()`
   shows each database it joined, and routing tables list it for a database
   once it votes there. Reads and writes go on throughout.

A database copied by the new member before it is added is not copied again
once it is added, unless the cluster discarded the Raft log since. A database
that is offline while a member is added is joined once it is started again.

`addMember` refuses a server that is not reachable, runs another Neo4j
version, still starts, belongs to another cluster (its databases came from
elsewhere) or was removed from this one. A server whose certificate the
members do not trust is not reachable: the error says why, for example
`unable to find valid certification path`.

### Removing a member

```
CALL dozendb.cluster.removeMember('<member id>')
```

with the id `dozendb.cluster.members()` shows. The call returns once the member
no longer votes in the group of `system`; the member then leaves the group of
every database, also of offline ones. A member removed while it is down learns
it when it starts again.

A removed member stops its databases, hands over any leadership it held, logs
an error saying that it was removed, and takes no further part in the cluster.
Shut it down. It cannot be added again as it is: to bring the server back,
delete its whole data directory, `data/server_id` included, and start it; it
then waits to be added as a new member.

### Replacing a failed member

Remove the failed member, then add its replacement as above. Adding the
replacement first works as well, at the cost of more members to reach a
majority while both are in the cluster.

### Changing an address

A member's Raft address is part of its membership. To change it, remove the
member, delete its data directory, and add it again with the new address. With
hostname verification, its certificate must name the new address.

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
   (Neo4j logs `Started.`) and `CALL dozendb.cluster.status()` on it shows the
   group of every database with the role `FOLLOWER` or `LEADER`. With one
   member down at a time, a cluster of three or more members keeps a majority
   and goes on serving writes.
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

In a cluster, a restored database cannot be put in place on every member by
hand. Restore it on one member, which prepares it there and prints the command
that creates the database from it:

```
CREATE DATABASE films OPTIONS {existingData: 'use', existingDataSeedServer: '<member id>'}
```

That member creates the database from the restored files, and the other
members copy it from that member. Drop a database before restoring it under
its old name.

### Creating a database from a backup or a dump

`CREATE DATABASE` can create a database from a full backup, the last backup of
a chain, or a dump made with `neo4j-admin database dump`:

```
CREATE DATABASE films OPTIONS {existingData: 'use', seedURI: 'file:///backups/films-2026-10-21T13-52-10.backup'}
```

Without clustering the server reads the seed. In a cluster, one member reads it
and creates the database, and the others copy it from that member: the leader
of `system`, or the member that `existingDataSeedServer` names. That member
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
   holds the restored databases forms the cluster with the others, creates
   every database that its `system` database lists, and the other members copy
   them.

Users and whether each database is online come with `system`.
