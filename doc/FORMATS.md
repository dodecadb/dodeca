# Formats between members

What members send each other and keep, and how to change it without breaking
rolling upgrades (`OPERATOR_GUIDE.md`, "Upgrading DozenDB").

## Formats

Format level 1 is every format below at the version shown.

| Format | Code | Version | Use |
|---|---|---|---|
| Replicated transaction | `TransactionEnvelope` | 1 | Raft log entry: sent, and kept in the Raft log. |
| Probe request and answer, and announcement | `ProbeCodec` | 2 | Sent. Texts are UTF-8 after their length in 4 bytes, so that what a member reports may exceed 64 KB, as the states of hundreds of databases do; a probe answer may be up to 4 MiB, gRPC's default. An announcement carries what the sender answers when probed, unasked, when the groups it leads change; a version without announcements refuses it as unimplemented, and the sender leaves it at that. |
| What members report when probed | `MemberMetadata` | keys | Sent, inside probe answers and announcements. Each report has a number (`dozendb.reportNumber`), in order within a run of the member's process, so that a member keeps another's latest report when an earlier one arrives late; reports of different runs, or without a number, are taken as they arrive. A member reports how it is configured to host `system` as `dozendb.systemDatabaseMode` (`PRIMARY` or `SECONDARY`), which the leader of `system` reads when it adds the member. It reports the cluster it belongs to as `dozendb.systemId`, the random id Neo4j gave the `system` database's node when it created the system graph, and as `dozendb.clusterId`, the id of the database `initial.dbms.default_database` names; a member that adds another compares `dozendb.systemId` when both report it, else `dozendb.clusterId`, as a version without `dozendb.systemId` does. |
| Database states, for `WAIT` and `SHOW DATABASES` | `DatabaseStatesCodec` | none | Sent, as one of those keys: a line per database with its name, id, status, whether the member knows its leader, why it failed, the mode it hosts the database in (`primary` or `secondary`), and whether a secondary's pulls are paused. A copy still being made is `COPYING`, a new one that has yet to join its Raft group `STARTING`. Later versions may add statuses and modes, which earlier ones read as no copy, and fields after these, which earlier ones leave out. A line without a mode is a primary's, and one without the pause is not paused. |
| Member login on the routing connector | `MemberLogins` | 1 | Sent: a Bolt auth token of the scheme `dozendb-member` naming the user whose transaction a member forwards, and the last transaction of `system` the member has (`system_transaction`). Only to members that report `dozendb.routingAddress`. |
| Forwarded transaction | `RemoteTransactions`, `RemoteTransaction` | Bolt | Sent: Neo4j's Bolt protocol, 5.7 or later, which both members' Neo4j negotiate. DozenDB adds only the member login. |
| Bulk transfer | `BulkService` | 1 | Sent; carries database copies, and backups on the backup port. |
| Database copy request | `StoreCopySource` | 2 | Sent. |
| Database copy | `StoreArchive` | 5 | Sent, and unpacked into the database's files. A primary's copy holds its Raft snapshot in the metadata (`CopySnapshot`); a secondary's, which it serves as a primary does, holds none. |
| Pull of transactions | `PullProtocol` | 1 | Sent, as a bulk transfer `dozendb.pull` from a secondary to its upstream: the database, and the append index and checksum of the secondary's last transaction. The answer is `OK` with the transactions after it, in Neo4j's command encoding with a CRC32C each, up to the upstream's last when it began and about 4 MiB; `BEHIND` with the upstream's last transaction, when it has none newer; or `PRUNED`, `DIVERGED` or `UNAVAILABLE` with a message. |
| Read barrier | `ReadBarrierProtocol` | 1 | Sent, as a bulk transfer `dozendb.readBarrier` from a secondary to a primary, before a linearizable read or a backup: the database. The answer is the primary's last transaction once it applied what the group had committed when asked (a Raft ReadIndex), or a message. |
| Term of a log entry | `EntryTermProtocol` | 1 | Sent, as a bulk transfer `dozendb.entryTerm` from a secondary that turns primary to the database's leader: the database and a Raft index. The answer is the term of the leader's entry at that index, or that its log no longer holds it. |
| Snapshot record | `SnapshotRecord` | 1 | Kept by each member, also as a joining member's seed. |
| Membership record | `MembershipFile` | 1 | Kept by each member, as `membership` in its Raft directory: its state, the members it formed the cluster with, the format level it last saw, the stores it has yet to detach, and `memberId`, the member the record and its Raft state belong to. A member that starts under another id refuses to; one without `memberId` takes the record as its own. |
| Secondary marks | `SecondaryMarks` | none | Kept by each member, as `secondaries` in its Raft directory: the ids of the databases it holds as secondaries, one per line. A secondary's store counts only with its id there, as a primary's counts only with its Raft group. |
| System graph properties and nodes | `Neo4jSystemGraph` | none | Replicated in `system`. |
| Roles and privileges | `PrivilegeRecords` | none | Replicated in `system`, in DozenDB's own nodes: a privilege on one database has scope `DATABASE` and the database's id, one on a remote alias scope `ALIAS` and the alias's name. |
| Server records | `ServerRecords`, with Neo4j's `TopologyGraphDbmsModel` names | none | Replicated in `system`, as Neo4j Enterprise keeps them: an `Instance` node for each enabled server, with `uuid`, `name`, `status` (`ENABLED`, `CORDONED` or `DEALLOCATING`), `tags`, `modeConstraint`, `allowedDatabases`, `deniedDatabases` and `discovered_at`, and on the `TopologyGraphSettings` node the flags `initial_instances_enabled` (the cluster's initial servers are recorded; until then every member counts as enabled) and `auto_enable_free_servers`. A server without an `Instance` node is Free. A dropped server's node is a `RemovedInstance` with `uuid`, `name`, `removed_at`, and DozenDB's `dozendb_raft_address` while it may still answer there. A server that hosts `system` as a secondary has a DozenDB node `DozenDbSystemSecondary` with `id`, `raft_address` and `added_at` from when the leader of `system` added it until it is dropped or turns primary, and `promote` while the leader asks it to turn primary. |
| Placements | `Placements`, with Neo4j's `TopologyGraphDbmsModel` names | none | Replicated in `system`, as Neo4j Enterprise keeps them: a `HOSTED_ON` relationship with `mode` (`raft` for a primary, `replica` for a secondary) and `installed_at` from a `Database` node to the `Instance` of each server that hosts a copy, the requested `primaries` and `secondaries` on the `Database` node, and `default_number_of_primaries` and `default_number_of_secondaries` on the `TopologyGraphSettings` node. `system` has none: every server hosts it, as a primary or, as its `DozenDbSystemSecondary` node says, as a secondary. DozenDB's `dozendb_created` on a `Database` node says that its store was created, so that its creator, placed on it again, copies it instead of creating it. |
| Store format of a new database | `StoreFormats`, with Neo4j's `TopologyGraphDbmsModel` name | none | Replicated in `system`: `creation_store_format` on the `Database` node, the format `CREATE DATABASE ... OPTIONS {storeFormat: ...}` asked for, which the member that creates the store reads. A version without it creates the store in `db.format`. |
| Database aliases | Neo4j's `TopologyGraphDbmsModel` | none | Replicated in `system`, exactly as Neo4j keeps them. A remote alias's password is encrypted by `AliasPasswords`: AES/GCM/NoPadding with a 128-bit tag, a random 12-byte iv kept as `iv`, under the key of the keystore that every member has. |
| Remote alias transaction | `RemoteAliasTransactions`, `RemoteTransaction` | Bolt | Sent to other DBMSs, not between members: Neo4j's Bolt protocol, 5.0 or later. Routing requests of `neo4j://` URLs carry the routing context `alias`, the alias's name, which Neo4j checks against alias chains. Not part of the format levels. |
| Backup request and response | `BackupProtocol` | 1 | Sent between a server and `neo4j-admin database backup`, which may be of another DozenDB version: each side refuses the other's version if it differs, with a message. Not part of the format levels. A full backup's copy is a database copy (`StoreArchive`). |
| Backup artifact | `ArtifactContainer`, `DifferentialBody` | 1 | Kept in `.backup` files, in Neo4j's container: `dozendb.format` in its header. Every later version reads it. |

Neo4j's own formats, such as the commands inside a replicated transaction,
are the same on every member: all members run the same Neo4j version. Ratis's
messages are Ratis's: a DozenDB version that upgrades Ratis must check that the
new Ratis talks to the previous one.

## Format levels

`FormatLevels.SUPPORTED` names the levels a DozenDB version reads, from
`oldest` to `newest`. The cluster's level is in the system graph (a `DozenDB`
node, absent until an upgrade raises it), and every member keeps the level it
last saw in its membership record. `ClusterService.finishUpgrade` raises the
level once every member supports a newer one; members then refuse to start,
rejoin or be added with a version that does not support it.

## Changing a format

1. Raise `FormatLevels.SUPPORTED.newest` to a new level. Keep `oldest` at most
   the previous version's `newest`, so that the two versions can run side by
   side.
2. Write the new format only while the cluster's level
   (`ClusterService.formatLevel`) is at least the new level; write the old one
   before.
3. Read both. A kept format must stay readable after an upgrade, also when it
   was written under an older level.
4. Test it with the previous release: a rolling upgrade under load, a return
   to the previous version before `finishUpgrade`, and its refusal after.

Probe metadata keys, membership record keys and system graph properties
change without a level: add new ones, never change what an existing one means,
and read a missing one as what a version without it means, as
`FormatLevels.parse(null)` does.

A new cluster starts at level 1. Once a newer level exists, form new clusters
at the newest level all initial members support instead, so that they do not
need `finishUpgrade` first.
