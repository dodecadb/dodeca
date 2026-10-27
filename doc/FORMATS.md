# Formats between members

What members send each other and keep, and how to change it without breaking
rolling upgrades (`OPERATOR_GUIDE.md`, "Upgrading DozenDB").

## Formats

Format level 1 is every format below at the version shown.

| Format | Code | Version | Use |
|---|---|---|---|
| Replicated transaction | `TransactionEnvelope` | 1 | Raft log entry: sent, and kept in the Raft log. |
| Probe request and answer | `ProbeCodec` | 1 | Sent. |
| What members report when probed | `MemberMetadata` | keys | Sent, inside probe answers. Each report has a number (`dozendb.reportNumber`), in order within a run of the member's process, so that a member keeps another's latest report when an earlier one arrives late; reports of different runs, or without a number, are taken as they arrive. |
| Database states, for `WAIT` and `SHOW DATABASES` | `DatabaseStatesCodec` | none | Sent, as one of those keys: a line per database with its name, id, status, whether the member knows its leader, and why it failed. A copy still being made is `COPYING`, a new one that has yet to join its Raft group `STARTING`. Later versions may add statuses, which earlier ones read as no copy, and fields after these, which earlier ones leave out. |
| Member login on the routing connector | `MemberLogins` | 1 | Sent: a Bolt auth token of the scheme `dozendb-member` naming the user whose transaction a member forwards, and the last transaction of `system` the member has (`system_transaction`). Only to members that report `dozendb.routingAddress`. |
| Forwarded transaction | `RemoteTransactions`, `RemoteTransaction` | Bolt | Sent: Neo4j's Bolt protocol, 5.7 or later, which both members' Neo4j negotiate. DozenDB adds only the member login. |
| Bulk transfer | `BulkService` | 1 | Sent; carries database copies, and backups on the backup port. |
| Database copy request | `StoreCopySource` | 2 | Sent. |
| Database copy | `StoreArchive` | 5 | Sent, and unpacked into the database's files. A member's copy holds its Raft snapshot in the metadata (`CopySnapshot`). |
| Snapshot record | `SnapshotRecord` | 1 | Kept by each member, also as a joining member's seed. |
| Membership record | `MembershipFile` | 1 | Kept by each member. |
| System graph properties and nodes | `Neo4jSystemGraph` | none | Replicated in `system`. |
| Roles and privileges | `PrivilegeRecords` | none | Replicated in `system`, in DozenDB's own nodes: a privilege on one database has scope `DATABASE` and the database's id, one on a remote alias scope `ALIAS` and the alias's name. |
| Server records | `ServerRecords`, with Neo4j's `TopologyGraphDbmsModel` names | none | Replicated in `system`, as Neo4j Enterprise keeps them: an `Instance` node for each enabled server, with `uuid`, `name`, `status` (`ENABLED`, `CORDONED` or `DEALLOCATING`), `tags`, `modeConstraint`, `allowedDatabases`, `deniedDatabases` and `discovered_at`, and on the `TopologyGraphSettings` node the flags `initial_instances_enabled` (the cluster's initial servers are recorded; until then every member counts as enabled) and `auto_enable_free_servers`. A server without an `Instance` node is Free. A dropped server's node is a `RemovedInstance` with `uuid`, `name`, `removed_at`, and DozenDB's `dozendb_raft_address` while it may still answer there. |
| Placements | `Placements`, with Neo4j's `TopologyGraphDbmsModel` names | none | Replicated in `system`, as Neo4j Enterprise keeps them: a `HOSTED_ON` relationship with `mode` (`raft`) and `installed_at` from a `Database` node to the `Instance` of each server that hosts a copy, the requested `primaries` and `secondaries` on the `Database` node, and `default_number_of_primaries` and `default_number_of_secondaries` on the `TopologyGraphSettings` node. `system` has none: every server hosts it. DozenDB's `dozendb_created` on a `Database` node says that its store was created, so that its creator, placed on it again, copies it instead of creating it. |
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
