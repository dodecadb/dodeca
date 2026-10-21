# Formats between members

What members send each other and keep, and how to change it without breaking
rolling upgrades (`OPERATOR_GUIDE.md`, "Upgrading DozenDB").

## Formats

Format level 1 is every format below at the version shown.

| Format | Code | Version | Use |
|---|---|---|---|
| Replicated transaction | `TransactionEnvelope` | 1 | Raft log entry: sent, and kept in the Raft log. |
| Probe request and answer | `ProbeCodec` | 1 | Sent. |
| What members report when probed | `MemberMetadata` | keys | Sent, inside probe answers. |
| Database states, for `WAIT` | `DatabaseStatesCodec` | none | Sent, as one of those keys. |
| Bulk transfer | `BulkService` | 1 | Sent; carries database copies, and backups on the backup port. |
| Database copy request | `StoreCopySource` | 2 | Sent. |
| Database copy | `StoreArchive` | 5 | Sent, and unpacked into the database's files. A member's copy holds its Raft snapshot in the metadata (`CopySnapshot`). |
| Snapshot record | `SnapshotRecord` | 1 | Kept by each member, also as a joining member's seed. |
| Membership record | `MembershipFile` | 1 | Kept by each member. |
| System graph properties and nodes | `Neo4jSystemGraph` | none | Replicated in `system`. |
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
