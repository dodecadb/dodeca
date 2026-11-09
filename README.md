# DozenDB

DozenDB adds to Neo4j Community Edition what Community leaves to Enterprise:
many databases (`CREATE`, `DROP`, `START` and `STOP DATABASE`), property
existence, key and type constraints, roles and privileges, local and remote
database aliases, online backup and restore, metrics and the query and
security logs, and Raft-based replication with automatic leader failover,
writes on any member, and database leaders balanced over the members. A
cluster of three or more voting members survives the loss of a minority of
members without losing acknowledged writes.

DozenDB is a single jar installed into Neo4j Community's `lib/`. It began as an
add-on to [DozerDB](https://dozerdb.org) and contains a Kotlin port of the
DozerDB code it built on; see `doc/UPSTREAM_TOUCHPOINTS.md`.

## Status

Under development. Not ready for production use.

## Repository layout

| Path | Contents |
|---|---|
| `consensus-api/` | The `ConsensusLog` and `ReplicatedStateMachine` interfaces and member, role and result types. No Neo4j or Ratis dependencies. |
| `consensus-ratis/` | The Apache Ratis implementation of `ConsensusLog`. The only module that depends on Ratis. |
| `dozendb-core/` | What DozenDB adds to Neo4j Community on every server, clustered or not: the edition module, database management, administration commands, constraints and settings. |
| `dozendb-backup/` | Online backup and restore, clustered or not: `neo4j-admin database backup`, `database restore`, `database aggregate-backup` and `backup inspect`, with Neo4j's `.backup` artifacts, full and differential. |
| `dozendb-cluster/` | Clustering on top of `dozendb-core`: commit interception, routing, reads, snapshots and membership. |
| `dozendb-plugin/` | The entry point, and the build of the single jar installed into Neo4j's `lib/`. |
| `dozendb-it/` | Integration, fault, crash and benchmark tests. |
| `doc/` | Compatibility matrix, upstream touchpoints, operator documentation, and the formats between members. |

## Building

The JDK comes from [mise](https://mise.jdx.dev) (`mise.toml`). Docker is needed
for the integration tests.

```sh
mise install
mise exec -- ./mvnw verify            # build and unit tests
mise exec -- ./mvnw verify -Pit       # also run the Testcontainers suites
```

The plugin jar is written to `dozendb-plugin/target/`.

Each module runs its test classes in 4 JVMs at once, and the Testcontainers
suites in 3, each with a cluster of its own; `-Dtest.forks=1` and
`-Dit.forks=1` run them one at a time, as when a test's log should be read on
its own. The 3 clusters need up to 12 GB of Docker's memory: with less free,
Docker's VM kills containers, so pass `-Dit.forks=1`.

## License

GNU General Public License v3.0, the same license as Neo4j Community Edition and
DozerDB. See `LICENSE`, and `NOTICE` for the code DozenDB derives from and
bundles.
