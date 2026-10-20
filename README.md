# DozenDB

DozenDB adds to Neo4j Community Edition what Community leaves to Enterprise:
many databases (`CREATE`, `DROP`, `START` and `STOP DATABASE`), property
existence constraints, and Raft-based replication with automatic leader
failover. A cluster of three or more voting members survives the loss of a
minority of members without losing acknowledged writes.

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
| `dozendb-cluster/` | Neo4j integration: bootstrap, settings, edition module, database management, administration commands, constraints, commit interception, routing, reads, snapshots and membership. |
| `dozendb-plugin/` | Builds the single jar installed into Neo4j's `lib/`. |
| `dozendb-it/` | Integration, fault, crash and benchmark tests. |
| `doc/` | Compatibility matrix, upstream touchpoints and operator documentation. |

## Building

The JDK comes from [mise](https://mise.jdx.dev) (`mise.toml`). Docker is needed
for the integration tests.

```sh
mise install
mise exec -- ./mvnw verify            # build and unit tests
mise exec -- ./mvnw verify -Pit       # also run the Testcontainers suites
```

The plugin jar is written to `dozendb-plugin/target/`.

## License

GNU General Public License v3.0, the same license as Neo4j Community Edition and
DozerDB. See `LICENSE`, and `NOTICE` for the code DozenDB derives from and
bundles.
