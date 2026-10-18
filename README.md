# DozenDB

DozenDB adds Raft-based replication with automatic leader failover to
[DozerDB](https://dozerdb.org), the plugin that enhances Neo4j Community
Edition. A cluster of three or more voting members survives the loss of a
minority of members without losing acknowledged writes.

DozenDB is a separate add-on. The upstream DozerDB source lives unmodified in
`dozerdb-core/` (a git subtree), and all clustering code lives in `cluster/`.

## Status

Under development. Not ready for production use.

## Repository layout

| Path | Contents |
|---|---|
| `dozerdb-core/` | Upstream DozerDB source, imported with `git subtree`. Only the edits listed in `cluster/docs/UPSTREAM_TOUCHPOINTS.md` differ from upstream. |
| `cluster/cluster-api/` | The `ConsensusLog` and `ReplicatedStateMachine` interfaces and member, role and result types. No Neo4j or Ratis dependencies. |
| `cluster/consensus-ratis/` | The Apache Ratis implementation of `ConsensusLog`. The only module that depends on Ratis. |
| `cluster/cluster-core/` | Neo4j integration: bootstrap, settings, commit interception, routing, reads, snapshots and membership. |
| `cluster/cluster-plugin/` | Builds the single plugin jar that replaces the DozerDB jar. |
| `cluster/cluster-it/` | Integration, fault, crash and benchmark tests. |
| `cluster/docs/` | Compatibility matrix, upstream touchpoints and operator documentation. |

## Building

The JDK comes from [mise](https://mise.jdx.dev) (`mise.toml`). Docker is needed
for the integration tests.

```sh
mise install
mise exec -- ./mvnw verify            # build and unit tests
mise exec -- ./mvnw verify -Pit       # also run the Testcontainers suites
```

The plugin jar is written to `cluster/cluster-plugin/target/`.

## License

GNU General Public License v3.0, the same license as DozerDB. See `LICENSE`.
