# DozenDB

DozenDB adds to Neo4j Community Edition much of what Neo4j keeps for its
Enterprise Edition, as one jar in Community's `lib/`:

- many databases (`CREATE`, `DROP`, `START` and `STOP DATABASE`), local and
  remote database aliases, property existence, key and type constraints, user
  status and home databases, and roles and privileges;
- clustering on Raft: replication with automatic failover, routing tables and
  server-side routing, writes on any member, servers and databases placed on
  them by their topologies, secondaries that pull transactions to serve
  reads, leaders balanced over the members, routing policies, and disaster
  recovery;
- online backup and restore, full and differential, and cloud storage on
  Amazon S3, Google Cloud Storage and Azure Blob Storage for backups, dumps,
  seeds, imports and `LOAD CSV`;
- metrics for Prometheus, JMX, CSV files and Graphite, the query log and the
  security log;
- incremental and resumable imports, `neo4j-admin database copy`, checkpoint
  policies with an I/O limit, and page cache warmup.

[`doc/COMPATIBILITY.md`](doc/COMPATIBILITY.md) compares DozenDB with Neo4j
Enterprise feature by feature, and lists what it does not support.

Each database of a cluster is replicated in a Raft group of its primaries. A
database with three primaries keeps taking writes, without losing any write it
acknowledged, while one of its servers is down; one with five, while two are.
`system` has a primary on every member, unless told otherwise. Other databases
get one primary by default, as in Neo4j Enterprise: set
`initial.dbms.default_primaries_count=3` when forming a cluster of three, or
give each database its topology (see
[Raising the default](doc/OPERATOR_GUIDE.md#raising-the-default)).

DozenDB began as an add-on to [DozerDB](https://dozerdb.org) and contains a
Kotlin port of the DozerDB code it built on; see
[`doc/UPSTREAM_TOUCHPOINTS.md`](doc/UPSTREAM_TOUCHPOINTS.md).

## Status

Under development. Not ready for production use.

## Requirements

- Neo4j Community 2026.10.2, exactly: each DozenDB build runs on the Neo4j
  version it was built for.
- Java 21 or 25, as Neo4j 2026.
- To build: [mise](https://mise.jdx.dev), which provides the JDK
  (`mise.toml`), `unzip`, which the Maven wrapper unpacks Maven with, and
  Docker for the integration tests.

## Installing

Each [release](https://github.com/dozendb/dozendb/releases) has the jar,
`dozendb-<version>-neo4j-2026.10.2.jar`, and an image. Copy the jar into the
`lib/` directory of Neo4j Community 2026.10.2, the version its name gives, and
start Neo4j. The image is Neo4j's official Community image with the jar in
its `lib/`, and runs as Neo4j's does:

```sh
docker run --publish 7474:7474 --publish 7687:7687 ghcr.io/dozendb/dozendb
```

The release notes say how to check that the jar and the image were built by
this repository's release workflow. To build the jar from source instead, see
[Building](#building).

To form a cluster, see
[Installing DozenDB](doc/OPERATOR_GUIDE.md#installing-dozendb) in the operator
guide, which ends with a Docker Compose file for three members.

## Documentation

| Document | Contents |
|---|---|
| [`doc/OPERATOR_GUIDE.md`](doc/OPERATOR_GUIDE.md) | Installing, forming and running a cluster, backups, monitoring, and DozenDB's settings. |
| [`doc/COMPATIBILITY.md`](doc/COMPATIBILITY.md) | DozenDB compared with Neo4j Enterprise, area by area, and moving from Neo4j Community, Enterprise or DozerDB. |
| [`doc/FORMATS.md`](doc/FORMATS.md) | What members send each other and keep, and how to change it without breaking rolling upgrades. |
| [`doc/UPSTREAM_TOUCHPOINTS.md`](doc/UPSTREAM_TOUCHPOINTS.md) | The Neo4j and Apache Ratis code DozenDB builds on, to recheck after upgrading them. |
| [`doc/RELEASING.md`](doc/RELEASING.md) | Versions, Neo4j and dependency updates, and how releases are made and published. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Contributing: the clean-room rule, tests, CI, documentation and commits. |
| [`SECURITY.md`](SECURITY.md) | Reporting a vulnerability, privately. |

## Repository layout

| Path | Contents |
|---|---|
| `consensus-api/` | The `ConsensusLog` and `ReplicatedStateMachine` interfaces and member, role and result types. No Neo4j or Ratis dependencies. |
| `consensus-ratis/` | The Apache Ratis implementation of `ConsensusLog`. The only module that depends on Ratis. |
| `neo4j-server-stubs/` | The signatures of the Neo4j web server classes that `dozendb-cluster` extends, to compile against: Neo4j does not publish the `neo4j-server` jar to Maven Central. Never packaged; Neo4j's own classes run. |
| `dozendb-core/` | What DozenDB adds to Neo4j Community on every server, clustered or not: the edition module, database management, administration commands, roles and privileges, aliases, constraints, metrics and logs, checkpoints, page cache warmup, imports, `database copy` and settings. |
| `dozendb-cloud/` | Cloud storage: Neo4j's storage interface for `s3://`, `gs://` and `azb://` URIs, over each cloud's SDK. |
| `dozendb-backup/` | Online backup and restore, clustered or not: `neo4j-admin database backup`, `database restore`, `backup aggregate` and `backup inspect`, with Neo4j's `.backup` artifacts, full and differential, and seeds of `CREATE DATABASE`. |
| `dozendb-cluster/` | Clustering on top of `dozendb-core`: membership, servers and placement, commit interception, routing, reads, secondaries, snapshots and disaster recovery. |
| `dozendb-plugin/` | The entry point, and the build of the single jar installed into Neo4j's `lib/`. |
| `dozendb-it/` | Integration, fault, crash and benchmark tests, in Docker against Neo4j's official image. |
| `docker/` | The Dockerfile of DozenDB's image. |
| `.github/` | CI and releases on GitHub Actions, and Dependabot's updates. |
| `doc/` | The documentation above. |
| `scripts/` | `test-ramdisk.sh`, which makes the tests faster on macOS (below). |

## Building

The JDK comes from [mise](https://mise.jdx.dev) (`mise.toml`). Docker is needed
for the integration tests.

```sh
mise install
mise exec -- ./mvnw verify            # build and unit tests
mise exec -- ./mvnw verify -Pit       # also run the Testcontainers suites
```

The jar to install is
`dozendb-plugin/target/dozendb-<version>-neo4j-<neo4j version>.jar`, about
70 MB, named after the Neo4j version it runs on; the small
`dozendb-plugin-<version>.jar` next to it is not the one.

Each module runs its test classes in 4 JVMs at once, and the Testcontainers
suites in 3, each with a cluster of its own; `-Dtest.forks=1` and
`-Dit.forks=1` run them one at a time, as when a test's log should be read on
its own. The 3 clusters need up to 12 GB of Docker's memory: with less free,
Docker's VM kills containers, so pass `-Dit.forks=1`.

Most of a test's time goes to forcing commits to disk. On macOS,
`scripts/test-ramdisk.sh` creates a RAM disk that the build keeps test data on
while it is mounted, which makes the tests several times faster. The
directories of failed tests are on it too, until it is ejected or the Mac
restarts; run the script again after a restart. On Linux, pass
`-Dtest.tmpdir=/dev/shm`, or another RAM disk, as long as it allows running
programs: the tests' JVMs unpack native libraries there. Containers mount
`/dev/shm` with `noexec`; mount a `tmpfs` without it instead.

## License

GNU General Public License v3.0, the same license as Neo4j Community Edition and
DozerDB. See `LICENSE`, and `NOTICE` for the code DozenDB derives from and
bundles.
