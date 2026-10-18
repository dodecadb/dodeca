# Compatibility

Each DozenDB build is pinned to exactly one Neo4j and DozerDB version. A node
refuses to join a cluster whose members run a different Neo4j version.

## Pinned versions

| Component | Version | Notes |
|---|---|---|
| Neo4j | 5.26.27 (Community) | The Neo4j version inside the `graphstack/dozerdb:5.26.27.0` image. |
| DozerDB source | `dozerdb-core` branch `neo4j-5.26.8`, commit `f76393e` | Built against Neo4j 5.26.27, see `UPSTREAM_TOUCHPOINTS.md`. |
| DozerDB image | `graphstack/dozerdb:5.26.27.0` | Test baseline and installation target. The DozenDB jar replaces `lib/dozerdb-core-5.26.27-1.1.0.jar`. |
| Apache Ratis | 3.3.1 | Uses the already shaded `ratis-thirdparty` 1.1.0 for gRPC, Netty and protobuf. |
| Kotlin | 2.4.21, kotlinx-coroutines 1.11.0 | Bundled and relocated to `org.dozendb.shaded`. |
| JDK | build 17, run 17 or 21 | Bytecode targets Java 17 like Neo4j 5.26. The DozerDB image runs Temurin 21. |

## Known differences from the DozerDB 5.26.27.0 release

The DozerDB 5.26.27.0 release jar was built from source that is not public.
Its bootstrap, edition module and Cypher classes match the public
`neo4j-5.26.8` source, but its constraint enforcement was rewritten. DozenDB
builds the public source, so it differs from the release in these ways:

- **Constraints.** The release also enforces property type, key, label
  existence and relationship endpoint label constraints. DozenDB enforces the
  existence constraints supported by the public source only.
- **Multi-database housekeeping.** The release contains small refactors of
  `MultiDatabaseManager`, `MultiDatabaseLifecycleService` and
  `SystemGraphTransactionEventListenerAdapter`. One of them changes behaviour:
  on shutdown the release no longer stops the default database twice.
