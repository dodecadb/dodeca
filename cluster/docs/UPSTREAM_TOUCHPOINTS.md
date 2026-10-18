# Upstream touchpoints

DozenDB keeps the DozerDB source in `dozerdb-core/` as a `git subtree` of
[dozerdb/dozerdb-core](https://github.com/dozerdb/dozerdb-core), tracked as the
git remote `upstream`. That tree must stay identical to upstream except for the
edits listed here. Upstream merges are expected to conflict only on these lines.

Current base: branch `neo4j-5.26.8`, commit `f76393e`.

## Rules

- All DozenDB code lives under `cluster/`. Never reformat, rename or clean up
  files in `dozerdb-core/`.
- A source edit to DozerDB is a last resort. It must be a single small hook
  that calls into our modules, marked `// CLUSTER-HOOK: <reason>`, and listed
  below with the file, line, reason and how to re-apply it.
- Version pins in the POMs are not marked inline. Upstream changes those lines
  on every release, so a marker comment next to them would add a conflict on
  every pull.

## Touchpoints

### 1. Neo4j version pin (POM only, no source change)

| File | Line | Upstream | DozenDB |
|---|---|---|---|
| `dozerdb-core/pom.xml` | 11 | parent `org.neo4j:parent` `5.26.8` | `5.26.27` |
| `dozerdb-core/pom.xml` | 17 | project version `5.26.8-1.1.0` | `5.26.27-1.1.0` |
| `dozerdb-core/pom.xml` | 22 | `neo4j.version` `5.26.8` | `5.26.27` |
| `dozerdb-core/pom.xml` | 25 | `dozerdb-core.version` `5.26.8-1.1.0` | `5.26.27-1.1.0` |
| `dozerdb-core/core/pom.xml` | 8 | parent version `5.26.8-1.1.0` | `5.26.27-1.1.0` |
| `dozerdb-core/core/pom.xml` | 14 | project version `5.26.8-1.1.0` | `5.26.27-1.1.0` |

Why: the newest public DozerDB source targets Neo4j 5.26.8, while the newest
DozerDB release (`graphstack/dozerdb:5.26.27.0`) runs Neo4j 5.26.27. The
source compiles unchanged against 5.26.27, and pinning it lets us test against
and install into the official release image. DozerDB makes the same edit for
each of its own releases.

Re-apply: set all six lines to the Neo4j version recorded in
`COMPATIBILITY.md`, and set `neo4j.version` and `dozerdb-core.version` in
`cluster/pom.xml` to match.

## DozerDB code that DozenDB builds on (no upstream edits)

DozenDB subclasses or ports these DozerDB classes. After every upstream pull,
diff them and carry relevant changes over.

| DozerDB class | DozenDB code | How |
|---|---|---|
| `DozerDbBootstrapper` | `ClusterBootstrapper` | Subclass; clustering disabled calls DozerDB's code unchanged. |
| `DozerDbEditionModule` | `ClusterEditionModule` | Subclass. `createDatabaseContextProvider` is a port of DozerDB's that swaps the database context factory; `registerDefaultDatabaseInitializer` is overridden because DozerDB keeps that starter in a private field. |
| `DozerDbDatabaseContextFactory` | `ClusterDatabaseContextFactory` | Port. Differs only for replicated databases (hosting mode, leases, commit process, ID allocation) and in the constraint semantics. |
| `MultiDatabaseManager`, `MultiDatabaseLifecycleService` | `ClusterEditionModule` | Used unchanged. `MultiDatabaseManager.stopDatabase` and `startDatabase` stop a replicated database while its copy is replaced (`CatchUp`). |
| `DozerDbConstraintSemantics`, `ConstraintChecker`, `ConstraintDelegator` | `DozenDbConstraintSemantics` | Subclass and use. Fixes `readConstraint` and the checker construction, see `COMPATIBILITY.md`. Only used with clustering enabled. |

## Build integration (no upstream edits)

- The root `pom.xml` is a plain aggregator over `dozerdb-core` and `cluster`.
  `dozerdb-core` keeps its own parent, `org.neo4j:parent`.
- `.mvn/maven.config` sets `spotless.skip=true`, so a root build never checks
  or formats the upstream tree. Upstream's Spotless check also fails on its
  own (its `scalafmt.conf` asks for scalafmt 3.7.3, the plugin resolves a
  different version, and the config path only resolves from inside
  `dozerdb-core/`). Our modules opt back in through `dozendb.spotless.skip` in
  `cluster/pom.xml`.
- A root build prints Maven warnings from the upstream POMs: duplicate
  dependency and plugin declarations, and a `parent.relativePath` warning
  because `dozerdb-core/pom.xml` does not set `<relativePath/>` for its
  `org.neo4j:parent` parent. They are harmless and are left as they are,
  because silencing them would mean editing upstream.

## Pulling upstream

```sh
git fetch upstream
git switch -c upstream-sync/<upstream-branch>
git subtree pull --prefix=dozerdb-core upstream <upstream-branch> --squash
# Conflicts on the version lines above resolve to the version we pin.
mise exec -- ./mvnw verify -Pit
```

Record every pull in the log below, including the conflicts and how they were
resolved.

## Merge log

| Date | Upstream ref | Conflicts | Resolution |
|---|---|---|---|
| 2026-10-18 | `neo4j-5.26.8` (`f76393e`) | none, initial import | Touchpoint 1 applied after import |
