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
| `DozerDbEditionModule` | `ClusterEditionModule` | Subclass. `createDatabaseContextProvider` is a port of DozerDB's that swaps the database context factory; `registerDefaultDatabaseInitializer` is overridden because DozerDB keeps that starter in a private field. `registerDatabaseInitializers` is overridden so that only the member founding the cluster initializes the system graph, with DozerDB's `getSystemGraphComponents()`. |
| `DozerDbDatabaseContextFactory` | `ClusterDatabaseContextFactory` | Port. Differs only for replicated databases (hosting mode, leases, commit process, ID allocation) and in the constraint semantics. |
| `MultiDatabaseManager`, `MultiDatabaseLifecycleService` | `ClusterEditionModule`, `Neo4jLocalDatabases` | Used unchanged. `MultiDatabaseManager` creates, starts, stops and drops databases for the `DatabaseReconciler` and for `CatchUp`. Of the lifecycle service, the system database starter, the default database starter and the stopper are used; `allDatabaseStarter` is not, because the reconciler starts the other databases. |
| `SystemGraphTransactionEventListenerAdapter` (`setupMultiDbSystemGraphAdapter`) | `DatabaseReconciler` | Not registered. It acts only on the member that runs a command, partly before the command commits; the reconciler applies the system graph's databases on every member. Carry over changes to what the adapter does on create, drop, start and stop. |
| `DozerDbAdministrationCommandRuntime` | `Neo4jSystemGraph` | Used unchanged. The reconciler reads the `Database` nodes its `CREATE`, `STOP`, `START` and `DROP DATABASE` write (`name`, `uuid`, `status`) and adds a `dozendb_creator` property. |
| `DozerDbConstraintSemantics`, `ConstraintChecker`, `ConstraintDelegator` | `DozenDbConstraintSemantics` | Subclass and use. Fixes `readConstraint` and the checker construction, see `COMPATIBILITY.md`. Only used with clustering enabled. |

## Neo4j behaviour that single fsync relies on (no upstream edits)

`SingleFsyncFileSystem` wraps Neo4j's file system instead of changing Neo4j
code. It depends on the following, verified against Neo4j 5.26. Recheck them
after every Neo4j upgrade; the unit and power-loss tests in `cluster-core`
catch most such changes.

- Transaction log and checkpoint files are opened for writing through the DBMS
  `FileSystemAbstraction` (`TransactionLogChannelAllocator`), which an external
  dependency may replace (`GlobalModule`), and are forced only through
  `StoreChannel.flush()` (`TransactionLogFile`, `DetachedCheckpointAppender`).
- They are named `neostore.transaction.db.<n>` and `checkpoint.<n>` in
  `<transactions root>/<database>/` (`TransactionLogFilesHelper`).
- The page cache writes store pages through `StoreChannel` write methods of
  channels from the same file system, while it holds the page's flush lock
  (`SingleFilePageSwapper`, `MuninnPageCache`).
- The checkpointer fixes the checkpointed position before it flushes the store,
  and writes the checkpoint record after (`CheckPointerImpl`).
- Recovery reads `internal.dbms.tx_log.fail_on_corrupted_log_files` from the
  DBMS configuration: `Config.Builder.fromConfig` drops per-database settings
  (`Recovery.recoveryOptimizedConfig`).
- `DetachedCheckpointAppender.shutdown` closes its channel twice; the wrapper's
  `close` is idempotent.

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
