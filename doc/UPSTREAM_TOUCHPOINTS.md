# Upstream touchpoints

DozenDB runs on an unmodified Neo4j Community: its single jar goes into Neo4j's
`lib/`. It builds on the Neo4j classes listed here. After every Neo4j upgrade,
diff each of them between the old and the new Neo4j version and carry relevant
changes over.

## Neo4j classes DozenDB builds on

| Neo4j class | DozenDB code | How |
|---|---|---|
| `NeoBootstrapper` | `DozenDbBootstrapper` | Subclass. Starts `DozenDbEditionModule`, or `ClusterEditionModule` with clustering enabled, and reports the Enterprise edition. |
| `CommunityEditionModule` | `DozenDbEditionModule` | Subclass. `createDatabaseContextProvider` is a copy of Community's that uses `DatabaseManager` instead of `DatabaseLifecycles`, which allows only two databases, and `DozenDbDatabaseContextFactory` instead of `DefaultDatabaseContextFactory`. `registerDefaultDatabaseInitializer` is overridden because Community keeps its default database starter in a private field. |
| `CommunityEditionModule` | `ClusterEditionModule` | Through `DozenDbEditionModule`. `registerDatabaseInitializers` is overridden so that only the member founding the cluster initializes the system graph, with `getSystemGraphComponents()`. |
| `DatabaseLifecycles` | `DatabaseManager` | Port, for many databases. |
| `DefaultDatabaseContextFactory` | `DozenDbDatabaseContextFactory` | Port. Differs in the constraint semantics, the Cypher engine, and for replicated databases in the hosting mode, leases, commit process and ID allocation. |
| `CommunityCypherEngineProvider` | `DozenDbCypherEngineProvider` | Subclass; overrides `makeCompilerFactory`. |
| `CommunityCompilerFactory` | `DozenDbCompilerFactory` | Port; uses `DozenDbAdministrationCommandRuntime` on `system`. |
| `CommunityAdministrationCommandRuntime` | `DozenDbAdministrationCommandRuntime` | Subclass. Adds `CREATE`, `DROP`, `START` and `STOP DATABASE` for the logical plans that `AdministrationCommandPlanBuilder` builds for them; recheck those plans after an upgrade. Uses the Scala companion `AdministrationCommandRuntime$` and the default arguments of `UpdatingSystemCommandExecutionPlan` and `ParameterTransformer` through their generated methods. |
| `StandardConstraintSemantics` | `DozenDbConstraintSemantics` | Subclass, registered as a service with a higher priority. |
| `TopologyGraphDbmsModel` | `Neo4jSystemGraph`, `StandaloneDatabases` | The `Database` nodes (`name`, `uuid`, `status`) that the database commands write and that the databases follow. `Neo4jSystemGraph` adds a `dozendb_creator` property. |

## Code ported from DozerDB

DozenDB began as an add-on to [DozerDB](https://dozerdb.org) and kept its
source as a git subtree. On 2026-10-19 the DozerDB code it built on was ported
to Kotlin into `dozendb-cluster` and DozerDB was removed. The port started from
[dozerdb/dozerdb-core](https://github.com/dozerdb/dozerdb-core) branch
`neo4j-5.26.8`, commit `f76393e`. Ported files keep the Neo4j and DozerDB
copyright notices and add a DozenDB modification notice; `COMPATIBILITY.md`
lists how DozenDB behaves differently.

| DozerDB class | DozenDB code |
|---|---|
| `DozerDbBootstrapper`, `DozerDbEntryPoint` | `DozenDbBootstrapper`, `DozenDbEntryPoint` |
| `DozerDbEditionModule` | `DozenDbEditionModule` |
| `DozerDbDatabaseContextFactory` | `DozenDbDatabaseContextFactory` |
| `MultiDatabaseManager`, `MultiDatabaseLifecycleService` | `DatabaseManager` |
| `SystemGraphTransactionEventListenerAdapter` | `StandaloneDatabases` (rewritten) |
| `DozerDbCypherEngineProvider`, `DozerDbCompilerFactory` | `DozenDbCypherEngineProvider`, `DozenDbCompilerFactory` |
| `DozerDbAdministrationCommandRuntime` | `DozenDbAdministrationCommandRuntime` |
| `DozerDbCreateUserExecutionPlanner`, `DozerDbAlterUserExecutionPlanner` | Not ported: Neo4j Community's planners. |
| `DozerDbConstraintSemantics`, `ConstraintChecker`, `ConstraintDelegator` | `DozenDbConstraintSemantics` |
| `DozerDbSettings` | `DozenDbSettings.maxDatabases` |

## Neo4j behaviour that single fsync relies on

`SingleFsyncFileSystem` wraps Neo4j's file system instead of changing Neo4j
code. It depends on the following, verified against Neo4j 5.26. Recheck them
after every Neo4j upgrade; the unit and power-loss tests in `dozendb-cluster`
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

## Build integration

The root `pom.xml` is the parent of every module and holds all build
configuration.
