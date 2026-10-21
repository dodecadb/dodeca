# Upstream touchpoints

DozenDB runs on an unmodified Neo4j Community: its single jar goes into Neo4j's
`lib/`. It builds on the Neo4j classes listed here. After every Neo4j upgrade,
diff each of them between the old and the new Neo4j version and carry relevant
changes over.

## Neo4j classes DozenDB builds on

| Neo4j class | DozenDB code | How |
|---|---|---|
| `NeoBootstrapper` | `DozenDbBootstrapper` | Subclass. Starts `DozenDbEditionModule`, or `ClusterEditionModule` with clustering enabled, and reports the Enterprise edition. |
| `CommunityEditionModule` | `DozenDbEditionModule` | Subclass. `createDatabaseContextProvider` is a copy of Community's that uses `DatabaseManager` instead of `DatabaseLifecycles`, which allows only two databases, and `DozenDbDatabaseContextFactory` instead of `DefaultDatabaseContextFactory`. `registerDefaultDatabaseInitializer` is overridden because Community keeps its default database starter in a private field. `createDefaultDatabaseResolver` and `createSecurityModule` are copies of Community's that use `DozenDbDefaultDatabaseResolver` and `DozenDbSecurityModule`. |
| `CommunityEditionModule` | `ClusterEditionModule` | Through `DozenDbEditionModule`. `registerDatabaseInitializers` is overridden so that only the member founding the cluster initializes the system graph, with `getSystemGraphComponents()`. |
| `DatabaseLifecycles` | `DatabaseManager` | Port, for many databases. |
| `DefaultDatabaseContextFactory` | `DozenDbDatabaseContextFactory` | Port. Differs in the constraint semantics, the Cypher engine, and for replicated databases in the hosting mode, leases, commit process and ID allocation. |
| `CommunityCypherEngineProvider` | `DozenDbCypherEngineProvider` | Subclass; overrides `makeCompilerFactory`. |
| `CommunityCompilerFactory` | `DozenDbCompilerFactory` | Port; uses `DozenDbAdministrationCommandRuntime` on `system`. |
| `CommunityAdministrationCommandRuntime` | `DozenDbAdministrationCommandRuntime` | Subclass. Adds `CREATE`, `DROP`, `START` and `STOP DATABASE` for the logical plans that `AdministrationCommandPlanBuilder` builds for them; recheck those plans after an upgrade. Uses the Scala companion `AdministrationCommandRuntime$` and the default arguments of `UpdatingSystemCommandExecutionPlan`, `SystemCommandExecutionPlan` and `ParameterTransformer` through their generated methods. |
| `CommunityAdministrationCommandRuntime` | `UserCommands` | Plans `CREATE USER` and `ALTER USER` with `makeCreateUserExecutionPlan` and `makeAlterUserExecutionPlan` of `AdministrationCommandRuntime$`, which store the status and home database that Community refuses, and `SHOW USERS` and `SHOW CURRENT USER` like Community but with those columns. |
| `AdministrationChainedExecutionPlan` | `WaitExecutionPlan` | Subclass, for `WAIT`. Commits the command with `TransactionalContext.commitAndRestartTx`, as Neo4j Enterprise does. |
| `CommunitySecurityModule`, `BasicSystemGraphRealm` | `DozenDbSecurityModule` | Subclasses. `setup` is a copy of Community's with a realm that refuses suspended users. |
| `CommunityDefaultDatabaseResolver` | `DozenDbDefaultDatabaseResolver` | Subclass; resolves users' home databases. |
| `StandardConstraintSemantics` | `DozenDbConstraintSemantics` | Subclass, registered as a service with a higher priority. |
| `TopologyGraphDbmsModel` | `Neo4jSystemGraph`, `StandaloneDatabases`, `WaitExecutionPlan` | The `Database` nodes (`name`, `uuid`, `status`) that the database commands write and that the databases follow. `Neo4jSystemGraph` adds `dozendb_creator` and `dozendb_initial_members` properties, and a `DozenDB` node with the cluster's format level. |
| `KnownCommunitySecurityComponentVersion` | `SystemGraphUsers` | The `User` nodes' `suspended` and `homeDatabase` properties, which the user commands write and DozenDB reads. |
| `BackupMetadataV1`, `BackupMetadataV2`, `BackupZstdFormatV2`, `BackupTarFormatV2` | `ArtifactContainer` | Writes their layout of a `.backup` file's header itself, because Community's writer keeps only its own key in the metadata map; reads it with `BackupMetadataV2.readFromStream`. A test compares the bytes with Community's writer. |
| `CommandProvider`, `CommandType.BACKUP`, `RESTORE`, `INSPECT_BACKUP`, `AGGREGATE_NEXT`, `AbstractAdminCommand`, `CheckDatabase` | `BackupCommand`, `RestoreCommand`, `InspectBackupCommand`, `AggregateBackupCommand`, `CheckBackup` | `neo4j-admin` finds commands and `database check` sources as services. Community has no provider of these command types; DozenDB's priority still differs from the default, as two providers of a type with the same priority break `neo4j-admin`. The commands' names, options and help follow Neo4j Enterprise's documentation. |
| `Dumper`, `Loader`, `LoadCommand` | `Artifacts`, `DifferentialBody` | A full backup is a `Dumper` archive in the backup container, which `LoadCommand` loads. In a compressed differential backup the body starts with `Dumper`'s archive size header, which `load --info` reads. |
| `CompleteBatchRepresentation`, `LogEntryWriter`, `TransactionLogWriter` | `ArtifactTransaction`, `DifferentialBody` | A differential backup keeps the fields of each transaction's start, command and commit log entries. Appending them as `TransactionLogWriter` does must reproduce the checksums Neo4j recorded, which a test checks. |
| `DefaultStoreSnapshotFactory`, `StoreCopyCheckPointMutex`, `CheckpointFile`, `TransactionIdStore` | `StoreSnapshots` | Copies of a running database: the snapshot checkpoints the database and holds off further checkpoints while it is open. A backup's copy ends its transaction logs at the last closed transaction's log position, which is where that transaction ends, starting from the log version of the latest checkpoint; a test recovers such copies strictly under writes. |
| `LogicalTransactionStore`, `NoSuchLogEntryException`, `StoreCopyCheckPointMutex` | `CommittedTransactions` | A differential backup reads the transactions after the backup's last from the database's logs, up to the last closed one, holding the store copy side of the mutex so that no checkpoint prunes the logs meanwhile. `NoSuchLogEntryException` means the logs are pruned, and the backup falls back to a full one. |
| `ExtensionFactory` (`ExtensionType.GLOBAL`), `ConnectorPortRegister`, `ConnectorType.BACKUP`, `SslPolicyScope.BACKUP` | `BackupExtensionFactory`, `BackupListener`, `BackupTls` | Neo4j creates global extensions found as services once the edition is set up, with dependencies from the global ones; DozenDB's editions register `LatestWrites`, without which the extension serves no backups. The listener registers its bound port as the backup connector, and takes TLS from the backup scope's SSL policy, whose settings Neo4j knows only once one of them is set. |
| `SslPolicyLoader`, `SslPolicy`, `SslPolicyConfig`, `SslPolicyScope.CLUSTER` | `ClusterTls` | The cluster scope's SSL policy, which `CommunityEditionModule` loads into its protected `sslPolicyLoader`, configures TLS between members. `ClusterTls` reads its key, certificate chain, trust manager factory (trusted and revoked certificates), TLS versions, ciphers, client authentication and hostname verification. |

## Code ported from DozerDB

DozenDB began as an add-on to [DozerDB](https://dozerdb.org) and kept its
source as a git subtree. On 2026-10-19 the DozerDB code it built on was ported
to Kotlin and DozerDB was removed; that code is now in `dozendb-core` and
`dozendb-plugin`. The port started from
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
| `DozerDbCreateUserExecutionPlanner`, `DozerDbAlterUserExecutionPlanner` | Not ported: `UserCommands` uses Neo4j's own plans. |
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
