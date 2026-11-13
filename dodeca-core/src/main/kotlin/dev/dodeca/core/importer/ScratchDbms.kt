package dev.dodeca.core.importer

import dev.dodeca.core.edition.DatabaseHosting
import dev.dodeca.core.edition.DodecaEditionModule
import dev.dodeca.core.edition.HostedDatabase
import dev.dodeca.core.monitoring.MetricsSettings
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlin.io.path.deleteIfExists
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DbmsRuntimeVersion
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.CommandCommitListeners
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.LogProvider
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.StorageEngineTransaction
import org.neo4j.wal.CompleteCommandBatch
import org.neo4j.wal.TransactionAppender

/**
 * One stopped database of a server, opened in a DBMS of its own beside the server, which may be
 * running, as `neo4j-admin` commands need it. The DBMS's home holds a new `system` database and, as
 * symbolic links, the directories of the database's store and transaction logs, so that Neo4j
 * locks, recovers and writes those of the server: the server cannot start the database meanwhile.
 *
 * The DBMS takes only the database's settings, `db.*`, from the server's configuration: it listens
 * on no port, reports no metrics and keeps its logs in a directory of its own. It runs the DBMS
 * runtime version of the store's kernel version, so that its writes do not upgrade the store, which
 * the servers of a cluster may not all read yet. Every transaction it commits takes the consensus
 * index of the store's last one, so that a clustered database's Raft group continues after it.
 */
internal class ScratchDbms
private constructor(
    private val home: Path,
    private val dbms: DatabaseManagementService,
    /** The database. */
    val database: GraphDatabaseAPI,
) : AutoCloseable {
    /** Stops the DBMS, which checkpoints the database, and removes its home. */
    override fun close() {
        try {
            dbms.shutdown()
        } finally {
            deleteHome(home)
        }
    }

    companion object {
        /** How long the database may take to start, recovery included. */
        private val START_TIMEOUT: Duration = Duration.ofHours(24)

        /**
         * Opens the database of [layout], a database of the server whose configuration is
         * [serverConfig], in a DBMS whose home is [home], replaced if left by an earlier run, and
         * whose logs go to [logs]. Read-only unless [writable]. [consensusIndex] and
         * [kernelVersion] are those of the store's last transaction.
         */
        fun open(
            layout: DatabaseLayout,
            serverConfig: Config,
            home: Path,
            logs: Path,
            writable: Boolean,
            consensusIndex: Long,
            kernelVersion: KernelVersion,
            pageCache: Long,
        ): ScratchDbms {
            val name = layout.databaseName
            deleteHome(home)
            link(home.resolve("data/databases").resolve(name), layout.databaseDirectory())
            link(home.resolve("data/transactions").resolve(name), layout.transactionLogsDirectory)
            val config = config(serverConfig, home, logs, name, writable, kernelVersion, pageCache)
            val dbms =
                try {
                    DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE) {
                            ScratchEditionModule(it, consensusIndex)
                        }
                        .build(config, false, GraphDatabaseDependencies.newDependencies())
                } catch (e: Exception) {
                    deleteHome(home)
                    throw e
                }
            try {
                val database = dbms.database(name) as GraphDatabaseAPI
                if (!database.isAvailable(START_TIMEOUT.toMillis())) {
                    throw IOException(
                        "Database '$name' did not start: ${statusMessage(dbms, name)}. Its log " +
                            "is in $logs."
                    )
                }
                return ScratchDbms(home, dbms, database)
            } catch (e: Exception) {
                try {
                    dbms.shutdown()
                } finally {
                    deleteHome(home)
                }
                throw e
            }
        }

        private fun statusMessage(dbms: DatabaseManagementService, name: String): String =
            dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME).executeTransactionally(
                "SHOW DATABASE \$name YIELD currentStatus, statusMessage",
                mapOf("name" to name),
            ) { result ->
                result
                    .asSequence()
                    .map { "${it["currentStatus"]}, ${it["statusMessage"]}" }
                    .firstOrNull() ?: "unknown"
            }

        private fun config(
            serverConfig: Config,
            home: Path,
            logs: Path,
            name: String,
            writable: Boolean,
            kernelVersion: KernelVersion,
            pageCache: Long,
        ): Config {
            val builder = Config.newBuilder()
            val declared = serverConfig.declaredSettings
            for (setting in declared.values) {
                if (setting.name().startsWith("db.") && serverConfig.isExplicitlySet(setting)) {
                    builder.set(setting, serverConfig.get(setting))
                }
            }
            builder
                .set(GraphDatabaseSettings.neo4j_home, home.toAbsolutePath())
                .set(GraphDatabaseSettings.logs_directory, logs.toAbsolutePath())
                .set(GraphDatabaseSettings.initial_default_database, name)
                .set(
                    GraphDatabaseSettings.read_only_databases,
                    if (writable) emptySet() else setOf(name),
                )
                .set(GraphDatabaseSettings.pagecache_memory, pageCache)
                .set(GraphDatabaseSettings.auth_enabled, false)
                // Large transactions, which only the merge runs.
                .set(GraphDatabaseSettings.memory_transaction_max_size, 0L)
                .set(GraphDatabaseSettings.memory_transaction_global_max_size, 0L)
                .set(GraphDatabaseSettings.transaction_timeout, Duration.ZERO)
                .set(BoltConnector.enabled, false)
                .set(HttpConnector.enabled, false)
                .set(HttpsConnector.enabled, false)
                .set(GraphDatabaseSettings.routing_enabled, false)
                .set(MetricsSettings.enabled, false)
                .set(
                    GraphDatabaseInternalSettings.latest_runtime_version,
                    runtimeVersion(kernelVersion).version,
                )
            // Dodeca's backup server, when its module is installed.
            if (BACKUP_ENABLED in declared) builder.setRaw(mapOf(BACKUP_ENABLED to "false"))
            return builder.build()
        }

        private const val BACKUP_ENABLED = "server.backup.enabled"

        /** The latest DBMS runtime version whose kernel version is [kernelVersion]. */
        internal fun runtimeVersion(kernelVersion: KernelVersion): DbmsRuntimeVersion =
            DbmsRuntimeVersion.VERSIONS.filter {
                    it.kernelVersion() == kernelVersion && it != DbmsRuntimeVersion.GLORIOUS_FUTURE
                }
                .maxByOrNull { it.version }
                ?: throw IOException(
                    "The store's kernel version $kernelVersion is not one this DBMS runs"
                )

        private fun link(link: Path, target: Path) {
            Files.createDirectories(link.parent)
            Files.createSymbolicLink(link, target.toAbsolutePath())
        }

        /** Deletes [home] without following its links into the server's directories. */
        internal fun deleteHome(home: Path) {
            if (!Files.exists(home, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return
            Files.walk(home).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { it.deleteIfExists() }
            }
        }
    }

    /** Dodeca's edition, whose transactions all take the consensus index [consensusIndex]. */
    private class ScratchEditionModule(globalModule: GlobalModule, consensusIndex: Long) :
        DodecaEditionModule(globalModule) {
        override val hosting: DatabaseHosting =
            object : DatabaseHosting {
                override val reusesFreedIdsAtStart = true

                override fun prepare(
                    namedDatabaseId: NamedDatabaseId,
                    storeExists: Boolean,
                ): HostedDatabase =
                    object : HostedDatabase {
                        override val hostedOnMode = HostedOnMode.SINGLE
                        override val leaseService: LeaseService = LeaseService.NO_LEASES

                        override fun commitProcessFactory(local: TransactionalProcessFactory) =
                            StampingProcessFactory(local, consensusIndex)

                        override fun attach(database: Database) {}
                    }
            }
    }

    private class StampingProcessFactory(
        private val local: TransactionalProcessFactory,
        private val consensusIndex: Long,
    ) : TransactionalProcessFactory by local {
        override fun create(
            appender: TransactionAppender,
            storageEngine: StorageEngine,
            readOnlyDatabaseChecker: DatabaseReadOnlyChecker,
            preAllocateSpaceInStoreFiles: Boolean,
            commandCommitListeners: CommandCommitListeners,
            prefetchPages: Boolean,
            logProvider: LogProvider,
        ): TransactionCommitProcess {
            val process =
                local.create(
                    appender,
                    storageEngine,
                    readOnlyDatabaseChecker,
                    preAllocateSpaceInStoreFiles,
                    commandCommitListeners,
                    prefetchPages,
                    logProvider,
                )
            return TransactionCommitProcess { batch, event, mode, memoryTracker ->
                var transaction: StorageEngineTransaction? = batch
                while (transaction != null) {
                    (transaction.commandBatch() as? CompleteCommandBatch)?.setConsensusIndex(
                        consensusIndex
                    )
                    transaction = transaction.next()
                }
                process.commit(batch, event, mode, memoryTracker)
            }
        }
    }
}
