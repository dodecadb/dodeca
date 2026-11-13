package dev.dodeca.backup.source

import dev.dodeca.backup.BackupProtocol
import dev.dodeca.backup.BackupSettings
import dev.dodeca.backup.seed.StoreSeeder
import dev.dodeca.core.edition.BackupSources
import dev.dodeca.core.edition.DatabaseSeeder
import dev.dodeca.core.edition.LatestWrites
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.identity.ServerIdentity
import org.neo4j.exceptions.UnsatisfiedDependencyException
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.pagecache.PageCache
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import org.neo4j.ssl.config.SslPolicyProvider

/**
 * Serves backups on Dodeca's servers, when `server.backup.enabled`, with a [BackupListener], and
 * creates databases from seeds there ([StoreSeeder]). Registered in `META-INF/services`; Neo4j
 * creates it once the edition is set up, and registers the [Lifecycle] it returns as a global
 * dependency, which is how the edition finds the [DatabaseSeeder].
 */
class BackupExtensionFactory :
    ExtensionFactory<BackupExtensionFactory.Dependencies>(ExtensionType.GLOBAL, "dodeca-backup") {
    /** What it needs of the DBMS, which Neo4j provides from its global dependencies. */
    interface Dependencies {
        fun config(): Config

        fun sslPolicyProvider(): SslPolicyProvider

        fun connectorPortRegister(): ConnectorPortRegister

        fun logService(): LogService

        /** Provided by Dodeca's editions only. */
        fun latestWrites(): LatestWrites

        /** Provided by Dodeca's editions only. */
        fun backupSources(): BackupSources

        fun databaseContextProvider(): DatabaseContextProvider<*>

        fun serverIdentity(): ServerIdentity

        fun fileSystemAbstraction(): FileSystemAbstraction

        fun pageCache(): PageCache
    }

    /** Backups and seeds of a Dodeca server. */
    private class Extension(private val listener: Lifecycle?, seeder: DatabaseSeeder) :
        LifecycleAdapter(), DatabaseSeeder by seeder {
        override fun start() {
            listener?.start()
        }

        override fun stop() {
            listener?.stop()
        }
    }

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        val latestWrites =
            try {
                dependencies.latestWrites()
            } catch (_: UnsatisfiedDependencyException) {
                // Not Dodeca's edition, such as Neo4j Community with this jar on its class path in
                // tests.
                return LifecycleAdapter()
            }
        val config = dependencies.config()
        val fs = dependencies.fileSystemAbstraction()
        val logProvider = dependencies.logService().internalLogProvider
        val seeder =
            StoreSeeder(
                fs,
                dependencies.pageCache(),
                config,
                logProvider,
                dependencies.serverIdentity().serverId().uuid(),
            )
        if (!config.get(BackupSettings.enabled)) return Extension(null, seeder)
        val source =
            BackupSource(
                dependencies.databaseContextProvider(),
                latestWrites,
                fs,
                dependencies.serverIdentity().serverId().uuid().toString(),
                logProvider,
                sources = dependencies::backupSources,
            )
        val listener =
            BackupListener(
                config,
                dependencies.sslPolicyProvider(),
                dependencies.connectorPortRegister(),
                logProvider,
            ) { name ->
                source.takeIf { name == BackupProtocol.NAME }
            }
        return Extension(listener, seeder)
    }
}
