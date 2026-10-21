package org.dozendb.backup.source

import org.dozendb.backup.BackupSettings
import org.dozendb.core.edition.LatestWrites
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.exceptions.UnsatisfiedDependencyException
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import org.neo4j.ssl.config.SslPolicyLoader

/**
 * Serves backups on DozenDB's servers, when `server.backup.enabled`, with a [BackupListener].
 * Registered in `META-INF/services`; Neo4j creates it once the edition is set up.
 */
class BackupExtensionFactory :
    ExtensionFactory<BackupExtensionFactory.Dependencies>(ExtensionType.GLOBAL, "dozendb-backup") {
    /** What it needs of the DBMS, which Neo4j provides from its global dependencies. */
    interface Dependencies {
        fun config(): Config

        fun sslPolicyLoader(): SslPolicyLoader

        fun connectorPortRegister(): ConnectorPortRegister

        fun logService(): LogService

        /** Provided by DozenDB's editions only. */
        fun latestWrites(): LatestWrites
    }

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        val config = dependencies.config()
        if (!config.get(BackupSettings.enabled)) return LifecycleAdapter()
        try {
            dependencies.latestWrites()
        } catch (_: UnsatisfiedDependencyException) {
            // Not DozenDB's edition, such as Neo4j Community with this jar on its class path in
            // tests.
            return LifecycleAdapter()
        }
        return BackupListener(
            config,
            dependencies.sslPolicyLoader(),
            dependencies.connectorPortRegister(),
            dependencies.logService().internalLogProvider,
        ) {
            null
        }
    }
}
