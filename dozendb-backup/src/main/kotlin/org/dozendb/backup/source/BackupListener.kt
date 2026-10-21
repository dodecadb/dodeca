package org.dozendb.backup.source

import java.io.IOException
import java.net.InetSocketAddress
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.BackupTls
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.ratis.BulkServer
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider
import org.neo4j.ssl.config.SslPolicyLoader

/**
 * Serves backups on `server.backup.listen_address`, with the sources that [sources] finds by name,
 * and registers the port it listens on as Neo4j's backup connector.
 */
internal class BackupListener(
    private val config: Config,
    private val policies: SslPolicyLoader,
    private val ports: ConnectorPortRegister,
    logProvider: InternalLogProvider,
    private val sources: (String) -> BulkSource?,
) : LifecycleAdapter() {
    private val log = logProvider.getLog(BackupListener::class.java)
    private var server: BulkServer? = null

    override fun start() {
        val address = config.get(BackupSettings.listenAddress)
        val tls = BackupTls.server(config, address, policies, log)
        val started =
            try {
                BulkServer.start(
                    InetSocketAddress(address.hostname, address.port),
                    tls,
                    "dozendb-backup",
                    sources,
                )
            } catch (e: IOException) {
                throw IllegalStateException(
                    "Unable to serve backups on $address (${BackupSettings.listenAddress.name()}): " +
                        (e.cause ?: e).message,
                    e,
                )
            }
        server = started
        ports.register(ConnectorType.BACKUP, InetSocketAddress(address.hostname, started.port))
        log.info(
            "Serving backups on %s:%d %s",
            address.hostname,
            started.port,
            if (tls == null) "in plaintext" else "over TLS",
        )
    }

    override fun stop() {
        val running = server ?: return
        server = null
        ports.deregister(ConnectorType.BACKUP)
        running.close()
    }
}
