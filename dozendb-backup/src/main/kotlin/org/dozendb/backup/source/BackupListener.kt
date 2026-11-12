package org.dozendb.backup.source

import java.io.IOException
import java.net.InetSocketAddress
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.BackupTls
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.ratis.BulkServer
import org.dozendb.consensus.ratis.ReloadableTls
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider
import org.neo4j.ssl.config.SslPolicyChangeListener
import org.neo4j.ssl.config.SslPolicyProvider

/**
 * Serves backups on `server.backup.listen_address`, with the sources that [sources] finds by name,
 * and registers the port it listens on as Neo4j's backup connector.
 */
internal class BackupListener(
    private val config: Config,
    private val policies: SslPolicyProvider,
    private val ports: ConnectorPortRegister,
    logProvider: InternalLogProvider,
    private val sources: (String) -> BulkSource?,
) : LifecycleAdapter() {
    private val log = logProvider.getLog(BackupListener::class.java)
    private var server: BulkServer? = null
    private var reloads: SslPolicyChangeListener? = null

    override fun start() {
        val address = config.get(BackupSettings.listenAddress)
        val tls = BackupTls.server(config, address, policies, log)?.let(::ReloadableTls)
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
                        e.message,
                    e,
                )
            }
        server = started
        reloads = tls?.let { BackupTls.follow(config, policies, it, log) }
        ports.register(ConnectorType.BACKUP, InetSocketAddress(address.hostname, started.port))
        log.info(
            "Serving backups on %s:%d %s",
            address.hostname,
            started.port,
            if (tls == null) "in plaintext" else "over TLS",
        )
    }

    override fun stop() {
        reloads?.let(policies::removePolicyChangeListener)
        reloads = null
        val running = server ?: return
        server = null
        ports.deregister(ConnectorType.BACKUP)
        running.close()
    }
}
