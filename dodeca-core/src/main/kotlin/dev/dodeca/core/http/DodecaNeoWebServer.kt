/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-11-12: Neo4j Community's CommunityNeoWebServer.createWebServer, with a JettyWebServer that
 * reloads its HTTPS certificates in place, configured as SslSocketConnectorFactory configures them.
 * 2026-11-16: HTTPS trusts the clients its SSL policy trusts, where SslSocketConnectorFactory leaves
 * Jetty to trust the JDK's authorities.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.http

import java.security.KeyStore
import java.security.cert.CRL
import java.util.UUID
import javax.net.ssl.TrustManager
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.SecureRequestCustomizer
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.SslConnectionFactory
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.neo4j.bolt.tx.TransactionManager
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.api.net.NetworkConnectionTracker
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryPools
import org.neo4j.monitoring.Monitors
import org.neo4j.server.CommunityNeoWebServer
import org.neo4j.server.security.ssl.SslSocketConnectorFactory
import org.neo4j.server.web.JettyThreadCalculator
import org.neo4j.server.web.JettyWebServer
import org.neo4j.server.web.WebServer
import org.neo4j.ssl.SslPolicy
import org.neo4j.time.SystemNanoClock

/**
 * Neo4j Community's web server, whose HTTPS takes the certificates and settings of a TLS reload
 * (`dbms.security.reloadTLS()`) on the connector it has, as Neo4j Enterprise documents: connections
 * made afterwards use them, open ones keep working.
 */
open class DodecaNeoWebServer(
    managementService: DatabaseManagementService,
    transactionManager: TransactionManager,
    globalDependencies: Dependencies,
    config: Config,
    userLogProvider: InternalLogProvider,
    dbmsInfo: DbmsInfo,
    memoryPools: MemoryPools,
    monitors: Monitors,
    clock: SystemNanoClock,
) :
    CommunityNeoWebServer(
        managementService,
        transactionManager,
        globalDependencies,
        config,
        userLogProvider,
        dbmsInfo,
        memoryPools,
        monitors,
        clock,
    ) {
    override fun createWebServer(): WebServer {
        val dependencies = globalDependencies
        val webServer =
            ReloadingJettyWebServer(
                userLogProvider,
                config,
                dependencies.resolveDependency(NetworkConnectionTracker::class.java),
                byteBufferPool,
            )
        dependencies.satisfyDependency(webServer)
        return webServer
    }
}

/**
 * Community's Jetty web server, which on a new SSL policy closes its HTTPS connector, with the
 * connections on it, and binds a new one to the port: the bind fails while the old socket has not
 * closed yet, and so do all reloads after it. This one reloads the SSL context of the connector it
 * has instead, which takes the policy's certificates, trusted authorities and settings, and keeps
 * the port and the connections.
 *
 * Its HTTPS trusts the client certificates that the SSL policy trusts, from the authorities in its
 * `trusted` directory and not in the revocation lists in its `revoked` directory, as Bolt does. The
 * connector factory of Community, which sets no trusted authorities, leaves Jetty to trust the
 * JDK's instead: those of public authorities, and none of the policy's. The connector is made with
 * the policy's trust before it starts, so that no connection is accepted with another.
 */
internal class ReloadingJettyWebServer(
    logProvider: InternalLogProvider,
    config: Config,
    connectionTracker: NetworkConnectionTracker,
    byteBufferPool: org.eclipse.jetty.io.ByteBufferPool,
) : JettyWebServer(logProvider, config, connectionTracker, byteBufferPool) {
    init {
        // Community makes its HTTPS connector with this factory when it starts, and has no hook
        // between making the connector and starting it.
        val factory =
            try {
                JettyWebServer::class.java.getDeclaredField(CONNECTOR_FACTORY_FIELD)
            } catch (e: NoSuchFieldException) {
                throw IllegalStateException(
                    "Neo4j's JettyWebServer has no field $CONNECTOR_FACTORY_FIELD for the HTTPS " +
                        "connector factory, which Dodeca replaces so that HTTPS trusts what its " +
                        "SSL policy trusts",
                    e,
                )
            }
        factory.isAccessible = true
        factory.set(this, PolicyConnectorFactory(connectionTracker, config, byteBufferPool))
    }

    override fun setSslPolicy(policy: SslPolicy) {
        // Before HTTPS starts, Community keeps the policy for the connector it creates.
        val connector = httpsConnector() ?: return super.setSslPolicy(policy)
        val password = UUID.randomUUID().toString()
        // Read first, so that a policy that cannot be read leaves the SSL context in use.
        val keyStore = policy.getKeyStore(password.toCharArray(), password.toCharArray())
        val ssl = connector.getConnectionFactory(SslConnectionFactory::class.java)
        try {
            ssl.sslContextFactory.reload {
                configure(it as PolicySslContextFactory, policy, keyStore, password)
            }
        } catch (e: Exception) {
            throw IllegalStateException("HTTPS could not take the new certificates", e)
        }
        connector.connectionFactories
            .filterIsInstance<HttpConfiguration.ConnectionFactory>()
            .map { it.httpConfiguration }
            .distinct()
            .mapNotNull { it.getCustomizer(SecureRequestCustomizer::class.java) }
            .forEach { it.isSniHostCheck = policy.isVerifyHostname }
    }

    /** The running connector of HTTPS, if any. */
    private fun httpsConnector(): ServerConnector? =
        jetty
            ?.takeIf { it.isRunning }
            ?.connectors
            ?.filterIsInstance<ServerConnector>()
            ?.firstOrNull { it.getConnectionFactory(SslConnectionFactory::class.java) != null }

    private companion object {
        const val CONNECTOR_FACTORY_FIELD = "sslSocketFactory"
    }
}

/**
 * Community's HTTPS connector factory, whose connectors take the SSL context of a
 * [PolicySslContextFactory] in place of the one Community configures: the same, but for the trust.
 */
private class PolicyConnectorFactory(
    connectionTracker: NetworkConnectionTracker,
    config: Config,
    byteBufferPool: org.eclipse.jetty.io.ByteBufferPool,
) : SslSocketConnectorFactory(connectionTracker, config, byteBufferPool) {
    override fun createConnector(
        server: Server,
        sslPolicy: SslPolicy,
        address: SocketAddress,
        jettyThreadCalculator: JettyThreadCalculator,
    ): ServerConnector {
        val connector = super.createConnector(server, sslPolicy, address, jettyThreadCalculator)
        val community = connector.getConnectionFactory(SslConnectionFactory::class.java)
        val password = UUID.randomUUID().toString()
        val factory =
            PolicySslContextFactory(sslPolicy).also {
                val keyStore = sslPolicy.getKeyStore(password.toCharArray(), password.toCharArray())
                configure(it, sslPolicy, keyStore, password)
            }
        val ours = SslConnectionFactory(factory, community.nextProtocol)
        connector.connectionFactories =
            connector.connectionFactories.map { if (it === community) ours else it }
        return connector
    }
}

/** Jetty's server SSL context factory, which trusts what [policy] trusts. */
private class PolicySslContextFactory(var policy: SslPolicy) : SslContextFactory.Server() {
    override fun getTrustManagers(
        trustStore: KeyStore?,
        crls: Collection<CRL>?,
    ): Array<TrustManager> = policy.trustManagerFactory.trustManagers
}

/**
 * [factory] for [policy], as Community's SslSocketConnectorFactory configures a new one, from
 * whatever it was: Jetty's defaults where the policy has no ciphers or TLS versions.
 */
private fun configure(
    factory: PolicySslContextFactory,
    policy: SslPolicy,
    keyStore: KeyStore,
    password: String,
) {
    val defaults = SslContextFactory.Server()
    factory.policy = policy
    factory.keyStore = keyStore
    factory.setKeyStorePassword(password)
    factory.setKeyManagerPassword(password)
    val ciphers = policy.cipherSuites
    if (ciphers != null) {
        factory.setIncludeCipherSuites(*ciphers.toTypedArray())
        factory.setExcludeCipherSuites()
    } else {
        factory.setIncludeCipherSuites(*defaults.includeCipherSuites)
        factory.setExcludeCipherSuites(*defaults.excludeCipherSuites)
    }
    val protocols = policy.tlsVersions
    if (protocols != null) {
        factory.setIncludeProtocols(*protocols)
        factory.setExcludeProtocols()
    } else {
        factory.setIncludeProtocols(*defaults.includeProtocols)
        factory.setExcludeProtocols(*defaults.excludeProtocols)
    }
    factory.needClientAuth = policy.clientAuth == ClientAuth.REQUIRE
    factory.wantClientAuth = policy.clientAuth == ClientAuth.OPTIONAL
}
