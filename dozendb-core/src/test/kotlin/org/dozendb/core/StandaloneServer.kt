package org.dozendb.core

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.dozendb.core.edition.DozenDbEditionModule
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.event.DatabaseEventListener
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI

/** A DBMS with clustering disabled, in this JVM, for tests. */
class StandaloneServer(
    root: Path,
    private val settings: Map<Setting<*>, Any> = emptyMap(),
    /** Listeners the DBMS calls before DozenDB's own. */
    private val databaseEventListeners: List<DatabaseEventListener> = emptyList(),
    /** The file system the DBMS runs on, if not Neo4j's default one. */
    private val fileSystem: FileSystemAbstraction? = null,
    /** Dependencies the DBMS takes instead of making its own. */
    private val dependencies: List<Any> = emptyList(),
) : AutoCloseable {
    val home: Path = Files.createDirectories(root.resolve("server"))

    @Volatile
    var dbms: DatabaseManagementService? = null
        private set

    fun start() {
        check(dbms == null) { "the server is running" }
        dbms =
            DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::DozenDbEditionModule)
                .build(
                    config(),
                    false,
                    GraphDatabaseDependencies.newDependencies()
                        .databaseEventListeners(databaseEventListeners)
                        .dependencies(
                            Dependencies().apply {
                                fileSystem?.let(::satisfyDependency)
                                dependencies.forEach(::satisfyDependency)
                            }
                        ),
                )
    }

    fun stop() {
        try {
            dbms?.shutdown()
        } finally {
            dbms = null
        }
    }

    fun restart() {
        stop()
        start()
    }

    fun database(name: String = DEFAULT_DATABASE): GraphDatabaseAPI =
        checkNotNull(dbms) { "the server is stopped" }.database(name) as GraphDatabaseAPI

    /** Runs [cypher] on `system`. */
    fun administer(cypher: String, parameters: Map<String, Any> = emptyMap()) =
        database(SYSTEM).executeTransactionally(cypher, parameters)

    fun rows(
        cypher: String,
        database: String = DEFAULT_DATABASE,
        parameters: Map<String, Any> = emptyMap(),
    ): List<Map<String, Any?>> =
        database(database).executeTransactionally(cypher, parameters) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The messages of the debug log. */
    fun debugMessages(): List<String> =
        DebugEntry.read(home.resolve("logs/debug.log")).map { it.message }

    /** The entries of the debug log. */
    fun debugEntries(): List<DebugEntry> = DebugEntry.read(home.resolve("logs/debug.log"))

    /** The address Bolt listens on, when enabled. */
    fun boltAddress(): String {
        val register =
            database(SYSTEM).dependencyResolver.resolveDependency(ConnectorPortRegister::class.java)
        val address = checkNotNull(register.getLocalAddress(ConnectorType.BOLT)) { "Bolt is off" }
        return "${address.host}:${address.port}"
    }

    /** The address the routing connector listens on, when Bolt is enabled. */
    fun routingAddress(): String {
        val register =
            database(SYSTEM).dependencyResolver.resolveDependency(ConnectorPortRegister::class.java)
        val address =
            checkNotNull(register.getLocalAddress(ConnectorType.INTRA_BOLT)) {
                "there is no routing connector"
            }
        return "${address.host}:${address.port}"
    }

    override fun close() = stop()

    private fun config(): Config {
        val builder =
            Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, home)
                .set(GraphDatabaseSettings.auth_enabled, false)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                // Neo4j preallocates every transaction log, 256 MB, on Linux: the tests' RAM disk
                // would fill.
                .set(GraphDatabaseSettings.preallocate_logical_logs, false)
                // A database that panics would end the tests' JVM; tests of it turn it on.
                .set(DozenDbSettings.shutdownOnPanic, false)
                .set(BoltConnector.enabled, false)
                .set(BoltConnector.listen_address, SocketAddress("localhost", 0))
                .set(GraphDatabaseSettings.routing_listen_address, SocketAddress("localhost", 0))
                .set(HttpConnector.enabled, false)
                .set(HttpsConnector.enabled, false)
                // Bolt's thread pools otherwise wait 5 s each for quiet before they stop.
                .set(
                    GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period,
                    Duration.ZERO,
                )
        // Neo4j's default, Cypher 5, unless the build runs the tests under another.
        System.getProperty(CYPHER_PROPERTY)?.let {
            builder.set(
                GraphDatabaseSettings.default_language,
                GraphDatabaseSettings.CypherVersion.valueOf(it),
            )
        }
        @Suppress("UNCHECKED_CAST")
        settings.forEach { (setting, value) -> builder.set(setting as Setting<Any>, value) }
        return builder.build()
    }

    companion object {
        const val DEFAULT_DATABASE = "neo4j"
        const val SYSTEM = "system"

        /** The Cypher version of queries that name none, as Neo4j names it (`Cypher25`). */
        const val CYPHER_PROPERTY = "dozendb.test.cypher"
    }
}
