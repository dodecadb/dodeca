package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import org.dozendb.cluster.internal.edition.DozenDbEditionModule
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI

/** A DBMS with clustering disabled, in this JVM, for tests. */
internal class StandaloneServer(
    root: Path,
    private val settings: Map<Setting<*>, Any> = emptyMap(),
) : AutoCloseable {
    val home: Path = Files.createDirectories(root.resolve("server"))

    @Volatile
    var dbms: DatabaseManagementService? = null
        private set

    fun start() {
        check(dbms == null) { "the server is running" }
        dbms =
            DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::DozenDbEditionModule)
                .build(config(), false, GraphDatabaseDependencies.newDependencies())
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

    override fun close() = stop()

    private fun config(): Config {
        val builder =
            Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, home)
                .set(GraphDatabaseSettings.auth_enabled, false)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .set(BoltConnector.enabled, false)
                .set(HttpConnector.enabled, false)
                .set(HttpsConnector.enabled, false)
        @Suppress("UNCHECKED_CAST")
        settings.forEach { (setting, value) -> builder.set(setting as Setting<Any>, value) }
        return builder.build()
    }

    companion object {
        const val DEFAULT_DATABASE = "neo4j"
        const val SYSTEM = "system"
    }
}
