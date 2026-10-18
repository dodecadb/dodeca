package org.dozendb.cluster.it

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Volume
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.DockerImageName

/** The outcome of one query, comparable across servers. */
sealed interface Outcome {
    data class Rows(val rows: List<Map<String, Any?>>) : Outcome

    data class Failed(val code: String, val message: String) : Outcome
}

/** A DozerDB server in Docker, either the stock image or the image with the DozenDB jar. */
class DozerDbContainer(image: DockerImageName, val alias: String) :
    GenericContainer<DozerDbContainer>(image) {
    init {
        withNetworkAliases(alias)
        withCreateContainerCmdModifier { it.withHostName(alias) }
        withEnv("NEO4J_AUTH", "neo4j/$PASSWORD")
        withEnv("NEO4J_server_memory_heap_initial__size", "512m")
        withEnv("NEO4J_server_memory_heap_max__size", "512m")
        withEnv("NEO4J_server_memory_pagecache_size", "64m")
        withEnv("NEO4J_server_default__advertised__address", alias)
        withExposedPorts(BOLT_PORT)
        waitingFor(
            Wait.forLogMessage(".*Started\\.\\s*", 1).withStartupTimeout(Duration.ofMinutes(3))
        )
    }

    private var driver: Driver? = null

    fun inNetwork(network: Network): DozerDbContainer = withNetwork(network)

    fun clusterMember(initialMembers: List<String>): DozerDbContainer =
        withEnv("NEO4J_dbms_dozendb_cluster_enabled", "true")
            .withEnv(
                "NEO4J_dbms_dozendb_cluster_initial__members",
                initialMembers.joinToString(","),
            )

    /**
     * Publishes Bolt on [hostPort] of this machine and advertises it there, so routing tables lead
     * drivers outside the Docker network to this member.
     */
    fun boltOnHostPort(hostPort: Int): DozerDbContainer {
        setPortBindings(listOf("$hostPort:$BOLT_PORT"))
        return withEnv("NEO4J_server_bolt_advertised__address", "127.0.0.1:$hostPort")
    }

    private fun driver(): Driver =
        driver
            ?: GraphDatabase.driver(
                    "bolt://$host:${getMappedPort(BOLT_PORT)}",
                    AuthTokens.basic("neo4j", PASSWORD),
                )
                .also { driver = it }

    /** Runs [cypher] with [parameters] against [database] and returns the rows or the error. */
    fun run(
        cypher: String,
        database: String = "neo4j",
        parameters: Map<String, Any?> = emptyMap(),
    ): Outcome =
        try {
            driver().session(SessionConfig.forDatabase(database)).use { session ->
                Outcome.Rows(session.run(cypher, parameters).list { it.asMap() })
            }
        } catch (e: Neo4jException) {
            Outcome.Failed(e.code(), e.message.orEmpty())
        }

    /** Like [run] but fails the test on an error. */
    fun rows(
        cypher: String,
        database: String = "neo4j",
        parameters: Map<String, Any?> = emptyMap(),
    ): List<Map<String, Any?>> =
        when (val outcome = run(cypher, database, parameters)) {
            is Outcome.Rows -> outcome.rows
            is Outcome.Failed -> throw AssertionError("$alias: $cypher failed: $outcome")
        }

    /** Neo4j's debug log, read from the container. */
    fun debugLog(): String =
        copyFileFromContainer("/logs/debug.log") { it.readAllBytes().decodeToString() }

    /** Saves the console and debug logs under [directory] for diagnosis. */
    fun saveLogs(directory: Path) {
        val target = Files.createDirectories(directory.resolve(alias))
        Files.writeString(target.resolve("neo4j.log"), logs)
        if (isRunning) Files.writeString(target.resolve("debug.log"), debugLog())
    }

    private var dataVolume: String? = null

    /** Keeps `/data` in the Docker volume [name], so that it survives restarts and can be wiped. */
    fun withDataVolume(name: String): DozerDbContainer {
        dataVolume = name
        return withCreateContainerCmdModifier { it.hostConfig!!.withBinds(Bind(name, DATA)) }
    }

    /** Kills the container's process abruptly, keeping the container for its logs. */
    fun kill() {
        driver?.close()
        driver = null
        DockerClientFactory.instance().client().killContainerCmd(containerId).exec()
    }

    /** Stops the server gracefully, keeping the container and its data. */
    fun shutDown() {
        driver?.close()
        driver = null
        DockerClientFactory.instance()
            .client()
            .stopContainerCmd(containerId)
            .withTimeout(SHUTDOWN_SECONDS)
            .exec()
    }

    /** Starts the stopped or killed container again and waits until it answers on Bolt. */
    fun restart() {
        DockerClientFactory.instance().client().startContainerCmd(containerId).exec()
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                assertThat(run("SHOW DATABASES YIELD name", "system"))
                    .describedAs("$alias answering")
                    .isInstanceOf(Outcome.Rows::class.java)
            }
    }

    /** Deletes [database] from the data volume of this stopped member. */
    fun wipeDatabase(database: String) {
        val volume = checkNotNull(dataVolume) { "$alias keeps its data in the container" }
        GenericContainer(dockerImageName)
            .withCreateContainerCmdModifier {
                it.withEntrypoint(
                    "rm",
                    "-rf",
                    "/data/databases/$database",
                    "/data/transactions/$database",
                )
                it.hostConfig!!.withBinds(Bind(volume, DATA))
            }
            .withStartupCheckStrategy(OneShotStartupCheckStrategy())
            .use { it.start() }
    }

    override fun stop() {
        driver?.close()
        driver = null
        super.stop()
    }

    companion object {
        const val BOLT_PORT = 7687
        const val PASSWORD = "dozendb-it-secret"
        private const val SHUTDOWN_SECONDS = 120
        private val DATA = Volume("/data")

        /** Removes the Docker volume [name], once no container uses it. */
        fun removeVolume(name: String) {
            DockerClientFactory.instance().client().removeVolumeCmd(name).exec()
        }

        private fun property(name: String): String =
            requireNotNull(System.getProperty(name)) {
                "system property $name is not set; run with -Pit"
            }

        /** The stock DozerDB image the plugin is compared with and installed into. */
        val stockImage: DockerImageName by lazy { DockerImageName.parse(property("dozerdb.image")) }

        /** The stock image with the DozerDB jar replaced by the DozenDB jar. */
        val dozenDbImage: DockerImageName by lazy {
            val jar = Path.of(property("dozendb.plugin.jar"))
            check(Files.isRegularFile(jar)) { "$jar does not exist; build cluster-plugin first" }
            val image =
                ImageFromDockerfile("dozendb-it", true)
                    .withFileFromPath("dozendb.jar", jar)
                    .withDockerfileFromBuilder {
                        it.from(stockImage.asCanonicalNameString())
                            .run("rm /var/lib/neo4j/lib/${property("dozerdb.image.jar")}")
                            .copy("dozendb.jar", "/var/lib/neo4j/lib/dozendb.jar")
                            .build()
                    }
            DockerImageName.parse(image.get())
        }

        val logsDirectory: Path by lazy { Path.of(property("it.logs")) }
    }
}
