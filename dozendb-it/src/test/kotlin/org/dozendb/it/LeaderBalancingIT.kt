package org.dozendb.it

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/** Leaders spread over the members, also through a rolling restart under write load. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LeaderBalancingIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
            .balancingLeaders(Duration.ofSeconds(2))
    }
    private val databases = listOf("neo4j") + (1..6).map { "db$it" }
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
        driver =
            GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.first()}",
                AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
                Config.builder()
                    .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                    .build(),
            )
        driver.session(SessionConfig.forDatabase("system")).use { session ->
            databases.drop(1).forEach { session.run("CREATE DATABASE $it WAIT").consume() }
        }
    }

    @AfterAll
    fun stop() {
        driver.close()
        members.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    /** How many databases each member leads, as each says; null for one that is down. */
    private fun led(): List<Int?> = members.map { member ->
        if (!member.isRunning) return@map null
        when (val outcome = member.run("CALL dozendb.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.count { it["role"] == "LEADER" }
            is Outcome.Failed -> null
        }
    }

    private fun awaitLed(vararg counts: Int) {
        await withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(led().filterNotNull()).containsExactlyInAnyOrder(*counts.toTypedArray())
            }
    }

    @Test
    fun `a rolling restart under write load ends balanced and loses no acknowledged write`() {
        // The system database and seven others.
        awaitLed(3, 3, 2)
        val acknowledged = ConcurrentHashMap<String, MutableSet<Long>>()
        val unknown = ConcurrentLinkedQueue<String>()
        val ids = AtomicLong()
        val done = AtomicBoolean()
        val writers = Executors.newFixedThreadPool(databases.size)
        for (database in databases) {
            writers.execute {
                val written =
                    acknowledged.computeIfAbsent(database) { ConcurrentHashMap.newKeySet() }
                while (!done.get()) {
                    val id = ids.incrementAndGet()
                    try {
                        driver.session(SessionConfig.forDatabase(database)).use { session ->
                            session.executeWrite { tx ->
                                tx.run("MERGE (:Written {id: \$id})", mapOf("id" to id)).consume()
                            }
                        }
                        written += id
                    } catch (e: Neo4jException) {
                        // Drivers retry what they can; this one gave up.
                        if ("outcome is unknown" in e.message.orEmpty())
                            unknown += "$database: ${e.message}"
                    }
                }
            }
        }

        for (member in members) {
            member.shutDown()
            awaitLed(4, 4)
            member.restart()
            awaitLed(3, 3, 2)
        }
        done.set(true)
        writers.shutdown()
        assertThat(writers.awaitTermination(1, TimeUnit.MINUTES)).isTrue()

        assertThat(unknown).isEmpty()
        // Members that stop gracefully, and those still running, log no errors.
        for (member in members) {
            assertThat(member.debugLog().lineSequence().filter { " ERROR " in it }.toList())
                .describedAs("errors in the debug log of ${member.alias}")
                .isEmpty()
        }
        for (database in databases) {
            val written = acknowledged.getValue(database)
            assertThat(written).describedAs(database).isNotEmpty()
            for (member in members) {
                await atMost
                    Duration.ofSeconds(30) untilAsserted
                    {
                        val present =
                            member
                                .rows("MATCH (n:Written) RETURN collect(n.id) AS ids", database)
                                .single()["ids"]
                                as List<*>
                        assertThat(present.map { (it as Number).toLong() })
                            .describedAs("${member.alias}: $database")
                            .containsAll(written)
                    }
            }
        }
    }
}
