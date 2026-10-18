package org.dozendb.cluster.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.api.testing.TestPorts
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Databases managed through a `neo4j://` driver are created, stopped, started and dropped
 * everywhere.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class MultiDatabaseIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DozerDbContainer(DozerDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
    }
    private val down = mutableSetOf<DozerDbContainer>()
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
        driver =
            GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.first()}",
                AuthTokens.basic("neo4j", DozerDbContainer.PASSWORD),
                Config.builder()
                    .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                    .build(),
            )
    }

    @AfterAll
    fun stop() {
        driver.close()
        members.forEach {
            it.saveLogs(DozerDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    /** Runs an administration command through the driver, which routes it to the system leader. */
    private fun administer(command: String) {
        val config =
            SessionConfig.builder()
                .withDatabase("system")
                .withDefaultAccessMode(AccessMode.WRITE)
                .build()
        driver.session(config).use { it.run(command).consume() }
    }

    /** Retries [assertion], also when it throws, for example while a member lacks a database. */
    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(500) atMost
            AWAIT untilAsserted
            {
                assertion()
            }
    }

    private fun live(): List<DozerDbContainer> = members - down

    /** The status of [database] as every live member reports it about itself, null if none. */
    private fun statuses(database: String): List<Any?> =
        live().map { member ->
            member
                .rows("SHOW DATABASES YIELD name, currentStatus", "system")
                .singleOrNull { it["name"] == database }
                ?.get("currentStatus")
        }

    private fun role(member: DozerDbContainer, database: String): Any? =
        member
            .rows("CALL dozendb.cluster.status()", "system")
            .singleOrNull { it["database"] == database }
            ?.get("role")

    private fun roles(database: String): List<Any?> = live().map { role(it, database) }

    private fun awaitOnline(database: String) {
        eventually {
            assertThat(statuses(database)).containsOnly("online")
            assertThat(roles(database)).containsExactlyInAnyOrder("LEADER", "FOLLOWER", "FOLLOWER")
        }
    }

    /** Creates a node labelled [label] in [database] through the driver. */
    private fun add(database: String, label: String) {
        driver.session(SessionConfig.forDatabase(database)).use { session ->
            session.executeWrite { tx -> tx.run("CREATE (:$label)").consume() }
        }
    }

    private fun awaitCount(database: String, label: String, expected: Long) {
        eventually {
            for (member in live()) {
                assertThat(
                        member.rows("MATCH (n:$label) RETURN count(n) AS c", database).single()["c"]
                    )
                    .describedAs(member.alias)
                    .isEqualTo(expected)
            }
        }
    }

    @Test
    @Order(1)
    fun `a database created through a routing driver runs replicated on every member`() {
        administer("CREATE DATABASE $FILMS")

        awaitOnline(FILMS)
        add(FILMS, "Film")
        awaitCount(FILMS, "Film", 1)
    }

    @Test
    @Order(2)
    fun `stopping and starting a database applies on every member`() {
        administer("STOP DATABASE $FILMS")

        eventually {
            assertThat(statuses(FILMS)).containsOnly("offline")
            assertThat(roles(FILMS)).containsOnlyNulls()
        }

        administer("START DATABASE $FILMS")

        awaitOnline(FILMS)
        add(FILMS, "Film")
        awaitCount(FILMS, "Film", 2)
    }

    @Test
    @Order(3)
    fun `a dropped database is gone from every member`() {
        administer("DROP DATABASE $FILMS")

        eventually {
            assertThat(statuses(FILMS)).containsOnlyNulls()
            assertThat(roles(FILMS)).containsOnlyNulls()
        }
    }

    @Test
    @Order(4)
    fun `a user created through a routing driver logs in on every member`() {
        administer("CREATE USER alice SET PASSWORD 'alice-secret' CHANGE NOT REQUIRED")

        for (port in boltPorts) {
            GraphDatabase.driver(
                    "bolt://127.0.0.1:$port",
                    AuthTokens.basic("alice", "alice-secret"),
                )
                .use { alice ->
                    eventually {
                        alice.session().use { session ->
                            assertThat(session.run("RETURN 1 AS one").single()["one"].asInt())
                                .isEqualTo(1)
                        }
                    }
                }
        }
    }

    @Test
    @Order(5)
    fun `a member killed while a database is created copies it when it restarts`() {
        val killed = members.first { role(it, "system") != "LEADER" }
        killed.kill()
        down += killed

        administer("CREATE DATABASE $SERIES")
        eventually { assertThat(roles(SERIES)).contains("LEADER") }
        add(SERIES, "Series")
        killed.restart()
        down -= killed

        awaitOnline(SERIES)
        awaitCount(SERIES, "Series", 1)
    }

    private companion object {
        const val FILMS = "films"
        const val SERIES = "series"
        val AWAIT: Duration = Duration.ofSeconds(60)
    }
}
