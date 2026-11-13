package dev.dodeca.it

import dev.dodeca.consensus.testing.TestPorts
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/** Databases are led by members that clients can write to. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LeadershipIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
    }
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
        driver =
            GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.first()}",
                AuthTokens.basic("neo4j", DodecaContainer.PASSWORD),
                Config.builder()
                    .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                    .build(),
            )
    }

    @AfterAll
    fun stop() {
        driver.close()
        members.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun role(member: DodecaContainer, database: String): Any? =
        when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == database }?.get("role")
            is Outcome.Failed -> null
        }

    private fun awaitLeader(database: String): DodecaContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                members.singleOrNull { role(it, database) == "LEADER" }
            }

    @Test
    fun `a member where a database is read-only stops leading it, and clients write on`() {
        val readOnly = awaitLeader("neo4j")
        readOnly.rows("CALL dbms.setConfigValue('server.databases.read_only', 'neo4j')", "system")
        try {
            await atMost
                Duration.ofSeconds(60) untilAsserted
                {
                    try {
                        driver.session().use { session ->
                            session.executeWrite { tx -> tx.run("MERGE (:Written)").consume() }
                        }
                    } catch (e: Neo4jException) {
                        throw AssertionError("The write failed with ${e.code()}: ${e.message}", e)
                    }
                }
            assertThat(awaitLeader("neo4j")).isNotSameAs(readOnly)
            // The member still follows the database it does not accept writes for.
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(readOnly.rows("MATCH (n:Written) RETURN count(n) AS c"))
                        .isEqualTo(listOf(mapOf("c" to 1L)))
                }
        } finally {
            readOnly.rows("CALL dbms.setConfigValue('server.databases.read_only', '')", "system")
        }
    }
}
