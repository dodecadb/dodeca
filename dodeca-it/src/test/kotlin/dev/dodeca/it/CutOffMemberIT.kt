package dev.dodeca.it

import dev.dodeca.consensus.testing.TestPorts
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
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
 * A member cut off from the others, whose Raft packets are dropped so that it answers nothing at
 * all, does not keep them from routing: a database created with WAIT takes writes at once through a
 * member that does not host it, which asks the others where it is without waiting for the one cut
 * off.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CutOffMemberIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.associateWith { TestPorts.free() }
    private val members = aliases.associateWith { alias ->
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts.getValue(alias))
    }
    private val drivers = mutableListOf<Driver>()
    private var faults: NetworkFaults? = null

    @BeforeAll
    fun start() {
        Startables.deepStart(members.values).join()
    }

    @AfterAll
    fun stop() {
        faults?.close()
        drivers.forEach(Driver::close)
        members.values.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    /** A `neo4j://` driver that knows the members called [known] only. */
    private fun routingDriver(vararg known: String): Driver =
        GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.getValue(known.first())}",
                AuthTokens.basic("neo4j", DodecaContainer.PASSWORD),
                Config.builder()
                    .withResolver {
                        known.map { ServerAddress.of("127.0.0.1", boltPorts.getValue(it)) }.toSet()
                    }
                    .build(),
            )
            .also { drivers += it }

    private fun Driver.administer(command: String): List<Map<String, Any?>> {
        val config =
            SessionConfig.builder()
                .withDatabase("system")
                .withDefaultAccessMode(AccessMode.WRITE)
                .build()
        return session(config).use { session -> session.run(command).list { it.asMap() } }
    }

    /** The role of [alias] in the group of each database that runs there, as it knows itself. */
    private fun roles(alias: String): Map<Any?, Any?> =
        members
            .getValue(alias)
            .rows("CALL dodeca.cluster.status() YIELD database, role", "system")
            .associate { it["database"] to it["role"] }

    @Test
    fun `a database created with WAIT takes writes at once through a member that does not host it`() {
        val names = (1..3).map { "waited$it" }
        // Each database is placed on core1, so that core2 routes it without hosting it.
        for (alias in listOf("core2", "core3")) {
            val id = members.getValue(alias).memberId()
            routingDriver("core1")
                .administer(
                    "ALTER SERVER '$id' SET OPTIONS {deniedDatabases: ${names.map { "'$it'" }}}"
                )
        }
        faults = NetworkFaults(members.getValue("core3")).also { it.cutRaft() }
        // The others elect a leader of `system` if core3 led it.
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(listOf("core1", "core2").map { roles(it)["system"] }).contains("LEADER")
            }
        // A driver that knows nothing yet: one that knew core3 as the leader of `system` would send
        // the next command there, without retrying.
        val administration = routingDriver("core1", "core2")
        val router = routingDriver("core2")

        for (name in names) {
            val waited = administration.administer("CREATE DATABASE $name TOPOLOGY 1 PRIMARY WAIT")
            assertThat(waited).allSatisfy { assertThat(it["success"]).isEqualTo(true) }
            assertThat(roles("core1")[name]).isEqualTo("LEADER")
            assertThat(roles("core2")).doesNotContainKey(name)

            // Without retrying, as Cypher Shell runs a script.
            router.session(SessionConfig.forDatabase(name)).use {
                it.run("CREATE (:Film {title: 'Up'})").consume()
            }
        }
    }
}
