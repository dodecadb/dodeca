package org.dozendb.cluster.internal.routing

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.cluster.internal.ClusterSettings
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig

/**
 * A server that does not host a database still serves it, as in Neo4j Enterprise: drivers that
 * route through it are sent to the servers that host it, server-side routing runs its reads and
 * writes there, and SHOW DATABASES shows where it is hosted. A Free server hosts only `system`, so
 * it stands for any server that does not host a database.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NonHostingServersTest {
    private lateinit var cluster: ClusterHarness
    private lateinit var hosts: List<ClusterHarness.Member>
    /** A Free server whose routing tables send drivers to the hosts. */
    private lateinit var router: ClusterHarness.Member
    /** A Free server that routes on the server: its routing tables name only itself. */
    private lateinit var serverRouter: ClusterHarness.Member
    private val drivers = HashMap<String, Driver>()

    @BeforeAll
    fun start(
        // Kept when a test fails, for the members' logs.
        @TempDir(cleanup = CleanupMode.ON_SUCCESS) root: Path
    ) {
        cluster =
            ClusterHarness(
                root,
                3,
                settings =
                    mapOf(
                        ClusterSettings.autoEnableFreeServers to false,
                        BoltConnector.enabled to true,
                    ),
            )
        cluster.start()
        hosts = cluster.members.take(3)
        administer("CREATE DATABASE films WAIT")
        router = admit()
        serverRouter =
            admit(
                mapOf(
                    GraphDatabaseSettings.routing_default_router to
                        GraphDatabaseSettings.RoutingMode.SERVER
                )
            )
        for (free in listOf(router, serverRouter)) {
            eventually {
                assertThat(free.hosted()).containsExactly(SYSTEM)
                assertThat(free.query("SHOW DATABASE films YIELD name").size).isEqualTo(3)
            }
        }
    }

    @AfterAll
    fun stop() {
        drivers.values.forEach(Driver::close)
        cluster.close()
    }

    @Test
    fun `a driver that routes through it is sent to the hosts`() {
        // `system` runs no UNWIND.
        val routes =
            router
                .query("CALL dbms.routing.getRoutingTable({}, 'films') YIELD servers")
                .single()["servers"]
                as List<*>
        val leader = cluster.awaitLeader(FILMS) { hosts }
        val addresses = { role: String ->
            routes
                .map { it as Map<*, *> }
                .filter { it["role"] == role }
                .flatMap { it["addresses"] as List<*> }
                .toSet()
        }
        assertThat(addresses("ROUTE")).isEqualTo(hosts.map { it.boltAddress() }.toSet())
        assertThat(addresses("WRITE")).containsExactly(leader.boltAddress())
        // As in Neo4j Enterprise, the leader takes no reads by default.
        assertThat(addresses("READ")).isEqualTo((hosts - leader).map { it.boltAddress() }.toSet())

        assertThat(writeAndRead("neo4j", router, "routed")).isEqualTo(1)
    }

    @Test
    fun `a database created with WAIT is served through it at once`() {
        val failures = mutableListOf<String>()
        var created = 0
        val routes =
            listOf(router, serverRouter).flatMap { member ->
                listOf(AccessMode.WRITE, AccessMode.READ).map { member to it }
            }
        // Each without retrying, as Cypher Shell runs a script, on a new database whose leader was
        // just elected.
        repeat(3) {
            for ((member, mode) in routes) {
                val name = "waited${++created}"
                administer("CREATE DATABASE $name TOPOLOGY 1 PRIMARY WAIT")
                val driver =
                    drivers.getOrPut("neo4j $member") {
                        GraphDatabase.driver("neo4j://${member.boltAddress()}", AuthTokens.none())
                    }
                val config =
                    SessionConfig.builder().withDatabase(name).withDefaultAccessMode(mode).build()
                try {
                    driver.session(config).use {
                        it.run(if (mode == AccessMode.WRITE) "CREATE ()" else "MATCH (n) RETURN n")
                            .consume()
                    }
                } catch (e: Exception) {
                    failures += "$mode of $name through $member: $e"
                }
            }
        }
        assertThat(failures).isEmpty()
    }

    @Test
    fun `server-side routing runs its reads and writes on the hosts`() {
        assertThat(writeAndRead("neo4j", serverRouter, "forwarded")).isEqualTo(1)
    }

    @Test
    fun `a direct connection to it fails, telling where the database is`() {
        val error = catchThrowable { writeAndRead("bolt", router, "direct") }

        assertThat(error)
            .hasMessageContaining("Database 'films' is not hosted on this server")
            .hasMessageContaining("neo4j://")
    }

    @Test
    fun `SHOW DATABASES on it shows the hosts, the leader as writer`() {
        val leader = cluster.awaitLeader(FILMS) { hosts }
        eventually {
            val rows = router.query("SHOW DATABASE films YIELD address, writer, currentStatus")
            assertThat(rows.map { it["address"] })
                .containsExactlyInAnyOrderElementsOf(hosts.map { it.boltAddress() })
            assertThat(rows.filter { it["writer"] == true }.map { it["address"] })
                .containsExactly(leader.boltAddress())
            assertThat(rows.map { it["currentStatus"] }).containsOnly("online")
        }
    }

    /** Creates a film titled [title] through [member] and counts those of the title. */
    private fun writeAndRead(scheme: String, member: ClusterHarness.Member, title: String): Long {
        val driver =
            drivers.getOrPut("$scheme $member") {
                GraphDatabase.driver("$scheme://${member.boltAddress()}", AuthTokens.none())
            }
        return driver.session(SessionConfig.forDatabase(FILMS)).use { session ->
            session.executeWrite { tx ->
                tx.run("CREATE (:Film {title: \$title})", mapOf("title" to title)).consume()
            }
            session.executeRead { tx ->
                tx.run(
                        "MATCH (f:Film {title: \$title}) RETURN count(f) AS c",
                        mapOf("title" to title),
                    )
                    .single()["c"]
                    .asLong()
            }
        }
    }

    /** Starts a new member with [settings] and admits it to the cluster, as a Free server. */
    private fun admit(settings: Map<org.neo4j.graphdb.config.Setting<*>, Any> = emptyMap()) =
        cluster.newMember().also {
            it.settings = settings
            it.start()
            administer("CALL dozendb.cluster.addMember('127.0.0.1:${it.raftPort}')")
        }

    private fun administer(query: String) {
        cluster.awaitLeader(SYSTEM) { hosts }.database(SYSTEM).executeTransactionally(query)
    }

    private fun ClusterHarness.Member.query(query: String): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally(query, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The databases whose Raft group runs on this member. */
    private fun ClusterHarness.Member.hosted(): List<String> =
        cluster.status().map { it.database.name }.sorted()

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
