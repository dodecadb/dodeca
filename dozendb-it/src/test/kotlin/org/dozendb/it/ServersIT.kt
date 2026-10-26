package org.dozendb.it

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
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
 * The servers of a cluster, as Neo4j Enterprise shows and changes them: each initial member is an
 * enabled server with the tags of `initial.server.tags`, a server that joins stays Free and hosts
 * only `system` until ENABLE SERVER, then the databases placed on it, and the members with a
 * database's priority tag lead it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ServersIT {
    private val network = Network.newNetwork()
    private val initialAliases = listOf("core1", "core2", "core3")
    private val aliases = initialAliases + "core4"
    private val tags =
        mapOf(
            "core1" to listOf("eu", "core1"),
            "core2" to listOf("us"),
            "core3" to listOf("eu"),
            "core4" to listOf("ap"),
        )
    private val boltPorts = aliases.associateWith { TestPorts.free() }
    private val members = aliases.associateWith { alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(initialAliases.map { "$it:7700" })
            .freeServers()
            .boltOnHostPort(boltPorts.getValue(alias))
            .withEnv("NEO4J_initial_server_tags", tags.getValue(alias).joinToString(","))
            // Only core2 has the tag.
            .withEnv("NEO4J_db_cluster_raft_leader__transfer_priority__tag_neo4j", "us")
    }
    private val started = CopyOnWriteArrayList<DozenDbContainer>()
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        val initial = initialAliases.map(members::getValue)
        Startables.deepStart(initial).join()
        started += initial
        driver =
            GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.getValue("core1")}",
                AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
                Config.builder()
                    .withResolver {
                        boltPorts.values.map { ServerAddress.of("127.0.0.1", it) }.toSet()
                    }
                    .build(),
            )
    }

    @AfterAll
    fun stop() {
        driver.close()
        started.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun member(alias: String): DozenDbContainer = members.getValue(alias)

    /** Retries [assertion], also when it throws, for example while a member lacks a database. */
    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofSeconds(1) atMost
            AWAIT untilAsserted
            {
                assertion()
            }
    }

    private fun administer(command: String) {
        val config =
            SessionConfig.builder()
                .withDatabase("system")
                .withDefaultAccessMode(AccessMode.WRITE)
                .build()
        driver.session(config).use { it.run(command).consume() }
    }

    /** SHOW SERVERS YIELD *, through the routing driver, by server id. */
    private fun servers(): Map<String, Map<String, Any?>> =
        driver.session(SessionConfig.forDatabase("system")).use { session ->
            session
                .run("SHOW SERVERS YIELD *")
                .list { it.asMap() }
                .associateBy {
                    it["serverId"] as String
                }
        }

    @Test
    @Order(1)
    fun `SHOW SERVERS over neo4j lists each initial member, enabled with its initial tags`() {
        val ids = initialAliases.associateBy { member(it).memberId() }

        eventually {
            val servers = servers()
            assertThat(servers.keys).containsExactlyInAnyOrderElementsOf(ids.keys)
            for ((id, row) in servers) {
                val alias = ids.getValue(id)
                assertThat(row["name"]).isEqualTo(id)
                assertThat(row["address"]).isEqualTo("127.0.0.1:${boltPorts.getValue(alias)}")
                assertThat(row["state"]).isEqualTo("Enabled")
                assertThat(row["health"]).isEqualTo("Available")
                assertThat(row["hosting"]).isEqualTo(listOf("system", "neo4j"))
                assertThat(row["tags"]).describedAs(alias).isEqualTo(tags.getValue(alias))
            }
        }
    }

    @Test
    @Order(2)
    fun `the member with the priority tag of a database leads it`() {
        eventually {
            for (member in started) {
                val neo4j =
                    member.rows("CALL dozendb.cluster.status()", "system").single {
                        it["database"] == "neo4j"
                    }
                assertThat(neo4j["role"])
                    .describedAs(member.alias)
                    .isEqualTo(if (member.alias == "core2") "LEADER" else "FOLLOWER")
            }
        }
    }

    @Test
    @Order(3)
    fun `a server that joins stays Free until ENABLE SERVER, then hosts what is placed on it`() {
        val joining = member("core4")
        joining.start()
        started += joining
        member("core1").rows("CALL dozendb.cluster.addMember('core4:7700')", "system")
        val id = joining.memberId()

        eventually {
            val free = servers().getValue(id)
            assertThat(free["state"]).isEqualTo("Free")
            assertThat(free["hosting"]).isEqualTo(listOf("system"))
            assertThat(free["requestedHosting"]).isEqualTo(emptyList<String>())
        }
        assertThat(joining.run("MATCH (n) RETURN count(n) AS c", "neo4j"))
            .isInstanceOf(Outcome.Failed::class.java)

        administer("ENABLE SERVER '$id'")

        eventually {
            val enabled = servers().getValue(id)
            assertThat(enabled["state"]).isEqualTo("Enabled")
            assertThat(enabled["requestedHosting"]).isEqualTo(listOf("system"))
            // The tags it started with, as ENABLE SERVER gave no options.
            assertThat(enabled["tags"]).isEqualTo(tags.getValue("core4"))
        }

        administer("ALTER DATABASE neo4j SET TOPOLOGY 4 PRIMARIES")

        eventually {
            assertThat(servers().getValue(id)["hosting"]).isEqualTo(listOf("system", "neo4j"))
            assertThat(joining.rows("MATCH (n) RETURN count(n) AS c", "neo4j")).hasSize(1)
        }
        // It does not have the priority tag of neo4j, which core2 still leads.
        assertThat(
                member("core2")
                    .rows("CALL dozendb.cluster.status()", "system")
                    .single {
                        it["database"] == "neo4j"
                    }["role"]
            )
            .isEqualTo("LEADER")
    }

    private companion object {
        val AWAIT: Duration = Duration.ofMinutes(3)
    }
}
