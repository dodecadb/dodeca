package org.dozendb.cluster.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
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
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/** A `neo4j://` driver writes through the leader, reads from followers and survives failover. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class RoutingIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DozerDbContainer(DozerDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
    }
    private val running = members.toMutableList()
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
        driver = driverSeededAt(members.first())
    }

    /**
     * A routing driver whose URI names [seed]. Its resolver adds every member, as a DNS name with
     * one record per member does in production: the driver resolves the default database through
     * its seed, so a seed that names one member would fail once that member is gone.
     */
    private fun driverSeededAt(seed: DozerDbContainer): Driver =
        GraphDatabase.driver(
            "neo4j://${boltAddress(seed)}",
            AuthTokens.basic("neo4j", DozerDbContainer.PASSWORD),
            Config.builder()
                .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                .build(),
        )

    @AfterAll
    fun stop() {
        driver.close()
        members.forEach {
            it.saveLogs(DozerDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun boltAddress(member: DozerDbContainer) =
        "127.0.0.1:${boltPorts[members.indexOf(member)]}"

    private fun role(member: DozerDbContainer, database: String = "neo4j"): Any? =
        when (val outcome = member.run("CALL dozendb.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == database }?.get("role")
            is Outcome.Failed -> null
        }

    private fun awaitLeader(database: String = "neo4j"): DozerDbContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                running.singleOrNull { role(it, database) == "LEADER" }
            }

    @Suppress("UNCHECKED_CAST")
    private fun routingTable(
        member: DozerDbContainer,
        database: String,
    ): Map<String, List<String>> {
        val row =
            member.rows("CALL dbms.routing.getRoutingTable({}, '$database')", "system").single()
        return (row["servers"] as List<Map<String, Any?>>).associate {
            it["role"] as String to (it["addresses"] as List<String>).sorted()
        }
    }

    private fun write(cypher: String, through: Driver = driver) =
        through.session().use { session ->
            session.executeWrite { tx -> tx.run(cypher).consume() }
            session.lastBookmarks()
        }

    @Test
    @Order(1)
    fun `routing tables send writes to the leader and reads to every member`() {
        val leader = awaitLeader()
        // Members learn each other's Bolt addresses within a refresh interval.
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                members.forEach { member ->
                    val table = routingTable(member, "neo4j")
                    assertThat(table["WRITE"]).containsExactly(boltAddress(leader))
                    assertThat(table["READ"])
                        .containsExactlyInAnyOrderElementsOf(members.map(::boltAddress))
                    assertThat(table["ROUTE"])
                        .containsExactlyInAnyOrderElementsOf(members.map(::boltAddress))
                }
            }
        val members = members.first().rows("CALL dozendb.cluster.members()", "system")
        assertThat(members.map { it["boltAddress"] })
            .containsExactlyInAnyOrderElementsOf(this.members.map(::boltAddress))
        // Administration commands go to the system leader.
        val systemLeader = awaitLeader("system")
        this.members.forEach { member ->
            assertThat(routingTable(member, "system")["WRITE"])
                .containsExactly(boltAddress(systemLeader))
        }
    }

    @Test
    @Order(2)
    fun `reads chained by bookmarks see the preceding write wherever they run`() {
        val readers = mutableSetOf<String>()

        repeat(100) { i ->
            val bookmarks = write("CREATE (:Causal {i: $i})")
            val config =
                SessionConfig.builder()
                    .withBookmarks(bookmarks)
                    .withDefaultAccessMode(AccessMode.READ)
                    .build()
            driver.session(config).use { session ->
                session.executeRead { tx ->
                    val result = tx.run("MATCH (n:Causal {i: $i}) RETURN count(n) AS c")
                    assertThat(result.single()["c"].asLong()).describedAs("read $i").isEqualTo(1)
                    readers += result.consume().server().address()
                }
            }
        }

        assertThat(readers).describedAs("servers that answered reads").hasSizeGreaterThan(1)
    }

    @Test
    @Order(3)
    fun `linearizable reads see every acknowledged write without bookmarks`() {
        val linearizable =
            TransactionConfig.builder()
                .withMetadata(mapOf("dozendb.consistency" to "linearizable"))
                .build()
        val readConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ).build()

        repeat(100) { i ->
            write("CREATE (:Linear {i: $i})")
            driver.session(readConfig).use { session ->
                val count =
                    session.executeRead(
                        { tx ->
                            tx.run("MATCH (n:Linear {i: $i}) RETURN count(n) AS c")
                                .single()["c"]
                                .asLong()
                        },
                        linearizable,
                    )
                assertThat(count).describedAs("read $i").isEqualTo(1)
            }
        }
    }

    @Test
    @Order(4)
    fun `an unknown read consistency is refused`() {
        val unknown =
            TransactionConfig.builder()
                .withMetadata(mapOf("dozendb.consistency" to "strong"))
                .build()

        val error = catchThrowable {
            driver.session().use { session ->
                session.executeRead({ tx -> tx.run("RETURN 1").consume() }, unknown)
            }
        }

        assertThat(error).isInstanceOfSatisfying(ClientException::class.java) {
            assertThat(it.code()).isEqualTo("Neo.ClientError.Request.Invalid")
        }
    }

    @Test
    @Order(5)
    fun `writes continue on a new leader soon after the leader dies`() {
        val leader = awaitLeader()
        val acknowledged = mutableListOf<Int>()
        // Seeded at the member about to die, so the driver must fall back to the other members.
        val failover =
            driverSeededAt(leader).use { seededAtLeader ->
                repeat(30) { n ->
                    write("CREATE (:Failover {n: $n})", seededAtLeader)
                    acknowledged += n
                }
                leader.saveLogs(DozerDbContainer.logsDirectory.resolve(javaClass.simpleName))

                val killed = System.nanoTime()
                leader.kill()
                running -= leader
                // Managed transactions retry until a new leader takes the write.
                write("CREATE (:Failover {n: 30})", seededAtLeader)
                val elapsed = Duration.ofNanos(System.nanoTime() - killed)
                acknowledged += 30
                (31 until 60).forEach { n ->
                    write("CREATE (:Failover {n: $n})", seededAtLeader)
                    acknowledged += n
                }
                elapsed
            }

        println("First write after the leader died took $failover")
        assertThat(failover)
            .describedAs("time to the first write after the leader died")
            .isLessThan(Duration.ofSeconds(10))
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                running.forEach { member ->
                    val stored =
                        member.rows("MATCH (n:Failover) RETURN n.n AS n").map {
                            (it["n"] as Long).toInt()
                        }
                    assertThat(stored)
                        .describedAs(member.alias)
                        .containsExactlyInAnyOrderElementsOf(acknowledged)
                }
            }
    }
}
