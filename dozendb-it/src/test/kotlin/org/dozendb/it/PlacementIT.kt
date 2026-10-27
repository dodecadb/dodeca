package org.dozendb.it

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
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
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Databases placed on some of a cluster's servers, as Neo4j Enterprise places them: a database with
 * two primaries runs on two of three servers, a `neo4j://` driver that knows only the third reads
 * and writes it, and a copy that moves to the third while clients write loses no acknowledged
 * write. MembershipIT deallocates and drops a server while clients write.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class PlacementIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.associateWith { TestPorts.free() }
    private val members = aliases.associateWith { alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts.getValue(alias))
    }
    private val ids by lazy { members.mapValues { it.value.memberId() } }
    private val writers = CopyOnWriteArrayList<Writer>()
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        Startables.deepStart(members.values).join()
        driver = routingDriver(aliases)
    }

    @AfterEach
    fun stopWriters() {
        // Also when a test fails: a writer left running fails whatever test this JVM runs next.
        writers.forEach { it.stop() }
        writers.clear()
    }

    @AfterAll
    fun stop() {
        driver.close()
        members.values.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    /** A `neo4j://` driver that knows the members called [known] only. */
    private fun routingDriver(known: List<String>): Driver =
        GraphDatabase.driver(
            "neo4j://127.0.0.1:${boltPorts.getValue(known.first())}",
            AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
            Config.builder()
                .withResolver {
                    known.map { ServerAddress.of("127.0.0.1", boltPorts.getValue(it)) }.toSet()
                }
                .build(),
        )

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

    /** The status of each copy of [database] that SHOW DATABASES lists, by member. */
    private fun copies(database: String): Map<String, String> {
        val rows =
            driver.session(SessionConfig.forDatabase("system")).use { session ->
                session.run("SHOW DATABASE $database YIELD serverID, currentStatus").list {
                    it.asMap()
                }
            }
        return rows.associate { row ->
            ids.entries.single { it.value == row["serverID"] }.key to row["currentStatus"] as String
        }
    }

    @Test
    @Order(1)
    fun `a database with two primaries runs on two servers, and a driver that knows the third uses it`() {
        administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")
        eventually {
            assertThat(copies("films")).hasSize(2).allSatisfy { _, status ->
                assertThat(status).isEqualTo("online")
            }
        }
        val other = (aliases - copies("films").keys).single()

        routingDriver(listOf(other)).use { through ->
            through.session(SessionConfig.forDatabase("films")).use { session ->
                session.executeWrite { it.run("CREATE (:Film {title: 'Alien'})").consume() }
                val titles = session.executeRead { tx ->
                    tx.run("MATCH (f:Film) RETURN f.title AS title").list {
                        it["title"].asString()
                    }
                }
                assertThat(titles).containsExactly("Alien")
            }
        }
        // Without routing, a server that does not host it says how to reach it.
        val direct = members.getValue(other).run("MATCH (f:Film) RETURN count(f) AS c", "films")
        assertThat(direct).isInstanceOf(Outcome.Failed::class.java)
        assertThat((direct as Outcome.Failed).message).contains("is not hosted on this server")
    }

    @Test
    @Order(2)
    fun `a copy moves to another server while clients write, and no acknowledged write is lost`() {
        val writer = Writer()
        eventually { assertThat(writer.acknowledged).hasSizeGreaterThanOrEqualTo(20) }
        val before = copies("films").keys
        val leaving = before.first()
        val after = before - leaving + (aliases - before).single()

        administer(
            "CALL dbms.cluster.deallocateDatabaseFromServer('${ids.getValue(leaving)}', 'films')"
        )

        eventually {
            assertThat(copies("films")).containsOnlyKeys(after).allSatisfy { _, status ->
                assertThat(status).isEqualTo("online")
            }
        }
        val moved = writer.acknowledged.size
        eventually { assertThat(writer.acknowledged.size).isGreaterThan(moved + 20) }
        writer.stop()
        for (host in after) {
            eventually {
                val stored =
                    members.getValue(host).rows("MATCH (a:Ack) RETURN a.n AS n", "films").map {
                        it["n"] as Long
                    }
                assertThat(stored)
                    .describedAs("films on $host")
                    .containsAll(writer.acknowledged)
                    .doesNotHaveDuplicates()
            }
        }
        assertThat(members.getValue(leaving).run("MATCH (n) RETURN count(n) AS c", "films"))
            .isInstanceOf(Outcome.Failed::class.java)
    }

    /** Writes `Ack` nodes to films through the routing driver, one at a time, until stopped. */
    private inner class Writer {
        val acknowledged = CopyOnWriteArrayList<Long>()
        private val running = AtomicBoolean(true)
        private val worker =
            thread(name = "placement-writer") {
                var n = 0L
                while (running.get()) {
                    n++
                    try {
                        driver.session(SessionConfig.forDatabase("films")).use { session ->
                            // MERGE, as the driver retries a write whose outcome it does not know.
                            session.executeWrite { tx -> tx.run("MERGE (:Ack {n: $n})").consume() }
                        }
                        acknowledged += n
                    } catch (_: Neo4jException) {
                        // Not acknowledged; it may or may not be committed.
                    }
                }
            }

        init {
            writers += this
        }

        fun stop() {
            running.set(false)
            worker.join()
        }
    }

    private companion object {
        val AWAIT: Duration = Duration.ofMinutes(3)
    }
}
