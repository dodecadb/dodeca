package org.dozendb.cluster.it

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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
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
 * A member is added to a running cluster and another one removed while a routing driver keeps
 * writing to two databases: every acknowledged write survives, and the remaining members end with
 * identical stores.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MembershipIT {
    private val network = Network.newNetwork()
    private val initialAliases = listOf("core1", "core2", "core3")
    private val aliases = initialAliases + "core4"
    private val boltPorts = aliases.associateWith { TestPorts.free() }
    private val members = aliases.associateWith { alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(initialAliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts.getValue(alias))
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

    /** The members of the cluster as [observer] knows them, by Raft address. */
    private fun membersSeenBy(observer: DozenDbContainer): Map<String, List<Any?>> =
        observer.rows("CALL dozendb.cluster.members()", "system").associate {
            it["raftAddress"] as String to (it["databases"] as List<*>)
        }

    /** Writes `Ack` nodes to both databases through the driver, one at a time, until stopped. */
    private inner class Writer {
        val acknowledged = DATABASES.associateWith { CopyOnWriteArrayList<Long>() }
        private val running = AtomicBoolean(true)
        private val worker =
            thread(name = "membership-writer") {
                var n = 0L
                while (running.get()) {
                    n++
                    for (database in DATABASES) {
                        try {
                            driver.session(SessionConfig.forDatabase(database)).use { session ->
                                // MERGE, as the driver retries a write whose outcome it does not
                                // know.
                                session.executeWrite { tx ->
                                    tx.run("MERGE (:Ack {n: $n})").consume()
                                }
                            }
                            acknowledged.getValue(database) += n
                        } catch (_: Neo4jException) {
                            // Not acknowledged; it may or may not be committed.
                        }
                    }
                }
            }

        fun stop() {
            running.set(false)
            worker.join()
        }
    }

    private fun contents(member: DozenDbContainer, database: String): List<Map<String, Any?>> =
        member.rows(
            "MATCH (n) RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props ORDER BY id",
            database,
        )

    @Test
    fun `a member is added and another removed during writes, and nothing acknowledged is lost`() {
        administer("CREATE DATABASE films")
        eventually {
            for (member in started) {
                assertThat(member.rows("MATCH (n) RETURN count(n) AS c", "films")).hasSize(1)
            }
        }
        val writer = Writer()
        eventually { assertThat(writer.acknowledged.values).allMatch { it.size >= 20 } }

        val joining = member("core4")
        joining.start()
        started += joining
        assertThat(joining.logs).contains("CALL dozendb.cluster.addMember('core4:7700')")
        // On a member that may not lead the system database: the change reaches the leader.
        member("core1").rows("CALL dozendb.cluster.addMember('core4:7700')", "system")
        eventually {
            for (member in started) {
                assertThat(membersSeenBy(member))
                    .describedAs("members seen by ${member.alias}")
                    .hasSize(4)
                    .containsEntry("core4:7700", listOf("system", "films", "neo4j"))
            }
        }

        val removed = member("core2")
        val removedId =
            removed
                .rows("CALL dozendb.cluster.members() YIELD memberId, self", "system")
                .single {
                    it["self"] == true
                }["memberId"]
        administer("CALL dozendb.cluster.removeMember('$removedId')")
        val remaining = started - removed
        eventually {
            assertThat(removed.debugLog()).contains("This member was removed from the cluster")
            for (member in remaining) {
                assertThat(membersSeenBy(member))
                    .describedAs("members seen by ${member.alias}")
                    .containsOnlyKeys("core1:7700", "core3:7700", "core4:7700")
                    .allSatisfy { _, databases ->
                        assertThat(databases).containsExactly("system", "films", "neo4j")
                    }
            }
        }
        val acknowledgedAfterChanges = writer.acknowledged.mapValues { it.value.size }
        eventually {
            assertThat(writer.acknowledged).allSatisfy { database, acks ->
                assertThat(acks.size)
                    .isGreaterThan(acknowledgedAfterChanges.getValue(database) + 20)
            }
        }
        writer.stop()

        for (database in DATABASES) {
            eventually {
                val expected = contents(remaining.first(), database)
                for (member in remaining.drop(1)) {
                    assertThat(contents(member, database))
                        .describedAs("$database on ${member.alias}")
                        .isEqualTo(expected)
                }
            }
            for (member in remaining) {
                val stored =
                    member.rows("MATCH (a:Ack) RETURN a.n AS n", database).map { it["n"] as Long }
                assertThat(stored)
                    .describedAs("$database on ${member.alias}")
                    .containsAll(writer.acknowledged.getValue(database))
                    .doesNotHaveDuplicates()
            }
        }
        val routing =
            member("core1")
                .rows(
                    "CALL dbms.routing.getRoutingTable({}, 'neo4j') YIELD servers",
                    "system",
                )
        val addresses =
            (routing.single()["servers"] as List<*>).flatMap {
                (it as Map<*, *>)["addresses"] as List<*>
            }
        assertThat(addresses)
            .contains("127.0.0.1:${boltPorts.getValue("core4")}")
            .doesNotContain("127.0.0.1:${boltPorts.getValue("core2")}")
    }

    private companion object {
        val DATABASES = listOf("neo4j", "films")
        val AWAIT: Duration = Duration.ofMinutes(3)
    }
}
