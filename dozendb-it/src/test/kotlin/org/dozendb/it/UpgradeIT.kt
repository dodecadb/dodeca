package org.dozendb.it

import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.lifecycle.Startables

/**
 * DozenDB is upgraded one member at a time while a client keeps writing. A member started with a
 * newer format level acts as the newer version; its container replaces the member's on the same
 * data volume, as an upgrade replaces the image.
 */
class UpgradeIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val aliases = listOf("core1", "core2", "core3")
    private val volumes = aliases.map { "dozendb-it-$run-$it" }
    // Fixed host ports, which each member's replacement takes over.
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.indices.map { member(it, newer = false) }.toMutableList()
    private val stopped = mutableListOf<DozenDbContainer>()

    private fun member(index: Int, newer: Boolean): DozenDbContainer =
        DozenDbContainer(DozenDbContainer.dozenDbImage, aliases[index])
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
            .withDataVolume(volumes[index])
            .apply {
                if (newer) withEnv("NEO4J_internal_dbms_dozendb_cluster_newest__format__level", "2")
            }

    /** Where the logs of the containers that newer ones replaced go, one directory each. */
    private val replacedLogs
        get() =
            DozenDbContainer.logsDirectory
                .resolve(javaClass.simpleName)
                .resolve("replaced-${stopped.size + 1}")

    @AfterEach
    fun stop() {
        members.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        stopped.forEach { it.stop() }
        volumes.forEach(DozenDbContainer::removeVolume)
        network.close()
    }

    /** Writes through a routing driver until closed, keeping the writes acknowledged. */
    private class Writer(private val driver: Driver) : AutoCloseable {
        val acknowledged: MutableList<Int> = CopyOnWriteArrayList()
        val failures: MutableList<String> = CopyOnWriteArrayList()
        @Volatile private var writing = true
        private val writes =
            thread(name = "writer") {
                var n = 0
                while (writing) {
                    try {
                        driver.session().use { session ->
                            session.executeWrite { tx ->
                                tx.run("CREATE (:Upgrade {n: ${'$'}n})", mapOf("n" to n)).consume()
                            }
                        }
                        acknowledged += n
                    } catch (e: Exception) {
                        failures += "${Instant.now()} write $n: $e"
                    }
                    n++
                    Thread.sleep(20)
                }
            }

        override fun close() {
            writing = false
            writes.join()
        }
    }

    private fun roles(member: DozenDbContainer): List<Any?> =
        when (val outcome = member.run("CALL dozendb.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.map { it["role"] }
            is Outcome.Failed -> emptyList()
        }

    /** Waits until [member] runs the group of every database, as the operator guide says. */
    private fun awaitServing(member: DozenDbContainer) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(roles(member)).describedAs(member.alias).hasSize(2).allMatch {
                    it == "LEADER" || it == "FOLLOWER"
                }
            }
    }

    private fun systemLeader(): DozenDbContainer = members.single { member ->
        when (val outcome = member.run("CALL dozendb.cluster.status()", "system")) {
            is Outcome.Rows ->
                outcome.rows.any { it["database"] == "system" && it["role"] == "LEADER" }
            is Outcome.Failed -> false
        }
    }

    private fun routingDriver(): Driver =
        GraphDatabase.driver(
            "neo4j://127.0.0.1:${boltPorts.first()}",
            AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
            Config.builder()
                .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                .build(),
        )

    /** Waits until [writer] acknowledged more writes. */
    private fun awaitWrites(writer: Writer) {
        val before = writer.acknowledged.size
        await atMost
            Duration.ofMinutes(1) untilAsserted
            {
                assertThat(writer.acknowledged.size).isGreaterThan(before + 10)
            }
    }

    @Test
    fun `members are upgraded one at a time while writes go on, then the upgrade is finished`() {
        Startables.deepStart(members).join()
        // As the operator guide says: start once every member runs every database, as a new
        // database's creator holds its only copy until the others copied it.
        members.forEach(::awaitServing)

        val writer =
            routingDriver().use { driver ->
                Writer(driver).use { writer ->
                    for (index in members.indices) {
                        members[index].shutDown()
                        members[index].saveLogs(replacedLogs)
                        stopped += members[index]
                        // The cluster goes on serving writes without the member.
                        awaitWrites(writer)
                        members[index] = member(index, newer = true).also { it.start() }
                        awaitServing(members[index])
                    }
                    await withPollInterval
                        Duration.ofSeconds(1) atMost
                        Duration.ofMinutes(1) untilAsserted
                        {
                            val reported =
                                systemLeader().rows("CALL dozendb.cluster.members()", "system")
                            assertThat(reported.map { it["formatLevels"] })
                                .containsExactly("1-2", "1-2", "1-2")
                        }

                    assertThat(
                            systemLeader().rows("CALL dozendb.cluster.finishUpgrade()", "system")
                        )
                        .containsExactly(mapOf("previousFormatLevel" to 1L, "formatLevel" to 2L))
                    awaitWrites(writer)
                    writer
                }
            }

        // Writes in flight when a leader stops may fail with an unknown outcome.
        println("${writer.acknowledged.size} writes acknowledged, ${writer.failures.size} failed")
        writer.failures.forEach(::println)
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                for (member in members) {
                    val stored =
                        member.rows("MATCH (u:Upgrade) RETURN u.n AS n").map {
                            (it["n"] as Long).toInt()
                        }
                    assertThat(stored).describedAs(member.alias).containsAll(writer.acknowledged)
                }
            }
        // Afterwards no member returns to the previous version.
        members[2].shutDown()
        members[2].saveLogs(replacedLogs)
        stopped += members[2]
        val refusal = "The cluster uses format level 2, as this member last saw it"
        members[2] =
            member(2, newer = false)
                .waitingFor(
                    Wait.forLogMessage(".*$refusal.*", 1).withStartupTimeout(Duration.ofMinutes(3))
                )
                .also { it.start() }
        assertThat(members[2].logs).doesNotContain("Started.")
    }
}
