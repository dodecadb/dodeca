package dev.dodeca.it

import dev.dodeca.consensus.testing.TestPorts
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Members that were down while the others discarded their Raft logs, or lost their database, catch
 * up with a copy of the database.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class CatchUpIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val aliases = listOf("core1", "core2", "core3")
    private val volumes = aliases.map { "dodeca-it-$run-$it" }
    // Fixed host ports: Docker may publish a restarted container on other ports.
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
            .withDataVolume(volumes[index])
            // Snapshots follow checkpoints; Neo4j's default interval is 15 minutes.
            .withEnv("NEO4J_db_checkpoint_interval_time", "2s")
    }
    private val blob = "x".repeat(1_000_000)

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
    }

    @AfterAll
    fun stop() {
        members.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        volumes.forEach(DodecaContainer::removeVolume)
        network.close()
    }

    private fun status(member: DodecaContainer, database: String = "neo4j"): Map<String, Any?>? =
        when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == database }
            is Outcome.Failed -> null
        }

    private fun awaitLeader(
        running: List<DodecaContainer>,
        database: String = "neo4j",
    ): DodecaContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                running.singleOrNull { status(it, database)?.get("role") == "LEADER" }
            }

    private fun count(member: DodecaContainer, label: String): Long =
        member.rows("MATCH (n:$label) RETURN count(n) AS c").single()["c"] as Long

    private fun awaitSameData(member: DodecaContainer, leader: DodecaContainer) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                for (label in listOf("Blob", "Marker")) {
                    val expected = count(leader, label)
                    assertThat(member.run("MATCH (n:$label) RETURN count(n) AS c"))
                        .describedAs("${member.alias} $label")
                        .isEqualTo(Outcome.Rows(listOf(mapOf("c" to expected))))
                }
            }
    }

    @Test
    @Order(1)
    fun `a member that was down while the logs were discarded catches up from a copy`() {
        val leader = awaitLeader(members)
        val behind = members.first { it != leader }
        val others = members - behind
        leader.rows("CREATE (:Marker {at: 'before'})")
        behind.kill()

        // More than two log segments of large entries, then small ones until a snapshot covers all
        // the large ones. The segment that holds the entries after the member stopped closed
        // before the last large one, so the snapshot covers it whole, and the leader discards it
        // as it takes the snapshot: Ratis discards only segments that a snapshot covers whole.
        repeat(70) {
            leader.rows("CREATE (:Blob {i: $it, data: \$data})", parameters = mapOf("data" to blob))
        }
        val written = status(leader)?.get("commitIndex") as Long
        var small = 0
        val snapshot =
            await withPollInterval
                Duration.ofSeconds(1) atMost
                Duration.ofMinutes(2) untilNotNull
                {
                    leader.rows("CREATE (:Blob {i: ${-1 - small++}, data: 'small'})")
                    (status(leader)?.get("snapshotIndex") as Long?)?.takeIf { it >= written }
                }

        behind.restart()
        awaitSameData(behind, leader)
        assertThat(status(behind)?.get("snapshotIndex") as Long?)
            .describedAs("snapshot installed with the copy")
            .isNotNull
            .isGreaterThanOrEqualTo(snapshot)
        leader.rows("CREATE (:Marker {at: 'after'})")
        awaitSameData(behind, leader)
    }

    @Test
    @Order(2)
    fun `a member whose database was deleted copies it at startup`() {
        val leader = awaitLeader(members)
        val wiped = members.first { it != leader }
        awaitLeader(members, "system")
            .rows("CREATE USER local SET PASSWORD 'local-secret' CHANGE NOT REQUIRED", "system")
        wiped.shutDown()
        leader.rows("CREATE (:Marker {at: 'while wiped'})")

        wiped.wipeDatabase("neo4j")
        wiped.restart()

        awaitSameData(wiped, leader)
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofSeconds(30) untilAsserted
            {
                val users = wiped.rows("SHOW USERS YIELD user", "system").map { it["user"] }
                assertThat(users).describedAs("the replicated user").contains("local")
            }
        leader.rows("CREATE (:Marker {at: 'after'})")
        awaitSameData(wiped, leader)
    }
}
