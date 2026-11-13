package dev.dodeca.it

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Volume
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.lifecycle.Startables

/**
 * A new cluster forms from a dump of another cluster's database, loaded into one server with
 * `neo4j-admin database load`, and takes writes at once: the dump's transactions carry the Raft
 * indexes of the other cluster's group, which the new group must not count as its own.
 */
class ClusterFromDumpIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val firstAliases = listOf("a1", "a2", "a3")
    private val secondAliases = listOf("b1", "b2", "b3")
    private val volumes =
        (firstAliases + secondAliases + "dumps").associateWith { "dodeca-it-$run-$it" }
    private val containers = mutableListOf<DodecaContainer>()

    private fun member(alias: String, cluster: List<String>): DodecaContainer =
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(cluster.map { "$it:7700" })
            .withDataVolume(volumes.getValue(alias))
            .also { containers += it }

    @AfterEach
    fun stop() {
        containers.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        volumes.values.forEach(DodecaContainer::removeVolume)
        network.close()
    }

    /** Runs [script] in a container of the Dodeca image with the volumes [binds] mounted. */
    private fun admin(script: String, vararg binds: Pair<String, String>) {
        GenericContainer(DodecaContainer.dodecaImage)
            .withCreateContainerCmdModifier {
                it.withEntrypoint("sh", "-c", script)
                it.hostConfig!!.withBinds(
                    binds.map { (volume, path) -> Bind(volumes.getValue(volume), Volume(path)) }
                )
            }
            .withStartupCheckStrategy(
                OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(3))
            )
            .use { it.start() }
    }

    private fun leader(members: List<DodecaContainer>): DodecaContainer {
        var leader: DodecaContainer? = null
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                leader = members.singleOrNull { member ->
                    when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
                        is Outcome.Rows ->
                            outcome.rows.any { it["database"] == "neo4j" && it["role"] == "LEADER" }
                        is Outcome.Failed -> false
                    }
                }
                assertThat(leader).describedAs("leader of neo4j").isNotNull
            }
        return leader!!
    }

    private fun count(member: DodecaContainer, label: String): Long =
        member.rows("MATCH (n:$label) RETURN count(n) AS c").single()["c"] as Long

    @Test
    fun `a cluster formed from another cluster's dump takes writes at once`() {
        val first = firstAliases.map { member(it, firstAliases) }
        Startables.deepStart(first).join()
        val firstLeader = leader(first)
        repeat(BEFORE) { n ->
            firstLeader.rows("CREATE (:Before {n: ${'$'}n})", parameters = mapOf("n" to n))
        }
        first.forEach { it.shutDown() }
        admin(
            "neo4j-admin database dump neo4j --to-path=/dumps",
            "a1" to "/data",
            "dumps" to "/dumps",
        )
        admin(
            "neo4j-admin database load neo4j --from-path=/dumps && chown -R 7474:7474 /data",
            "b1" to "/data",
            "dumps" to "/dumps",
        )

        val second = secondAliases.map { member(it, secondAliases) }
        Startables.deepStart(second).join()
        val secondLeader = leader(second)
        val failures =
            (0 until AFTER).mapNotNull { n ->
                secondLeader.run("CREATE (:After {n: ${'$'}n})", parameters = mapOf("n" to n))
                    as? Outcome.Failed
            }

        assertThat(failures).describedAs("failed writes").isEmpty()
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                second.forEach {
                    assertThat(count(it, "Before")).describedAs(it.alias).isEqualTo(BEFORE.toLong())
                    assertThat(count(it, "After")).describedAs(it.alias).isEqualTo(AFTER.toLong())
                }
            }
    }

    private companion object {
        const val BEFORE = 200
        const val AFTER = 300
    }
}
