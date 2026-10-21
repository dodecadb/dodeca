package org.dozendb.it

import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestAuthority
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * A lost cluster recreated from backups of every database: restored on one server, which forms a
 * new cluster with empty members, as `OPERATOR_GUIDE.md` describes.
 */
class DisasterRecoveryIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val authority = TestAuthority("DozenDB IT backup CA")
    private val containers = mutableListOf<GenericContainer<*>>()
    private val volumes = mutableListOf<String>()

    private fun volume(name: String): String = "dozendb-it-$run-$name".also { volumes += it }

    @AfterEach
    fun stop() {
        containers.forEach {
            if (it is DozenDbContainer) {
                it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            }
            it.stop()
        }
        volumes.forEach(DozenDbContainer::removeVolume)
        network.close()
    }

    private fun cluster(
        aliases: List<String>,
        founderData: String? = null,
    ): List<DozenDbContainer> {
        val members = aliases.map { alias ->
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .backupServer(authority)
                .apply {
                    if (alias == aliases.first() && founderData != null) withDataVolume(founderData)
                }
                .also { containers += it }
        }
        Startables.deepStart(members).join()
        return members
    }

    /** Runs [cypher] on whichever of [members] leads [database], once one does. */
    private fun write(members: List<DozenDbContainer>, database: String, cypher: String) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(members.map { it.run(cypher, database) }.count { it is Outcome.Rows })
                    .describedAs("$database written by its leader")
                    .isEqualTo(1)
            }
    }

    private val data =
        mapOf(
            "neo4j" to ("Thing" to 100L),
            "films" to ("Film" to 200L),
            "books" to ("Book" to 300L),
        )

    @Test
    fun `a cluster restored from backups of every database on one server forms anew`() {
        val lost = cluster(listOf("a1", "a2", "a3"))
        for (database in listOf("films", "books")) {
            write(lost, "system", "CREATE DATABASE $database WAIT")
        }
        write(
            lost,
            "system",
            "CREATE USER alice SET PASSWORD 'a long password' CHANGE NOT REQUIRED",
        )
        for ((database, nodes) in data) {
            write(
                lost,
                database,
                "UNWIND range(1, ${nodes.second}) AS i CREATE (:${nodes.first} {i: i})",
            )
        }
        val backups = volume("backups")
        val client =
            AdminContainer(
                    DozenDbContainer.dozenDbImage,
                    "client",
                    network,
                    mapOf(backups to "/backups"),
                )
                .also {
                    containers += it
                    it.start()
                }
                .backupClient(authority.issue(names = listOf("client")))
        client.adminOk(
            "database",
            "backup",
            "--from=a2:${DozenDbContainer.BACKUP_PORT}",
            "--to-path=/backups",
            "--type=FULL",
            "*",
        )
        lost.forEach { it.shutDown() }

        val founderData = volume("founder")
        AdminContainer(
                DozenDbContainer.dozenDbImage,
                "restorer",
                network,
                mapOf(backups to "/backups", founderData to "/data"),
            )
            .also {
                containers += it
                it.start()
            }
            .apply {
                val restored = adminOk("database", "restore", "--from-path=/backups")
                for (database in data.keys + "system") {
                    assertThat(restored).contains("Restored database '$database'")
                }
                shell("chown -R 7474:7474 /data")
            }
        val recovered = cluster(listOf("b1", "b2", "b3"), founderData)

        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                for (member in recovered) {
                    for ((database, nodes) in data) {
                        assertThat(
                                member.run(
                                    "MATCH (n:${nodes.first}) RETURN count(n) AS c",
                                    database,
                                )
                            )
                            .describedAs("$database on ${member.alias}")
                            .isEqualTo(Outcome.Rows(listOf(mapOf("c" to nodes.second))))
                    }
                }
            }
        for (member in recovered) {
            assertThat(member.rows("SHOW USERS YIELD user", "system").map { it["user"] })
                .describedAs(member.alias)
                .contains("alice")
        }
        for ((database, nodes) in data) {
            write(recovered, database, "CREATE (:${nodes.first} {i: 0})")
        }
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                for (member in recovered) {
                    for ((database, nodes) in data) {
                        assertThat(
                                member.run(
                                    "MATCH (n:${nodes.first}) RETURN count(n) AS c",
                                    database,
                                )
                            )
                            .describedAs("$database on ${member.alias}")
                            .isEqualTo(Outcome.Rows(listOf(mapOf("c" to nodes.second + 1))))
                    }
                }
            }
    }
}
