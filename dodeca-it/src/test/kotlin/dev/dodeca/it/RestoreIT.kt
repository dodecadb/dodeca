package dev.dodeca.it

import dev.dodeca.consensus.testing.TestAuthority
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
import org.testcontainers.lifecycle.Startables

/** Databases restored from backups: on a standalone server, and created from them in a cluster. */
class RestoreIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val authority = TestAuthority("Dodeca IT backup CA")
    private val containers = mutableListOf<GenericContainer<*>>()
    private val volumes = mutableListOf<String>()

    private fun volume(name: String): String = "dodeca-it-$run-$name".also { volumes += it }

    private fun admin(alias: String, volumes: Map<String, String>): AdminContainer =
        AdminContainer(DodecaContainer.dodecaImage, alias, network, volumes).also {
            containers += it
            it.start()
        }

    @AfterEach
    fun stop() {
        containers.forEach {
            if (it is DodecaContainer) {
                it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            }
            it.stop()
        }
        volumes.forEach(DodecaContainer::removeVolume)
        network.close()
    }

    private fun count(server: DodecaContainer, label: String, database: String = "neo4j"): Long =
        server.rows("MATCH (n:$label) RETURN count(n) AS c", database).single()["c"] as Long

    @Test
    fun `a standalone server's backups restore a database as of a point in time`() {
        val backups = volume("backups")
        val source =
            DodecaContainer(DodecaContainer.dodecaImage, "s1")
                .inNetwork(network)
                .withVolume(backups, "/backups")
                .also {
                    containers += it
                    it.start()
                }
        source.execInContainer("chown", "neo4j:neo4j", "/backups")
        fun backup() {
            // In the server's container, from its loopback address, as the neo4j user.
            val result =
                source.execInContainer(
                    "su-exec",
                    "neo4j:neo4j",
                    "neo4j-admin",
                    "database",
                    "backup",
                    "--to-path=/backups",
                    "neo4j",
                )
            assertThat(result.exitCode).describedAs(result.stdout + result.stderr).isZero
        }
        source.rows("UNWIND range(1, 100) AS i CREATE (:A {i: i})")
        backup()
        source.rows("UNWIND range(1, 100) AS i CREATE (:B {i: i})")
        Thread.sleep(1_100)
        val point =
            source
                .rows("RETURN toString(datetime.realtime('UTC')) AS now")
                .single()["now"]
                .toString()
                .take(19)
                .replace('T', ' ')
        Thread.sleep(1_100)
        source.rows("UNWIND range(1, 100) AS i CREATE (:C {i: i})")
        backup()

        val restored = volume("restored")
        admin("restorer", mapOf(backups to "/backups", restored to "/data")).apply {
            adminOk("database", "restore", "--from-path=/backups", "--restore-until=$point")
            shell("chown -R 7474:7474 /data")
        }
        val target =
            DodecaContainer(DodecaContainer.dodecaImage, "r1")
                .inNetwork(network)
                .withDataVolume(restored)
                .also {
                    containers += it
                    it.start()
                }
        assertThat(listOf("A", "B", "C").map { count(target, it) }).containsExactly(100, 100, 0)
    }

    private val aliases = listOf("c1", "c2", "c3")

    /** A cluster whose members serve backups over TLS and read seeds from `/backups`. */
    private fun cluster(backups: String): List<DodecaContainer> {
        val members = aliases.map { alias ->
            DodecaContainer(DodecaContainer.dodecaImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .backupServer(authority)
                .withVolume(backups, "/backups")
                .also { containers += it }
        }
        Startables.deepStart(members).join()
        return members
    }

    private fun systemLeader(members: List<DodecaContainer>): DodecaContainer {
        var leader: DodecaContainer? = null
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                leader = members.singleOrNull { member ->
                    when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
                        is Outcome.Rows ->
                            outcome.rows.any {
                                it["database"] == "system" && it["role"] == "LEADER"
                            }
                        is Outcome.Failed -> false
                    }
                }
                assertThat(leader).describedAs("leader of system").isNotNull
            }
        return leader!!
    }

    /** Runs [cypher] on whichever of [members] leads [database], once one does. */
    private fun write(members: List<DodecaContainer>, database: String, cypher: String) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(members.map { it.run(cypher, database) }.count { it is Outcome.Rows })
                    .describedAs("$database written by its leader")
                    .isEqualTo(1)
            }
    }

    /** Waits until [database] holds [count] `Item` nodes on every one of [members]. */
    private fun awaitEverywhere(members: List<DodecaContainer>, database: String, count: Long) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                for (member in members) {
                    assertThat(member.run("MATCH (n:Item) RETURN count(n) AS c", database))
                        .describedAs("${member.alias}: $database")
                        .isEqualTo(Outcome.Rows(listOf(mapOf("c" to count))))
                }
            }
    }

    @Test
    fun `a cluster creates databases from a backup every member can read, or one member restored`() {
        val backups = volume("backups")
        val members = cluster(backups)
        val leader = systemLeader(members)
        write(members, "neo4j", "UNWIND range(1, 500) AS i CREATE (:Item {i: i})")
        val client =
            admin("client", mapOf(backups to "/backups"))
                .backupClient(authority.issue(names = listOf("client")))
        client.adminOk(
            "database",
            "backup",
            "--from=c1:${DodecaContainer.BACKUP_PORT},c2:${DodecaContainer.BACKUP_PORT}",
            "--to-path=/backups",
            "neo4j",
        )
        val file = client.shell("ls /backups | grep '^neo4j-.*\\.backup$'").trim()

        // From a seed URI, which the system leader reads.
        val seeded =
            leader.rows(
                "CREATE DATABASE films OPTIONS {existingData: 'use', seedURI: " +
                    "'file:///backups/$file'} WAIT",
                "system",
            )
        assertThat(seeded.map { it["success"] }).describedAs("$seeded").containsOnly(true)
        awaitEverywhere(members, "films", 500)

        // From the files a restore prepared on one member, as the neo4j user.
        val restorer = members.last()
        val restore =
            restorer.execInContainer(
                "su-exec",
                "neo4j:neo4j",
                "neo4j-admin",
                "database",
                "restore",
                "--from-path=/backups/$file",
                "books",
            )
        assertThat(restore.exitCode).describedAs(restore.stdout + restore.stderr).isZero
        val create =
            checkNotNull(Regex("CREATE DATABASE books OPTIONS \\{[^}]+\\}").find(restore.stdout)) {
                restore.stdout
            }
        val restored = systemLeader(members).rows("${create.value} WAIT", "system")
        assertThat(restored.map { it["success"] }).describedAs("$restored").containsOnly(true)
        awaitEverywhere(members, "books", 500)

        // Both take writes, which reach every member.
        for (database in listOf("films", "books")) {
            write(members, database, "CREATE (:Item {i: 0})")
            awaitEverywhere(members, database, 501)
        }
    }
}
