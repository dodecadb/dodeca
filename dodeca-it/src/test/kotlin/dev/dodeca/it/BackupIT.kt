package dev.dodeca.it

import dev.dodeca.consensus.testing.TestAuthority
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

/**
 * Backups taken over the network from a Dodeca server, with TLS and client certificates as in
 * production, restored by Dodeca and loaded by the official Community image.
 */
class BackupIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val authority = TestAuthority("Dodeca IT backup CA")
    private val containers = mutableListOf<GenericContainer<*>>()
    private val volumes = mutableListOf<String>()

    private fun volume(name: String): String = "dodeca-it-$run-$name".also { volumes += it }

    private fun server(alias: String = "s1", vararg env: Pair<String, String>): DodecaContainer =
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .backupServer(authority)
            .apply { env.forEach { (key, value) -> withEnv(key, value) } }
            .also {
                containers += it
                it.start()
            }

    private fun admin(
        alias: String,
        volumes: Map<String, String> = emptyMap(),
        image: DockerImageName = DodecaContainer.dodecaImage,
    ): AdminContainer =
        AdminContainer(image, alias, network, volumes).also {
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

    private fun count(server: DodecaContainer, label: String): Long =
        server.rows("MATCH (n:$label) RETURN count(n) AS c").single()["c"] as Long

    @Test
    fun `backups over TLS restore, and the official image loads a full one`() {
        val source = server()
        source.rows("UNWIND range(1, 2000) AS i CREATE (:Item {i: i})-[:NEXT]->(:Item {i: -i})")
        source.rows("CREATE INDEX item_i FOR (n:Item) ON (n.i)")
        val backups = volume("backups")
        val client =
            admin("client", mapOf(backups to "/backups"))
                .backupClient(authority.issue(names = listOf("client")))
        val before = count(source, "Later")
        val writing = AtomicBoolean(true)
        val writer = thread {
            while (writing.get()) source.rows("CREATE (:Later)")
        }
        val full =
            try {
                client.adminOk(
                    "database",
                    "backup",
                    "--from=s1:${DodecaContainer.BACKUP_PORT}",
                    "--to-path=/backups",
                    "--type=FULL",
                    "neo4j",
                )
            } finally {
                writing.set(false)
                writer.join()
            }
        assertThat(full).contains("neo4j")
        val written = count(source, "Later")
        client.adminOk(
            "database",
            "backup",
            "--from=s1:${DodecaContainer.BACKUP_PORT}",
            "--to-path=/backups",
            "neo4j",
        )

        val inspected = client.adminOk("backup", "inspect", "/backups", "--show-metadata")
        assertThat(inspected.lines().filter { ".backup" in it }).hasSize(2)
        client.adminOk("database", "check", "--from-path=/backups", "neo4j")

        // The chain restored by Dodeca holds every write.
        val restored = volume("restored")
        admin("restorer", mapOf(backups to "/backups", restored to "/data")).apply {
            adminOk("database", "restore", "--from-path=/backups")
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
        assertThat(count(target, "Item")).isEqualTo(4000)
        assertThat(count(target, "Later")).isEqualTo(written)

        // The full backup alone, loaded by Neo4j Community, holds what preceded it at least.
        val fullFile =
            client.shell("ls /backups | sort | head -1").trim().also {
                assertThat(it).startsWith("neo4j-").endsWith(".backup")
            }
        client.shell("mkdir -p /backups/full && cp /backups/$fullFile /backups/full/")
        val loaded = volume("loaded")
        admin(
                "loader",
                mapOf(backups to "/backups", loaded to "/data"),
                DodecaContainer.communityImage,
            )
            .apply {
                adminOk("database", "load", "--from-path=/backups/full", "neo4j")
                shell("chown -R 7474:7474 /data")
            }
        val community =
            DodecaContainer(DodecaContainer.communityImage, "c1")
                .inNetwork(network)
                .withDataVolume(loaded)
                .also {
                    containers += it
                    it.start()
                }
        assertThat(count(community, "Item")).isEqualTo(4000)
        assertThat(count(community, "Later")).isBetween(before, written)
        assertThat(
                community.rows("MATCH (:Item)-[r:NEXT]->(:Item) RETURN count(r) AS c").single()["c"]
            )
            .isEqualTo(2000L)
    }

    @Test
    fun `only clients with a trusted certificate back up, from the server they name`() {
        val source = server()
        source.rows("CREATE (:Item)")
        val backups = volume("backups")
        val stranger =
            admin("stranger", mapOf(backups to "/backups"))
                .backupClient(TestAuthority("Another CA").issue(names = listOf("stranger")))
        val plaintext = admin("plaintext", mapOf(backups to "/backups"))
        val trusted =
            admin("trusted", mapOf(backups to "/backups"))
                .backupClient(authority.issue(names = listOf("trusted")))
        val ip = NetworkFaults.ipOf(source)

        for ((client, from) in listOf(stranger to "s1", plaintext to "s1", trusted to ip)) {
            val result =
                client.admin(
                    "database",
                    "backup",
                    "--from=$from:${DodecaContainer.BACKUP_PORT}",
                    "--to-path=/backups",
                    "neo4j",
                )
            assertThat(result.exitCode).describedAs("${client.alias} from $from").isEqualTo(1)
            assertThat(result.stdout + result.stderr)
                .describedAs("${client.alias} from $from")
                .contains("$from:${DodecaContainer.BACKUP_PORT}")
        }
        assertThat(trusted.shell("ls /backups").trim()).isEmpty()

        trusted.adminOk(
            "database",
            "backup",
            "--from=s1:${DodecaContainer.BACKUP_PORT}",
            "--to-path=/backups",
            "neo4j",
        )
        assertThat(trusted.shell("ls /backups").lines().filter { it.endsWith(".backup") })
            .hasSize(1)
    }

    @Test
    fun `a client that disappears mid-backup leaves the server checkpointing and serving backups`() {
        val source = server("s1", "NEO4J_db_checkpoint_interval_time" to "1s")
        repeat(20) {
            source.rows(
                "UNWIND range(1, 2000) AS i CREATE (:Item {i: i, text: \$text})",
                parameters = mapOf("text" to "x".repeat(500)),
            )
        }
        val backups = volume("backups")
        val client =
            admin("client", mapOf(backups to "/backups"))
                .backupClient(authority.issue(names = listOf("client")))
        // About 20 MB at 2 Mbit/s: over a minute.
        NetworkFaults(source).use { faults ->
            faults.shapeTo(client, "rate", "2mbit")
            val backup = thread {
                client.admin(
                    "database",
                    "backup",
                    "--from=s1:${DodecaContainer.BACKUP_PORT}",
                    "--to-path=/backups",
                    "neo4j",
                )
            }
            // The copy arrives in a temporary directory there.
            await withPollInterval
                Duration.ofMillis(200) atMost
                Duration.ofMinutes(1) untilAsserted
                {
                    assertThat(client.shell("ls -A /backups").trim()).isNotEmpty()
                }
            Thread.sleep(2_000)
            // Every process of the client but its init: the connection closes at once.
            client.execInContainer("sh", "-c", "kill -9 -1")
            backup.join()
        }

        // The server lets go of its copy at once, which held checkpoints off. A checkpoint needs
        // new transactions since the last one.
        val before = scheduledCheckpoints(source)
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofSeconds(20) untilAsserted
            {
                source.rows("CREATE (:After)")
                assertThat(scheduledCheckpoints(source)).isGreaterThanOrEqualTo(before + 2)
            }
        assertThat(source.debugLog())
            .contains("Serving a full backup of neo4j failed: the receiver cancelled the transfer")
        assertThat(source.logs).doesNotContain("Exception")
        admin("again", mapOf(backups to "/backups"))
            .backupClient(authority.issue(names = listOf("again")))
            .adminOk(
                "database",
                "backup",
                "--from=s1:${DodecaContainer.BACKUP_PORT}",
                "--to-path=/backups",
                "--type=FULL",
                "neo4j",
            )
    }

    /** How many scheduled checkpoints [server] has completed, by its debug log. */
    private fun scheduledCheckpoints(server: DodecaContainer): Int =
        server.debugEntries().count {
            it.message.contains("Checkpoint triggered by \"Scheduled checkpoint") &&
                it.message.contains("checkpoint completed")
        }
}
