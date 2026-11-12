package org.dozendb.it

import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
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
import org.neo4j.driver.Bookmark
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Secondaries, as Neo4j Enterprise runs them: two servers that take only secondaries join three
 * that form a cluster, one of them hosting `system` as a secondary too, and a database with two
 * secondaries runs on them. Routing tables send reads to them; a read with a bookmark on one sees
 * the write before it without waiting for the next pull; a paused one pulls nothing and leaves the
 * routing tables until resumed; one that was down while its upstreams discarded their transaction
 * logs catches up from a copy. Copies change modes in place while clients write and lose no
 * acknowledged write, and the system secondary turns primary once it restarts as one and is added
 * again.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class SecondariesIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val cores = listOf("core1", "core2", "core3")
    private val readers = listOf("read1", "read2")
    private val aliases = cores + readers
    // Fixed host ports: routing tables name them, and read2 starts again in another container.
    private val boltPorts = aliases.associateWith { TestPorts.free() }
    private val volume = "dozendb-it-$run-read2"
    private val members = aliases.associateWith { server(it) }.toMutableMap()
    private lateinit var ids: Map<String, String>
    private val writers = CopyOnWriteArrayList<Writer>()
    private lateinit var driver: Driver
    private val blob = "x".repeat(100_000)

    /**
     * The server [alias]. core1 to core3 form the cluster; read1 and read2 join it and host
     * databases only as secondaries. read1 pulls only for reads that wait, as its interval is an
     * hour. read2 hosts `system` in [systemMode], keeping its data in a volume, pulls from
     * primaries only, and waits 5 s at most for a bookmark.
     */
    private fun server(alias: String, systemMode: String = "SECONDARY"): DozenDbContainer {
        val server =
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(cores.map { "$it:7700" })
                .boltOnHostPort(boltPorts.getValue(alias))
                // Transaction logs of 128 KiB, of which each checkpoint keeps the last only, so
                // that upstreams discard what a stopped secondary needs within seconds.
                .withEnv("NEO4J_db_checkpoint_interval_time", "2s")
                .withEnv("NEO4J_db_tx__log_rotation_size", "128k")
                .withEnv("NEO4J_db_tx__log_rotation_retention__policy", "false")
        when (alias) {
            "read1" ->
                server
                    .withEnv(MODE_CONSTRAINT, "SECONDARY")
                    .withEnv("NEO4J_db_cluster_catchup_pull__interval", "1h")
            "read2" ->
                server
                    .withEnv(MODE_CONSTRAINT, "SECONDARY")
                    .withEnv("NEO4J_server_cluster_system__database__mode", systemMode)
                    .withEnv(
                        "NEO4J_server_cluster_catchup_upstream__strategy",
                        "connect-to-random-primary-server",
                    )
                    .withEnv("NEO4J_db_transaction_bookmark__ready__timeout", "5s")
                    .withDataVolume(volume)
        }
        return server
    }

    @BeforeAll
    fun start() {
        Startables.deepStart(cores.map(members::getValue)).join()
        driver =
            GraphDatabase.driver(
                "neo4j://${address("core1")}",
                AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
                Config.builder()
                    .withResolver {
                        cores.map { ServerAddress.of("127.0.0.1", boltPorts.getValue(it)) }.toSet()
                    }
                    .build(),
            )
        Startables.deepStart(readers.map(members::getValue)).join()
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
            runCatching { it.saveLogs(logs) }
            it.stop()
        }
        DozenDbContainer.removeVolume(volume)
        network.close()
    }

    private val logs
        get() = DozenDbContainer.logsDirectory.resolve(javaClass.simpleName)

    private fun member(alias: String): DozenDbContainer = members.getValue(alias)

    /** Where drivers reach [alias], as routing tables name it. */
    private fun address(alias: String): String = "127.0.0.1:${boltPorts.getValue(alias)}"

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

    /** Runs [cypher] on films through the routing driver; returns its transaction's bookmarks. */
    private fun write(cypher: String, parameters: Map<String, Any?> = emptyMap()): Set<Bookmark> =
        driver.session(SessionConfig.forDatabase("films")).use { session ->
            session.executeWrite { it.run(cypher, parameters).consume() }
            session.lastBookmarks()
        }

    /**
     * Reads films with [cypher] on [alias] over a direct connection, once it has the transactions
     * that [bookmarks] name, and returns the rows or the error.
     */
    private fun readOn(
        alias: String,
        cypher: String,
        bookmarks: Set<Bookmark> = emptySet(),
    ): Outcome =
        GraphDatabase.driver(
                "bolt://${address(alias)}",
                AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
            )
            .use { direct ->
                val config =
                    SessionConfig.builder()
                        .withDatabase("films")
                        .withBookmarks(bookmarks)
                        .withDefaultAccessMode(AccessMode.READ)
                        .build()
                try {
                    direct.session(config).use { session ->
                        Outcome.Rows(session.run(cypher).list { it.asMap() })
                    }
                } catch (e: Neo4jException) {
                    Outcome.Failed(e.code(), e.message.orEmpty())
                }
            }

    private fun count(c: Long): Outcome = Outcome.Rows(listOf(mapOf("c" to c)))

    /** The role and status of each copy of [database] that SHOW DATABASES lists, by server. */
    private fun copies(database: String): Map<String, String> =
        driver.session(SessionConfig.forDatabase("system")).use { session ->
            session
                .run("SHOW DATABASE $database YIELD serverID, role, currentStatus")
                .list { it.asMap() }
                .associate { row ->
                    ids.entries.single { it.value == row["serverID"] }.key to
                        "${row["role"]} ${row["currentStatus"]}"
                }
        }

    /** The status message of the copy of films on [alias], as SHOW DATABASES shows it. */
    private fun statusMessage(alias: String): String =
        driver.session(SessionConfig.forDatabase("system")).use { session ->
            session
                .run("SHOW DATABASE films YIELD serverID, statusMessage")
                .list { it.asMap() }
                .single { it["serverID"] == ids.getValue(alias) }["statusMessage"]
                as String
        }

    /** The servers of the cluster, as [alias] sees them: their roles by id. */
    private fun servers(alias: String): Map<String, String> =
        member(alias)
            .rows("CALL dozendb.cluster.members() YIELD memberId, role", "system")
            .associate { it["memberId"] as String to it["role"] as String }

    @Suppress("UNCHECKED_CAST")
    private fun routingTable(alias: String, database: String): Map<String, List<String>> {
        val row =
            member(alias)
                .rows("CALL dbms.routing.getRoutingTable({}, '$database')", "system")
                .single()
        return (row["servers"] as List<Map<String, Any?>>).associate {
            it["role"] as String to (it["addresses"] as List<String>).sorted()
        }
    }

    /** The versions of the transaction log files of films on [alias]. */
    private fun logVersions(alias: String): List<Long> =
        member(alias)
            .execInContainer("ls", "/data/transactions/films")
            .stdout
            .lines()
            .mapNotNull { TRANSACTION_LOG.matchEntire(it.trim())?.groupValues?.get(1)?.toLong() }
            .also { assertThat(it).describedAs("transaction logs of films on $alias").isNotEmpty }

    private fun occurrences(text: String, of: String): Int = text.split(of).size - 1

    @Test
    @Order(1)
    fun `servers that take only secondaries join, one hosting system as a secondary, and a database's secondaries run on them`() {
        administer("CALL dozendb.cluster.addMember('read1:7700')")
        administer("CALL dozendb.cluster.addMember('read2:7700')")
        eventually { ids = aliases.associateWith { member(it).memberId() } }
        eventually {
            val servers = servers("core1")
            assertThat(servers).containsOnlyKeys(ids.values)
            for ((alias, id) in ids) {
                assertThat(servers.getValue(id))
                    .describedAs(alias)
                    .isIn(
                        if (alias == "read2") listOf("SECONDARY") else listOf("LEADER", "FOLLOWER")
                    )
            }
        }

        administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES 2 SECONDARIES WAIT")

        eventually {
            assertThat(copies("films"))
                .isEqualTo(
                    cores.associateWith { "primary online" } +
                        readers.associateWith { "secondary online" }
                )
            assertThat(copies("system"))
                .isEqualTo(
                    aliases.associateWith { "primary online" } + ("read2" to "secondary online")
                )
        }
        val counts =
            driver.session(SessionConfig.forDatabase("system")).use { session ->
                session
                    .run(
                        "SHOW DATABASE films YIELD requestedSecondariesCount, " +
                            "currentSecondariesCount RETURN DISTINCT *"
                    )
                    .list { it.asMap() }
            }
        assertThat(counts)
            .containsExactly(
                mapOf("requestedSecondariesCount" to 2L, "currentSecondariesCount" to 2L)
            )
        assertThat(member("read2").run("CREATE (:Film {title: 'Brazil'})", "films"))
            .describedAs("a write on a secondary")
            .isInstanceOf(Outcome.Failed::class.java)
    }

    @Test
    @Order(2)
    fun `routing tables send reads to the secondaries, those of system too`() {
        eventually {
            for (alias in aliases) {
                val films = routingTable(alias, "films")
                assertThat(films.getValue("READ"))
                    .describedAs("films readers from $alias")
                    .contains(address("read1"), address("read2"))
                assertThat(films.getValue("WRITE"))
                    .describedAs("films writers from $alias")
                    .hasSize(1)
                    .isSubsetOf(cores.map(::address))
                assertThat(routingTable(alias, "system").getValue("READ"))
                    .describedAs("system readers from $alias")
                    .contains(address("read2"))
            }
        }
    }

    @Test
    @Order(3)
    fun `a read with a bookmark on a secondary sees the write before it, without waiting for the next pull`() {
        val bookmarks = write("CREATE (:Film {title: 'Heat'})")
        val query = "MATCH (f:Film {title: 'Heat'}) RETURN count(f) AS c"

        // read1 pulls next in an hour; it pulls at once for a read that waits.
        assertThat(readOn("read1", query)).describedAs("without a bookmark").isEqualTo(count(0))
        assertThat(readOn("read1", query, bookmarks)).isEqualTo(count(1))
    }

    @Test
    @Order(4)
    fun `a paused secondary pulls nothing and leaves the routing tables until it is resumed`() {
        val read2 = member("read2")
        val paused = "Secondary replication is paused"

        assertThat(
                read2.rows("CALL dbms.cluster.secondaryReplicationDisable('films', true)", "system")
            )
            .containsExactly(mapOf("state" to "paused"))

        eventually {
            assertThat(statusMessage("read2")).isEqualTo(paused)
            for (alias in aliases) {
                assertThat(routingTable(alias, "films").getValue("READ"))
                    .describedAs("films readers from $alias")
                    .doesNotContain(address("read2"))
            }
        }
        val bookmarks = write("CREATE (:Film {title: 'Ronin'})")
        val query = "MATCH (f:Film {title: 'Ronin'}) RETURN count(f) AS c"
        val waited = readOn("read2", query, bookmarks)
        assertThat(waited).isInstanceOf(Outcome.Failed::class.java)
        assertThat((waited as Outcome.Failed).code)
            .isEqualTo("Neo.TransientError.Transaction.BookmarkTimeout")

        // Its deprecated name, which Cypher 25, the image's default, no longer has.
        assertThat(
                catchThrowable {
                    read2.rows("CALL dbms.cluster.readReplicaToggle('films', false)", "system")
                }
            )
            .hasMessageContaining("dbms.cluster.readReplicaToggle")
        assertThat(
                read2.rows(
                    "CYPHER 5 CALL dbms.cluster.readReplicaToggle('films', false)",
                    "system",
                )
            )
            .containsExactly(mapOf("state" to "running"))

        assertThat(readOn("read2", query, bookmarks)).isEqualTo(count(1))
        eventually {
            assertThat(statusMessage("read2")).isNotEqualTo(paused)
            for (alias in aliases) {
                assertThat(routingTable(alias, "films").getValue("READ"))
                    .describedAs("films readers from $alias")
                    .contains(address("read2"))
            }
        }
    }

    @Test
    @Order(5)
    fun `copies change modes in place while clients write, and no acknowledged write is lost`() {
        val writer = Writer()
        eventually { assertThat(writer.acknowledged).hasSizeGreaterThanOrEqualTo(20) }
        // Nothing else could host core3's primaries.
        assertThatThrownBy {
                administer(
                    "ALTER SERVER '${ids.getValue("core3")}' SET OPTIONS {modeConstraint: 'SECONDARY'}"
                )
            }
            .hasMessageContaining("no other server can host")
            .hasMessageContaining("in place of ${ids.getValue("core3")}")
        // read1, which pulls only for reads that wait, has what its store needs to join the group.
        val marked = write("CREATE (:Marker {at: 'changes'})")
        assertThat(readOn("read1", "MATCH (m:Marker {at: 'changes'}) RETURN count(m) AS c", marked))
            .isEqualTo(count(1))

        administer("ALTER SERVER '${ids.getValue("read1")}' SET OPTIONS {modeConstraint: 'NONE'}")
        administer(
            "ALTER SERVER '${ids.getValue("core3")}' SET OPTIONS {modeConstraint: 'SECONDARY'}"
        )

        eventually {
            assertThat(copies("films"))
                .isEqualTo(
                    mapOf(
                        "core1" to "primary online",
                        "core2" to "primary online",
                        "read1" to "primary online",
                        "core3" to "secondary online",
                        "read2" to "secondary online",
                    )
                )
        }
        val changed = writer.acknowledged.size
        eventually { assertThat(writer.acknowledged.size).isGreaterThan(changed + 20) }
        writer.stop()
        for (alias in aliases) {
            eventually {
                val stored =
                    member(alias).rows("MATCH (a:Ack) RETURN a.n AS n", "films").map {
                        it["n"] as Long
                    }
                assertThat(stored)
                    .describedAs("films on $alias")
                    .containsAll(writer.acknowledged)
                    .doesNotHaveDuplicates()
            }
        }
        assertThat(member("core3").debugLog()).contains("turned secondary from its own store")
        assertThat(member("read1").debugLog()).contains("turned primary from its own store")
    }

    @Test
    @Order(6)
    fun `a secondary that was down while its upstreams discarded their logs catches up from a copy`() {
        val read2 = member("read2")
        val primaries = copies("films").filterValues { it == "primary online" }.keys
        val before = write("CREATE (:Marker {at: 'before'})")
        // Each has it, and so all that read2 has.
        for (alias in primaries + "read2") {
            val markers = "MATCH (m:Marker {at: 'before'}) RETURN count(m) AS c"
            assertThat(readOn(alias, markers, before)).describedAs(alias).isEqualTo(count(1))
        }
        val replaced = occurrences(read2.debugLog(), CAUGHT_UP_BY_COPY)
        read2.shutDown()
        val kept = primaries.associateWith { logVersions(it).max() }

        // A log rotates every other blob, and a checkpoint every 2 s discards all but the last.
        var blobs = 0L
        await.ignoreExceptions() withPollInterval
            Duration.ofSeconds(1) atMost
            AWAIT untilAsserted
            {
                // MERGE, as the driver retries a write whose outcome it does not know.
                write("MERGE (b:Blob {i: $blobs}) SET b.data = \$data", mapOf("data" to blob))
                blobs++
                for (alias in primaries) {
                    assertThat(logVersions(alias).min())
                        .describedAs("oldest transaction log on $alias")
                        .isGreaterThan(kept.getValue(alias))
                }
            }
        val after = write("CREATE (:Marker {at: 'after'})")

        read2.restart()

        eventually {
            assertThat(occurrences(read2.debugLog(), CAUGHT_UP_BY_COPY)).isEqualTo(replaced + 1)
        }
        eventually {
            assertThat(readOn("read2", "MATCH (b:Blob) RETURN count(b) AS c", after))
                .isEqualTo(count(blobs))
            assertThat(readOn("read2", "MATCH (m:Marker {at: 'after'}) RETURN count(m) AS c"))
                .isEqualTo(count(1))
        }
    }

    @Test
    @Order(7)
    fun `a system secondary restarted as a primary turns primary once it is added again`() {
        val secondary = member("read2")
        secondary.shutDown()
        secondary.saveLogs(logs.resolve("as-system-secondary"))
        secondary.stop()
        val primary = server("read2", systemMode = "PRIMARY")
        members["read2"] = primary

        primary.start()

        // Until it is added again, it hosts system as before, and says so.
        eventually { assertThat(primary.logs).contains("but hosts it as a secondary") }
        assertThat(servers("core1").getValue(ids.getValue("read2"))).isEqualTo("SECONDARY")

        // The leader may hold failed lookups of read2's name from while its container was
        // replaced, which it tries again.
        administer("CALL dozendb.cluster.addMember('read2:7700')")

        eventually {
            assertThat(copies("system")).isEqualTo(aliases.associateWith { "primary online" })
            assertThat(servers("core1")).containsOnlyKeys(ids.values).allSatisfy { _, role ->
                assertThat(role).isIn("LEADER", "FOLLOWER")
            }
            assertThat(primary.rows("CALL dozendb.cluster.status()", "system")).anySatisfy {
                assertThat(it["database"]).isEqualTo("system")
            }
        }
        // Its databases stay as they were.
        assertThat(copies("films").getValue("read2")).isEqualTo("secondary online")
        // Every server, read2 among them, applies what the system leader writes.
        administer("CREATE DATABASE books WAIT")
        assertThat(
                primary.rows(
                    "SHOW DATABASES YIELD name WHERE name = 'books' RETURN DISTINCT name",
                    "system",
                )
            )
            .hasSize(1)
    }

    /** Writes `Ack` nodes to films through the routing driver, one at a time, until stopped. */
    private inner class Writer {
        val acknowledged = CopyOnWriteArrayList<Long>()
        private val running = AtomicBoolean(true)
        private val worker =
            thread(name = "secondaries-writer") {
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
        const val MODE_CONSTRAINT = "NEO4J_initial_server_mode__constraint"
        val TRANSACTION_LOG = Regex("""neostore\.transaction\.db\.(\d+)""")
        const val CAUGHT_UP_BY_COPY = "Secondary films runs again, from a copy"
    }
}
