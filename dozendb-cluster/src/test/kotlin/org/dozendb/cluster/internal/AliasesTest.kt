package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.KeyGenerator
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.core.DozenDbSettings
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.string.SecureString

/**
 * Local aliases in a cluster: created on the leader of `system`, they reach the other members
 * through Raft, where Neo4j resolves them like on a single server.
 */
class AliasesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null
    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        harness?.close()
    }

    private fun cluster(
        settings: Map<Setting<*>, Any> = emptyMap(),
        fileSystem: ((ClusterHarness.Member) -> FileSystemAbstraction)? = null,
    ): ClusterHarness =
        ClusterHarness(root, 3, fileSystem, mapOf(BoltConnector.enabled to true) + settings).also {
            harness = it
            it.start()
        }

    /** The settings of a keystore, which every member shares, for passwords of remote aliases. */
    private fun keystore(): Map<Setting<*>, Any> {
        val path = root.resolve("aliases.p12")
        val key = KeyGenerator.getInstance("AES").apply { init(256) }.generateKey()
        val store = KeyStore.getInstance("PKCS12").apply { load(null, null) }
        store.setEntry(
            "aliases",
            KeyStore.SecretKeyEntry(key),
            KeyStore.PasswordProtection(KEYSTORE_SECRET.toCharArray()),
        )
        Files.newOutputStream(path).use { store.store(it, KEYSTORE_SECRET.toCharArray()) }
        return mapOf(
            DozenDbSettings.keystorePath to path,
            DozenDbSettings.keystorePassword to SecureString(KEYSTORE_SECRET),
            DozenDbSettings.keyName to "aliases",
        )
    }

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    /** A driver that connects to this member only, or with [scheme] `neo4j` routes from it. */
    private fun ClusterHarness.Member.driver(scheme: String = "bolt"): Driver =
        GraphDatabase.driver("$scheme://${boltAddress()}", AuthTokens.none()).also {
            drivers += it
        }

    private fun Driver.run(cypher: String, database: String): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(cypher).list { it.asMap() }
        }

    /** The database that a session in [alias] uses on this member. */
    private fun ClusterHarness.Member.databaseOf(alias: String): String =
        driver().run("CALL db.info() YIELD name", alias).single()["name"] as String

    /** The aliases this member shows, by name, with their databases. */
    private fun ClusterHarness.Member.aliases(): List<Pair<Any?, Any?>> =
        driver().run("SHOW ALIASES FOR DATABASE", SYSTEM).map { it["name"] to it["database"] }

    /** The routing table this member gives for [name], by role. */
    private fun ClusterHarness.Member.routingTable(name: String): Map<Any?, Any?> =
        driver()
            .run(
                "CALL dbms.routing.getRoutingTable({}, '$name') YIELD servers " +
                    "UNWIND servers AS server RETURN server.role AS role, server.addresses AS " +
                    "addresses",
                DEFAULT,
            )
            .associate { it["role"] to it["addresses"] }

    private fun eventually(assertion: () -> Unit) {
        // A member that has yet to learn of an alias fails sessions in it for a moment.
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `aliases reach every member, which route and write by their name`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films WAIT")
        cluster.administer("CREATE ALIAS movies FOR DATABASE films")

        for (member in cluster.members) {
            eventually {
                assertThat(member.databaseOf("movies")).isEqualTo("films")
                // Read at once, once films has a leader: the tables change while it elects one.
                val table = member.routingTable("films")
                assertThat(table).containsKey("WRITE")
                assertThat(member.routingTable("movies")).isEqualTo(table)
            }
        }
        // Drivers that route ask for the table of the name they were given, and write on the
        // leader of its database, from whichever member they start at.
        for (member in cluster.members) {
            member.driver("neo4j").session(SessionConfig.forDatabase("movies")).use { session ->
                session.executeWrite { it.run("CREATE (:Film)").consume() }
            }
        }
        for (member in cluster.members) {
            eventually {
                assertThat(
                        member.database("films").executeTransactionally(
                            "MATCH (f:Film) RETURN count(f) AS films",
                            emptyMap(),
                        ) {
                            it.next()["films"]
                        }
                    )
                    .isEqualTo(3L)
            }
        }

        cluster.administer("ALTER ALIAS movies SET DATABASE TARGET neo4j")

        for (member in cluster.members) {
            eventually { assertThat(member.databaseOf("movies")).isEqualTo(DEFAULT) }
        }

        cluster.administer("ALTER ALIAS movies SET DATABASE TARGET films")
        cluster.awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(
            "DROP DATABASE movies CASCADE ALIASES WAIT",
            emptyMap(),
        ) { result ->
            assertThat(result.stream().map { it["success"] }.toList()).containsOnly(true)
        }

        for (member in cluster.members) {
            eventually { assertThat(member.aliases()).isEmpty() }
            assertThat(member.dbms!!.listDatabases()).doesNotContain("films")
        }
    }

    @Test
    fun `followers refuse alias commands`() {
        val cluster = cluster()
        cluster.administer("CREATE ALIAS movies FOR DATABASE neo4j")
        val follower = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        eventually { assertThat(follower.databaseOf("movies")).isEqualTo(DEFAULT) }

        for (command in
            listOf(
                "CREATE ALIAS classics FOR DATABASE neo4j",
                "CREATE OR REPLACE ALIAS movies FOR DATABASE neo4j",
                "ALTER ALIAS movies SET DATABASE PROPERTIES {rating: 5}",
                "DROP ALIAS movies FOR DATABASE",
            )) {
            assertThatThrownBy { follower.database(SYSTEM).executeTransactionally(command) }
                .describedAs(command)
                .isInstanceOf(QueryExecutionException::class.java)
                .hasMessageContaining("must be executed on the LEADER")
                .matches {
                    (it as QueryExecutionException).statusCode ==
                        Status.Cluster.NotALeader.code().serialize()
                }
        }
        assertThat(follower.aliases()).containsExactly("movies" to DEFAULT)
    }

    @Test
    fun `a member that was down learns the aliases it missed`() {
        val cluster = cluster()
        val down = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        down.stop()

        cluster.administer("CREATE DATABASE films")
        cluster.administer("CREATE ALIAS movies FOR DATABASE neo4j")
        cluster.administer("CREATE ALIAS gone FOR DATABASE neo4j")
        cluster.administer("ALTER ALIAS movies SET DATABASE TARGET films PROPERTIES {rating: 5}")
        cluster.administer("DROP ALIAS gone FOR DATABASE")
        down.start()

        eventually { assertThat(down.databaseOf("movies")).isEqualTo("films") }
        assertThat(
                down
                    .driver()
                    .run("SHOW ALIASES FOR DATABASE YIELD name, database, properties", SYSTEM)
            )
            .containsExactly(
                mapOf(
                    "name" to "movies",
                    "database" to "films",
                    "properties" to mapOf("rating" to 5L),
                )
            )
    }

    @Test
    fun `a lagging follower routes new aliases, and serves them to sessions with bookmarks`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster = cluster { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } }
        val leader = cluster.awaitLeader(SYSTEM)
        val follower = cluster.members.first { it != leader }
        // The follower applies each transaction of system late: it falls behind by a few.
        fileSystems.getValue(follower.index).slow = true
        var roles = 0
        fun createLagging(alias: String) =
            leader.driver().session(SessionConfig.forDatabase(SYSTEM)).use { session ->
                repeat(20) { session.run("CREATE ROLE role${roles++}").consume() }
                session.run("CREATE ALIAS $alias FOR DATABASE neo4j").consume()
                session.lastBookmarks()
            }

        // Neo4j resolves a session's database before it waits for its bookmarks.
        val bookmarks = createLagging("movies")
        val database =
            follower
                .driver()
                .session(
                    SessionConfig.builder().withDatabase("movies").withBookmarks(bookmarks).build()
                )
                .use { it.run("CALL db.info() YIELD name").single()["name"].asString() }
        assertThat(database).isEqualTo(DEFAULT)

        // ROUTE carries bookmarks that Neo4j does not wait for: without any, the member confirms
        // with the leader of system that it knows every name before it says it knows none.
        createLagging("films")
        // Found at once, though the member may not know the Bolt address of every other yet.
        assertThat(follower.routingTable("films")).containsKey("ROUTE")
        eventually {
            assertThat(follower.routingTable("films")).isEqualTo(follower.routingTable(DEFAULT))
        }
    }

    @Test
    fun `members run transactions in remote aliases on the DBMS they name`() {
        val cluster = cluster(settings = keystore())
        // The DBMS the alias names is this cluster, which routes its writes to the leader.
        val first = cluster.members.first()
        cluster.administer(
            "CREATE ALIAS elsewhere FOR DATABASE neo4j AT 'neo4j://${first.boltAddress()}' " +
                "USER anyone PASSWORD 'any-secret' DRIVER {ssl_enforced: false}"
        )

        for (member in cluster.members) {
            eventually {
                member.driver().session(SessionConfig.forDatabase("elsewhere")).use { session ->
                    session.executeWrite {
                        it.run("MERGE (:Remote {by: \$by})", mapOf("by" to member.index)).consume()
                    }
                }
            }
        }
        val leader = cluster.awaitLeader()
        assertThat(
                leader.database().executeTransactionally(
                    "MATCH (r:Remote) RETURN r.by AS by ORDER BY by",
                    emptyMap(),
                ) {
                    it.stream().map { row -> row["by"] }.toList()
                }
            )
            .containsExactly(0L, 1L, 2L)
    }

    private companion object {
        const val KEYSTORE_SECRET = "keystore-secret"
        const val SYSTEM = "system"
        const val DEFAULT = ClusterHarness.DEFAULT_DATABASE
    }
}
