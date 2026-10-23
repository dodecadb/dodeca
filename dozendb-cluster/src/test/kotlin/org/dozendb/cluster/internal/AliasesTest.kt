package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.io.fs.FileSystemAbstraction

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
        fileSystem: ((ClusterHarness.Member) -> FileSystemAbstraction)? = null
    ): ClusterHarness =
        ClusterHarness(root, 3, fileSystem, mapOf(BoltConnector.enabled to true)).also {
            harness = it
            it.start()
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
    fun `a lagging follower routes new aliases, and serves them to sessions with bookmarks`() {
        val fileSystems = mutableMapOf<Int, SlowLogFileSystem>()
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

    private companion object {
        const val SYSTEM = "system"
        const val DEFAULT = ClusterHarness.DEFAULT_DATABASE
    }
}
