package org.dozendb.cluster.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.internal.ClusterHarness.Companion.DEFAULT_DATABASE
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/**
 * The default Cypher version of a database in a cluster: every member runs the queries without a
 * prefix in the version the system graph gives the database, as soon as it has the change.
 */
class DefaultLanguageTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3).also {
            harness = it
            it.start()
        }

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    /**
     * The Cypher version that queries without a prefix run in on [database] here: `coll.distinct`
     * is a function of Cypher 25 only.
     */
    private fun ClusterHarness.Member.language(database: String): String =
        database(database).executeTransactionally(PROBE, emptyMap()) {
            if (it.next()["count"] == 1L) CYPHER_25 else CYPHER_5
        }

    private fun ClusterHarness.awaitLanguage(database: String, language: String) =
        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(running().associateWith { it.language(database) }).allSatisfy { _, it ->
                    assertThat(it).isEqualTo(language)
                }
            }

    @Test
    fun `every member runs a database's queries in the version ALTER or CREATE DATABASE gives it`() {
        val cluster = cluster()
        assertThat(cluster.running().map { it.language(DEFAULT_DATABASE) }).containsOnly(CYPHER_5)

        cluster.administer("ALTER DATABASE $DEFAULT_DATABASE SET DEFAULT LANGUAGE CYPHER 25")
        cluster.awaitLanguage(DEFAULT_DATABASE, CYPHER_25)

        cluster.administer("CREATE DATABASE films DEFAULT LANGUAGE CYPHER 25 WAIT")
        cluster.awaitLanguage("films", CYPHER_25)

        // A member that starts again reads it from the system graph, as Community's lookup does,
        // a little after it started.
        val member = cluster.running().last()
        member.stop()
        member.start()
        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(member.language(DEFAULT_DATABASE)).isEqualTo(CYPHER_25)
            }
    }

    private companion object {
        const val SYSTEM = "system"
        const val PROBE =
            "SHOW FUNCTIONS YIELD name WHERE name = 'coll.distinct' RETURN count(*) AS count"
        const val CYPHER_5 = "CYPHER 5"
        const val CYPHER_25 = "CYPHER 25"
    }
}
