package org.dozendb.cluster.internal.storecopy

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.core.pagecache.PageCacheWarmup
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings

/**
 * A member that copies a database from another one copies its page cache profiles too, as Neo4j
 * Enterprise's store copies do, and warms up from them.
 */
class CopiedProfilesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    private fun ClusterHarness.Member.profile(file: String): Path =
        home.resolve("data/databases/neo4j/profiles/$file.cacheprof")

    private fun ClusterHarness.Member.persons(): Long =
        database().executeTransactionally("MATCH (p:Person) RETURN count(p) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    @Test
    fun `a member that copies a database warms up from the profiles of the copy`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.pagecache_warmup_profiling_interval to
                                Duration.ofMillis(100)
                        ),
                )
                .also { harness = it }
        cluster.start()
        cluster
            .awaitLeader()
            .database()
            .executeTransactionally(
                "UNWIND range(1, $PERSONS) AS i CREATE (:Person {id: i, name: 'person ' + i})"
            )
        eventually {
            for (member in cluster.members) {
                assertThat(member.persons()).isEqualTo(PERSONS.toLong())
                assertThat(member.profile("neostore.nodestore.db")).isRegularFile()
            }
        }

        val joining = cluster.newMember()
        joining.start()
        val system = cluster.awaitLeader("system") { cluster.members.take(3) }.database("system")
        system.executeTransactionally(
            "CALL dozendb.cluster.addMember('127.0.0.1:${joining.raftPort}')"
        )
        eventually {
            system.executeTransactionally("ALTER DATABASE neo4j SET TOPOLOGY 4 PRIMARIES")
        }

        eventually { assertThat(joining.persons()).isEqualTo(PERSONS.toLong()) }
        assertThat(joining.profile("neostore.nodestore.db")).isRegularFile()
        val warmup =
            joining
                .database()
                .dependencyResolver
                .resolveTypeDependencies(PageCacheWarmup::class.java)
                .single()
        eventually { assertThat(warmup.warmedUp).isTrue() }
        assertThat(warmup.pagesLoaded).describedAs("pages the copied profiles name").isPositive()
    }

    private companion object {
        const val PERSONS = 20_000
    }
}
