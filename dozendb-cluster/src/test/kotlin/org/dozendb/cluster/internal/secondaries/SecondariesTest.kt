package org.dozendb.cluster.internal.secondaries

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.cluster.internal.ClusterMetrics
import org.dozendb.core.DozenDbSettings
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * Secondaries copy a database, pull its transactions from the members that host it, and take no
 * writes; they vote in no Raft group. The last of three members may host only secondaries.
 */
class SecondariesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(settings: Map<Setting<*>, Any> = emptyMap()): ClusterHarness =
        ClusterHarness(root, 3, settings = settings).also {
            harness = it
            it.members[2].settings =
                mapOf(
                    DozenDbSettings.initialServerModeConstraint to InstanceModeConstraint.SECONDARY
                )
            it.start()
        }

    private val ClusterHarness.secondary: ClusterHarness.Member
        get() = members[2]

    private val ClusterHarness.primaries: List<ClusterHarness.Member>
        get() = members.take(2)

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    private fun ClusterHarness.write(statement: String) {
        awaitLeader(FILMS) { primaries.filter { it.running } }
            .database(FILMS)
            .executeTransactionally(statement)
    }

    private fun ClusterHarness.Member.registered(name: String) =
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(DatabaseContextProvider::class.java)
            .getDatabaseContext(name)
            .orElse(null)

    private fun ClusterHarness.Member.films(): Long =
        database(FILMS).executeTransactionally("MATCH (f:Film) RETURN count(f) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    private fun ClusterHarness.Member.lastTransaction() =
        database(FILMS)
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransaction

    private fun ClusterHarness.Member.marked(): String =
        home.resolve("data/raft/secondaries").let {
            if (Files.exists(it)) Files.readString(it) else ""
        }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    private fun ClusterHarness.awaitSecondary() = eventually {
        assertThat(secondary.registered(FILMS)?.database()?.isStarted).isTrue()
    }

    @Test
    fun `a secondary copies a database, pulls its writes and takes none`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()

        cluster.write("UNWIND range(1, 100) AS i CREATE (:Film {id: i})")

        eventually { assertThat(cluster.secondary.films()).isEqualTo(100) }
        val leader = cluster.awaitLeader(FILMS) { cluster.primaries }
        assertThat(cluster.secondary.lastTransaction()).isEqualTo(leader.lastTransaction())
        assertThat(cluster.secondary.status(FILMS)).describedAs("it runs no Raft group").isNull()
        assertThat(leader.cluster.groupMembers(leader.status(FILMS)!!.database).map { it.id })
            .containsExactlyInAnyOrderElementsOf(cluster.primaries.map { it.cluster.self.id })
        assertThat(cluster.secondary.marked())
            .contains(
                cluster.secondary
                    .registered(FILMS)!!
                    .database()
                    .namedDatabaseId
                    .databaseId()
                    .uuid()
                    .toString()
            )
        val write = catchThrowable {
            cluster.secondary.database(FILMS).executeTransactionally("CREATE (:Film {id: 0})")
        }
        assertThat(
                generateSequence(write) { it.cause }.filterIsInstance<Status.HasStatus>().toList()
            )
            .anyMatch { it.status() == Status.Cluster.NotALeader }
        assertThat(write).hasMessageContaining("leads database '$FILMS', not this member")
    }

    @Test
    fun `a secondary pulls what it missed while stopped, from where it stopped`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")
        eventually { assertThat(cluster.secondary.films()).isEqualTo(10) }

        cluster.secondary.stop()
        cluster.write("UNWIND range(11, 30) AS i CREATE (:Film {id: i})")
        cluster.secondary.start()

        eventually { assertThat(cluster.secondary.films()).isEqualTo(30) }
        assertThat(cluster.secondary.metrics().copiesReceived.sum())
            .describedAs("pulled, not copied again")
            .isZero()
    }

    @Test
    fun `a secondary whose upstreams no longer hold what it lacks is replaced with a copy`() {
        val cluster =
            cluster(
                mapOf(
                    GraphDatabaseSettings.logical_log_rotation_threshold to ByteUnit.kibiBytes(128),
                    GraphDatabaseSettings.keep_logical_logs to "false",
                )
            )
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        cluster.write("CREATE (:Film {id: 0})")
        eventually { assertThat(cluster.secondary.films()).isEqualTo(1) }

        cluster.secondary.stop()
        repeat(100) {
            cluster.write("CREATE (:Film {id: ${it + 1}, title: '${"x".repeat(4000)}'})")
        }
        for (primary in cluster.primaries) {
            primary
                .database(FILMS)
                .dependencyResolver
                .resolveDependency(CheckPointer::class.java)
                .forceCheckPoint(SimpleTriggerInfo("test"))
        }
        cluster.secondary.start()

        eventually { assertThat(cluster.secondary.films()).isEqualTo(101) }
        assertThat(cluster.secondary.metrics().copiesReceived.sum()).isEqualTo(1)
        cluster.write("CREATE (:Film {id: 102})")
        eventually { assertThat(cluster.secondary.films()).isEqualTo(102) }
    }

    @Test
    fun `a secondary whose store holds another history fails loudly`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.administer("CREATE DATABASE books TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        eventually {
            assertThat(cluster.secondary.registered("books")?.database()?.isStarted).isTrue()
        }
        repeat(20) {
            cluster
                .awaitLeader("books") { cluster.primaries }
                .database("books")
                .executeTransactionally("CREATE (:Book)")
        }
        eventually {
            assertThat(
                    cluster.secondary.database("books").executeTransactionally(
                        "MATCH (b:Book) RETURN count(b) AS c",
                        emptyMap(),
                    ) {
                        it.next()["c"] as Long
                    }
                )
                .isEqualTo(20)
        }
        cluster.secondary.stop()

        // As Neo4j names it, symbolic links resolved.
        val data = cluster.secondary.home.resolve("data").toRealPath()
        for (directory in listOf("databases", "transactions")) {
            FileUtils.deleteDirectory(data.resolve("$directory/$FILMS"))
            FileUtils.copyDirectory(
                data.resolve("$directory/books"),
                data.resolve("$directory/$FILMS"),
            )
        }
        cluster.secondary.start()

        eventually {
            val health = cluster.secondary.registered(FILMS)?.database()?.databaseHealth
            assertThat(health?.hasNoPanic()).isFalse()
            assertThat(health!!.causeOfPanic())
                .hasMessageContaining("holds transactions that its upstream does not have")
        }
    }

    @Test
    fun `a secondary is deleted with its database`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        val id =
            cluster.secondary.registered(FILMS)!!.database().namedDatabaseId.databaseId().uuid()

        cluster.administer("DROP DATABASE $FILMS WAIT")

        eventually {
            assertThat(cluster.secondary.registered(FILMS)).isNull()
            assertThat(cluster.secondary.home.resolve("data/databases/$FILMS")).doesNotExist()
            assertThat(cluster.secondary.marked()).doesNotContain(id.toString())
        }
    }

    private fun ClusterHarness.Member.metrics(): ClusterMetrics.Counters =
        database(SYSTEM).dependencyResolver.resolveDependency(ClusterMetrics::class.java).of(FILMS)

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
