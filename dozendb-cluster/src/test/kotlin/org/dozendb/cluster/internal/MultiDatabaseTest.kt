package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.StoreId

/** Databases created, stopped, started and dropped on the system leader follow on every member. */
class MultiDatabaseTest {
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

    /** The database [name] of this member as the database manager has it, if registered. */
    private fun ClusterHarness.Member.registered(name: String) =
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(DatabaseContextProvider::class.java)
            .getDatabaseContext(name)
            .orElse(null)
            ?.database()

    private fun ClusterHarness.Member.storeId(name: String): StoreId =
        database(name)
            .dependencyResolver
            .resolveDependency(StorageEngine::class.java)
            .retrieveStoreId()

    private fun ClusterHarness.Member.films(): Long =
        database(FILMS).executeTransactionally("MATCH (f:Film) RETURN count(f) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    private fun ClusterHarness.Member.hasFiles(name: String, id: NamedDatabaseId): Boolean =
        listOf(
                home.resolve("data/databases").resolve(name),
                home.resolve("data/transactions").resolve(name),
                home.resolve("data/raft").resolve(id.databaseId().uuid().toString()),
            )
            .any { it.toFile().exists() }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    /** Waits until [name] runs on every running member as one database, and returns its id. */
    private fun ClusterHarness.awaitEverywhere(name: String): NamedDatabaseId {
        var id: NamedDatabaseId? = null
        eventually {
            val databases = running().map { it.registered(name) }
            assertThat(databases).describedAs("$name on every member").doesNotContainNull()
            assertThat(databases).allMatch { it!!.isStarted }
            assertThat(databases.map { it!!.namedDatabaseId }.distinct()).hasSize(1)
            id = databases.first()!!.namedDatabaseId
        }
        awaitLeader(name)
        return checkNotNull(id)
    }

    private fun ClusterHarness.awaitFilms(expected: Long) {
        eventually {
            for (member in running()) {
                assertThat(member.films()).describedAs("films on $member").isEqualTo(expected)
            }
        }
    }

    private fun ClusterHarness.addFilm(title: String) {
        awaitLeader(FILMS)
            .database(FILMS)
            .executeTransactionally("CREATE (:Film {title: '$title'})")
    }

    private fun ClusterHarness.awaitGone(name: String, id: NamedDatabaseId) {
        eventually {
            for (member in running()) {
                assertThat(member.registered(name)).describedAs("$name on $member").isNull()
                assertThat(member.hasFiles(name, id))
                    .describedAs("files of $id on $member")
                    .isFalse()
            }
        }
    }

    @Test
    fun `the default database cannot be dropped, so that members can restart`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()
        val member = cluster.members.single()

        assertThat(catchThrowable { cluster.administer("DROP DATABASE $DEFAULT") })
            .hasMessage("Not allowed to delete the default database '$DEFAULT'.")
        member.stop()
        member.start()

        assertThat(member.registered(DEFAULT)?.isStarted).isTrue()
    }

    @Test
    fun `a database created on the system leader runs on every member as one replicated database`() {
        val cluster = cluster()

        cluster.administer("CREATE DATABASE $FILMS")

        cluster.awaitEverywhere(FILMS)
        assertThat(cluster.running().map { it.storeId(FILMS) }.distinct()).hasSize(1)
        cluster.addFilm("Alien")
        cluster.awaitFilms(1)
    }

    @Test
    fun `stopping and starting a database applies on every member`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS")
        cluster.awaitEverywhere(FILMS)

        cluster.administer("STOP DATABASE $FILMS")

        eventually {
            for (member in cluster.running()) {
                assertThat(member.registered(FILMS)?.isStarted).describedAs("$member").isFalse()
            }
        }

        cluster.administer("START DATABASE $FILMS")

        cluster.awaitEverywhere(FILMS)
        cluster.addFilm("Brazil")
        cluster.awaitFilms(1)
    }

    @Test
    fun `dropping a database deletes its store and Raft group on every member`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS")
        val id = cluster.awaitEverywhere(FILMS)
        cluster.addFilm("Alien")

        cluster.administer("DROP DATABASE $FILMS")

        cluster.awaitGone(FILMS, id)
    }

    @Test
    fun `a database dropped and created again with its name is a new database everywhere`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS")
        val dropped = cluster.awaitEverywhere(FILMS)
        cluster.addFilm("Alien")
        cluster.awaitFilms(1)

        cluster.administer("DROP DATABASE $FILMS")
        cluster.administer("CREATE DATABASE $FILMS")

        val created = cluster.awaitEverywhere(FILMS)
        assertThat(created).isNotEqualTo(dropped)
        cluster.awaitFilms(0)
        cluster.addFilm("Brazil")
        cluster.awaitFilms(1)
        for (member in cluster.running()) {
            assertThat(
                    member.home.resolve("data/raft").resolve(dropped.databaseId().uuid().toString())
                )
                .doesNotExist()
        }
    }

    @Test
    fun `a database is created everywhere when the system leader stops right after creating it`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)

        leader.database(SYSTEM).executeTransactionally("CREATE DATABASE $FILMS")
        leader.stop()
        leader.start()

        cluster.awaitEverywhere(FILMS)
        cluster.addFilm("Alien")
        cluster.awaitFilms(1)
    }

    @Test
    fun `a member stopped while it starts a database stops without errors`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        val entered = CountDownLatch(1)
        val holding = AtomicReference<ClusterHarness.Member>()
        // Holds the start of films on the member chosen below, as a slow start would, until
        // stopping the member interrupts it.
        for (member in cluster.members) {
            member.databaseEventListeners =
                listOf(
                    object : DatabaseEventListenerAdapter() {
                        override fun databaseStart(eventContext: DatabaseEventContext) {
                            if (eventContext.databaseName != FILMS || holding.get() != member)
                                return
                            entered.countDown()
                            val deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos()
                            while (!Thread.interrupted() && System.nanoTime() < deadline) {
                                LockSupport.parkNanos(Duration.ofMillis(10).toNanos())
                            }
                            // DozenDB's own listener, next, sees the interruption.
                            Thread.currentThread().interrupt()
                        }
                    }
                )
        }
        cluster.start()
        // A member other than the system leader copies films instead of creating it.
        val stopping = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        holding.set(stopping)

        cluster.administer("CREATE DATABASE $FILMS")
        assertThat(entered.await(1, TimeUnit.MINUTES)).isTrue()
        stopping.stop()

        assertThat(Files.readAllLines(stopping.home.resolve("logs/debug.log"))).noneMatch {
            " ERROR " in it
        }
    }

    /** The rows of [command], run on the system leader. */
    private fun ClusterHarness.waited(command: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    @Test
    fun `WAIT returns once every member did what a database command asks`() {
        val cluster = cluster()

        val created = cluster.waited("CREATE DATABASE $FILMS WAIT")

        assertThat(created).hasSize(3).allSatisfy {
            assertThat(it).containsEntry("state", "CaughtUp").containsEntry("success", true)
        }
        assertThat(created.map { it["address"] }).doesNotHaveDuplicates()
        // At once, without waiting any further.
        assertThat(cluster.members).allSatisfy {
            assertThat(it.registered(FILMS)?.isStarted).isTrue()
        }

        assertThat(cluster.waited("STOP DATABASE $FILMS WAIT")).allSatisfy {
            assertThat(it).containsEntry("state", "CaughtUp")
        }
        assertThat(cluster.members).allSatisfy {
            assertThat(it.registered(FILMS)?.isStarted).isFalse()
        }
        assertThat(cluster.waited("DROP DATABASE $FILMS WAIT")).allSatisfy {
            assertThat(it).containsEntry("state", "CaughtUp")
        }
        assertThat(cluster.members).allSatisfy { assertThat(it.registered(FILMS)).isNull() }
    }

    @Test
    fun `WAIT reports a member that did not catch up in time`() {
        val cluster = cluster()
        val down = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        down.stop()

        val rows = cluster.waited("CREATE DATABASE $FILMS WAIT 2 SECONDS")

        val (missing, others) = rows.partition { it["address"] == "127.0.0.1:${down.raftPort}" }
        assertThat(missing.single())
            .containsEntry("state", "TimedOut")
            .containsEntry("success", false)
        assertThat(missing.single()["message"] as String).startsWith("could not be asked")
        assertThat(others).hasSize(2).allSatisfy {
            assertThat(it).containsEntry("state", "CaughtUp").containsEntry("success", false)
        }
    }

    @Test
    fun `a member that was down creates and drops the databases it missed`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS")
        val films = cluster.awaitEverywhere(FILMS)
        val down = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        down.stop()

        cluster.administer("DROP DATABASE $FILMS")
        cluster.administer("CREATE DATABASE $SERIES")
        cluster.awaitEverywhere(SERIES)
        cluster
            .awaitLeader(SERIES)
            .database(SERIES)
            .executeTransactionally("CREATE (:Series {title: 'Twin Peaks'})")
        down.start()

        cluster.awaitGone(FILMS, films)
        cluster.awaitEverywhere(SERIES)
        eventually {
            assertThat(
                    down.database(SERIES).executeTransactionally(
                        "MATCH (s:Series) RETURN count(s) AS c",
                        emptyMap(),
                    ) {
                        it.next()["c"] as Long
                    }
                )
                .isEqualTo(1)
        }
    }

    private companion object {
        const val SYSTEM = "system"
        const val DEFAULT = "neo4j"
        const val FILMS = "films"
        const val SERIES = "series"
    }
}
