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
import org.dozendb.cluster.internal.ClusterSettings
import org.dozendb.cluster.internal.ConsensusTuning
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.edition.DatabaseState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters
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

    /**
     * [size] members, the first two of which may host primaries, the others only secondaries, with
     * [own] settings each.
     */
    private fun cluster(
        settings: Map<Setting<*>, Any> = emptyMap(),
        size: Int = 3,
        own: Map<Int, Map<Setting<*>, Any>> = emptyMap(),
        tuning: ConsensusTuning? = null,
    ): ClusterHarness =
        ClusterHarness(root, size, settings = settings, tuning = tuning).also {
            harness = it
            for (member in it.members) {
                val constraint =
                    if (member.index < 2) emptyMap()
                    else
                        mapOf<Setting<*>, Any>(
                            DozenDbSettings.initialServerModeConstraint to
                                InstanceModeConstraint.SECONDARY
                        )
                member.settings = constraint + own[member.index].orEmpty()
            }
            it.start()
        }

    private val ClusterHarness.secondary: ClusterHarness.Member
        get() = members[2]

    private val ClusterHarness.primaries: List<ClusterHarness.Member>
        get() = members.take(2)

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    /** Writes on the leader, as the members that run the database's group agree on it. */
    private fun ClusterHarness.write(statement: String) {
        awaitLeader(FILMS) { members.filter { it.running && it.status(FILMS) != null } }
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
        val last = leader.lastTransaction().id()
        eventually {
            val pulls = cluster.secondary.metrics()
            assertThat(pulls.pullsMade.sum()).isPositive()
            assertThat(pulls.highestRequested.get()).describedAs("requested").isEqualTo(last)
            assertThat(pulls.highestReceived.get()).describedAs("received").isEqualTo(last)
        }
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
        // Its upstreams are behind its foreign store: their last transaction is not its own.
        cluster.write("CREATE (:Film {id: 1})")

        eventually {
            val health = cluster.secondary.registered(FILMS)?.database()?.databaseHealth
            assertThat(health?.hasNoPanic()).isFalse()
            assertThat(health!!.causeOfPanic())
                .hasMessageContaining("holds transactions that its upstream does not have")
        }
    }

    @Test
    fun `a secondary pulls from the upstream its strategy chooses, another secondary too`() {
        val cluster =
            cluster(
                size = 4,
                own =
                    mapOf(
                        2 to
                            mapOf(
                                DozenDbSettings.initialServerTags to listOf("feeder"),
                                ClusterSettings.upstreamStrategy to
                                    listOf("connect-to-random-primary-server"),
                            ),
                        3 to
                            mapOf(
                                ClusterSettings.upstreamStrategy to listOf("user-defined"),
                                ClusterSettings.userDefinedUpstreamStrategy to
                                    "tags(feeder); halt()",
                            ),
                    ),
            )
        val (feeder, fed) = cluster.members.drop(2)
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 2 SECONDARIES")
        eventually {
            for (member in listOf(feeder, fed)) {
                assertThat(member.registered(FILMS)?.database()?.isStarted).isTrue()
            }
        }
        // Until it has heard that the feeder runs the database, its rules find no upstream, and
        // it pulls from a random primary, the last resort.
        val directory = checkNotNull(fed.cluster.directory)
        eventually {
            directory.refreshSoon()
            assertThat(directory.answering(feeder.cluster.self.id)).isTrue()
            assertThat(directory.databaseStates(feeder.cluster.self.id).orEmpty()).anyMatch {
                it.name == FILMS && it.status == DatabaseState.Status.ONLINE
            }
        }

        cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")

        eventually { assertThat(fed.films()).isEqualTo(10) }
        assertThat(feeder.metrics().pullsServed.sum()).isPositive()
        assertThat(fed.metrics().pullsServed.sum()).isZero()
    }

    @Test
    fun `a secondary ahead of the secondary it pulls from keeps running`() {
        val cluster =
            cluster(
                mapOf(BoltConnector.enabled to true),
                size = 4,
                own =
                    mapOf(
                        2 to
                            mapOf(
                                DozenDbSettings.initialServerTags to listOf("feeder"),
                                ClusterSettings.pullInterval to Duration.ofHours(1),
                            ),
                        3 to
                            mapOf(
                                ClusterSettings.upstreamStrategy to listOf("user-defined"),
                                ClusterSettings.userDefinedUpstreamStrategy to
                                    "tags(feeder); halt()",
                            ),
                    ),
            )
        val (feeder, fed) = cluster.members.drop(2)
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 2 SECONDARIES")
        eventually {
            for (member in listOf(feeder, fed)) {
                assertThat(member.registered(FILMS)?.database()?.isStarted).isTrue()
            }
            // Its first pull, after which it pulls again in an hour.
            assertThat(feeder.metrics().pullsMade.sum()).isPositive()
        }
        cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")

        // A linearizable read pulls from a primary.
        GraphDatabase.driver("bolt://${fed.boltAddress()}", AuthTokens.none()).use { driver ->
            driver.session(SessionConfig.forDatabase(FILMS)).use {
                val linearizable =
                    TransactionConfig.builder()
                        .withMetadata(mapOf("dozendb.consistency" to "linearizable"))
                        .build()
                assertThat(
                        it.run("MATCH (f:Film) RETURN count(f) AS c", linearizable)
                            .single()["c"]
                            .asLong()
                    )
                    .isEqualTo(10)
            }
        }
        assertThat(feeder.films()).describedAs("the feeder lags").isZero()

        val pulls = fed.metrics().pullsMade.sum()
        eventually {
            assertThat(fed.registered(FILMS)?.database()?.databaseHealth?.hasNoPanic())
                .describedAs("it did not panic")
                .isTrue()
            assertThat(fed.metrics().pullsMade.sum()).isGreaterThan(pulls + 1)
        }
        assertThat(fed.films()).isEqualTo(10)
    }

    @Test
    fun `every member shows a secondary in SHOW DATABASES and SHOW SERVERS`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        val secondary = cluster.secondary.serverId

        for (member in cluster.members) {
            eventually {
                val rows =
                    member.query(
                        "SHOW DATABASE $FILMS YIELD serverID, role, writer, currentStatus, " +
                            "currentPrimariesCount, currentSecondariesCount, " +
                            "requestedPrimariesCount, requestedSecondariesCount"
                    )
                assertThat(rows.associate { it["serverID"] to it["role"] })
                    .describedAs("on $member")
                    .isEqualTo(
                        cluster.primaries.associate { it.serverId to "primary" } +
                            (secondary to "secondary")
                    )
                assertThat(rows.map { it["currentStatus"] }).containsOnly("online")
                assertThat(rows.single { it["serverID"] == secondary }["writer"]).isEqualTo(false)
                for ((column, count) in
                    listOf(
                        "currentPrimariesCount" to 2L,
                        "currentSecondariesCount" to 1L,
                        "requestedPrimariesCount" to 2L,
                        "requestedSecondariesCount" to 1L,
                    )) {
                    assertThat(rows.map { it[column] }).describedAs(column).containsOnly(count)
                }
                val hosting =
                    member.query("SHOW SERVERS YIELD serverId, hosting, requestedHosting").single {
                        it["serverId"] == secondary
                    }
                assertThat(hosting["hosting"] as List<*>).contains(FILMS)
                assertThat(hosting["requestedHosting"] as List<*>).contains(FILMS)
            }
        }
    }

    @Test
    fun `routing tables send reads to secondaries, and only to them where primaries take none`() {
        val cluster =
            cluster(
                mapOf(BoltConnector.enabled to true),
                own = mapOf(0 to mapOf(ClusterSettings.readsOnPrimaries to false)),
            )
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        val everyone = cluster.members.map { it.boltAddress() }.toSet()

        for (member in cluster.members) {
            eventually {
                val routes = member.routes(FILMS)
                val leader = cluster.awaitLeader(FILMS) { cluster.primaries }
                assertThat(routes["ROUTE"]).describedAs("on $member").isEqualTo(everyone)
                assertThat(routes["WRITE"]).containsExactly(leader.boltAddress())
                assertThat(routes["READ"])
                    .isEqualTo(
                        if (member.index == 0) setOf(cluster.secondary.boltAddress())
                        else everyone - leader.boltAddress()
                    )
            }
        }
    }

    @Test
    fun `with server-side routing a secondary forwards writes, and takes reads of other servers`() {
        val cluster =
            cluster(
                mapOf(
                    BoltConnector.enabled to true,
                    GraphDatabaseSettings.routing_default_router to
                        GraphDatabaseSettings.RoutingMode.SERVER,
                    ClusterSettings.readsOnPrimaries to false,
                ),
                size = 4,
            )
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        val runs = { member: ClusterHarness.Member ->
            member.registered(FILMS)?.database()?.isStarted == true
        }
        eventually { assertThat(cluster.members.drop(2).count(runs)).isEqualTo(1) }
        val (secondary, other) =
            cluster.members.drop(2).partition(runs).let { (on, off) ->
                on.single() to off.single()
            }

        GraphDatabase.driver("neo4j://${secondary.boltAddress()}", AuthTokens.none()).use {
            it.session(SessionConfig.forDatabase(FILMS)).use { session ->
                session.run("CREATE (:Film {id: 1})").consume()
            }
        }
        eventually { assertThat(secondary.films()).isEqualTo(1) }
        // Until it learns that the secondary runs the database, it reads on a follower; SHOW
        // DATABASES asks the members.
        eventually { assertThat(other.roles(FILMS)[secondary.serverId]).isEqualTo("secondary") }

        val started = { secondary.transactions().numberOfStartedTransactions }
        val before = started()
        GraphDatabase.driver("neo4j://${other.boltAddress()}", AuthTokens.none()).use {
            val config =
                SessionConfig.builder()
                    .withDatabase(FILMS)
                    .withDefaultAccessMode(AccessMode.READ)
                    .build()
            it.session(config).use { session ->
                assertThat(
                        session.run("MATCH (f:Film) RETURN count(f) AS c").single()["c"].asLong()
                    )
                    .isEqualTo(1)
            }
        }
        assertThat(started()).describedAs("ran on the secondary").isGreaterThan(before)
    }

    @Test
    fun `reads on a secondary that must see earlier writes pull them at once`() {
        val cluster =
            cluster(
                mapOf(
                    BoltConnector.enabled to true,
                    // Only the reads make it pull what it lacks.
                    ClusterSettings.pullInterval to Duration.ofHours(1),
                )
            )
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        val linearizable =
            TransactionConfig.builder()
                .withMetadata(mapOf("dozendb.consistency" to "linearizable"))
                .build()
        val count = "MATCH (f:Film) RETURN count(f) AS c"
        val routing =
            GraphDatabase.driver("neo4j://${cluster.primaries[0].boltAddress()}", AuthTokens.none())
        val direct =
            GraphDatabase.driver("bolt://${cluster.secondary.boltAddress()}", AuthTokens.none())
        try {
            cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")
            direct.session(SessionConfig.forDatabase(FILMS)).use {
                assertThat(it.run(count, linearizable).single()["c"].asLong())
                    .describedAs("a linearizable read")
                    .isEqualTo(10)
            }

            val bookmarks =
                routing.session(SessionConfig.forDatabase(FILMS)).use { session ->
                    session.executeWrite {
                        it.run("UNWIND range(11, 20) AS i CREATE (:Film)").consume()
                    }
                    session.lastBookmarks()
                }
            val config = SessionConfig.builder().withDatabase(FILMS).withBookmarks(bookmarks)
            direct.session(config.build()).use {
                assertThat(it.run(count).single()["c"].asLong())
                    .describedAs("a read after the write its bookmark names")
                    .isEqualTo(20)
            }

            cluster.primaries.forEach { it.stop() }
            val refused =
                catchThrowable {
                    direct.session(SessionConfig.forDatabase(FILMS)).use {
                        it.run(count, linearizable).consume()
                    }
                }
                    as Neo4jException
            assertThat(refused.code()).isEqualTo("Neo.TransientError.Cluster.ReplicationFailure")
            assertThat(refused).hasMessageContaining("could not be confirmed with a primary")
        } finally {
            routing.close()
            direct.close()
        }
    }

    @Test
    fun `a copy placed on its server in the other mode switches mode from its own store`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        cluster.write("UNWIND range(1, 100) AS i CREATE (:Film {id: i})")
        val served = { cluster.members.sumOf { it.metrics().copiesServed.sum() } }
        val copies = served()

        cluster.administer("ALTER DATABASE $FILMS SET TOPOLOGY 1 PRIMARY 2 SECONDARIES")

        eventually {
            assertThat(cluster.members[0].roles(FILMS).values)
                .containsExactlyInAnyOrder("primary", "secondary", "secondary")
        }
        val roles = cluster.members[0].roles(FILMS)
        val switched = cluster.primaries.single { roles[it.serverId] == "secondary" }
        assertThat(switched.status(FILMS)).describedAs("it runs no Raft group").isNull()
        assertThat(switched.films()).isEqualTo(100)
        cluster.write("CREATE (:Film {id: 101})")
        eventually { assertThat(switched.films()).isEqualTo(101) }
        assertThat(served()).describedAs("nothing copied").isEqualTo(copies)

        cluster.administer("ALTER DATABASE $FILMS SET TOPOLOGY 2 PRIMARIES 1 SECONDARY")

        eventually {
            assertThat(switched.status(FILMS)?.role).describedAs("it runs the group").isNotNull()
            val leader = cluster.awaitLeader(FILMS) { cluster.primaries }
            assertThat(leader.cluster.groupMembers(leader.status(FILMS)!!.database).map { it.id })
                .containsExactlyInAnyOrderElementsOf(cluster.primaries.map { it.cluster.self.id })
        }
        cluster.write("CREATE (:Film {id: 102})")
        eventually { assertThat(switched.films()).isEqualTo(102) }
        assertThat(cluster.members[0].roles(FILMS).filterKeys { it == switched.serverId }.values)
            .containsExactly("primary")
        assertThat(served()).describedAs("nothing copied").isEqualTo(copies)
    }

    @Test
    fun `a secondary turns primary from its own store after the leader discarded its log`() {
        val cluster =
            cluster(
                tuning = {
                    it.copy(
                        maxEntrySize = 1 shl 20,
                        logSegmentSize = 1,
                        snapshotInterval = Duration.ofMillis(200),
                    )
                }
            )
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 1 PRIMARY 2 SECONDARIES")
        eventually {
            assertThat(cluster.members[0].roles(FILMS).values)
                .containsExactlyInAnyOrder("primary", "secondary", "secondary")
        }
        val roles = cluster.members[0].roles(FILMS)
        val primary = cluster.primaries.single { roles[it.serverId] == "primary" }
        val candidate = cluster.primaries.single { it != primary }
        val id = primary.status(FILMS)!!.database.id
        var written = 0
        await withPollInterval
            Duration.ofMillis(300) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                cluster.write("CREATE (:Film {id: ${written++}, data: '${"x".repeat(100_000)}'})")
                primary
                    .database(FILMS)
                    .dependencyResolver
                    .resolveDependency(CheckPointer::class.java)
                    .forceCheckPoint(SimpleTriggerInfo("test"))
                assertThat(primary.firstLogIndex(id)).isGreaterThan(2)
            }
        eventually {
            assertThat(candidate.lastTransaction()).isEqualTo(primary.lastTransaction())
        }
        val served = { cluster.members.sumOf { it.metrics().copiesServed.sum() } }
        val copies = served()

        cluster.administer("ALTER DATABASE $FILMS SET TOPOLOGY 2 PRIMARIES 1 SECONDARY")

        eventually {
            assertThat(primary.cluster.groupMembers(primary.status(FILMS)!!.database).map { it.id })
                .containsExactlyInAnyOrder(primary.cluster.self.id, candidate.cluster.self.id)
            assertThat(candidate.status(FILMS)?.leader).isNotNull()
        }
        cluster.write("CREATE (:Film {id: -1})")
        eventually { assertThat(candidate.films()).isEqualTo(written + 1L) }
        assertThat(served()).describedAs("nothing copied").isEqualTo(copies)
    }

    @Test
    fun `a server's new mode constraint switches the modes of its copies in place`() {
        val cluster = cluster()
        // Else the default database's two primaries would need both servers that host primaries.
        cluster.administer("ALTER DATABASE neo4j SET TOPOLOGY 1 PRIMARY")
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 1 PRIMARY 2 SECONDARIES")
        eventually {
            assertThat(cluster.members[0].roles(FILMS).values)
                .containsExactlyInAnyOrder("primary", "secondary", "secondary")
        }
        cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")
        val before = cluster.members[0].roles(FILMS)
        val primary = cluster.primaries.single { before[it.serverId] == "primary" }
        val secondary = cluster.primaries.single { it != primary }
        val served = { cluster.members.sumOf { it.metrics().copiesServed.sum() } }
        val copies = served()

        cluster.administer(
            "ALTER SERVER '${primary.serverId}' SET OPTIONS {modeConstraint: 'SECONDARY'}"
        )

        eventually {
            assertThat(cluster.members[0].roles(FILMS))
                .isEqualTo(
                    mapOf(
                        primary.serverId to "secondary",
                        secondary.serverId to "primary",
                        cluster.secondary.serverId to "secondary",
                    )
                )
        }
        cluster.write("CREATE (:Film {id: 11})")
        eventually {
            for (member in cluster.members) assertThat(member.films()).isEqualTo(11)
        }
        assertThat(served()).describedAs("nothing copied").isEqualTo(copies)
    }

    @Test
    fun `DEALLOCATE moves a secondary to another server`() {
        val cluster = cluster(size = 4)
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        val runs = { member: ClusterHarness.Member ->
            member.registered(FILMS)?.database()?.isStarted == true
        }
        eventually { assertThat(cluster.members.drop(2).count(runs)).isEqualTo(1) }
        cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")
        val (old, new) =
            cluster.members.drop(2).partition(runs).let { (on, off) ->
                on.single() to off.single()
            }

        cluster.administer("DEALLOCATE DATABASES FROM SERVER '${old.serverId}'")

        eventually {
            assertThat(new.registered(FILMS)?.database()?.isStarted).isTrue()
            assertThat(new.films()).isEqualTo(10)
            assertThat(old.registered(FILMS)).isNull()
            assertThat(
                    cluster.members[0]
                        .query("SHOW SERVERS YIELD serverId, state")
                        .single { it["serverId"] == old.serverId }["state"]
                )
                .isEqualTo("Deallocated")
        }
    }

    @Test
    fun `a paused secondary pulls nothing and is left out of routing tables until resumed`() {
        val cluster =
            cluster(
                mapOf(
                    BoltConnector.enabled to true,
                    GraphDatabaseSettings.bookmark_ready_timeout to Duration.ofSeconds(1),
                )
            )
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY")
        cluster.awaitSecondary()
        cluster.write("UNWIND range(1, 10) AS i CREATE (:Film {id: i})")
        eventually { assertThat(cluster.secondary.films()).isEqualTo(10) }
        val toggle = { pause: Boolean ->
            cluster.secondary
                .query("CALL dbms.cluster.secondaryReplicationDisable('$FILMS', $pause)")
                .single()["state"]
        }

        assertThat(toggle(true)).isEqualTo("paused")

        eventually {
            for (member in cluster.members) {
                assertThat(member.routes(FILMS)["READ"])
                    .describedAs("on $member")
                    .doesNotContain(cluster.secondary.boltAddress())
            }
            assertThat(
                    cluster.members[0]
                        .query("SHOW DATABASE $FILMS YIELD serverID, statusMessage")
                        .single { it["serverID"] == cluster.secondary.serverId }["statusMessage"]
                )
                .isEqualTo("Secondary replication is paused")
        }
        val routing =
            GraphDatabase.driver("neo4j://${cluster.primaries[0].boltAddress()}", AuthTokens.none())
        val direct =
            GraphDatabase.driver("bolt://${cluster.secondary.boltAddress()}", AuthTokens.none())
        try {
            val bookmarks =
                routing.session(SessionConfig.forDatabase(FILMS)).use { session ->
                    session.executeWrite {
                        it.run("UNWIND range(11, 20) AS i CREATE (:Film {id: i})").consume()
                    }
                    session.lastBookmarks()
                }
            val config = SessionConfig.builder().withDatabase(FILMS).withBookmarks(bookmarks)
            assertThat(
                    catchThrowable {
                        direct.session(config.build()).use {
                            it.run("MATCH (f:Film) RETURN count(f)").consume()
                        }
                    }
                )
                .describedAs("a read after a later write")
                .isNotNull()
            val linearizable =
                TransactionConfig.builder()
                    .withMetadata(mapOf("dozendb.consistency" to "linearizable"))
                    .build()
            assertThat(
                    catchThrowable {
                        direct.session(SessionConfig.forDatabase(FILMS)).use {
                            it.run("MATCH (f:Film) RETURN count(f)", linearizable).consume()
                        }
                    }
                )
                .hasMessageContaining("is paused on this member")
        } finally {
            routing.close()
            direct.close()
        }
        assertThat(cluster.secondary.films()).isEqualTo(10)

        assertThat(toggle(false)).isEqualTo("running")
        eventually { assertThat(cluster.secondary.films()).isEqualTo(20) }
        eventually {
            assertThat(cluster.members[0].routes(FILMS)["READ"])
                .contains(cluster.secondary.boltAddress())
        }
        assertThat(
                catchThrowable {
                    cluster.primaries[0].query(
                        "CALL dbms.cluster.secondaryReplicationDisable('$FILMS', true)"
                    )
                }
            )
            .hasMessageContaining("Database 'films' does not run as a secondary on this server")
        assertThat(
                cluster.secondary
                    .query("CALL dbms.cluster.readReplicaToggle('$FILMS', true)")
                    .single()["state"]
            )
            .describedAs("the deprecated name")
            .isEqualTo("paused")
    }

    @Test
    fun `WAIT waits for secondaries too`() {
        val cluster = cluster()

        val created = cluster.waited("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY WAIT")

        assertThat(created).hasSize(3).allSatisfy {
            assertThat(it).containsEntry("state", "CaughtUp")
        }
        // At once, without waiting any further.
        assertThat(cluster.secondary.registered(FILMS)?.database()?.isStarted).isTrue()

        assertThat(cluster.waited("DROP DATABASE $FILMS WAIT")).hasSize(3).allSatisfy {
            assertThat(it).containsEntry("state", "CaughtUp")
        }
        assertThat(cluster.secondary.registered(FILMS)).isNull()
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

    /** The addresses of each role of the routing table [member] has for [database]. */
    private fun ClusterHarness.Member.routes(database: String): Map<String, Set<Any?>> =
        (query("CALL dbms.routing.getRoutingTable({}, '$database') YIELD servers")
                .single()["servers"]
                as List<*>)
            .map { it as Map<*, *> }
            .groupBy({ it["role"] as String }, { it["addresses"] as List<*> })
            .mapValues { (_, addresses) -> addresses.flatten().toSet() }

    private fun ClusterHarness.Member.transactions(): TransactionCounters =
        database(FILMS).dependencyResolver.resolveDependency(TransactionCounters::class.java)

    /** The role of each server that runs [database] online, by server id, as [this] shows it. */
    private fun ClusterHarness.Member.roles(database: String): Map<Any?, Any?> =
        query("SHOW DATABASE $database YIELD serverID, role, currentStatus")
            .filter { it["currentStatus"] == "online" }
            .associate { it["serverID"] to it["role"] }

    private fun ClusterHarness.waited(command: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).query(command)

    private fun ClusterHarness.Member.query(statement: String): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally(statement, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    private fun ClusterHarness.Member.metrics(): ClusterMetrics.Counters =
        database(SYSTEM).dependencyResolver.resolveDependency(ClusterMetrics::class.java).of(FILMS)

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
