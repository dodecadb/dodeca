package dev.dodeca.cluster.internal

import dev.dodeca.cluster.internal.ClusterHarness.Companion.DEFAULT_DATABASE
import dev.dodeca.cluster.internal.leadership.BalancingStrategy
import dev.dodeca.cluster.internal.replication.ReplicatedDatabase
import dev.dodeca.cluster.internal.replication.ReplicationService
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.DodecaSettings
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.during
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.api.LeaseService

/** Moving the leadership of a database on purpose. */
class LeadershipTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(
        size: Int = 3,
        settings: Map<Setting<*>, Any> = emptyMap(),
        memberSettings: (ClusterHarness.Member) -> Map<Setting<*>, Any> = { emptyMap() },
        fileSystem: ((ClusterHarness.Member) -> FileSystemAbstraction)? = null,
    ): ClusterHarness {
        println("Cluster harness in $root")
        return ClusterHarness(root, size, fileSystem, settings).also { cluster ->
            harness = cluster
            cluster.members.forEach { it.settings = memberSettings(it) }
            cluster.start()
        }
    }

    private fun ClusterHarness.Member.replica(name: String = DEFAULT_DATABASE): ReplicatedDatabase {
        val database = database(name)
        return checkNotNull(
            database.dependencyResolver
                .resolveDependency(ReplicationService::class.java)
                .replica(database.databaseId())
        )
    }

    private fun ClusterHarness.Member.count(cypher: String): Long =
        database().executeTransactionally(cypher, emptyMap()) { it.next().values.single() as Long }

    private fun statusOf(error: Throwable?): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    /** Creates databases until there are [DATABASES]. */
    private fun ClusterHarness.createDatabases() {
        DATABASES.drop(2).forEach { administer("CREATE DATABASE $it WAIT") }
    }

    /**
     * How many of [DATABASES] each running member leads, once they agree on every leader; null
     * until they do.
     */
    private fun ClusterHarness.led(): Map<ClusterHarness.Member, Int>? {
        val members = running()
        val leaders = DATABASES.map { name ->
            members.map { it.status(name)?.leader }.distinct().singleOrNull() ?: return null
        }
        return members.associateWith { member -> leaders.count { it == member.cluster.self.id } }
    }

    /**
     * Moves the leadership of every one of [DATABASES] to [member]: elections after CREATE DATABASE
     * do not always make the creator the leader.
     */
    private fun ClusterHarness.gatherOn(member: ClusterHarness.Member) {
        for (name in DATABASES) {
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    val leader = awaitLeader(name)
                    if (leader != member) {
                        leader
                            .replica(name)
                            .transferLeadership(
                                member.cluster.self.id,
                                "a test",
                                Duration.ofSeconds(5),
                            )
                    }
                    assertThat(awaitLeader(name)).isEqualTo(member)
                }
        }
    }

    /** The member that leads each of [DATABASES], once the running members agree; null until. */
    private fun ClusterHarness.leaders(): Map<String, MemberId>? {
        val members = running()
        return DATABASES.associateWith { name ->
            members.map { it.status(name)?.leader }.distinct().singleOrNull() ?: return null
        }
    }

    private val ClusterHarness.Member.id: MemberId
        get() = cluster.self.id

    /**
     * A cluster with balancing whose `db1` should be led by members tagged `us`, as member 1 is,
     * and every other database by members tagged `eu`, as member 2 is. Member 0 has no tag.
     */
    private fun prioritized(): ClusterHarness =
        cluster(
            settings =
                BALANCING +
                    mapOf(
                        ClusterSettings.leaderPriorityTag to "eu",
                        DatabasePriorityTag.of("db1") to "us",
                    ),
            memberSettings = {
                mapOf(DodecaSettings.initialServerTags to TAGS.getValue(it.index))
            },
        )

    /** Each of [DATABASES] led by [db1], or else by [others]. */
    private fun leading(db1: ClusterHarness.Member, others: ClusterHarness.Member) =
        DATABASES.associateWith {
            if (it == "db1") db1.id else others.id
        }

    private fun ClusterHarness.Member.setBalancing(strategy: BalancingStrategy) {
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(Config::class.java)
            .setDynamic(ClusterSettings.balancingStrategy, strategy, "a test")
    }

    private fun ClusterHarness.Member.debugLog(): String =
        Files.readString(home.resolve("logs/debug.log"))

    private fun ClusterHarness.Member.makeReadOnly(vararg databases: String) {
        database("system")
            .dependencyResolver
            .resolveDependency(Config::class.java)
            .setDynamic(GraphDatabaseSettings.read_only_databases, databases.toSet(), "a test")
    }

    @Test
    fun `a member where a database turns read-only hands its leadership to one where it is not`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()

        leader.makeReadOnly(DEFAULT_DATABASE)

        val next =
            await atMost
                Duration.ofSeconds(30) untilNotNull
                {
                    cluster.awaitLeader().takeIf { it != leader }
                }
        // Ratis and Dodeca's consensus log there, for every member.
        assertThat(Files.readString(root.resolve("raft.log")))
            .contains("Transferred leadership of $DEFAULT_DATABASE(")
            .contains("the database is read-only on this member")
        next.database().executeTransactionally("CREATE (:Written)")
        // The member still follows the database it does not write to.
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(leader.count("MATCH (n:Written) RETURN count(n)")).isEqualTo(1)
            }
    }

    @Test
    fun `a database read-only on every member keeps its leader`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()

        cluster.running().filter { it != leader }.forEach { it.makeReadOnly(DEFAULT_DATABASE) }
        leader.makeReadOnly(DEFAULT_DATABASE)

        // The leader asks the others before it hands over, and finds no member to take over.
        await during
            Duration.ofSeconds(3) atMost
            Duration.ofSeconds(10) untilAsserted
            {
                assertThat(cluster.awaitLeader()).isEqualTo(leader)
            }
        val error = catchThrowable {
            leader.database().executeTransactionally("CREATE (:Refused)")
        }
        assertThat(statusOf(error)).isEqualTo(Status.General.WriteOnReadOnlyAccessDatabase)
        assertThat(leader.debugLog().lines().filter { "no other member can take it over" in it })
            .hasSize(1)
    }

    @Test
    fun `a transfer lets the commits in flight finish and refuses later ones for a retry`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val target = cluster.running().first { it != leader }
        val acknowledged = ConcurrentLinkedQueue<Long>()
        val failures = ConcurrentLinkedQueue<Throwable>()
        val ids = AtomicLong()
        val done = AtomicBoolean()
        val writers = Executors.newFixedThreadPool(WRITERS)
        repeat(WRITERS) {
            writers.execute {
                while (!done.get()) {
                    val id = ids.incrementAndGet()
                    try {
                        leader.database().executeTransactionally("CREATE (:Written {id: $id})")
                        acknowledged += id
                    } catch (e: Exception) {
                        failures += e
                    }
                }
            }
        }
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(acknowledged).hasSizeGreaterThan(200)
            }

        val transferred =
            leader
                .replica()
                .transferLeadership(target.cluster.self.id, "a test", Duration.ofSeconds(5))
        done.set(true)
        writers.shutdown()
        assertThat(writers.awaitTermination(30, TimeUnit.SECONDS)).isTrue()

        assertThat(transferred).isTrue()
        assertThat(cluster.awaitLeader()).isEqualTo(target)
        // Drivers retry both; neither committed.
        assertThat(failures.map(::statusOf).distinct())
            .isSubsetOf(Status.Cluster.NotALeader, Status.Transaction.LeaseExpired)
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                cluster.running().forEach {
                    assertThat(it.count("MATCH (n:Written) RETURN count(n)"))
                        .describedAs("$it")
                        .isEqualTo(acknowledged.size.toLong())
                }
            }
    }

    @Test
    fun `a leader that stops lets the commits under way finish`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster = cluster { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } }
        val leader = cluster.awaitLeader()
        val follower = cluster.running().first { it != leader }
        val leaderFiles = fileSystems.getValue(leader.index)
        // A new label commits its token first, in a transaction of its own.
        leader.database().executeTransactionally("CREATE (n:InFlight) DELETE n")
        val replica = leader.replica()
        val hold = SlowLogFileSystem.Hold(DEFAULT_DATABASE)
        leaderFiles.holding = hold
        val writes: List<CompletableFuture<Void>>
        val stopped: CompletableFuture<Void>
        try {
            // The leader applies the first and holds it; the second waits behind it.
            val first = CompletableFuture.runAsync {
                leader.database().executeTransactionally("CREATE (:InFlight)")
            }
            assertThat(hold.held.await(30, TimeUnit.SECONDS)).isTrue()
            val second = CompletableFuture.runAsync {
                leader.database().executeTransactionally("CREATE (:InFlight)")
            }
            writes = listOf(first, second)
            // Both committed: the followers applied them.
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(follower.count("MATCH (n:InFlight) RETURN count(n)")).isEqualTo(2)
                }

            stopped = CompletableFuture.runAsync { leader.stop() }
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(replica.leaseService.currentLease).isEqualTo(LeaseService.NO_LEASE)
                }
        } finally {
            leaderFiles.holding = null
            hold.release()
        }

        for (write in writes) {
            assertThatCode { write.get(30, TimeUnit.SECONDS) }.doesNotThrowAnyException()
        }
        stopped.get(60, TimeUnit.SECONDS)
    }

    @Test
    fun `a transfer waits for the commit in flight, which succeeds`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster = cluster { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } }
        val leader = cluster.awaitLeader()
        val target = cluster.running().first { it != leader }
        val leaderFiles = fileSystems.getValue(leader.index)
        // A new label commits its token first, in a transaction of its own.
        leader.database().executeTransactionally("CREATE (n:InFlight) DELETE n")
        val hold = SlowLogFileSystem.Hold(DEFAULT_DATABASE)
        leaderFiles.holding = hold
        val write: CompletableFuture<Void>
        val transfer: CompletableFuture<Boolean>
        try {
            write = CompletableFuture.runAsync {
                leader.database().executeTransactionally("CREATE (:InFlight)")
            }
            // Committed: the followers applied it, while the leader has yet to, and to answer.
            assertThat(hold.held.await(30, TimeUnit.SECONDS)).isTrue()
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(target.count("MATCH (n:InFlight) RETURN count(n)")).isEqualTo(1)
                }
            val replica = leader.replica()

            transfer = CompletableFuture.supplyAsync {
                replica.transferLeadership(target.cluster.self.id, "a test", Duration.ofSeconds(5))
            }
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(transfer.isDone || replica.handingOver).isTrue()
                }
        } finally {
            // Also if the test fails before, so that the leader can stop.
            leaderFiles.holding = null
            hold.release()
        }

        assertThatCode { write.get(30, TimeUnit.SECONDS) }.doesNotThrowAnyException()
        assertThat(transfer.get(30, TimeUnit.SECONDS)).isTrue()
        assertThat(cluster.awaitLeader()).isEqualTo(target)
    }

    @Test
    fun `with equal balancing every member leads its share of the databases`() {
        val cluster = cluster(settings = BALANCING)

        cluster.createDatabases()

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.led()?.values).containsExactlyInAnyOrder(3, 3, 2)
            }
        // Balanced, it stays so.
        val balanced = cluster.led()
        await during
            Duration.ofSeconds(3) atMost
            Duration.ofSeconds(10) untilAsserted
            {
                assertThat(cluster.led()).isEqualTo(balanced)
            }
    }

    @Test
    fun `a member that stops hands its leaderships to those leading the fewest`() {
        val cluster = cluster()
        cluster.createDatabases()
        // Without balancing, the creator of each database leads it.
        val busiest =
            await atMost
                Duration.ofSeconds(30) untilNotNull
                {
                    cluster.led()?.maxBy { it.value }?.key
                }

        busiest.stop()

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                val led = cluster.led()
                assertThat(led?.values?.sum()).isEqualTo(DATABASES.size)
                assertThat(led!!.values.max() - led.values.min()).isLessThanOrEqualTo(1)
            }
        assertThat(Files.readString(root.resolve("raft.log")))
            .contains("this member stopped applying its entries")
    }

    @Test
    fun `a member that comes back gets its share of the leaderships`() {
        val cluster = cluster(settings = BALANCING)
        cluster.createDatabases()
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.led()?.values).containsExactlyInAnyOrder(3, 3, 2)
            }
        val restarted = cluster.running().first()

        restarted.stop()
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.led()?.values).containsExactlyInAnyOrder(4, 4)
            }
        restarted.start()

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.led()?.values).containsExactlyInAnyOrder(3, 3, 2)
            }
    }

    @Test
    fun `members with the priority tag of a database lead it, and balancing leaves it there`() {
        val cluster = prioritized()
        val (_, us, eu) = cluster.members
        cluster.createDatabases()

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, eu))
            }
        // A transfer away from the tag is undone.
        val untagged = cluster.members[0]
        assertThat(
                eu.replica("db2").transferLeadership(untagged.id, "a test", Duration.ofSeconds(5))
            )
            .isTrue()
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, eu))
            }
        await during
            Duration.ofSeconds(3) atMost
            Duration.ofSeconds(10) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, eu))
            }
        assertThat(Files.readString(root.resolve("raft.log")))
            .contains("to a member with the database's priority tag")
    }

    @Test
    fun `leadership follows the priority tag to the members ALTER SERVER gives it to`() {
        val cluster = prioritized()
        val (untagged, us, eu) = cluster.members
        cluster.createDatabases()
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, eu))
            }

        cluster.administer("ALTER SERVER '${eu.id.uuid}' SET OPTIONS {tags: []}")
        cluster.administer("ALTER SERVER '${untagged.id.uuid}' SET OPTIONS {tags: ['eu']}")

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, untagged))
            }
    }

    @Test
    fun `while no member with the priority tag runs, others lead, until one is back`() {
        val cluster = prioritized()
        val (untagged, us, eu) = cluster.members
        cluster.createDatabases()
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, eu))
            }

        val stopped = eu.id
        eu.stop()

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.leaders()?.values).isNotNull.doesNotContain(stopped)
            }
        cluster.awaitLeader(DEFAULT_DATABASE).database().executeTransactionally("CREATE ()")
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(untagged.debugLog() + us.debugLog())
                    .contains("should be led by a member with the server tag eu, but none can")
            }
        eu.start()
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.leaders()).isEqualTo(leading(us, eu))
            }
    }

    @Test
    fun `without balancing leaders stay put, until balancing is turned on`() {
        val cluster =
            cluster(settings = mapOf(ClusterSettings.balancingDelay to Duration.ofSeconds(1)))
        cluster.createDatabases()
        val gatherer = cluster.awaitLeader(SYSTEM)
        cluster.gatherOn(gatherer)
        val unbalanced = await atMost Duration.ofSeconds(30) untilNotNull { cluster.led() }
        assertThat(unbalanced.getValue(gatherer)).isEqualTo(DATABASES.size)

        await during
            Duration.ofSeconds(3) atMost
            Duration.ofSeconds(10) untilAsserted
            {
                assertThat(cluster.led()).isEqualTo(unbalanced)
            }
        cluster.running().forEach { it.setBalancing(BalancingStrategy.EQUAL_BALANCING) }

        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(cluster.led()?.values).containsExactlyInAnyOrder(3, 3, 2)
            }
    }

    private companion object {
        const val WRITERS = 4
        const val SYSTEM = "system"
        val DATABASES = listOf(SYSTEM, DEFAULT_DATABASE) + (1..6).map { "db$it" }

        /** The initial server tags of each member of [prioritized]. */
        val TAGS = mapOf(0 to emptyList(), 1 to listOf("us"), 2 to listOf("eu", "member-2"))

        /** Equal balancing, once leaders stayed put for a second. */
        val BALANCING: Map<Setting<*>, Any> =
            mapOf(
                ClusterSettings.balancingStrategy to BalancingStrategy.EQUAL_BALANCING,
                ClusterSettings.balancingDelay to Duration.ofSeconds(1),
            )
    }
}
