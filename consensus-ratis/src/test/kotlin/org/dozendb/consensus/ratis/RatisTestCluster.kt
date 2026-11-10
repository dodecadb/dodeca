package org.dozendb.consensus.ratis

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Role
import org.dozendb.consensus.fake.RecordingStateMachine
import org.dozendb.consensus.testing.TestPorts

/**
 * Real Ratis members in one JVM, on loopback ports, for tests. [configure] adjusts each member's
 * configuration.
 */
class RatisTestCluster(
    private val root: Path,
    size: Int,
    private val configure: (RatisConsensusConfig) -> RatisConsensusConfig = { it },
) : AutoCloseable {
    val database = DatabaseRef("neo4j", UUID.randomUUID())

    inner class Node(val id: MemberId, val port: Int = TestPorts.free()) {
        val directory: Path = Files.createDirectories(root.resolve(id.toString()))
        val machine = RecordingStateMachine()

        var log: RatisConsensusLog? = null
            private set

        val consensus: RatisConsensusLog
            get() = checkNotNull(log) { "node $id is stopped" }

        val running: Boolean
            get() = log != null

        fun start() {
            check(log == null) { "node $id is running" }
            log =
                RatisConsensusLog(
                        configure(
                            RatisConsensusConfig(
                                memberId = id,
                                listenHost = "127.0.0.1",
                                listenPort = port,
                                advertisedAddress = null,
                                storageDirectory = directory,
                                electionTimeoutMin = ELECTION_TIMEOUT_MIN,
                                electionTimeoutMax = ELECTION_TIMEOUT_MAX,
                                metadata = { mapOf("neo4j.version" to "5.26.27") },
                                snapshotInterval = SNAPSHOT_INTERVAL,
                            )
                        )
                    )
                    .also { it.start() }
        }

        fun stop() {
            log?.close()
            log = null
        }
    }

    private val all = MutableList(size) { Node(MemberId(UUID.randomUUID())) }

    val nodes: List<Node>
        get() = all

    /** A new node, with an id of its own, that listens on the port of [replaced]. */
    fun replacement(replaced: Node): Node =
        Node(MemberId(UUID.randomUUID()), replaced.port).also { all += it }

    /** The directory of [node]'s Raft log for the group. */
    fun logDirectory(node: Node): Path =
        node.directory.resolve(database.id.toString()).resolve("current")

    /** Starts [initial] and creates the group with them as its voting members. */
    fun form(
        initial: List<Node> = nodes,
        stateMachine: (Node) -> ReplicatedStateMachine = { it.machine },
    ) {
        initial.forEach { it.start() }
        val members = initial.map { it.consensus.self }
        initial.forEach { it.consensus.startGroup(database, stateMachine(it), members) }
    }

    fun running(): List<Node> = nodes.filter { it.running }

    fun node(id: MemberId): Node = nodes.single { it.id == id }

    /**
     * Waits until every node taking part in the group sees the same leader, which considers itself
     * leader.
     */
    fun awaitLeader(): Node =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(50) atMost
            AWAIT untilNotNull
            {
                val statuses =
                    running()
                        .map { it.consensus.status(database) }
                        .filter { it.role != Role.Inactive }
                val leader = statuses.map { it.leader }.distinct().singleOrNull()
                leader
                    ?.takeIf {
                        node(it).running && node(it).consensus.role(database) == Role.Leader
                    }
                    ?.let(::node)
            }

    /** Waits until a node other than [previous] leads, as [awaitLeader] sees it. */
    fun awaitLeaderOtherThan(previous: Node): Node =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(50) atMost
            AWAIT untilNotNull
            {
                awaitLeader().takeIf { it != previous }
            }

    override fun close() {
        nodes.forEach { it.stop() }
    }

    companion object {
        val ELECTION_TIMEOUT_MIN: Duration = Duration.ofMillis(300)
        val ELECTION_TIMEOUT_MAX: Duration = Duration.ofMillis(600)
        val AWAIT: Duration = Duration.ofSeconds(20)
        val SNAPSHOT_INTERVAL: Duration = Duration.ofMillis(200)
    }
}
