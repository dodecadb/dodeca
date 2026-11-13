package dev.dodeca.cluster.internal.routing

import dev.dodeca.cluster.internal.ClusterHarness
import dev.dodeca.cluster.internal.ClusterSettings
import dev.dodeca.core.DodecaSettings
import java.nio.file.Path
import java.time.Duration
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
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.graphdb.QueryExecutionException

/**
 * Routing tables follow the routing policy a driver names, as Neo4j Enterprise's multi-data centre
 * examples have them: five servers, two in data centre north1 and one in north2 of region north,
 * and two in region south.
 */
class RoutingPolicyTablesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(
                root,
                5,
                settings =
                    mapOf(
                        BoltConnector.enabled to true,
                        // Every server is a reader candidate, the leader too.
                        ClusterSettings.readsOnWriters to true,
                        ServerPolicy.of("north1_only") to "tags(north1)->min(2); halt();",
                        ServerPolicy.of("north") to "tags(north1)->min(2); tags(north); halt()",
                        ServerPolicy.of("south") to "tags(south)",
                    ),
            )
            .also { cluster ->
                harness = cluster
                for (member in cluster.members) {
                    member.settings =
                        mapOf(DodecaSettings.initialServerTags to TAGS.getValue(member.index))
                }
                cluster.start()
            }

    /** The addresses of each role of the routing table [this] has for `neo4j` under [policy]. */
    private fun ClusterHarness.Member.routes(policy: String? = null): Map<String, Set<Any?>> {
        val context = policy?.let { "{policy: '$it'}" } ?: "{}"
        return database("system").executeTransactionally(
            "CALL dbms.routing.getRoutingTable($context, 'neo4j') YIELD servers",
            emptyMap(),
        ) { result ->
            (result.next()["servers"] as List<*>)
                .map { it as Map<*, *> }
                .associate { it["role"] as String to (it["addresses"] as List<*>).toSet() }
        }
    }

    private fun addresses(members: Collection<ClusterHarness.Member>): Set<Any?> =
        members.map { it.boltAddress() }.toSet()

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `readers and routers follow the policy, the writer is the leader, and unknown policies fail`() {
        val cluster = cluster()
        val (north1a, north1b, north2, south1, south2) = cluster.members
        val leader = cluster.awaitLeader()
        val asked = south1
        eventually {
            assertThat(asked.routes())
                .describedAs("drivers that name no policy get the default one, all()")
                .isEqualTo(
                    mapOf(
                        "WRITE" to addresses(listOf(leader)),
                        "READ" to addresses(cluster.members),
                        "ROUTE" to addresses(cluster.members),
                    )
                )
        }

        for ((policy, servers) in
            listOf(
                "north1_only" to listOf(north1a, north1b),
                "north" to listOf(north1a, north1b),
                "south" to listOf(south1, south2),
            )) {
            assertThat(asked.routes(policy))
                .describedAs(policy)
                .isEqualTo(
                    mapOf(
                        "WRITE" to addresses(listOf(leader)),
                        "READ" to addresses(servers),
                        "ROUTE" to addresses(servers),
                    )
                )
        }
        assertThat(north2.routes("south")).isEqualTo(asked.routes("south"))

        assertThat(catchThrowable { asked.routes("North1_only") })
            .isInstanceOf(QueryExecutionException::class.java)
            .hasMessageContaining("Policy definition for 'North1_only' could not be found.")
            .hasMessageContaining(
                "The routing policies of this server are default, north, north1_only, south."
            )
    }

    @Test
    fun `a server that stops moves a min rule on, and a halted policy leaves the leader as router`() {
        val cluster = cluster()
        val (north1a, north1b, north2, south1, _) = cluster.members
        // Stop a north1 server that does not lead neo4j, so that its leader stays put.
        val leader = cluster.awaitLeader()
        val stopped = if (leader == north1a) north1b else north1a
        val kept = if (stopped == north1a) north1b else north1a
        val asked = south1
        eventually {
            assertThat(asked.routes("north")["READ"]).isEqualTo(addresses(listOf(north1a, north1b)))
        }

        stopped.stop()

        eventually {
            assertThat(asked.routes("north"))
                .describedAs("tags(north), as tags(north1) -> min(2) passes none")
                .isEqualTo(
                    mapOf(
                        "WRITE" to addresses(listOf(leader)),
                        "READ" to addresses(listOf(kept, north2)),
                        "ROUTE" to addresses(listOf(kept, north2)),
                    )
                )
        }
        assertThat(asked.routes("north1_only"))
            .describedAs("halted: no reader, which the table leaves out, and the leader routes")
            .isEqualTo(
                mapOf(
                    "WRITE" to addresses(listOf(leader)),
                    "ROUTE" to addresses(listOf(leader)),
                )
            )
        assertThat(asked.routes()["READ"])
            .describedAs("a server that does not answer is no reader")
            .isEqualTo(addresses(cluster.members - stopped))

        stopped.start()

        eventually {
            assertThat(asked.routes("north1_only")["READ"])
                .isEqualTo(addresses(listOf(north1a, north1b)))
        }
    }

    @Test
    fun `a server with other policies is warned about, once`() {
        val cluster =
            ClusterHarness(root, 3, settings = mapOf(ServerPolicy.of("north") to "tags(north)"))
                .also { harness = it }
        val (first, second, odd) = cluster.members
        odd.settings =
            mapOf(
                ServerPolicy.of("north") to "tags(north)",
                ServerPolicy.of("south") to "tags(south)",
            )
        cluster.start()
        fun ClusterHarness.Member.warnings(about: ClusterHarness.Member) =
            home.resolve("logs/debug.log").toFile().readLines().count {
                "Server ${about.serverId} at" in it && "has other routing policies" in it
            }

        eventually {
            assertThat(first.warnings(about = odd)).isEqualTo(1)
            assertThat(second.warnings(about = odd)).isEqualTo(1)
            assertThat(odd.warnings(about = first)).isEqualTo(1)
            assertThat(odd.warnings(about = second)).isEqualTo(1)
        }
        // Later reports of the odd server, which carry its last transaction, warn no more.
        fun lastCommitted() =
            first.database("system").executeTransactionally(
                "SHOW DATABASE neo4j YIELD serverID, lastCommittedTxn " +
                    "WHERE serverID = '${odd.serverId}'",
                emptyMap(),
            ) {
                it.next()["lastCommittedTxn"] as Long
            }
        val before = lastCommitted()
        cluster.awaitLeader().database("neo4j").executeTransactionally("CREATE ()")
        eventually { assertThat(lastCommitted()).isGreaterThan(before) }
        assertThat(first.warnings(about = odd)).isEqualTo(1)
        assertThat(first.warnings(about = second)).isZero()
    }

    private companion object {
        val TAGS =
            mapOf(
                0 to listOf("north1", "north"),
                1 to listOf("north1", "north"),
                2 to listOf("north2", "north"),
                3 to listOf("south"),
                4 to listOf("south"),
            )
    }
}
