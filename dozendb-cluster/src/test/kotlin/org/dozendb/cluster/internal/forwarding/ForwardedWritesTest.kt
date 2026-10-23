package org.dozendb.cluster.internal.forwarding

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.consensus.testing.TestAuthority
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.graphdb.config.Setting

/** Writes that reach a member which does not lead their database run on the leader. */
class ForwardedWritesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(
        settings: Map<Setting<*>, Any> = BOLT,
        authority: TestAuthority? = TestAuthority(),
    ): ClusterHarness =
        ClusterHarness(root, 3, settings = settings, authority = authority).also {
            harness = it
            it.start()
        }

    /** The routing address of each member, as this member sees it, by Raft port. */
    private fun ClusterHarness.Member.routingAddresses(): List<String?> =
        database("system").executeTransactionally(
            "CALL dozendb.cluster.members() YIELD raftAddress, routingAddress " +
                "RETURN routingAddress ORDER BY raftAddress",
            emptyMap(),
        ) { result ->
            result.stream().map { it["routingAddress"] as String? }.toList()
        }

    private fun ClusterHarness.Member.boundRoutingAddress(): String {
        val address =
            checkNotNull(
                database("system")
                    .dependencyResolver
                    .resolveDependency(ConnectorPortRegister::class.java)
                    .getLocalAddress(ConnectorType.INTRA_BOLT)
            ) {
                "$this has no routing connector"
            }
        return "127.0.0.1:${address.port}"
    }

    @Test
    fun `members with mutual TLS report where they take forwarded writes`() {
        val cluster = cluster()

        val expected = cluster.members.sortedBy { it.raftPort }.map { it.boundRoutingAddress() }
        for (member in cluster.members) {
            await withPollInterval
                Duration.ofMillis(100) atMost
                ClusterHarness.AWAIT untilAsserted
                {
                    assertThat(member.routingAddresses()).isEqualTo(expected)
                }
        }
    }

    @Test
    fun `members in plaintext take no forwarded writes`() {
        val cluster = cluster(authority = null)

        for (member in cluster.members) {
            assertThat(member.routingAddresses()).containsOnlyNulls().hasSize(3)
        }
    }

    private companion object {
        val BOLT: Map<Setting<*>, Any> = mapOf(BoltConnector.enabled to true)
    }
}
