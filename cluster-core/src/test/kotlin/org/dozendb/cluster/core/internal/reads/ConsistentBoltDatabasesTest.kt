package org.dozendb.cluster.core.internal.reads

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cluster.core.internal.ReadConsistency
import org.dozendb.cluster.core.internal.replication.CatchUp
import org.dozendb.cluster.core.internal.replication.ReplicationService
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.fake.InMemoryConsensus
import org.junit.jupiter.api.Test
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.logging.NullLog
import org.neo4j.logging.NullLogProvider

class ConsistentBoltDatabasesTest {
    private val consensus = InMemoryConsensus().newMember()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val replication =
        ReplicationService(
            consensus,
            system,
            "neo4j",
            CatchUp(
                consensus,
                system,
                { error("not used") },
                { _, _, _ -> error("not used") },
                NullLog.getInstance(),
            ),
            NullLogProvider.getInstance(),
            { error("not used") },
        )
    private val unused = BoltGraphDatabaseManagementServiceSPI { _, _ -> error("not used") }

    private fun databases(default: ReadConsistency) =
        ConsistentBoltDatabases(unused, replication, default)

    @Test
    fun `the metadata key picks the consistency, ignoring case`() {
        val databases = databases(ReadConsistency.CAUSAL)

        assertThat(databases.consistencyOf(null)).isEqualTo(ReadConsistency.CAUSAL)
        assertThat(databases.consistencyOf(mapOf("other" to 1))).isEqualTo(ReadConsistency.CAUSAL)
        assertThat(databases.consistencyOf(mapOf(ReadConsistency.METADATA_KEY to "Linearizable")))
            .isEqualTo(ReadConsistency.LINEARIZABLE)
        assertThat(
                databases(ReadConsistency.LINEARIZABLE)
                    .consistencyOf(mapOf(ReadConsistency.METADATA_KEY to "causal"))
            )
            .isEqualTo(ReadConsistency.CAUSAL)
        assertThat(databases(ReadConsistency.LINEARIZABLE).consistencyOf(emptyMap()))
            .isEqualTo(ReadConsistency.LINEARIZABLE)
    }

    @Test
    fun `an unknown consistency is a client error`() {
        assertThatThrownBy {
                databases(ReadConsistency.CAUSAL)
                    .consistencyOf(mapOf(ReadConsistency.METADATA_KEY to "strong"))
            }
            .isInstanceOfSatisfying(ReadConsistencyException::class.java) {
                assertThat(it.status()).isEqualTo(Status.Request.Invalid)
            }
            .hasMessageContaining("causal, linearizable")
    }
}
