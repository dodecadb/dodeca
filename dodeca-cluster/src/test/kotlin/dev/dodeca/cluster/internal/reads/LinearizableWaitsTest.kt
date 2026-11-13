package dev.dodeca.cluster.internal.reads

import dev.dodeca.cluster.internal.ReadConsistency
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.neo4j.boltmessages.AccessMode
import org.neo4j.boltmessages.request.connection.RoutingContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.router.transaction.TransactionInfo

class LinearizableWaitsTest {
    private val films = DatabaseIdFactory.from("films", UUID.randomUUID())
    private val waited = mutableListOf<NamedDatabaseId>()
    private var failure: RuntimeException? = null
    private val waits =
        LinearizableWaits(ReadConsistency.CAUSAL) { database ->
            waited += database
            failure?.let { throw it }
        }

    private fun transaction(consistency: String?) =
        TransactionInfo(
            DatabaseReferenceImpl.Internal(NormalizedDatabaseName("films"), films, true),
            KernelTransaction.Type.EXPLICIT,
            null,
            null,
            emptyList(),
            Duration.ZERO,
            AccessMode.READ,
            consistency?.let { mapOf(ReadConsistency.METADATA_KEY to it) } ?: emptyMap(),
            RoutingContext(false, emptyMap()),
            QueryExecutionConfiguration.DEFAULT_CONFIG,
        )

    @Test
    fun `only transactions that ask for linearizable reads wait`() {
        waits.await(transaction(null), films)
        waits.await(transaction("causal"), films)
        waits.await(transaction("linearizable"), films)

        assertThat(waited).containsExactly(films)
    }

    @Test
    fun `a transaction whose wait failed fails again without waiting again`() {
        val transaction = transaction("linearizable")
        failure = ReadConsistencyException(Status.Cluster.ReplicationFailure, "no leader")

        val first = catchThrowable { waits.await(transaction, films) }
        failure = null
        val second = catchThrowable { waits.await(transaction, films) }

        assertThat(second).isSameAs(first).isNotNull()
        assertThat(waited).hasSize(1)
    }

    @Test
    fun `another transaction waits again, even if equal to one whose wait failed`() {
        val failed = transaction("linearizable")
        failure = ReadConsistencyException(Status.Cluster.ReplicationFailure, "no leader")
        catchThrowable { waits.await(failed, films) }
        failure = null

        val next = transaction("linearizable")
        assertThat(next).isEqualTo(failed)
        waits.await(next, films)
        waits.await(failed, DatabaseIdFactory.from("music", UUID.randomUUID()))

        assertThat(waited).hasSize(3)
    }
}
