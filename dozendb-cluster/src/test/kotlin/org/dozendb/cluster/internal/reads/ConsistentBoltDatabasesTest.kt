package org.dozendb.cluster.internal.reads

import java.lang.reflect.Proxy
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cluster.internal.ReadConsistency
import org.junit.jupiter.api.Test
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI
import org.neo4j.bolt.dbapi.BoltTransaction
import org.neo4j.bolt.protocol.common.message.AccessMode
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/** Transactions that ask for a read consistency nobody knows fail before they begin anywhere. */
class ConsistentBoltDatabasesTest {
    private var begun = 0

    private val delegate =
        object : BoltGraphDatabaseManagementServiceSPI {
            override fun database(
                databaseName: String,
                memoryTracker: MemoryTracker,
            ): BoltGraphDatabaseServiceSPI =
                object : BoltGraphDatabaseServiceSPI {
                    override fun beginTransaction(
                        type: KernelTransaction.Type,
                        loginContext: LoginContext,
                        clientInfo: ClientConnectionInfo,
                        bookmarks: List<String>,
                        txTimeout: Duration?,
                        accessMode: AccessMode,
                        txMetadata: Map<String, Any>?,
                        routingContext: RoutingContext,
                        queryExecutionConfiguration: QueryExecutionConfiguration,
                    ): BoltTransaction {
                        begun++
                        return NO_TRANSACTION
                    }

                    override fun getDatabaseReference(): DatabaseReference =
                        throw UnsupportedOperationException()
                }
        }

    private fun begin(metadata: Map<String, Any>?) {
        ConsistentBoltDatabases(delegate, ReadConsistency.CAUSAL) { error("no bookmarks") }
            .database("neo4j", EmptyMemoryTracker.INSTANCE)
            .beginTransaction(
                KernelTransaction.Type.EXPLICIT,
                LoginContext.AUTH_DISABLED,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                emptyList(),
                null,
                AccessMode.READ,
                metadata,
                RoutingContext(true, emptyMap()),
                QueryExecutionConfiguration.DEFAULT_CONFIG,
            )
    }

    @Test
    fun `transactions that ask for a known consistency or none begin`() {
        begin(null)
        begin(mapOf(ReadConsistency.METADATA_KEY to "linearizable"))

        assertThat(begun).isEqualTo(2)
    }

    @Test
    fun `a transaction that asks for an unknown consistency fails before it begins`() {
        assertThatThrownBy { begin(mapOf(ReadConsistency.METADATA_KEY to "strong")) }
            .isInstanceOfSatisfying(ReadConsistencyException::class.java) {
                assertThat(it.status()).isEqualTo(Status.Request.Invalid)
            }

        assertThat(begun).isZero()
    }

    private companion object {
        /** A transaction no test uses. */
        val NO_TRANSACTION: BoltTransaction =
            Proxy.newProxyInstance(
                BoltTransaction::class.java.classLoader,
                arrayOf(BoltTransaction::class.java),
            ) { _, _, _ ->
                throw UnsupportedOperationException()
            } as BoltTransaction
    }
}
