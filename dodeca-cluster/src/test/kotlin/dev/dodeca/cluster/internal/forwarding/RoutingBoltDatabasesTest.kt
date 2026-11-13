package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.cluster.internal.ClusterSettings
import dev.dodeca.core.security.ForwardedLogin
import java.lang.reflect.Proxy
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI
import org.neo4j.bolt.dbapi.BoltTransaction
import org.neo4j.boltmessages.AccessMode
import org.neo4j.boltmessages.request.connection.RoutingContext
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/** Which transactions may be forwarded, as their routing context tells Neo4j's query router. */
class RoutingBoltDatabasesTest {
    private var seen: RoutingContext? = null
    private val config =
        Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, Path.of("/neo4j")).build()

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
                        seen = routingContext
                        return NO_TRANSACTION
                    }

                    override fun getDatabaseReference(): DatabaseReference =
                        throw UnsupportedOperationException()
                }
        }

    private fun routed(login: LoginContext, context: RoutingContext): RoutingContext {
        RoutingBoltDatabases(delegate, config)
            .database("neo4j", EmptyMemoryTracker.INSTANCE)
            .beginTransaction(
                KernelTransaction.Type.EXPLICIT,
                login,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                emptyList(),
                null,
                AccessMode.WRITE,
                null,
                context,
                QueryExecutionConfiguration.DEFAULT_CONFIG,
            )
        return checkNotNull(seen)
    }

    private val routing = RoutingContext(true, mapOf("address" to "core1:7687"))
    private val direct = RoutingContext(false, emptyMap())
    private val client = LoginContext.AUTH_DISABLED
    private val member = ForwardedLogin(LoginContext.AUTH_DISABLED)

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

    @Test
    fun `clients that allow routing may be forwarded, bolt clients only with the setting`() {
        assertThat(routed(client, routing).isServerRoutingEnabled).isTrue()
        assertThat(routed(client, direct).isServerRoutingEnabled).isFalse()

        config.setDynamic(ClusterSettings.forwardBoltWrites, true, "test")

        assertThat(routed(client, direct).isServerRoutingEnabled).isTrue()
    }

    @Test
    fun `transactions that a member forwarded are never forwarded again`() {
        config.setDynamic(ClusterSettings.forwardBoltWrites, true, "test")

        assertThat(routed(member, routing).isServerRoutingEnabled).isFalse()
        assertThat(routed(member, routing).parameters()).isEqualTo(routing.parameters())
        assertThat(routed(member, direct).isServerRoutingEnabled).isFalse()
    }
}
