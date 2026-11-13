package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.consensus.HostPort
import dev.dodeca.core.Dodeca
import dev.dodeca.core.remote.BoltValues
import dev.dodeca.core.remote.DebugLogLogging
import dev.dodeca.core.remote.NoObservations
import dev.dodeca.core.remote.RemoteFailure
import dev.dodeca.core.remote.RemoteFailures
import dev.dodeca.core.remote.RemoteTransaction
import dev.dodeca.core.security.MemberLogins
import java.net.URI
import java.time.Clock
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import javax.net.ssl.SSLContext
import org.neo4j.bolt.connection.AuthToken
import org.neo4j.bolt.connection.AuthTokens
import org.neo4j.bolt.connection.BoltAgent
import org.neo4j.bolt.connection.BoltConnectionParameters
import org.neo4j.bolt.connection.BoltProtocolVersion
import org.neo4j.bolt.connection.NotificationConfig
import org.neo4j.bolt.connection.SecurityPlans
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory
import org.neo4j.bolt.connection.pooled.AuthTokenManager
import org.neo4j.bolt.connection.pooled.PooledBoltConnectionSource
import org.neo4j.boltmessages.AccessMode
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.fabric.bookmark.TransactionBookmarkManager
import org.neo4j.fabric.executor.Location
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory
import org.neo4j.logging.InternalLogProvider
import org.neo4j.router.transaction.DatabaseTransaction
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.router.transaction.TransactionInfo

/**
 * Transactions that run on other members, over their routing connectors, as the users they run for
 * here ([MemberLogins]): one pool of Bolt connections per member, with the TLS between members
 * ([ssl]). Pools follow Neo4j's `dbms.routing.driver.*` settings in [config]. Each login carries
 * the last transaction of `system` on this member, from [systemTransaction].
 */
internal class RemoteTransactions(
    private val config: Config,
    ssl: SSLContext,
    verifyHostname: Boolean,
    private val systemTransaction: () -> Long,
    logProvider: InternalLogProvider,
) : DatabaseTransactionFactory<Location.Remote>, AutoCloseable {
    private val logging = DebugLogLogging(logProvider, config.get(DRIVER_LOGGING_LEVEL))
    private val security = SecurityPlans.encrypted(ssl, verifyHostname)
    private val provider =
        NettyBoltConnectionProviderFactory()
            .create(
                logging,
                BoltValues,
                NoObservations,
                mapOf("eventLoopThreadNamePrefix" to "dodeca-forwarding"),
            )
    private val pools = ConcurrentHashMap<SocketAddress, PooledBoltConnectionSource>()
    private val open = ConcurrentHashMap<RemoteTransaction, Forwarded>()
    @Volatile private var closed = false

    /**
     * The transactions that have not ended, to give up on those of a member that stopped leading.
     */
    fun open(): Collection<ForwardedTransaction> = open.values.toList()

    override fun beginTransaction(
        location: Location.Remote,
        transactionInfo: TransactionInfo,
        bookmarkManager: TransactionBookmarkManager,
        terminationCallback: Consumer<Status>,
        // Null when Neo4j's query router begins the transaction with BEGIN, before any query.
        constituentTransactionFactory: ConstituentTransactionFactory?,
    ): DatabaseTransaction {
        if (closed) throw shuttingDown(location.databaseName)
        val member =
            location as? Location.Remote.Internal.Direct
                ?: throw IllegalArgumentException(
                    "Only members run forwarded transactions: $location"
                )
        val address = member.uri.addresses.single()
        val subject = transactionInfo.loginContext().subject()
        val transaction =
            RemoteTransaction(
                member,
                transactionInfo,
                {
                    pool(address, member.databaseName)
                        .getConnection(
                            BoltConnectionParameters.builder()
                                .withAuthToken(token(subject))
                                .withMinVersion(MIN_VERSION)
                                .build()
                        )
                },
                subject.executingUser().takeIf { it != subject.authenticatedUser() },
                bookmarkManager
                    .getBookmarksForRemote(location)
                    .map { it.serializedState() }
                    .toSet(),
                bookmarkManager,
                terminationCallback,
                LeaderFailures(address),
                { open.remove(it) },
            )
        open[transaction] =
            Forwarded(
                transaction,
                member,
                address,
                transactionInfo.accessMode() == AccessMode.WRITE,
            )
        return transaction
    }

    /**
     * Opens a connection to the routing connector at [address], logged in as the user of [subject],
     * as forwarded transactions are: null once it opens, else why not. A new connection, not one of
     * the pool's, which may have idled since the member went away.
     */
    fun connectionFailure(address: SocketAddress, subject: AuthSubject): CompletionStage<String?> {
        val timeout = config.get(GraphDatabaseSettings.routing_driver_connect_timeout).toMillis()
        return provider
            .connect(
                uri(address),
                null,
                AGENT,
                USER_AGENT,
                timeout.toInt(),
                timeout,
                security,
                token(subject),
                MIN_VERSION,
                NotificationConfig.defaultConfig(),
                null,
            )
            .thenCompose { it.close() }
            .handle { _, failure ->
                val cause = (failure as? CompletionException)?.cause ?: failure
                cause?.let { it.message ?: it.toString() }
            }
    }

    /** A login on another member as the user of [subject] (see [MemberLogins]). */
    private fun token(subject: AuthSubject): AuthToken =
        AuthTokens.custom(
            mapOf(
                SCHEME to BoltValues.value(MemberLogins.SCHEME),
                PRINCIPAL to BoltValues.value(subject.authenticatedUser()),
                CREDENTIALS to BoltValues.value(""),
                MemberLogins.SYSTEM_TRANSACTION to BoltValues.value(systemTransaction()),
            )
        )

    private fun uri(address: SocketAddress) =
        URI("bolt", null, address.hostname, address.port, null, null, null)

    private fun pool(address: SocketAddress, database: String): PooledBoltConnectionSource =
        pools.computeIfAbsent(address) {
            if (closed) throw shuttingDown(database)
            PooledBoltConnectionSource(
                logging,
                Clock.systemUTC(),
                uri(address),
                provider,
                NoDefaultLogin,
                { CompletableFuture.completedStage(security) },
                config.get(GraphDatabaseSettings.routing_driver_max_connection_pool_size).let {
                    if (it < 0) Int.MAX_VALUE else it
                },
                config
                    .get(GraphDatabaseSettings.routing_driver_connection_acquisition_timeout)
                    .toMillis(),
                config.get(GraphDatabaseSettings.routing_driver_max_connection_lifetime).toMillis(),
                config
                    .get(GraphDatabaseSettings.routing_driver_idle_time_before_connection_test)
                    ?.toMillis() ?: -1,
                NoObservations,
                null,
                AGENT,
                USER_AGENT,
                config.get(GraphDatabaseSettings.routing_driver_connect_timeout).toMillis().toInt(),
                NotificationConfig.defaultConfig(),
                PooledBoltConnectionSource.TimeoutPolicy.LEGACY,
            )
        }

    /**
     * Why a transaction of [database] cannot run on another member: this member is shutting down,
     * while it still serves connections that were busy when it began. Clients retry, as on another
     * member that is unavailable.
     */
    private fun shuttingDown(database: String) =
        RemoteFailure(
            Status.General.DatabaseUnavailable,
            "This member is shutting down and no longer runs transactions of database " +
                "'$database' on other members. Retry through another member.",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, database)
                .build(),
        )

    /** Closes the pools of members that are not at [addresses], such as removed members. */
    fun retain(addresses: Set<SocketAddress>) {
        for (address in pools.keys - addresses) {
            pools.remove(address)?.close()
        }
    }

    override fun close() {
        closed = true
        for (transaction in open.keys.toList()) {
            transaction.terminate(Status.General.DatabaseUnavailable)
        }
        val closing = pools.values.map { it.close().toCompletableFuture() }
        pools.clear()
        try {
            CompletableFuture.allOf(*closing.toTypedArray()).get(CLOSE_TIMEOUT, TimeUnit.SECONDS)
        } finally {
            provider.close().toCompletableFuture().get(CLOSE_TIMEOUT, TimeUnit.SECONDS)
        }
    }

    /** Every connection logs in as the user of its transaction: there is no other login. */
    private object NoDefaultLogin : AuthTokenManager {
        override fun getToken(): CompletionStage<AuthToken> =
            CompletableFuture.failedStage(
                IllegalStateException("forwarded transactions log in as their users")
            )

        override fun handleBoltFailureException(
            authToken: AuthToken,
            exception: BoltFailureException,
        ): BoltFailureException = exception
    }

    companion object {
        /** Bolt 5.7, the first with GQL errors; every member runs the same Neo4j version. */
        val MIN_VERSION = BoltProtocolVersion(5, 7)

        private const val SCHEME = "scheme"
        private const val PRINCIPAL = "principal"
        private const val CREDENTIALS = "credentials"
        private const val CLOSE_TIMEOUT = 10L
        private val DRIVER_LOGGING_LEVEL = GraphDatabaseSettings.routing_driver_logging_level
        private val AGENT =
            BoltAgent(
                "dodeca/${Dodeca.version}",
                "${System.getProperty("os.name")}; ${System.getProperty("os.arch")}",
                "Java/${System.getProperty("java.version")}",
                null,
            )
        private val USER_AGENT = "dodeca-forwarding/${Dodeca.version}"
    }
}

/** A forwarded [transaction] of the database at [location], as [ForwardingLocations] watches it. */
private class Forwarded(
    private val transaction: RemoteTransaction,
    location: Location.Remote.Internal.Direct,
    address: SocketAddress,
    override val write: Boolean,
) : ForwardedTransaction {
    override val database: String = location.databaseName

    override val databaseId: NamedDatabaseId = location.databaseReference.databaseId()

    override val member: HostPort = HostPort(address.hostname, address.port)

    override fun leaderGone(why: String) = transaction.abandon(why, Status.Transaction.LeaderSwitch)
}

/** What clients learn when a transaction on the leader at [address] fails. */
private class LeaderFailures(private val address: SocketAddress) : RemoteFailures {
    /** As the leader reported it, but for a login it refused. */
    override fun answered(database: String, failure: BoltFailureException): RuntimeException =
        if (failure.code() == UNAUTHORIZED) refused(database, failure)
        else RemoteFailure.of(failure)

    override fun unreachable(database: String, cause: Throwable): RuntimeException =
        RemoteFailure(
            Status.General.DatabaseUnavailable,
            "Could not reach the leader of database '$database' at $address, to which this " +
                "member forwards writes: ${cause.message}",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, database)
                .build(),
            cause,
        )

    /** The leader refused to run the transaction as its user, who it does not know or suspended. */
    private fun refused(database: String, cause: BoltFailureException): RuntimeException =
        RemoteFailure(
            Status.Security.Forbidden,
            "The leader of database '$database' refused to run the transaction as its user: " +
                cause.message,
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF).build(),
            cause,
        )

    override fun outcomeUnknown(database: String, cause: Throwable): RuntimeException =
        RemoteFailure(
            Status.Transaction.TransactionCommitFailed,
            "The leader of database '$database' at $address did not answer the commit of a " +
                "forwarded transaction, whose outcome is unknown: it may or may not be committed. " +
                "Check the data before retrying.",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN01).build(),
            cause,
        )

    private companion object {
        const val UNAUTHORIZED = "Neo.ClientError.Security.Unauthorized"
    }
}
