package org.dozendb.cluster.internal.forwarding

import java.net.URI
import java.time.Clock
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.function.Supplier
import javax.net.ssl.SSLContext
import org.dozendb.core.DozenDb
import org.dozendb.core.security.MemberLogins
import org.neo4j.bolt.connection.AuthToken
import org.neo4j.bolt.connection.AuthTokens
import org.neo4j.bolt.connection.BoltAgent
import org.neo4j.bolt.connection.BoltProtocolVersion
import org.neo4j.bolt.connection.LoggingProvider
import org.neo4j.bolt.connection.NotificationConfig
import org.neo4j.bolt.connection.SecurityPlans
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory
import org.neo4j.bolt.connection.observation.BoltExchangeObservation
import org.neo4j.bolt.connection.observation.HttpExchangeObservation
import org.neo4j.bolt.connection.observation.ImmutableObservation
import org.neo4j.bolt.connection.observation.Observation
import org.neo4j.bolt.connection.pooled.AuthTokenManager
import org.neo4j.bolt.connection.pooled.PooledBoltConnectionSource
import org.neo4j.bolt.connection.pooled.observation.PoolObservationProvider
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.fabric.bookmark.TransactionBookmarkManager
import org.neo4j.fabric.executor.Location
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory
import org.neo4j.logging.InternalLog
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.Level
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
                mapOf("eventLoopThreadNamePrefix" to "dozendb-forwarding"),
            )
    private val pools = ConcurrentHashMap<SocketAddress, PooledBoltConnectionSource>()
    private val open: MutableSet<RemoteTransaction> = ConcurrentHashMap.newKeySet()
    @Volatile private var closed = false

    /**
     * The transactions that have not ended, to give up on those of a member that stopped leading.
     */
    fun open(): Collection<RemoteTransaction> = open.toList()

    override fun beginTransaction(
        location: Location.Remote,
        transactionInfo: TransactionInfo,
        bookmarkManager: TransactionBookmarkManager,
        terminationCallback: Consumer<Status>,
        // Null when Neo4j's query router begins the transaction with BEGIN, before any query.
        constituentTransactionFactory: ConstituentTransactionFactory?,
    ): DatabaseTransaction {
        check(!closed) { "this member is shutting down" }
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
                address,
                transactionInfo,
                { pool(address) },
                token(subject),
                subject.executingUser().takeIf { it != subject.authenticatedUser() },
                bookmarkManager
                    .getBookmarksForRemote(location)
                    .map { it.serializedState() }
                    .toSet(),
                bookmarkManager,
                terminationCallback,
                open::remove,
            )
        open += transaction
        return transaction
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

    private fun pool(address: SocketAddress): PooledBoltConnectionSource =
        pools.computeIfAbsent(address) {
            check(!closed) { "this member is shutting down" }
            PooledBoltConnectionSource(
                logging,
                Clock.systemUTC(),
                URI("bolt", null, address.hostname, address.port, null, null, null),
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

    /** Closes the pools of members that are not at [addresses], such as removed members. */
    fun retain(addresses: Set<SocketAddress>) {
        for (address in pools.keys - addresses) {
            pools.remove(address)?.close()
        }
    }

    override fun close() {
        closed = true
        for (transaction in open()) transaction.terminate(Status.General.DatabaseUnavailable)
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
                "dozendb/${DozenDb.version}",
                "${System.getProperty("os.name")}; ${System.getProperty("os.arch")}",
                "Java/${System.getProperty("java.version")}",
                null,
            )
        private val USER_AGENT = "dozendb-forwarding/${DozenDb.version}"
    }
}

/** bolt-connection's logs, in this member's debug log, from [threshold] on. */
private class DebugLogLogging(
    private val logs: InternalLogProvider,
    private val threshold: Level,
) : LoggingProvider {
    override fun getLog(cls: Class<*>): System.Logger =
        Logger(cls.name, logs.getLog(cls), threshold)

    override fun getLog(name: String): System.Logger = Logger(name, logs.getLog(name), threshold)

    private class Logger(
        private val name: String,
        private val log: InternalLog,
        private val threshold: Level,
    ) : System.Logger {
        override fun getName(): String = name

        private fun level(level: System.Logger.Level): Level? =
            when (level) {
                System.Logger.Level.ALL,
                System.Logger.Level.TRACE,
                System.Logger.Level.DEBUG -> Level.DEBUG
                System.Logger.Level.INFO -> Level.INFO
                System.Logger.Level.WARNING -> Level.WARN
                System.Logger.Level.ERROR -> Level.ERROR
                System.Logger.Level.OFF -> null
            }

        override fun isLoggable(level: System.Logger.Level): Boolean {
            val mapped = level(level) ?: return false
            return threshold != Level.NONE && mapped.ordinal >= threshold.ordinal
        }

        // The JDK's own overloads pass a null array of parameters, which Kotlin's varargs refuse.
        override fun log(level: System.Logger.Level, message: String?) {
            if (isLoggable(level)) write(level, message ?: "", null)
        }

        override fun log(level: System.Logger.Level, message: Supplier<String>) {
            if (isLoggable(level)) write(level, message.get(), null)
        }

        override fun log(level: System.Logger.Level, value: Any?) {
            if (isLoggable(level)) write(level, value.toString(), null)
        }

        override fun log(
            level: System.Logger.Level,
            bundle: java.util.ResourceBundle?,
            message: String?,
            thrown: Throwable?,
        ) {
            if (!isLoggable(level)) return
            write(level, message ?: "", thrown)
        }

        override fun log(
            level: System.Logger.Level,
            bundle: java.util.ResourceBundle?,
            format: String?,
            vararg params: Any?,
        ) {
            if (!isLoggable(level)) return
            val message =
                if (params.isEmpty()) format ?: ""
                else runCatching { String.format(format ?: "", *params) }.getOrDefault(format ?: "")
            write(level, message, null)
        }

        private fun write(level: System.Logger.Level, message: String, thrown: Throwable?) {
            when (level(level)) {
                Level.DEBUG -> log.debug(message, thrown)
                Level.INFO -> log.info(message, thrown)
                Level.WARN -> log.warn(message, thrown)
                Level.ERROR -> log.error(message, thrown)
                else -> {}
            }
        }
    }
}

/** No observations of connections and pools: Neo4j's metrics and logs observe the transactions. */
private object NoObservations : PoolObservationProvider {
    private object Nothing : BoltExchangeObservation, HttpExchangeObservation {
        override fun onWrite(messageName: String) = this

        override fun onRecord() = this

        override fun onSummary(messageName: String) = this

        override fun onHeaders(headers: Map<String, List<String>>) = this

        override fun onResponse(response: HttpExchangeObservation.Response) = this

        override fun error(error: Throwable) = this

        override fun stop() {}
    }

    override fun connectionPoolCreate(id: String, uri: URI, maxSize: Int): Observation = Nothing

    override fun connectionPoolClose(id: String, uri: URI): Observation = Nothing

    override fun pooledConnectionCreate(id: String, uri: URI): Observation = Nothing

    override fun pooledConnectionClose(id: String, uri: URI): Observation = Nothing

    override fun pooledConnectionAcquire(id: String, uri: URI): Observation = Nothing

    override fun pooledConnectionInUse(
        parentObsevation: ImmutableObservation?,
        id: String,
        uri: URI,
    ): Observation = Nothing

    override fun boltExchange(
        observationParent: ImmutableObservation?,
        host: String,
        port: Int,
        boltVersion: BoltProtocolVersion,
        setter: BiConsumer<String, String>,
    ): BoltExchangeObservation = Nothing

    override fun httpExchange(
        observationParent: ImmutableObservation?,
        uri: URI,
        method: String,
        uriTemplate: String,
        setter: BiConsumer<String, String>,
    ): HttpExchangeObservation = Nothing

    override fun scopedObservation(): ImmutableObservation? = null

    override fun <T> supplyInScope(observation: ImmutableObservation?, supplier: Supplier<T>): T =
        supplier.get()
}
