package dev.dodeca.core.remote

import dev.dodeca.core.Dodeca
import dev.dodeca.core.security.AliasPasswords
import java.net.URI
import java.net.URLEncoder
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import javax.net.ssl.SSLException
import kotlin.text.Charsets.UTF_8
import org.neo4j.bolt.connection.AccessMode
import org.neo4j.bolt.connection.AuthToken
import org.neo4j.bolt.connection.AuthTokens
import org.neo4j.bolt.connection.BoltAgent
import org.neo4j.bolt.connection.BoltConnection
import org.neo4j.bolt.connection.BoltConnectionProvider
import org.neo4j.bolt.connection.BoltProtocolVersion
import org.neo4j.bolt.connection.DatabaseName
import org.neo4j.bolt.connection.DefaultDomainNameResolver
import org.neo4j.bolt.connection.NotificationConfig
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters
import org.neo4j.bolt.connection.SecurityPlan
import org.neo4j.bolt.connection.SecurityPlans
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory
import org.neo4j.bolt.connection.pooled.AuthTokenManager
import org.neo4j.bolt.connection.pooled.PooledBoltConnectionSource
import org.neo4j.bolt.connection.routed.RoutedBoltConnectionSource
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphDbmsModel
import org.neo4j.dbms.systemgraph.DriverSettings
import org.neo4j.fabric.bookmark.TransactionBookmarkManager
import org.neo4j.fabric.executor.Location
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory
import org.neo4j.logging.InternalLogProvider
import org.neo4j.router.transaction.DatabaseTransaction
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.router.transaction.TransactionInfo

/**
 * Transactions in remote aliases, which run in the database each targets on the DBMS at its URL,
 * logged in with the alias's credentials, which [passwords] decrypts. Each version of an alias has
 * a pool of connections, routed for `neo4j` schemes and direct for `bolt` ones, encrypted as the
 * scheme says, following the alias's driver settings and, for those it does not set, Neo4j's
 * `dbms.routing.driver.*` settings in [config]. ALTER ALIAS makes a new version; the pool of the
 * old one closes when its last transaction ends, and so does that of a dropped alias, as found
 * every [sweepInterval]. The credentials and driver settings are read in [system].
 */
class RemoteAliasTransactions(
    private val config: Config,
    private val system: () -> GraphDatabaseService,
    private val passwords: AliasPasswords,
    private val logProvider: InternalLogProvider,
    private val sweepInterval: Duration = Duration.ofMinutes(1),
) : DatabaseTransactionFactory<Location.Remote>, AutoCloseable {
    // Made with the first remote alias used: its event loop threads are otherwise not needed.
    private val provider: BoltConnectionProvider by lazy {
        NettyBoltConnectionProviderFactory()
            .create(
                DebugLogLogging(logProvider, config.get(LOGGING_LEVEL)),
                BoltValues,
                NoObservations,
                mapOf("eventLoopThreadNamePrefix" to "dodeca-remote-aliases"),
            )
    }
    private val log = logProvider.getLog(RemoteAliasTransactions::class.java)
    private val connections = ConcurrentHashMap<UUID, Connections>()
    @Volatile private var closed = false
    // Started with the first pool; guarded by this.
    private var sweeper: ScheduledExecutorService? = null

    override fun beginTransaction(
        location: Location.Remote,
        transactionInfo: TransactionInfo,
        bookmarkManager: TransactionBookmarkManager,
        terminationCallback: Consumer<Status>,
        // Null when Neo4j's query router begins the transaction with BEGIN, before any query.
        constituentTransactionFactory: ConstituentTransactionFactory?,
    ): DatabaseTransaction {
        val alias =
            location as? Location.Remote.External
                ?: throw IllegalArgumentException("Not a remote alias: $location")
        val subject = transactionInfo.loginContext().subject()
        if (subject.executingUser() != subject.authenticatedUser()) {
            throw impersonating(alias.locationName())
        }
        val connections = connections(alias.databaseReference())
        // The alias as it is now, which a transaction that began as it changed may not know yet.
        val current = Location.Remote.External(alias.graphId(), connections.reference)
        val bookmarks =
            bookmarkManager.getBookmarksForRemote(current).map { it.serializedState() }.toSet()
        val parameters =
            RoutedBoltConnectionParameters.builder()
                .withAuthToken(connections.token)
                .withMinVersion(MIN_VERSION)
                .withAccessMode(
                    if (transactionInfo.accessMode() == org.neo4j.boltmessages.AccessMode.READ) {
                        AccessMode.READ
                    } else {
                        AccessMode.WRITE
                    }
                )
                .withDatabaseName(DatabaseName.database(current.databaseName))
                .withBookmarks(bookmarks)
                .build()
        val transaction =
            RemoteTransaction(
                current,
                transactionInfo,
                { connections.connect(parameters) },
                null,
                bookmarks,
                bookmarkManager,
                terminationCallback,
                AliasFailures(alias.locationName()),
                connections::ended,
            )
        connections.open += transaction
        return transaction
    }

    /**
     * The connections of the version of the alias that [reference] names, or of its version now if
     * it changed, made on first use.
     */
    private fun connections(reference: DatabaseReferenceImpl.External): Connections =
        connections[reference.id()]
            ?: synchronized(this) {
                if (closed) throw shuttingDown(reference.alias().name())
                system().beginTx().use { tx ->
                    val model = CommunityTopologyGraphDbmsModel(tx)
                    val current =
                        model.getDatabaseRefByAlias(reference.catalogEntry()).orElse(null)
                            as? DatabaseReferenceImpl.External
                            ?: throw DatabaseNotFoundHelper.databaseNotFound(
                                reference.alias().name()
                            )
                    connections[current.id()]
                        ?: connect(model, current).also { made ->
                            // Older versions of the alias go once their transactions end.
                            for (old in connections.values) {
                                if (old.reference.alias() == current.alias()) old.retire()
                            }
                            connections[current.id()] = made
                            startSweeping()
                        }
                }
            }

    /** A pool of connections for [reference], with its credentials and settings in [model]. */
    private fun connect(
        model: CommunityTopologyGraphDbmsModel,
        reference: DatabaseReferenceImpl.External,
    ): Connections {
        val alias = reference.alias().name()
        val credentials =
            model.getExternalDatabaseCredentials(reference).orElseThrow {
                IllegalStateException("The remote alias '$alias' has no credentials.")
            }
        val settings =
            model
                .getDriverSettings(
                    alias,
                    reference
                        .namespace()
                        .map { it.name() }
                        .orElse(CommunityTopologyGraphDbmsModel.DEFAULT_NAMESPACE),
                )
                .orElse(null)
        val password =
            String(
                passwords.decrypt(
                    AliasPasswords.Encrypted(credentials.password(), credentials.iv())
                ),
                UTF_8,
            )
        val token = AuthTokens.basic(credentials.username(), password, null, BoltValues)
        val remote = reference.externalUri()
        val scheme = remote.scheme
        val address = remote.addresses.single()
        val logging =
            DebugLogLogging(
                logProvider,
                settings?.loggingLevel()?.orElse(null) ?: config.get(LOGGING_LEVEL),
            )
        val plan = securityPlan(scheme)
        val pool = { uri: URI, hostname: String? ->
            PooledBoltConnectionSource(
                logging,
                Clock.systemUTC(),
                uri,
                provider,
                NoDefaultLogin,
                {
                    CompletableFuture.completedStage(
                        if (plan != null && hostname != null && plan.expectedHostname() == null) {
                            SecurityPlans.encrypted(
                                plan.sslContext(),
                                plan.verifyHostname(),
                                hostname,
                            )
                        } else {
                            plan
                        }
                    )
                },
                (settings?.connectionPoolMaxSize()?.takeIf { it.isPresent }?.asInt
                        ?: config.get(MAX_POOL_SIZE))
                    .let { if (it < 0) Int.MAX_VALUE else it },
                setting(
                        settings,
                        DriverSettings::connectionPoolAcquisitionTimeout,
                        ACQUISITION_TIMEOUT,
                    )
                    .toMillis(),
                setting(settings, DriverSettings::connectionMaxLifetime, MAX_LIFETIME).toMillis(),
                (settings?.connectionPoolIdleTest()?.orElse(null) ?: config.get(IDLE_TEST))
                    ?.toMillis() ?: -1,
                NoObservations,
                "${address.hostname}:${address.port}",
                AGENT,
                USER_AGENT,
                setting(settings, DriverSettings::connectionTimeout, CONNECT_TIMEOUT)
                    .toMillis()
                    .toInt(),
                NotificationConfig.defaultConfig(),
                PooledBoltConnectionSource.TimeoutPolicy.LEGACY,
            )
        }
        return if (scheme.startsWith("neo4j")) {
            // The context of routing requests names the alias, which the other DBMS refuses to
            // route to a remote alias of its own: alias chains are not allowed.
            val query =
                listOfNotNull(
                        remote.query?.takeIf { it.isNotEmpty() },
                        "$ROUTING_ALIAS=${URLEncoder.encode(alias, UTF_8)}",
                    )
                    .joinToString("&")
            val routed =
                RoutedBoltConnectionSource(
                    { uri, hostname -> pool(uri, hostname) },
                    { setOf(it) },
                    DefaultDomainNameResolver.getInstance(),
                    ROUTING_TABLE_PURGE_DELAY.toMillis(),
                    null,
                    Clock.systemUTC(),
                    logging,
                    URI(scheme, null, address.hostname, address.port, null, query, null),
                    0,
                    emptyList(),
                    NoObservations,
                )
            Connections(reference, token, routed::getConnection) { routed.close() }
        } else {
            val direct =
                pool(URI(scheme, null, address.hostname, address.port, null, null, null), null)
            Connections(reference, token, direct::getConnection) { direct.close() }
        }
    }

    /** [setting] of the alias's driver settings if it sets it, else Neo4j's [default] setting. */
    private fun <T : Any> setting(
        settings: DriverSettings?,
        setting: (DriverSettings) -> java.util.Optional<T>,
        default: org.neo4j.graphdb.config.Setting<T>,
    ): T = settings?.let(setting)?.orElse(null) ?: config.get(default)

    /** The pools there are, of versions of aliases that are or were in use. */
    internal fun pools(): Int = connections.size

    /** Retires the connections of versions of aliases that changed or were dropped since. */
    internal fun sweep() {
        if (connections.isEmpty()) return
        val current =
            system().beginTx().use { tx ->
                // Remote aliases are in the root namespace: Dodeca has no composite databases.
                CommunityTopologyGraphDbmsModel(tx)
                    .allDatabaseReferences
                    .filterIsInstance<DatabaseReferenceImpl.External>()
                    .map { it.id() }
                    .toSet()
            }
        for (each in connections.values) {
            if (each.reference.id() !in current) each.retire()
        }
    }

    private fun startSweeping() {
        if (sweeper != null) return
        sweeper =
            Executors.newSingleThreadScheduledExecutor { runnable ->
                    Thread(runnable, "dodeca-remote-alias-pools").apply { isDaemon = true }
                }
                .also {
                    it.scheduleWithFixedDelay(
                        {
                            try {
                                sweep()
                            } catch (e: Exception) {
                                log.warn(
                                    "Closing pools of remote aliases that changed failed: %s",
                                    e.toString(),
                                )
                            }
                        },
                        sweepInterval.toMillis(),
                        sweepInterval.toMillis(),
                        TimeUnit.MILLISECONDS,
                    )
                }
    }

    /**
     * Why a transaction in remote alias [alias] cannot run: this server is shutting down, while it
     * still serves connections that were busy when it began. Clients retry, as on a server that is
     * unavailable.
     */
    private fun shuttingDown(alias: String) =
        RemoteFailure(
            Status.General.DatabaseUnavailable,
            "This server is shutting down and runs no more transactions in remote alias " +
                "'$alias'. Retry once it runs again, or through another server.",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, alias)
                .build(),
        )

    override fun close() {
        val closing =
            synchronized(this) {
                closed = true
                sweeper?.shutdownNow()
                sweeper = null
                connections.values.toList().also { connections.clear() }
            }
        for (each in closing) {
            for (transaction in each.open.toList()) {
                transaction.terminate(Status.General.DatabaseUnavailable)
            }
        }
        val closed = closing.map { it.close().toCompletableFuture() }
        try {
            CompletableFuture.allOf(*closed.toTypedArray()).get(CLOSE_TIMEOUT, TimeUnit.SECONDS)
        } finally {
            if (closing.isNotEmpty()) {
                provider.close().toCompletableFuture().get(CLOSE_TIMEOUT, TimeUnit.SECONDS)
            }
        }
    }

    /**
     * The connections of one version of an alias, [reference], logged in with [token], which
     * [connect] gives; [close] closes them. Retired, they close once [open] is empty.
     */
    private inner class Connections(
        val reference: DatabaseReferenceImpl.External,
        val token: AuthToken,
        val connect: (RoutedBoltConnectionParameters) -> CompletionStage<BoltConnection>,
        val close: () -> CompletionStage<Void>,
    ) {
        val open: MutableSet<RemoteTransaction> = ConcurrentHashMap.newKeySet()
        @Volatile private var retired = false

        fun retire() {
            retired = true
            closeIfDone()
        }

        fun ended(transaction: RemoteTransaction) {
            open -= transaction
            closeIfDone()
        }

        private fun closeIfDone() {
            if (!retired || open.isNotEmpty()) return
            val removed =
                synchronized(this@RemoteAliasTransactions) {
                    connections.values.remove(this) && open.isEmpty()
                }
            if (removed) close()
        }
    }

    /** Every connection logs in with the alias's credentials: there is no other login. */
    private object NoDefaultLogin : AuthTokenManager {
        override fun getToken(): CompletionStage<AuthToken> =
            CompletableFuture.failedStage(
                IllegalStateException("remote alias transactions log in with their credentials")
            )

        override fun handleBoltFailureException(
            authToken: AuthToken,
            exception: BoltFailureException,
        ): BoltFailureException = exception
    }

    companion object {
        /** Bolt 5.0, of Neo4j 5.0, the oldest version a remote alias can name. */
        val MIN_VERSION = BoltProtocolVersion(5, 0)

        /** The key of the routing context that names the alias a request comes from. */
        const val ROUTING_ALIAS = "alias"

        private const val CLOSE_TIMEOUT = 10L
        private val ROUTING_TABLE_PURGE_DELAY = Duration.ofSeconds(30)
        private val LOGGING_LEVEL = GraphDatabaseSettings.routing_driver_logging_level
        private val MAX_POOL_SIZE = GraphDatabaseSettings.routing_driver_max_connection_pool_size
        private val ACQUISITION_TIMEOUT =
            GraphDatabaseSettings.routing_driver_connection_acquisition_timeout
        private val MAX_LIFETIME = GraphDatabaseSettings.routing_driver_max_connection_lifetime
        private val IDLE_TEST =
            GraphDatabaseSettings.routing_driver_idle_time_before_connection_test
        private val CONNECT_TIMEOUT = GraphDatabaseSettings.routing_driver_connect_timeout
        private val AGENT =
            BoltAgent(
                "dodeca/${Dodeca.version}",
                "${System.getProperty("os.name")}; ${System.getProperty("os.arch")}",
                "Java/${System.getProperty("java.version")}",
                null,
            )
        private val USER_AGENT = "dodeca-remote-alias/${Dodeca.version}"

        /** The encryption the URL's [scheme] asks for: none, or TLS trusting what it says. */
        private fun securityPlan(scheme: String): SecurityPlan? =
            when {
                scheme.endsWith("+ssc") -> SecurityPlans.encryptedForAnyCertificate()
                scheme.endsWith("+s") -> SecurityPlans.encryptedForSystemCASignedCertificates()
                else -> null
            }

        private fun impersonating(alias: String) =
            RemoteFailure(
                Status.Statement.UnsupportedOperationError,
                "Impersonation is not supported through the remote database alias '$alias', " +
                    "whose transactions run as the user it names.",
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N31)
                    .withParam(GqlParams.StringParam.item, "Impersonation")
                    .withParam(GqlParams.StringParam.context, "remote database alias `$alias`")
                    .build(),
            )
    }
}

/**
 * What clients learn when a transaction in the remote alias [alias] fails: never the other DBMS's
 * own status, which would tell the client's driver about this server, as a remote "not a leader" or
 * "unauthorized" would, but a remote execution failure, whose GQL cause is the other DBMS's.
 */
private class AliasFailures(private val alias: String) : RemoteFailures {
    override fun answered(database: String, failure: BoltFailureException): RuntimeException {
        val (status, gql) =
            when (failure.code().split('.').getOrNull(1)) {
                "ClientError" ->
                    Status.Statement.RemoteExecutionClientError to GqlStatusInfoCodes.STATUS_50N06
                "TransientError" ->
                    Status.Statement.RemoteExecutionTransientError to
                        GqlStatusInfoCodes.STATUS_50N16
                else -> Status.Statement.RemoteExecutionFailed to GqlStatusInfoCodes.STATUS_50N17
            }
        return RemoteFailure(
            status,
            "Remote execution in database '$database' of remote alias '$alias' failed with " +
                "${failure.code()}: ${failure.message}",
            ErrorGqlStatusObjectImplementation.from(gql)
                .withCause(RemoteFailure.of(failure).gqlStatusObject())
                .build(),
            failure,
        )
    }

    override fun unreachable(database: String, cause: Throwable): RuntimeException {
        // A routing request the other DBMS refused, such as for an alias chain, comes as one for
        // which no router was available, with the refusal among the errors it suppressed.
        answer(cause)?.let {
            return answered(database, it)
        }
        // A certificate it does not trust will not change by trying again.
        val (status, gql) =
            if (generateSequence(cause) { it.cause }.any { it is SSLException }) {
                Status.Statement.RemoteExecutionClientError to GqlStatusInfoCodes.STATUS_08N16
            } else {
                Status.Statement.RemoteExecutionTransientError to GqlStatusInfoCodes.STATUS_08N17
            }
        val message = "Could not reach the DBMS of remote alias '$alias': ${cause.message}"
        return RemoteFailure(
            status,
            message,
            ErrorGqlStatusObjectImplementation.from(gql)
                .withParam(GqlParams.StringParam.msg, message)
                .build(),
            cause,
        )
    }

    /** The answer of the other DBMS in [error], its causes or the errors they suppressed. */
    private fun answer(error: Throwable): BoltFailureException? =
        generateSequence(error) { it.cause }
            .flatMap {
                sequenceOf(it) +
                    it.suppressed.asSequence().flatMap { suppressed ->
                        generateSequence(suppressed) { e -> e.cause }
                    }
            }
            .filterIsInstance<BoltFailureException>()
            .firstOrNull()

    override fun outcomeUnknown(database: String, cause: Throwable): RuntimeException =
        RemoteFailure(
            Status.Transaction.TransactionCommitFailed,
            "The DBMS of remote alias '$alias' did not answer the commit of a transaction, whose " +
                "outcome is unknown: it may or may not be committed. Check the data before " +
                "retrying.",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN01).build(),
            cause,
        )
}
