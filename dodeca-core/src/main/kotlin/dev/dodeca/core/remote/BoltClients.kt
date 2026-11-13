package dev.dodeca.core.remote

import java.net.URI
import java.util.function.BiConsumer
import java.util.function.Supplier
import org.neo4j.bolt.connection.BoltProtocolVersion
import org.neo4j.bolt.connection.LoggingProvider
import org.neo4j.bolt.connection.observation.BoltExchangeObservation
import org.neo4j.bolt.connection.observation.HttpExchangeObservation
import org.neo4j.bolt.connection.observation.ImmutableObservation
import org.neo4j.bolt.connection.observation.Observation
import org.neo4j.bolt.connection.pooled.observation.PoolObservationProvider
import org.neo4j.logging.InternalLog
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.Level

/** bolt-connection's logs, in this server's debug log, from [threshold] on. */
class DebugLogLogging(
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
            // Not vararg: the JDK's System.Logger.log(Level, String), which bolt-connection's
            // channel loggers inherit, passes a null array on.
            params: Array<out Any?>?,
        ) {
            if (!isLoggable(level)) return
            val message =
                if (params.isNullOrEmpty()) format ?: ""
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
object NoObservations : PoolObservationProvider {
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
