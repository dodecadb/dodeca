package dev.dodeca.cluster.internal

import java.io.OutputStream
import java.util.concurrent.atomic.AtomicReference
import org.neo4j.logging.InternalLog
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.log4j.Log4jLog
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.logging.log4j.LoggerTarget
import org.neo4j.server.Bootstrapper
import org.neo4j.server.logging.slf4j.SLF4JLogBridge
import org.neo4j.util.FeatureToggles
import org.slf4j.LoggerFactory
import org.slf4j.helpers.NOPLogger

/**
 * Routes the SLF4J logging of Ratis and Dodeca to Neo4j's debug log.
 *
 * Neo4j discards all SLF4J logging unless the `useNeo4jSlf4jProvider` feature toggle is on, in
 * which case it forwards Jetty only, to the user log. Either way Raft errors would be invisible.
 * This keeps Jetty exactly as Neo4j configures it and adds Ratis and Dodeca.
 *
 * Neo4j's bridge binds its logger factory once, when SLF4J initialises, so the routing is installed
 * by the bootstrapper before anything logs through SLF4J. A logger is bound to its destination when
 * it is created, which for Ratis and Dodeca happens after the edition module has supplied the debug
 * log provider.
 */
internal object Slf4jLogRouting {
    private const val JETTY = "org.eclipse.jetty"
    private val routedPrefixes = listOf("org.apache.ratis", "dev.dodeca")
    private val debugLogProvider = AtomicReference<Log4jLogProvider?>()

    /** Installs the routing. Returns false, after logging why, if it could not be installed. */
    fun install(userLogProvider: InternalLogProvider, log: InternalLog): Boolean {
        val user =
            userLogProvider as? Log4jLogProvider
                ?: run {
                    log.warn("Raft library logs are discarded: unexpected user log provider")
                    return false
                }
        val neo4jForwardsJetty =
            FeatureToggles.flag(Bootstrapper::class.java, "useNeo4jSlf4jProvider", false)
        val prefixes = if (neo4jForwardsJetty) routedPrefixes + JETTY else routedPrefixes
        SLF4JLogBridge.setInstantiationContext(DispatchingLogProvider(user), prefixes)
        // Initialises SLF4J now, with the routing above, unless something initialised it earlier.
        val routed = LoggerFactory.getLogger("dev.dodeca.cluster.LogRoutingCheck") !is NOPLogger
        if (!routed) {
            log.warn("Raft library logs are discarded: SLF4J was initialised before Dodeca started")
        }
        return routed
    }

    /** Supplies the debug log, once Neo4j has created it. */
    fun useDebugLogProvider(provider: InternalLogProvider) {
        debugLogProvider.set(provider as? Log4jLogProvider)
    }

    /** Sends Jetty to the user log, as Neo4j does, and Dodeca and Ratis to the debug log. */
    private class DispatchingLogProvider(private val user: Log4jLogProvider) :
        Log4jLogProvider(OutputStream.nullOutputStream()) {
        private fun destination(name: String): Log4jLogProvider =
            if (name.startsWith(JETTY)) user else debugLogProvider.get() ?: user

        override fun getLog(name: String): Log4jLog = destination(name).getLog(name)

        override fun getLog(loggingClass: Class<*>): Log4jLog =
            destination(loggingClass.name).getLog(loggingClass)

        override fun getLog(target: LoggerTarget): Log4jLog =
            destination(target.target).getLog(target)
    }
}
