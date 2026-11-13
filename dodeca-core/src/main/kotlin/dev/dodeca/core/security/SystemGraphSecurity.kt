package dev.dodeca.core.security

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.logging.InternalLogProvider

/**
 * The users, roles and privileges of the system graph, read once and kept until [clear], which runs
 * after every change to the system graph.
 */
class SystemGraphSecurity(
    private val systemDatabase: SystemDatabaseProvider,
    logProvider: InternalLogProvider,
) {
    private class Cached(val generation: Long, val graph: SecurityGraph)

    private val log = logProvider.getLog(javaClass)
    private val generation = AtomicLong()
    private val cached = AtomicReference<Cached?>()

    fun isSuspended(user: String): Boolean = current().isSuspended(user)

    /** The home database of [user], or null for the DBMS default database. */
    fun homeDatabase(user: String): String? = current().homeDatabase(user)

    /** Forgets what was read: the system graph changed. */
    fun clear() {
        generation.incrementAndGet()
    }

    fun current(): SecurityGraph {
        // A read that overlaps a clear() keeps the generation from before it, so is read again.
        val current = generation.get()
        val last = cached.get()
        if (last != null && last.generation == current) return last.graph
        val graph =
            try {
                systemDatabase.query { SecurityGraph.read(it, log) }
            } catch (e: RuntimeException) {
                // As while a cluster member replaces system with a copy: keep what was read last.
                return last?.graph ?: throw e
            }
        cached.set(Cached(current, graph))
        return graph
    }
}
