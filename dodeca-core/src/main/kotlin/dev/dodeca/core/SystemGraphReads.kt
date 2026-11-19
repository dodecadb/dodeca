package dev.dodeca.core

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException

/**
 * Reads of the system graph that other transactions change while they read. Neo4j reads committed
 * data: a node or relationship that a read found, as by a scan, can be deleted by another
 * transaction, as a DROP applied from the Raft log, before the read gets to its properties or
 * relationships, which then fail with a [NotFoundException]. Read again, in a new transaction, it
 * is gone.
 */
object SystemGraphReads {
    /** How many times a read is made before its failure stands. */
    const val ATTEMPTS = 3

    /**
     * Runs [read] in a new transaction of [system], and again in a new one while a node or
     * relationship it read was deleted meanwhile (see [again]).
     */
    fun <T> read(system: GraphDatabaseService, read: (Transaction) -> T): T = again {
        system.beginTx().use(read)
    }

    /**
     * Runs [read], which reads the system graph in transactions of its own, and again while a node
     * or relationship it read was deleted meanwhile, up to [ATTEMPTS] times in all.
     */
    fun <T> again(read: () -> T): T {
        repeat(ATTEMPTS - 1) {
            try {
                return read()
            } catch (e: RuntimeException) {
                if (!vanished(e)) throw e
            }
        }
        return read()
    }

    /**
     * Whether [failure] tells that a node or relationship was deleted while it was read, unlike a
     * [NotFoundException] for a property or an id that was never there.
     */
    fun vanished(failure: Throwable): Boolean =
        generateSequence(failure) { it.cause }.any { it is EntityNotFoundException }
}
