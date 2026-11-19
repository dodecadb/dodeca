package dev.dodeca.core

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.Transaction

/**
 * [graph], where the first [times] scans of the nodes with [label] each meet a node that
 * [vanishing] picks deleted by another transaction just after they found it, as a DROP applied
 * meanwhile does under read committed: reading it then fails as a read of the system graph does.
 */
class VanishingGraph(
    private val graph: GraphDatabaseService,
    private val label: Label,
    private val times: Int = 1,
    private val vanishing: (Node) -> Boolean,
) : GraphDatabaseService by graph {
    /** How many nodes were deleted so far. */
    val deleted = AtomicInteger()

    override fun beginTx(): Transaction = Scanning(graph.beginTx())

    override fun beginTx(timeout: Long, unit: TimeUnit): Transaction =
        Scanning(graph.beginTx(timeout, unit))

    private inner class Scanning(private val tx: Transaction) : Transaction by tx {
        override fun findNodes(label: Label): ResourceIterator<Node> {
            val nodes = tx.findNodes(label)
            if (label != this@VanishingGraph.label) return nodes
            var deletedHere = false
            return object : ResourceIterator<Node> by nodes {
                override fun next(): Node {
                    val node = nodes.next()
                    if (!deletedHere && deleted.get() < times && vanishing(node)) {
                        delete(node.elementId)
                        deletedHere = true
                        deleted.incrementAndGet()
                    }
                    return node
                }
            }
        }
    }

    private fun delete(elementId: String) {
        graph.beginTx().use { tx ->
            val node = tx.getNodeByElementId(elementId)
            node.relationships.forEach { it.delete() }
            node.delete()
            tx.commit()
        }
    }
}
