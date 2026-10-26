package org.dozendb.core.servers

import java.time.ZonedDateTime
import java.util.UUID
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction

/**
 * Where the system graph places databases, with Neo4j's names for it (`TopologyGraphDbmsModel`), as
 * Neo4j Enterprise keeps it: a `HOSTED_ON` relationship, with `mode` `raft`, from a `Database` node
 * to the `Instance` node of each server that hosts a primary copy, and the requested numbers of
 * copies in the database's `primaries` and `secondaries`. `system` has none: every server hosts it.
 * The default numbers for new databases are on the `TopologyGraphSettings` node.
 *
 * A database without any `HOSTED_ON` is not placed yet: the default database of a new cluster, or a
 * database of a server that formed a cluster with databases of its own.
 */
object Placements {
    private val DATABASE = TopologyGraphDbmsModel.DATABASE_LABEL
    private val INSTANCE = TopologyGraphDbmsModel.INSTANCE_LABEL
    private val HOSTED_ON = TopologyGraphDbmsModel.HOSTED_ON_RELATIONSHIP
    private val SETTINGS = TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_LABEL
    private const val PRIMARIES = TopologyGraphDbmsModel.DATABASE_PRIMARIES_PROPERTY
    private const val SECONDARIES = TopologyGraphDbmsModel.DATABASE_SECONDARIES_PROPERTY
    private const val DEFAULT_PRIMARIES =
        TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_PRIMARIES_PROPERTY
    private const val DEFAULT_SECONDARIES =
        TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_SECONDARIES_PROPERTY
    private const val SYSTEM = "system"
    private const val RAFT = "raft"

    /**
     * The default numbers of copies, as `initial.dbms.default_*_count` or the procedure set them.
     */
    data class Defaults(val primaries: Int, val secondaries: Int)

    /** Every database but `system`, with the servers it is placed on, as [Allocator] takes them. */
    fun placed(tx: Transaction): List<Placed> =
        tx.findNodes(DATABASE).use { nodes ->
            nodes
                .asSequence()
                .mapNotNull { node ->
                    val name = nameOrNull(node) ?: return@mapNotNull null
                    if (name == SYSTEM) return@mapNotNull null
                    val hosts = hosts(node)
                    Placed(
                        name,
                        primaries(node) ?: hosts.size,
                        online =
                            node.getProperty(
                                TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY,
                                null,
                            ) == TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName(),
                        hosts = hosts,
                    )
                }
                .toList()
        }

    /** The servers that the database [node] is placed on. */
    fun hosts(node: Node): Set<UUID> =
        node.getRelationships(Direction.OUTGOING, HOSTED_ON).use { relationships ->
            relationships.asSequence().mapNotNull { uuid(it.endNode) }.toSet()
        }

    /** The requested number of primaries of the database [node], if recorded. */
    fun primaries(node: Node): Int? = (node.getProperty(PRIMARIES, null) as? Number)?.toInt()

    /**
     * Places the database [node] on [hosts], with [primaries] requested: its `HOSTED_ON` follow,
     * and those to other servers go. A server without a record, a Free one, cannot host it.
     */
    fun place(tx: Transaction, node: Node, hosts: Set<UUID>, primaries: Int = hosts.size) {
        node.setProperty(PRIMARIES, primaries.toLong())
        node.setProperty(SECONDARIES, 0L)
        val kept = HashSet<UUID>()
        node.getRelationships(Direction.OUTGOING, HOSTED_ON).use { relationships ->
            for (relationship in relationships) {
                val id = uuid(relationship.endNode)
                if (id in hosts && kept.add(id!!)) continue
                relationship.delete()
            }
        }
        for (id in hosts - kept) {
            val server =
                tx.findNode(INSTANCE, TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "$id")
                    ?: throw IllegalArgumentException("server $id has no record")
            node.createRelationshipTo(server, HOSTED_ON).apply {
                setProperty(TopologyGraphDbmsModel.HOSTED_ON_MODE_PROPERTY, RAFT)
                setProperty(
                    TopologyGraphDbmsModel.HOSTED_ON_INSTALLED_AT_PROPERTY,
                    ZonedDateTime.now(),
                )
            }
        }
    }

    /** Records the hosts that [allocator] gives the databases that [moves] move. */
    fun move(tx: Transaction, allocator: Allocator, moves: List<Move>) {
        for (name in moves.map { it.database }.distinct()) {
            val node = database(tx, name) ?: continue
            place(tx, node, allocator.hosts(name), primaries(node) ?: allocator.hosts(name).size)
        }
    }

    /** The database node named [name], `system` aside. */
    fun database(tx: Transaction, name: String): Node? =
        tx.findNode(DATABASE, TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, name)?.takeIf {
            name != SYSTEM
        }

    /** The databases placed on the server [id], by name. */
    fun placedOn(tx: Transaction, id: UUID): Set<String> =
        tx.findNode(INSTANCE, TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "$id")
            ?.getRelationships(Direction.INCOMING, HOSTED_ON)
            ?.use { relationships ->
                relationships.asSequence().mapNotNull { nameOrNull(it.startNode) }.toSet()
            }
            .orEmpty()

    /** The default numbers of copies, or [fallback] while none are recorded. */
    fun defaults(tx: Transaction, fallback: Defaults): Defaults {
        val settings =
            tx.findNodes(SETTINGS).use { it.asSequence().firstOrNull() } ?: return fallback
        return Defaults(
            (settings.getProperty(DEFAULT_PRIMARIES, null) as? Number)?.toInt()
                ?: fallback.primaries,
            (settings.getProperty(DEFAULT_SECONDARIES, null) as? Number)?.toInt()
                ?: fallback.secondaries,
        )
    }

    /** Records the default numbers of copies. */
    fun setDefaults(tx: Transaction, defaults: Defaults) {
        val settings =
            tx.findNodes(SETTINGS).use { it.asSequence().firstOrNull() } ?: tx.createNode(SETTINGS)
        settings.setProperty(DEFAULT_PRIMARIES, defaults.primaries.toLong())
        settings.setProperty(DEFAULT_SECONDARIES, defaults.secondaries.toLong())
    }

    /**
     * An [Allocator] over the databases of the system graph and the recorded servers, unavailable
     * where [available] says so.
     */
    fun allocator(tx: Transaction, available: (UUID) -> Boolean): Allocator =
        Allocator(
            ServerRecords.records(tx).values.map { Host(it, available(it.id)) },
            placed(tx),
        )

    /** The name of the database [node]. */
    fun name(node: Node): String =
        node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY) as String

    private fun nameOrNull(node: Node): String? =
        node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null) as? String

    private fun uuid(node: Node): UUID? =
        (node.getProperty(TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, null) as? String)?.let {
            runCatching { UUID.fromString(it) }.getOrNull()
        }
}
