package org.dozendb.core.servers

import java.time.ZonedDateTime
import java.util.UUID
import org.dozendb.core.DozenDbSettings
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction

/**
 * An enabled server, as the system graph records it. A server of the cluster without a record is
 * Free: it hosts only `system`, until ENABLE SERVER records it.
 *
 * @property name the name it is known by, its id until RENAME SERVER changes it.
 * @property status enabled, cordoned (it takes no new copies) or deallocating (its copies move
 *   away).
 */
data class ServerRecord(
    val id: UUID,
    val name: String = id.toString(),
    val options: ServerOptions = ServerOptions(),
    val status: InstanceStatus = InstanceStatus.ENABLED,
)

/**
 * A server that DROP SERVER removed from the cluster.
 *
 * @property address where the cluster reached it, kept while it may still answer there.
 */
data class DroppedServer(val id: UUID, val name: String, val address: String?)

/** The options of a server that ENABLE SERVER and ALTER SERVER set, all of them each time. */
data class ServerOptions(
    val tags: List<String> = emptyList(),
    val modeConstraint: InstanceModeConstraint = InstanceModeConstraint.NONE,
    val allowedDatabases: List<String> = emptyList(),
    val deniedDatabases: List<String> = emptyList(),
) {
    companion object {
        /**
         * The options of the `initial.server.*` settings of [config], which a server is enabled
         * with when it is not given others.
         *
         * @throws IllegalArgumentException if they set both allowed and denied databases.
         */
        fun initial(config: Config): ServerOptions {
            val allowed = config.get(DozenDbSettings.initialServerAllowedDatabases)
            val denied = config.get(DozenDbSettings.initialServerDeniedDatabases)
            require(allowed.isEmpty() || denied.isEmpty()) {
                "${DozenDbSettings.initialServerAllowedDatabases.name()} and " +
                    "${DozenDbSettings.initialServerDeniedDatabases.name()} cannot both be set"
            }
            return ServerOptions(
                config.get(DozenDbSettings.initialServerTags),
                config.get(DozenDbSettings.initialServerModeConstraint),
                allowed,
                denied,
            )
        }
    }
}

/**
 * The servers in the system graph, with Neo4j's names for them (`TopologyGraphDbmsModel`), as Neo4j
 * Enterprise keeps them: an `Instance` node for each enabled server, and a `TopologyGraphSettings`
 * node whose `initial_instances_enabled` flag tells that the servers the cluster was formed with,
 * or had when it first recorded servers, are recorded, and whose `auto_enable_free_servers` flag
 * tells whether servers that join are enabled at once. A dropped server's node is a
 * `RemovedInstance` with the time it was removed, and DozenDB's `dozendb_raft_address` while it may
 * still answer there.
 */
object ServerRecords {
    private val INSTANCE = TopologyGraphDbmsModel.INSTANCE_LABEL
    private val REMOVED = TopologyGraphDbmsModel.REMOVED_INSTANCE_LABEL
    private const val ADDRESS = "dozendb_raft_address"
    private val SETTINGS = TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_LABEL
    private const val INITIAL_ENABLED =
        TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_INITIAL_INSTANCES_ENABLED_FLAG
    private const val AUTO_ENABLE =
        TopologyGraphDbmsModel.TOPOLOGY_GRAPH_CONFIG_AUTO_ENABLE_FREE_SERVERS_FLAG

    /**
     * Takes the lock that every change of the records takes first, so that changes on the `system`
     * leader happen one after the other: on the `system` database's node, which every system graph
     * has, as two changes that each created the settings node could not lock the other out.
     */
    fun lock(tx: Transaction) {
        val system =
            checkNotNull(
                tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                    SYSTEM_DATABASE_NAME,
                )
            ) {
                "the system graph has no node for the system database"
            }
        tx.acquireWriteLock(system)
    }

    /**
     * Merges duplicate settings nodes and records, which leaders in quick succession can write
     * before they see each other's; returns whether it changed anything.
     */
    fun removeDuplicates(tx: Transaction): Boolean {
        var changed = false
        val settings = tx.findNodes(SETTINGS).use { it.asSequence().toList() }
        if (settings.size > 1) {
            val kept = settings.first()
            for (extra in settings.drop(1)) {
                for (flag in listOf(INITIAL_ENABLED, AUTO_ENABLE)) {
                    if (extra.getProperty(flag, false) == true) kept.setProperty(flag, true)
                }
                extra.delete()
            }
            changed = true
        }
        val instances = tx.findNodes(INSTANCE).use { it.asSequence().toList() }
        instances
            .groupBy { it.getProperty(TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, null) }
            .values
            .filter { it.size > 1 }
            .forEach { copies ->
                copies.drop(1).forEach { it.delete() }
                changed = true
            }
        return changed
    }

    /** The recorded servers, by id. */
    fun records(tx: Transaction): Map<UUID, ServerRecord> =
        tx.findNodes(INSTANCE).use { nodes ->
            nodes.asSequence().mapNotNull(::record).associateBy { it.id }
        }

    /** Whether the servers the cluster started with are recorded; until then, all count enabled. */
    fun initialServersEnabled(tx: Transaction): Boolean =
        settings(tx)?.getProperty(INITIAL_ENABLED, false) == true

    /** Whether servers that join are enabled at once. */
    fun autoEnable(tx: Transaction): Boolean = settings(tx)?.getProperty(AUTO_ENABLE, false) == true

    /**
     * Records that the initial servers are recorded, with [autoEnable] as the first auto-enable.
     */
    fun markInitialServersEnabled(tx: Transaction, autoEnable: Boolean) {
        val settings = settings(tx) ?: tx.createNode(SETTINGS)
        settings.setProperty(INITIAL_ENABLED, true)
        if (!settings.hasProperty(AUTO_ENABLE)) settings.setProperty(AUTO_ENABLE, autoEnable)
    }

    fun setAutoEnable(tx: Transaction, autoEnable: Boolean) {
        (settings(tx) ?: tx.createNode(SETTINGS)).setProperty(AUTO_ENABLE, autoEnable)
    }

    /** Records [record], replacing the record of the same server. */
    fun write(tx: Transaction, record: ServerRecord) {
        val node =
            tx.findNode(INSTANCE, TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "${record.id}")
                ?: tx.createNode(INSTANCE).also {
                    it.setProperty(TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "${record.id}")
                    it.setProperty(
                        TopologyGraphDbmsModel.INSTANCE_DISCOVERED_AT_PROPERTY,
                        ZonedDateTime.now(),
                    )
                }
        node.setProperty(TopologyGraphDbmsModel.INSTANCE_NAME_PROPERTY, record.name)
        node.setProperty(TopologyGraphDbmsModel.INSTANCE_STATUS_PROPERTY, record.status.name)
        val options = record.options
        node.setProperty(TopologyGraphDbmsModel.INSTANCE_TAGS_PROPERTY, options.tags.toTypedArray())
        node.setProperty(
            TopologyGraphDbmsModel.INSTANCE_MODE_CONSTRAINT_PROPERTY,
            options.modeConstraint.name,
        )
        node.setProperty(
            TopologyGraphDbmsModel.INSTANCE_ALLOWED_DATABASES_PROPERTY,
            options.allowedDatabases.toTypedArray(),
        )
        node.setProperty(
            TopologyGraphDbmsModel.INSTANCE_DENIED_DATABASES_PROPERTY,
            options.deniedDatabases.toTypedArray(),
        )
    }

    /** Deletes the record of the server [id], if there is one, with the databases placed on it. */
    fun delete(tx: Transaction, id: UUID) {
        tx.findNode(INSTANCE, TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "$id")?.let(::detach)
    }

    /**
     * Records the server [id] dropped, as DROP SERVER does, with its [name] and the [address] where
     * the cluster reaches it: its record, if it has one, becomes a `RemovedInstance`.
     */
    fun drop(tx: Transaction, id: UUID, name: String, address: String?) {
        val node =
            tx.findNode(INSTANCE, TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "$id")?.also {
                it.relationships.use { relationships -> relationships.forEach { r -> r.delete() } }
                it.removeLabel(INSTANCE)
                it.addLabel(REMOVED)
            }
                ?: tx.createNode(REMOVED).also {
                    it.setProperty(TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "$id")
                }
        node.setProperty(TopologyGraphDbmsModel.INSTANCE_NAME_PROPERTY, name)
        node.setProperty(
            TopologyGraphDbmsModel.REMOVED_INSTANCE_REMOVED_AT_PROPERTY,
            ZonedDateTime.now(),
        )
        if (address != null) node.setProperty(ADDRESS, address)
    }

    /** The dropped servers, by id. */
    fun dropped(tx: Transaction): Map<UUID, DroppedServer> =
        tx.findNodes(REMOVED).use { nodes ->
            nodes
                .asSequence()
                .mapNotNull { node ->
                    val id = id(node) ?: return@mapNotNull null
                    DroppedServer(
                        id,
                        node.getProperty(TopologyGraphDbmsModel.INSTANCE_NAME_PROPERTY, null)
                            as? String ?: id.toString(),
                        node.getProperty(ADDRESS, null) as? String,
                    )
                }
                .associateBy { it.id }
        }

    /** Forgets the address of the dropped server [id], which no longer answers there. */
    fun forgetAddress(tx: Transaction, id: UUID) {
        tx.findNodes(REMOVED, TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, "$id").use { nodes ->
            nodes.forEach { it.removeProperty(ADDRESS) }
        }
    }

    /**
     * Deletes every record, with the databases placed on them, the dropped servers, and both flags,
     * as when a cluster adopts another one's system graph.
     */
    fun deleteAll(tx: Transaction) {
        tx.findNodes(INSTANCE).use { nodes -> nodes.forEach(::detach) }
        tx.findNodes(REMOVED).use { nodes -> nodes.forEach(::detach) }
        tx.findNodes(SETTINGS).use { nodes ->
            nodes.forEach {
                it.removeProperty(INITIAL_ENABLED)
                it.removeProperty(AUTO_ENABLE)
            }
        }
    }

    private fun detach(node: Node) {
        node.relationships.use { relationships -> relationships.forEach { it.delete() } }
        node.delete()
    }

    private fun settings(tx: Transaction): Node? =
        tx.findNodes(SETTINGS).use { nodes -> nodes.asSequence().firstOrNull() }

    private fun id(node: Node): UUID? =
        (node.getProperty(TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY, null) as? String)?.let {
            runCatching { UUID.fromString(it) }.getOrNull()
        }

    private fun record(node: Node): ServerRecord? {
        val id = id(node) ?: return null
        return ServerRecord(
            id,
            node.getProperty(TopologyGraphDbmsModel.INSTANCE_NAME_PROPERTY, null) as? String
                ?: id.toString(),
            ServerOptions(
                strings(node, TopologyGraphDbmsModel.INSTANCE_TAGS_PROPERTY),
                (node.getProperty(TopologyGraphDbmsModel.INSTANCE_MODE_CONSTRAINT_PROPERTY, null)
                        as? String)
                    ?.let { runCatching { InstanceModeConstraint.valueOf(it) }.getOrNull() }
                    ?: InstanceModeConstraint.NONE,
                strings(node, TopologyGraphDbmsModel.INSTANCE_ALLOWED_DATABASES_PROPERTY),
                strings(node, TopologyGraphDbmsModel.INSTANCE_DENIED_DATABASES_PROPERTY),
            ),
            (node.getProperty(TopologyGraphDbmsModel.INSTANCE_STATUS_PROPERTY, null) as? String)
                ?.let { runCatching { InstanceStatus.getInstanceStatus(it) }.getOrNull() }
                ?: InstanceStatus.ENABLED,
        )
    }

    private fun strings(node: Node, property: String): List<String> =
        (node.getProperty(property, null) as? Array<*>)?.map { it as String }.orEmpty()
}
