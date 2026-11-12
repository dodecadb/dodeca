package org.dozendb.core.edition

import java.util.UUID
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListenerAdapter
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider

/**
 * Makes this server's databases follow the system graph when clustering is disabled; with
 * clustering the `DatabaseReconciler` does this.
 *
 * Once at startup, and after every commit to `system` that touches a `Database` node, it creates
 * the databases the system graph names, drops the ones it no longer names, and starts or stops them
 * as their status asks. Only after commit, so a DROP DATABASE that fails drops nothing, and every
 * database a transaction changes is followed.
 *
 * `system` and the default database are created at startup and never created or dropped here; the
 * default database is started and stopped like any other. A database that failed to start is
 * started again when it is asked to start again: by START DATABASE, which records when it was
 * started even if its status stays online, as Neo4j Enterprise documents for retrying a start.
 *
 * A database in [quarantines] is left as it is, stopped, until its quarantine is lifted, which
 * follows the system graph again; one that is dropped goes with its quarantine. The quarantines of
 * databases that the system graph no longer names are forgotten.
 */
internal class StandaloneDatabases(
    private val manager: DatabaseManager,
    private val repository: DatabaseRepository<StandaloneDatabaseContext>,
    private val seeder: () -> DatabaseSeeder?,
    private val quarantines: Quarantines,
    logProvider: InternalLogProvider,
) : TransactionEventListenerAdapter<Any?>() {
    private val log = logProvider.getLog(javaClass)

    @Volatile private var started = false

    /** What each database was last asked, guarded by `this`. */
    private val requested = HashMap<NamedDatabaseId, Request>()

    /**
     * What the system graph asks of a database: whether it is online, its seed if any, and whether
     * it is the default database.
     */
    private data class Desired(
        val online: Boolean,
        val seed: UriSeed?,
        val default: Boolean,
        val request: Request,
    )

    /**
     * A request for a database's status: each START DATABASE records when it was started, so that
     * starting it again is a new request even if it is online already.
     */
    private data class Request(val online: Boolean, val startedAt: Any?)

    /** Follows the system graph from when it starts, once `system` and the default database run. */
    fun starter(): Lifecycle =
        object : LifecycleAdapter() {
            override fun start() {
                started = true
                follow()
            }

            override fun stop() {
                started = false
            }
        }

    override fun afterCommit(data: TransactionData, state: Any?, system: GraphDatabaseService) {
        if (started && touchesDatabases(data)) follow()
    }

    private fun touchesDatabases(data: TransactionData): Boolean =
        data.assignedLabels().any { it.label() == TopologyGraphDbmsModel.DATABASE_LABEL } ||
            data.removedLabels().any { it.label() == TopologyGraphDbmsModel.DATABASE_LABEL } ||
            data.assignedNodeProperties().any {
                it.key() == TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY ||
                    it.key() == TopologyGraphDbmsModel.DATABASE_STARTED_AT_PROPERTY
            }

    /** Follows the system graph now, as once a quarantine is lifted. */
    @Synchronized
    fun follow() {
        if (!started) return
        val desired = desired()
        val default = desired.entries.firstOrNull { it.value.default }?.key
        repository
            .registeredDatabases()
            .keys
            .filter { !it.isSystemDatabase && it != default && it !in desired }
            .forEach { id -> attempt("drop", id) { manager.dropDatabase(id) } }
        requested.keys.retainAll(desired.keys)
        quarantines.retainOnly(
            (desired.keys + repository.registeredDatabases().keys).mapTo(HashSet()) {
                it.databaseId().uuid()
            }
        )
        desired.forEach { (id, wanted) -> attempt("follow", id) { follow(id, wanted) } }
    }

    private fun follow(id: NamedDatabaseId, desired: Desired) {
        val online = desired.online
        val changed = requested.put(id, desired.request) != desired.request
        val context = repository.getDatabaseContext(id).orElse(null)
        val seed = desired.seed?.let { DatabaseManager.Seed.Uri(it, seeder) }
        if (context != null && quarantines.of(id.databaseId().uuid()) != null) return
        when {
            context == null && desired.default -> Unit
            context == null -> {
                val created = manager.createDatabase(id)
                if (online) manager.startDatabase(created, seed)
            }
            online -> {
                if (!context.database().isStarted && (changed || !context.isFailed)) {
                    manager.startDatabase(context, seed)
                }
            }
            context.database().isStarted || changed -> manager.stopDatabase(context)
        }
    }

    private fun attempt(action: String, id: NamedDatabaseId, work: () -> Unit) {
        try {
            work()
        } catch (e: Exception) {
            log.error("Failed to $action '$id'.", e)
        }
    }

    /** The databases the system graph names other than `system`, and what it asks of them. */
    private fun desired(): Map<NamedDatabaseId, Desired> {
        val system =
            repository
                .getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow()
                .databaseFacade()
        return system.beginTx().use { tx ->
            tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                nodes
                    .asSequence()
                    .mapNotNull { node ->
                        val name =
                            node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null)
                                as? String
                        val uuid =
                            node.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY, null)
                                as? String
                        if (name == null || uuid == null) return@mapNotNull null
                        val status =
                            node.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY, null)
                        val online =
                            status == TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName()
                        DatabaseIdFactory.from(name, UUID.fromString(uuid)) to
                            Desired(
                                online,
                                UriSeed.of(node),
                                node.getProperty(
                                    DefaultDatabase.PROPERTY,
                                    false,
                                ) == true,
                                Request(
                                    online,
                                    node.getProperty(
                                        TopologyGraphDbmsModel.DATABASE_STARTED_AT_PROPERTY,
                                        null,
                                    ),
                                ),
                            )
                    }
                    .filter { (id, _) -> !id.isSystemDatabase }
                    .toMap()
            }
        }
    }
}
