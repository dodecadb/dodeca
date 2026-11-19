package dev.dodeca.core.edition

import dev.dodeca.core.SystemGraphReads
import java.util.Optional
import java.util.UUID
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.dbms.systemgraph.DefaultQueryLanguageLookup
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.TransactionFailureException
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListener
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryLimitExceededException
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobScheduler

/**
 * The default Cypher version of each database and remote alias, as the system graph gives it, which
 * Neo4j's query router asks for with the id of the session's database: a database's from its
 * `Database` node, a remote alias's from its node, under the id its reference has (the alias's
 * `version`). Ported from Neo4j Community's `CommunityDefaultQueryLanguageLookup`, which knows
 * databases only: it reads the system graph again, a little later, after each commit there or
 * [refresh], so a change shows after a short delay, as Neo4j documents.
 */
internal class DefaultLanguages(
    private val system: SystemDatabaseProvider,
    private val scheduler: JobScheduler,
    logs: InternalLogProvider,
) : DefaultQueryLanguageLookup {
    private val log = logs.getLog(javaClass)
    @Volatile private var running = false
    @Volatile private var languages: Map<NamedDatabaseId, CypherVersion> = emptyMap()

    override fun defaultLanguage(dbId: NamedDatabaseId?): Optional<CypherVersion> =
        Optional.ofNullable(dbId?.let { languages[it] })

    /** Has the system graph read again, a little later, unless stopped. */
    fun refresh() {
        if (running) scheduler.schedule(Group.TOPOLOGY_GRAPH_DBMS_MODEL, ::update)
    }

    /** Reads the system graph when started, before the server takes queries. */
    val life =
        object : LifecycleAdapter() {
            override fun start() {
                scheduler
                    .schedule(Group.TOPOLOGY_GRAPH_DBMS_MODEL) {
                        running = true
                        update()
                    }
                    .get()
            }

            override fun stop() {
                running = false
            }
        }

    /**
     * Refreshes after each commit to `system` here. It selects nothing of the commit, so Neo4j
     * gives it no data, transaction or database.
     */
    val listener =
        object : TransactionEventListener<Unit> {
            override fun beforeCommit(
                data: TransactionData?,
                transaction: Transaction?,
                databaseService: GraphDatabaseService?,
            ) {}

            override fun afterCommit(
                data: TransactionData?,
                state: Unit?,
                databaseService: GraphDatabaseService?,
            ) = refresh()

            override fun afterRollback(
                data: TransactionData?,
                state: Unit?,
                databaseService: GraphDatabaseService?,
            ) {}

            // Reads nothing of the commit.
            override fun transactionDataSelection(): Set<TransactionData.DataSelection> = emptySet()
        }

    private fun update() {
        var failures = 0
        while (running) {
            try {
                languages = SystemGraphReads.again { system.query(::read) }
                log.debug("Default query language snapshot update")
                return
            } catch (e: SystemDatabaseProvider.SystemDatabasePanickedException) {
                // A system database that panicked never comes back.
                running = false
                log.warn("Default query language update failed because of system db panic", e)
                return
            } catch (e: Exception) {
                if (
                    e !is TransactionFailureException &&
                        e !is MemoryLimitExceededException &&
                        e !is SystemDatabaseProvider.SystemDatabaseUnavailableException
                ) {
                    log.error("Failed to read default query language", e)
                    return
                }
                if (failures++ % 10 == 0) {
                    log.debug("Default query language update failed (attempt $failures)", e)
                }
                if (failures >= ATTEMPTS) {
                    log.error("Aborting default query language update after $failures attempts", e)
                    return
                }
                Thread.sleep(PAUSE_MILLIS)
            }
        }
    }

    private fun read(tx: Transaction): Map<NamedDatabaseId, CypherVersion> {
        val databases =
            tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                nodes
                    .asSequence()
                    .mapNotNull { node ->
                        language(node)?.let {
                            DatabaseIdFactory.from(
                                node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY)
                                    as String,
                                UUID.fromString(
                                    node.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY)
                                        as String
                                ),
                            ) to it
                        }
                    }
                    .toList()
            }
        // Remote aliases of the default namespace: there are no composite databases, whose
        // constituents would be in others.
        val aliases =
            tx.findNodes(TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL).use { nodes ->
                nodes
                    .asSequence()
                    .filter {
                        it.getProperty(TopologyGraphDbmsModel.NAMESPACE_PROPERTY, null) ==
                            TopologyGraphDbmsModel.DEFAULT_NAMESPACE
                    }
                    .mapNotNull { node ->
                        val version =
                            node.getProperty(TopologyGraphDbmsModel.VERSION_PROPERTY, null)
                                as? String
                        val language = language(node)
                        if (version == null || language == null) {
                            null
                        } else {
                            DatabaseIdFactory.from(
                                node.getProperty(TopologyGraphDbmsModel.NAME_PROPERTY) as String,
                                UUID.fromString(version),
                            ) to language
                        }
                    }
                    .toList()
            }
        return (databases + aliases).toMap()
    }

    private fun language(node: Node): CypherVersion? =
        CypherVersion.fromStoredValueOptional(
                node.getProperty(TopologyGraphDbmsModel.DATABASE_DEFAULT_LANGUAGE_PROPERTY, null)
            )
            .orElse(null)

    private companion object {
        const val ATTEMPTS = 100
        const val PAUSE_MILLIS = 100L
    }
}
