package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.cluster.FormatLevelStore
import org.dozendb.cluster.FormatLevels
import org.dozendb.cluster.internal.replication.DesiredDatabase
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.replication.SystemGraph
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.core.servers.ServerRecords
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * The databases in this member's copy of the system graph: its `Database` nodes, with Neo4j's
 * `seedURI` and `seeding_servers` properties of a seeded database, a `dozendb_creator` property
 * that names the member creating the database's store, and a `dozendb_initial_members` property
 * that lists the initial members of its group, each as `id@host:port`. A `DozenDB` node holds the
 * cluster's format level in `dozendb_format_level`, once an upgrade raised it.
 */
internal class Neo4jSystemGraph(
    private val system: () -> GraphDatabaseService,
    private val replication: ReplicationService,
) : SystemGraph, FormatLevelStore {
    override fun databases(): List<DesiredDatabase> = replication.readingSystem {
        system().beginTx().use { tx ->
            tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                nodes.asSequence().mapNotNull(::desired).toList()
            }
        }
    }

    private fun desired(node: Node): DesiredDatabase? {
        val name = node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null) as? String
        val uuid = node.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY, null) as? String
        if (name == null || uuid == null || name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
            return null
        }
        val status = node.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY, null)
        return DesiredDatabase(
            DatabaseIdFactory.from(name, UUID.fromString(uuid)),
            online = status == TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName(),
            creator = (node.getProperty(CREATOR_PROPERTY, null) as? String)?.let(MemberId::parse),
            seedUri =
                node.getProperty(TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY, null)
                    as? String,
            designatedSeeder =
                (node.getProperty(TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY, null)
                        as? Array<*>)
                    ?.firstOrNull()
                    ?.let { MemberId.parse(it as String) },
        )
    }

    // Not while reading: a member behind the log confirms only once `system` was replaced.
    override fun awaitCurrent(): Boolean = replication.awaitSystemCurrent()

    override fun leading(): Boolean = replication.systemWritable

    override fun nameCreator(
        database: NamedDatabaseId,
        creator: MemberId,
        initialMembers: List<Member>,
    ) = replication.readingSystem { writeCreator(database, creator, initialMembers) }

    private fun writeCreator(
        database: NamedDatabaseId,
        creator: MemberId,
        initialMembers: List<Member>,
    ) {
        system().beginTx().use { tx ->
            val node = tx.findDatabase(database) ?: return
            if (node.hasProperty(CREATOR_PROPERTY)) return
            node.setProperty(CREATOR_PROPERTY, creator.toString())
            node.setProperty(
                INITIAL_MEMBERS_PROPERTY,
                initialMembers.map { it.toText() }.toTypedArray(),
            )
            tx.commit()
        }
    }

    /**
     * Makes [creator] the creator of every database, with [initialMembers] as the initial members
     * of its group, replacing what another cluster recorded, servers included, which the new
     * cluster's leader records anew, and returns the databases. Only for the member that forms a
     * cluster with a system database of its own, before `system` replicates: it holds the
     * databases' stores, or creates them.
     */
    fun adopt(creator: MemberId, initialMembers: List<Member>): List<NamedDatabaseId> =
        system().beginTx().use { tx ->
            ServerRecords.deleteAll(tx)
            val members = initialMembers.map { it.toText() }.toTypedArray()
            val databases =
                tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                    nodes
                        .asSequence()
                        .mapNotNull { node ->
                            desired(node)?.id?.also {
                                node.setProperty(CREATOR_PROPERTY, creator.toString())
                                node.setProperty(INITIAL_MEMBERS_PROPERTY, members)
                            }
                        }
                        .toList()
                }
            tx.commit()
            databases.sortedBy { it.name() }
        }

    override fun formatLevel(): Int = replication.readingSystem {
        system().beginTx().use { tx ->
            tx.findNodes(DOZENDB_LABEL).use { nodes ->
                nodes
                    .asSequence()
                    .mapNotNull {
                        (it.getProperty(FORMAT_LEVEL_PROPERTY, null) as? Number)?.toInt()
                    }
                    .maxOrNull() ?: FormatLevels.INITIAL
            }
        }
    }

    override fun recordFormatLevel(level: Int) = replication.readingSystem {
        system().beginTx().use { tx ->
            val node =
                tx.findNodes(DOZENDB_LABEL).use { it.asSequence().firstOrNull() }
                    ?: tx.createNode(DOZENDB_LABEL)
            node.setProperty(FORMAT_LEVEL_PROPERTY, level.toLong())
            tx.commit()
        }
    }

    /** The initial members of the group of [database], or null if none are recorded. */
    fun initialMembers(database: NamedDatabaseId): List<Member>? = replication.readingSystem {
        system().beginTx().use { tx ->
            val members = tx.findDatabase(database)?.getProperty(INITIAL_MEMBERS_PROPERTY, null)
            (members as? Array<*>)?.map { Member.parse(it as String) }
        }
    }

    private fun Transaction.findDatabase(database: NamedDatabaseId): Node? =
        findNode(
            TopologyGraphDbmsModel.DATABASE_LABEL,
            TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY,
            database.databaseId().uuid().toString(),
        )

    private companion object {
        const val CREATOR_PROPERTY = "dozendb_creator"
        const val INITIAL_MEMBERS_PROPERTY = "dozendb_initial_members"
        const val FORMAT_LEVEL_PROPERTY = "dozendb_format_level"
        val DOZENDB_LABEL: Label = Label.label("DozenDB")
    }
}
