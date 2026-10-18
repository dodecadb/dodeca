package org.dozendb.cluster.core.internal

import java.util.UUID
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.core.internal.replication.DesiredDatabase
import org.dozendb.cluster.core.internal.replication.ReplicationService
import org.dozendb.cluster.core.internal.replication.SystemGraph
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * The databases in this member's copy of the system graph: DozerDB's `Database` nodes, with a
 * `dozendb_creator` property that names the member creating the database's store.
 */
internal class Neo4jSystemGraph(
    private val system: () -> GraphDatabaseService,
    private val replication: ReplicationService,
) : SystemGraph {
    override fun databases(): List<DesiredDatabase> =
        system().beginTx().use { tx ->
            tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                nodes.asSequence().mapNotNull(::desired).toList()
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
        )
    }

    override fun awaitCurrent(): Boolean = replication.awaitSystemCurrent()

    override fun leading(): Boolean = replication.systemWritable

    override fun nameCreator(database: NamedDatabaseId, creator: MemberId) {
        system().beginTx().use { tx ->
            val node =
                tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY,
                    database.databaseId().uuid().toString(),
                ) ?: return
            if (node.hasProperty(CREATOR_PROPERTY)) return
            node.setProperty(CREATOR_PROPERTY, creator.toString())
            tx.commit()
        }
    }

    private companion object {
        const val CREATOR_PROPERTY = "dozendb_creator"
    }
}
