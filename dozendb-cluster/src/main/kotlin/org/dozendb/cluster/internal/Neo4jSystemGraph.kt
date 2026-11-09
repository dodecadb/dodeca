package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.cluster.FormatLevelStore
import org.dozendb.cluster.FormatLevels
import org.dozendb.cluster.internal.replication.DesiredDatabase
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.replication.SystemGraph
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.core.servers.CopyMode
import org.dozendb.core.servers.Placements
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
 * The databases in this member's copy of the system graph: its `Database` nodes, with the servers
 * they are placed on (see [Placements]), Neo4j's `seedURI` and `seeding_servers` properties of a
 * seeded database, a `dozendb_creator` property that names the member creating the database's
 * store, a `dozendb_initial_members` property that lists the initial members of its group, each as
 * `id@host:port`, and a `dozendb_created` property, true once the store was created, after which
 * every server it is placed on copies it, its creator too. A `DozenDB` node holds the cluster's
 * format level in `dozendb_format_level`, once an upgrade raised it.
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
            creator = creator(node),
            seedUri =
                node.getProperty(TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY, null)
                    as? String,
            designatedSeeder =
                (node.getProperty(TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY, null)
                        as? Array<*>)
                    ?.firstOrNull()
                    ?.let { MemberId.parse(it as String) },
            hosts = Placements.hosts(node, CopyMode.PRIMARY).mapTo(HashSet(), ::MemberId),
            secondaries = Placements.hosts(node, CopyMode.SECONDARY).mapTo(HashSet(), ::MemberId),
            created = created(node),
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
     * Makes [creator] the creator of every database whose store it [holds], and the only initial
     * member of its group, and returns those databases; forgets the creators and initial members
     * that another cluster recorded of the others, and its servers and placements, which the new
     * cluster's leader records anew. Only for the member that forms a cluster with databases of its
     * own, before `system` replicates. Their groups start with it alone; the system leader places
     * the databases once it recorded the servers, those that hold them first, and the other servers
     * they are placed on copy them and join their groups. The others are created anew.
     */
    fun adopt(creator: Member, holds: (String) -> Boolean): List<NamedDatabaseId> =
        system().beginTx().use { tx ->
            ServerRecords.deleteAll(tx)
            val members = arrayOf(creator.toText())
            val databases =
                tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                    nodes
                        .asSequence()
                        .mapNotNull { node ->
                            val id = desired(node)?.id ?: return@mapNotNull null
                            if (holds(id.name())) {
                                node.setProperty(CREATOR_PROPERTY, creator.id.toString())
                                node.setProperty(INITIAL_MEMBERS_PROPERTY, members)
                                node.setProperty(CREATED_PROPERTY, true)
                                id
                            } else {
                                node.removeProperty(CREATOR_PROPERTY)
                                node.removeProperty(INITIAL_MEMBERS_PROPERTY)
                                node.removeProperty(CREATED_PROPERTY)
                                null
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

    /**
     * Whether [member] is an enabled server, which databases can be placed on, as is every member
     * while the servers the cluster started with are not recorded yet. A Free one hosts only
     * `system`.
     */
    fun enabled(member: MemberId): Boolean = replication.readingSystem {
        system().beginTx().use { tx ->
            !ServerRecords.initialServersEnabled(tx) || member.uuid in ServerRecords.records(tx)
        }
    }

    /**
     * The servers that host a copy of the database [name], a primary or a secondary; none if it is
     * not placed, or does not exist.
     */
    fun hosts(name: String): Set<MemberId> = replication.readingSystem {
        system().beginTx().use { tx ->
            Placements.database(tx, name)
                ?.let(Placements::copies)
                ?.keys
                ?.mapTo(HashSet(), ::MemberId) ?: emptySet()
        }
    }

    /**
     * The servers that host a secondary of the database [name]: for `system`, the system
     * secondaries; none if it does not exist.
     */
    fun secondaries(name: String): Set<MemberId> = replication.readingSystem {
        system().beginTx().use { tx ->
            if (name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
                SystemSecondaryRecords.all(tx).mapTo(HashSet()) { it.id }
            } else {
                Placements.database(tx, name)
                    ?.let { Placements.hosts(it, CopyMode.SECONDARY) }
                    ?.mapTo(HashSet(), ::MemberId) ?: emptySet()
            }
        }
    }

    /** The system secondaries, servers that pull `system` instead of voting in its group. */
    fun systemSecondaries(): List<Member> = replication.readingSystem {
        system().beginTx().use { tx -> SystemSecondaryRecords.all(tx) }
    }

    /** The server tags of each enabled server, from its server record. */
    fun serverTags(): Map<MemberId, Set<String>> = replication.readingSystem {
        system().beginTx().use { tx ->
            ServerRecords.records(tx).entries.associate { (id, record) ->
                MemberId(id) to record.options.tags.toSet()
            }
        }
    }

    /** The id of the database [name], or null if the system graph has none of that name. */
    fun databaseId(name: String): UUID? = replication.readingSystem {
        system().beginTx().use { tx ->
            val node =
                tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                    name,
                )
            (node?.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY, null) as? String)
                ?.let(UUID::fromString)
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

    companion object {
        /** The property that names the creator of a database, see [nameCreator]. */
        const val CREATOR_PROPERTY = "dozendb_creator"
        private const val CREATED_PROPERTY = "dozendb_created"

        /** The member that creates the store of the database [node], once one is named. */
        fun creator(node: Node): MemberId? =
            (node.getProperty(CREATOR_PROPERTY, null) as? String)?.let(MemberId::parse)

        /** Whether the store of the database [node] was created. */
        fun created(node: Node): Boolean = node.getProperty(CREATED_PROPERTY, false) == true

        /** Records that the store of the database [node] was created. */
        fun markCreated(node: Node) {
            node.setProperty(CREATED_PROPERTY, true)
        }

        private const val INITIAL_MEMBERS_PROPERTY = "dozendb_initial_members"
        private const val FORMAT_LEVEL_PROPERTY = "dozendb_format_level"
        private val DOZENDB_LABEL: Label = Label.label("DozenDB")
    }
}
