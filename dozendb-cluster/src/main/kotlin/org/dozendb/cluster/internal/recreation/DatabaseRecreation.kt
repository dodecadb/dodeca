package org.dozendb.cluster.internal.recreation

import java.time.ZonedDateTime
import java.util.UUID
import org.dozendb.cluster.internal.ClusterPlacement
import org.dozendb.cluster.internal.Neo4jSystemGraph
import org.dozendb.core.edition.DatabaseSeeder
import org.dozendb.core.security.PrivilegeRecords
import org.dozendb.core.servers.AllocationException
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerRecords
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * `dbms.cluster.recreateDatabase`, as Neo4j Enterprise documents it: recreates a database, as one
 * that lost a majority of its primaries, in one system transaction on the `system` leader, under
 * the records' lock.
 *
 * The database gets a new id: a Raft group that lost its majority cannot be re-formed under its old
 * id, as the old members coming back would lead it. So Neo4j sees one database dropped and one
 * created under the same name: every server drops the old copies and their groups, which need no
 * leader for that, and the new database is created from the seed. Its name, access, aliases,
 * options and default flag stay, and so do the privileges on it, which name it by id
 * ([PrivilegeRecords.retarget]); its topology is the one given, else the old one. It is placed
 * anew, as a new database, and requested online. [Neo4jSystemGraph.RECREATED_FROM_PROPERTY] names
 * the old id.
 *
 * @property reading runs a block while the system graph may be read, as catch-up may replace it.
 * @property seeder reads seeds at URIs, if this server can.
 */
@Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for these refusals.
class DatabaseRecreation
internal constructor(
    private val system: () -> GraphDatabaseService,
    private val reading: (() -> Unit) -> Unit,
    private val leading: () -> Boolean,
    private val placement: ClusterPlacement,
    private val defaults: Placements.Defaults,
    private val seeder: () -> DatabaseSeeder?,
) {
    /**
     * Recreates the database [name] as [options] ask (see [RecreateOptions]); returns its new id.
     */
    fun recreate(name: String, options: Map<String, Any?>): UUID {
        val failed = "Could not recreate database '$name'"
        val asked = RecreateOptions.parse(options, failed)
        if (asked.seedingServers != null && asked.seedingServers.size != 1) {
            throw InvalidArgumentException(
                "$failed: recreating a database from the stores of several of its servers, or of " +
                    "every one, is not supported yet. Name one server in " +
                    "${RecreateOptions.SEEDING_SERVERS}, or give ${RecreateOptions.SEED_URI} alone."
            )
        }
        val seedingServer =
            asked.seedingServers?.single()?.let {
                runCatching { UUID.fromString(it) }.getOrNull()
                    ?: throw InvalidArgumentException(
                        "$failed: '$it' in ${RecreateOptions.SEEDING_SERVERS} is not a server id."
                    )
            }
        val uri = asked.seedUri
        if (uri != null) {
            val seeder =
                seeder()
                    ?: throw InvalidArgumentException(
                        "$failed: this server cannot recreate databases from seeds."
                    )
            try {
                seeder.check(uri)
            } catch (e: IllegalArgumentException) {
                throw InvalidArgumentException("$failed: ${e.message?.removeSuffix(".")}.", e)
            }
        }
        check(leading()) {
            "$failed: this member does not lead the system database; run it on the one that does."
        }
        val normalized = NormalizedDatabaseName(name).name()
        val id = UUID.randomUUID()
        reading {
            system().beginTx().use { tx ->
                ServerRecords.lock(tx)
                val node = database(tx, normalized, failed)
                val old = UUID.fromString(node.getProperty(UUID_PROPERTY) as String)
                PrivilegeRecords.retarget(tx, old, id)
                node.setProperty(UUID_PROPERTY, "$id")
                node.setProperty(Neo4jSystemGraph.RECREATED_FROM_PROPERTY, "$old")
                node.setProperty(
                    TopologyGraphDbmsModel.DATABASE_CREATED_AT_PROPERTY,
                    ZonedDateTime.now(),
                )
                Neo4jSystemGraph.forgetCreation(node)
                if (uri != null) {
                    node.setProperty(TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY, uri)
                } else {
                    node.removeProperty(TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY)
                }
                if (seedingServer != null) {
                    node.setProperty(
                        TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY,
                        arrayOf("$seedingServer"),
                    )
                } else {
                    node.removeProperty(TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY)
                }
                node.setProperty(
                    TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY,
                    TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName(),
                )
                val current = Placements.defaults(tx, defaults)
                val primaries = asked.primaries ?: Placements.primaries(node) ?: current.primaries
                val secondaries =
                    asked.secondaries ?: Placements.secondaries(node) ?: current.secondaries
                try {
                    placement.replace(tx, normalized, primaries, secondaries, seedingServer)
                } catch (e: AllocationException) {
                    throw InvalidArgumentException("$failed: ${e.message}.", e)
                }
                tx.commit()
            }
        }
        return id
    }

    /** The node of the database [name], refusing `system`, aliases and unknown names. */
    private fun database(tx: Transaction, name: String, failed: String): Node {
        if (name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
            throw InvalidArgumentException(
                "$failed: the system database is recovered with neo4j-admin dbms " +
                    "unbind-system-db, as the disaster recovery guide shows."
            )
        }
        Placements.database(tx, name)?.let {
            return it
        }
        val alias =
            tx.findNode(
                TopologyGraphDbmsModel.DATABASE_NAME_LABEL,
                TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                name,
            )
        if (alias != null) {
            throw InvalidArgumentException(
                "$failed: it is an alias. Recreate the database it stands for."
            )
        }
        throw InvalidArgumentException("$failed: it does not exist.")
    }

    private companion object {
        val UUID_PROPERTY: String = TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY
    }
}
