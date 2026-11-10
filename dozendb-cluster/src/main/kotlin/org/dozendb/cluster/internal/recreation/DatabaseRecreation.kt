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
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus
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
 * Its seeders, `seedingServers`, are placed on it, primaries first in their order: those named, or
 * for none named every enabled server that hosts a copy, primaries first; refused while a server
 * that hosts a copy does not answer and is not cordoned, and when they are more than its copies.
 * Seeders whose copies are in quarantine are refused.
 *
 * @property reading runs a block while the system graph may be read, as catch-up may replace it.
 * @property seeder reads seeds at URIs, if this server can.
 * @property quarantined the servers whose copies of a database, by id, are in quarantine.
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
    private val quarantined: (UUID) -> Set<UUID>,
) {
    /**
     * Recreates the database [name] as [options] ask (see [RecreateOptions]); returns its new id.
     */
    fun recreate(name: String, options: Map<String, Any?>): UUID {
        val failed = "Could not recreate database '$name'"
        val asked = RecreateOptions.parse(options, failed)
        val named =
            asked.seedingServers?.map {
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
                val current = Placements.defaults(tx, defaults)
                val primaries = asked.primaries ?: Placements.primaries(node) ?: current.primaries
                val secondaries =
                    asked.secondaries ?: Placements.secondaries(node) ?: current.secondaries
                val seeders = seeders(tx, normalized, named, old, primaries + secondaries, failed)
                if (named != null && seeders.isEmpty() && uri == null) {
                    throw InvalidArgumentException(
                        "$failed: no enabled server hosts a copy of it. Give " +
                            "${RecreateOptions.SEED_URI}, a backup or dump to recreate it from."
                    )
                }
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
                if (seeders.isNotEmpty()) {
                    node.setProperty(
                        TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY,
                        seeders.map { "$it" }.toTypedArray(),
                    )
                } else {
                    node.removeProperty(TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY)
                }
                node.setProperty(
                    TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY,
                    TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName(),
                )
                try {
                    placement.replace(tx, normalized, primaries, secondaries, seeders)
                } catch (e: AllocationException) {
                    throw InvalidArgumentException("$failed: ${e.message}.", e)
                }
                tx.commit()
            }
        }
        return id
    }

    /**
     * The seeders of the database [name], whose id is [old]: those [named], which must be servers,
     * or for an empty list every enabled server that hosts a copy, primaries first, refused while
     * one that hosts a copy does not answer and is not cordoned. At most [copies]; none in
     * quarantine.
     */
    private fun seeders(
        tx: Transaction,
        name: String,
        named: List<UUID>?,
        old: UUID,
        copies: Int,
        failed: String,
    ): List<UUID> {
        if (named == null) return emptyList()
        // Asks the servers, so that the states of their copies are current too.
        val holders = placement.holders(tx, name)
        val seeders =
            if (named.isNotEmpty()) {
                val servers = ServerRecords.records(tx)
                named.distinct().onEach {
                    if (it !in servers) {
                        throw InvalidArgumentException(
                            "$failed: '$it' in ${RecreateOptions.SEEDING_SERVERS} is not a server."
                        )
                    }
                }
            } else {
                val missing = holders.filter {
                    !it.available && it.record.status != InstanceStatus.CORDONED
                }
                if (missing.size == 1) {
                    throw InvalidArgumentException(
                        "$failed: server ${missing[0].record.name} hosts a copy of it but is not " +
                            "available. Start it, or cordon it to recreate the database without it."
                    )
                }
                if (missing.isNotEmpty()) {
                    throw InvalidArgumentException(
                        "$failed: servers ${missing.joinToString { it.record.name }} host copies " +
                            "of it but are not available. Start them, or cordon them to recreate " +
                            "the database without them."
                    )
                }
                holders.filter { it.record.status == InstanceStatus.ENABLED }.map { it.record.id }
            }
        if (seeders.size > copies) {
            throw InvalidArgumentException(
                "$failed: it would have $copies copies, fewer than its ${seeders.size} seeding " +
                    "servers, which each host one. Name fewer servers in " +
                    "${RecreateOptions.SEEDING_SERVERS}, or give more " +
                    "${RecreateOptions.PRIMARIES} or ${RecreateOptions.SECONDARIES}."
            )
        }
        val inQuarantine = quarantined(old).let { servers -> seeders.filter { it in servers } }
        if (inQuarantine.isNotEmpty()) {
            throw InvalidArgumentException(
                "$failed: its copies on ${inQuarantine.joinToString()} are in quarantine. Lift " +
                    "their quarantine first, or leave them out of " +
                    "${RecreateOptions.SEEDING_SERVERS}."
            )
        }
        return seeders
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
