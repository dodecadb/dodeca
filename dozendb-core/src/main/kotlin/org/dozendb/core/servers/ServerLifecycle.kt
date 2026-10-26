package org.dozendb.core.servers

import java.util.UUID
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * The servers as the server commands and procedures find and change them, in a system transaction
 * on the `system` leader that took the records' lock first (see [ServerRecords.lock]). A server is
 * named by its name or its id. A refusal is an [InvalidArgumentException] whose message starts with
 * the failure given, such as "Could not cordon server 'a'".
 */
@Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for most refusals of server commands.
object ServerLifecycle {
    /**
     * A server as a command finds it: [enabled] with its record, or without one while the servers
     * the cluster started with are not recorded yet, as always without clustering; Free otherwise.
     */
    data class Target(
        val id: UUID,
        val name: String,
        val options: ServerOptions,
        val enabled: Boolean,
        val status: InstanceStatus = InstanceStatus.ENABLED,
    ) {
        /** Its record, with [status]. */
        fun record(status: InstanceStatus = this.status) = ServerRecord(id, name, options, status)
    }

    /** The server called [name], by its name or its id, among the [servers]. */
    fun target(tx: Transaction, name: String, servers: Servers, failed: String): Target {
        val records = ServerRecords.records(tx)
        val dropped = ServerRecords.dropped(tx)
        val id =
            (records.values.firstOrNull { it.name == name }?.id
                ?: uuid(name)?.takeIf { it in servers.ids() || it in records || it in dropped })
                ?: throw InvalidArgumentException(
                    "$failed: there is no such server. SHOW SERVERS lists the servers."
                )
        if (id in dropped) throw InvalidArgumentException("$failed: it was dropped.")
        val record = records[id]
        return Target(
            id,
            record?.name ?: id.toString(),
            record?.options ?: servers.initialOptions(id) ?: ServerOptions(),
            record != null || !ServerRecords.initialServersEnabled(tx),
            record?.status ?: InstanceStatus.ENABLED,
        )
    }

    /** [server], if it is enabled. */
    fun enabled(server: Target, failed: String): Target {
        if (!server.enabled) {
            throw InvalidArgumentException(
                "$failed: it is Free. ENABLE SERVER enables it, with its options."
            )
        }
        return server
    }

    /**
     * Cordons the server [name], as `dbms.cluster.cordonServer` does: it keeps the copies it hosts,
     * and takes no new ones.
     */
    fun cordon(tx: Transaction, name: String, servers: Servers, failed: String) {
        val server = enabled(target(tx, name, servers, failed), failed)
        when (server.status) {
            InstanceStatus.CORDONED -> {}
            InstanceStatus.DEALLOCATING ->
                throw InvalidArgumentException("$failed: it is deallocating.")
            else -> ServerRecords.write(tx, server.record(InstanceStatus.CORDONED))
        }
    }

    /**
     * Enables the cordoned server [name] again, as `dbms.cluster.uncordonServer` and ENABLE SERVER
     * do: it takes new copies again.
     */
    fun uncordon(tx: Transaction, name: String, servers: Servers, failed: String) {
        uncordon(tx, enabled(target(tx, name, servers, failed), failed), failed)
    }

    /** Enables [server] again if it is cordoned. */
    fun uncordon(tx: Transaction, server: Target, failed: String) {
        when (server.status) {
            InstanceStatus.CORDONED ->
                ServerRecords.write(tx, server.record(InstanceStatus.ENABLED))
            InstanceStatus.DEALLOCATING ->
                throw InvalidArgumentException("$failed: it is deallocating.")
            else -> {}
        }
    }

    /**
     * DROP SERVER [name]: a Free server, or a Deallocated one, which holds no database but
     * `system`, leaves the cluster (see [ServerRecords.drop]), which then removes it; it stops its
     * databases.
     */
    fun drop(tx: Transaction, name: String, servers: Servers, failed: String) {
        val server = target(tx, name, servers, failed)
        if (server.enabled && server.status != InstanceStatus.DEALLOCATING) {
            val state = if (server.status == InstanceStatus.CORDONED) "Cordoned" else "Enabled"
            throw InvalidArgumentException(
                "$failed: it is $state. DEALLOCATE DATABASES FROM SERVER moves its databases " +
                    "off first."
            )
        }
        val holding = (Placements.placedOn(tx, server.id) + servers.holding(tx, server.id)).sorted()
        if (holding.isNotEmpty()) {
            throw InvalidArgumentException(
                "$failed: it still holds ${holding.joinToString { "'$it'" }}. Drop it once SHOW " +
                    "SERVERS shows it Deallocated."
            )
        }
        if ((servers.ids() - server.id).isEmpty()) {
            throw InvalidArgumentException("$failed: it is the only server.")
        }
        ServerRecords.drop(tx, server.id, server.name, servers.address(server.id))
    }

    /**
     * DEALLOCATE DATABASES FROM SERVERS [names]: unless [dryRun], they become deallocating, so that
     * they take no new copies, and every copy they host moves to other servers (see
     * [DatabasePlacement.deallocate]); returns the moves.
     */
    fun deallocate(
        tx: Transaction,
        names: List<String>,
        servers: Servers,
        placement: DatabasePlacement,
        dryRun: Boolean,
        failed: String,
    ): List<Move> {
        val targets = names.map { enabled(target(tx, it, servers, failed), failed) }
        if (!dryRun) {
            for (server in targets.filter { it.status != InstanceStatus.DEALLOCATING }) {
                ServerRecords.write(tx, server.record(InstanceStatus.DEALLOCATING))
            }
        }
        return moving(failed) {
            placement.deallocate(tx, targets.mapTo(HashSet()) { it.id }, dryRun = dryRun)
        }
    }

    /**
     * Moves copies off the servers [names], which stay as they are, as the deallocate procedures
     * do: of [database] only, of [limit] of their databases, or of all; records the moves unless
     * [dryRun]; returns them.
     */
    fun deallocateCopies(
        tx: Transaction,
        names: List<String>,
        servers: Servers,
        placement: DatabasePlacement,
        database: String?,
        limit: Int?,
        dryRun: Boolean,
        failed: String,
    ): List<Move> {
        val ids = names.mapTo(HashSet()) { enabled(target(tx, it, servers, failed), failed).id }
        val name = database(tx, database, failed)
        checkLimit(limit, failed)
        return moving(failed) { placement.deallocate(tx, ids, name, limit, dryRun) }
    }

    /**
     * REALLOCATE DATABASES, and the reallocate procedures: moves copies to even out how many each
     * server hosts, of [database] only, at most [limit] moves, or all (see
     * [DatabasePlacement.reallocate]); records the moves unless [dryRun]; returns them.
     */
    fun reallocate(
        tx: Transaction,
        placement: DatabasePlacement,
        database: String?,
        limit: Int?,
        dryRun: Boolean,
        failed: String,
    ): List<Move> {
        val name = database(tx, database, failed)
        checkLimit(limit, failed)
        return moving(failed) { placement.reallocate(tx, name, limit, dryRun) }
    }

    /** The normalized name of [database], if given, which must exist. */
    private fun database(tx: Transaction, database: String?, failed: String): String? {
        val name = database?.let { NormalizedDatabaseName(it).name() }
        if (name != null && Placements.database(tx, name) == null) {
            throw InvalidArgumentException("$failed: there is no database '$database'.")
        }
        return name
    }

    private fun checkLimit(limit: Int?, failed: String) {
        if (limit != null && limit < 1) {
            throw InvalidArgumentException(
                "$failed: the number of databases is $limit, not 1 or more."
            )
        }
    }

    /** What [moving] moves, refused as [failed] says if a copy cannot move. */
    fun <T> moving(failed: String, moving: () -> T): T =
        try {
            moving()
        } catch (e: AllocationException) {
            throw InvalidArgumentException("$failed: ${e.message}.")
        }

    /** [text] as a server id, if it is one. */
    fun uuid(text: String): UUID? = runCatching { UUID.fromString(text) }.getOrNull()
}
