package org.dozendb.core.servers

import java.util.UUID
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Transaction

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
        val id =
            (records.values.firstOrNull { it.name == name }?.id
                ?: uuid(name)?.takeIf { it in servers.ids() || it in records })
                ?: throw InvalidArgumentException(
                    "$failed: there is no such server. SHOW SERVERS lists the servers."
                )
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

    /** [text] as a server id, if it is one. */
    fun uuid(text: String): UUID? = runCatching { UUID.fromString(text) }.getOrNull()
}
