package org.dozendb.core.servers

import java.util.UUID
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus

/**
 * A server as the [Allocator] sees it: its record, and whether it answers now. A Free server has no
 * record and hosts no copies, so it is not one.
 */
data class Host(val record: ServerRecord, val available: Boolean = true) {
    val id: UUID
        get() = record.id
}

/**
 * A database as the [Allocator] places it: [primaries] copies requested, on [hosts], the servers it
 * is allocated to, and [online] if it is requested online.
 */
data class Placed(
    val name: String,
    val primaries: Int,
    val online: Boolean = true,
    val hosts: Set<UUID> = emptySet(),
)

/** A copy of [database] moved from server [from] to server [to]. */
data class Move(val database: String, val from: UUID, val to: UUID)

/** The servers cannot host the copies asked for; the message tells why. */
class AllocationException(message: String) : Exception(message)

/**
 * Decides which servers host the copies of databases, as the commands that place them ask: CREATE
 * and ALTER DATABASE, DEALLOCATE, REALLOCATE and ALTER SERVER. Neo4j Enterprise leaves its
 * algorithm undocumented; this one is deterministic, so that the same records place the same.
 *
 * A server can take a new copy of a database if it is enabled (not cordoned or deallocating), its
 * mode constraint allows primaries, and its allowed and denied databases let it host the database;
 * one copy per server. A cordoned server keeps the copies it has. New copies go to servers that
 * answer, those that host the fewest copies first, then by id; an unavailable server is chosen only
 * when no other can be, as in Enterprise, where only cordoning keeps new copies off a server.
 *
 * An allocator starts from the current [servers] and [databases], and each operation changes its
 * view, so that operations can follow one another; one that fails with [AllocationException] leaves
 * it unchanged.
 */
class Allocator(servers: Collection<Host>, databases: Collection<Placed>) {
    private val servers: Map<UUID, Host> = servers.associateBy { it.id }
    private val databases: MutableMap<String, Placed> =
        databases.associateByTo(LinkedHashMap()) { it.name }

    /** The servers that host [database], or none if it is not placed. */
    fun hosts(database: String): Set<UUID> = databases[database]?.hosts.orEmpty()

    /** Every database with the servers that host it. */
    fun placements(): Map<String, Set<UUID>> = databases.mapValues { it.value.hosts }

    /**
     * Places the new database [name] on [primaries] servers, [first] among them if given, as a
     * server named to seed it, then those of [prefer] that can host it, as servers that already
     * hold its store. With [bestEffort], on as many as can host it, at least one.
     */
    fun place(
        name: String,
        primaries: Int,
        first: UUID? = null,
        prefer: List<UUID> = emptyList(),
        bestEffort: Boolean = false,
    ): Set<UUID> {
        require(primaries >= 1) { "a database needs at least one primary" }
        val chosen = LinkedHashSet<UUID>()
        if (first != null) {
            val server =
                servers[first]
                    ?: throw AllocationException("server $first is not an enabled server")
            if (!canHost(server, name)) {
                throw AllocationException(
                    "server ${server.record.name} cannot host '$name' (${why(server, name)})"
                )
            }
            chosen += first
        }
        for (id in prefer) {
            if (chosen.size >= primaries) break
            val server = servers[id] ?: continue
            if (canHost(server, name)) chosen += id
        }
        while (chosen.size < primaries) {
            val next = candidates(name, chosen).firstOrNull() ?: break
            chosen += next.id
        }
        if (chosen.isEmpty() || (chosen.size < primaries && !bestEffort)) {
            throw unsatisfiable(name, primaries)
        }
        databases[name] = Placed(name, chosen.size, hosts = chosen)
        return chosen
    }

    /**
     * Changes the number of copies of [database] to [primaries]. New copies go where [place] puts
     * them; fewer copies leave cordoned servers first, then unavailable ones, then those that host
     * the most copies.
     */
    fun resize(database: String, primaries: Int): Set<UUID> {
        require(primaries >= 1) { "a database needs at least one primary" }
        val placed = placed(database)
        val hosts = LinkedHashSet(placed.hosts)
        if (primaries > hosts.size) {
            val added = candidates(database, hosts).take(primaries - hosts.size).map { it.id }
            if (hosts.size + added.size < primaries) throw unsatisfiable(database, primaries)
            hosts += added
        } else {
            // A server without a record, which left, goes first.
            hosts -=
                hosts
                    .sortedWith(
                        compareBy<UUID> { it in servers }
                            .thenBy { servers[it]?.record?.status != InstanceStatus.CORDONED }
                            .thenBy { servers[it]?.available }
                            .thenByDescending { load(it) }
                            .thenByDescending { it }
                    )
                    .take(hosts.size - primaries)
                    .toSet()
        }
        databases[database] = placed.copy(primaries = primaries, hosts = hosts)
        return hosts
    }

    /**
     * Moves the copies on the servers [from] to other servers: of every database they host, of
     * [database] only, or of [limit] of their databases, by name. Refused for a database requested
     * offline, which cannot be copied, and for one with a quorum of its copies on cordoned servers.
     */
    fun deallocate(from: Set<UUID>, database: String? = null, limit: Int? = null): List<Move> {
        val affected =
            databases.values
                .filter { placed -> placed.hosts.any { it in from } }
                .filter { database == null || it.name == database }
                .sortedBy { it.name }
                .let { if (limit == null) it else it.take(limit) }
        for (placed in affected) {
            if (!placed.online) {
                throw AllocationException(
                    "database '${placed.name}' is offline, and an offline database cannot be " +
                        "copied: start it first"
                )
            }
            val cordoned =
                placed.hosts.count {
                    servers[it]?.record?.status == InstanceStatus.CORDONED
                }
            if (cordoned > placed.hosts.size / 2) {
                throw AllocationException(
                    "a quorum of the servers that host '${placed.name}' is cordoned"
                )
            }
        }
        return changing {
            affected.flatMap { placed ->
                placed.hosts.filter { it in from }.map { source -> move(placed.name, source, from) }
            }
        }
    }

    /**
     * Moves the copies that their servers can no longer keep, as after ALTER SERVER changed what a
     * server may host, or a server started deallocating.
     */
    fun conform(): List<Move> = changing { conforming(null, null) }

    /**
     * Moves copies to even out how many each server hosts: first those their servers can no longer
     * keep, then, one at a time, a copy from a server with the most to one with at least two fewer,
     * until none helps. Only copies of [database] if given; at most [limit] moves if given.
     */
    fun reallocate(database: String? = null, limit: Int? = null): List<Move> = changing {
        val moves = conforming(database, limit).toMutableList()
        while (limit == null || moves.size < limit) {
            moves += balancingMove(database) ?: break
        }
        moves
    }

    /** The next move that evens out the copies, or null if none helps. */
    private fun balancingMove(database: String?): Move? {
        val givers =
            servers.values.sortedWith(compareByDescending<Host> { load(it.id) }.thenBy { it.id })
        for (giver in givers) {
            val count = load(giver.id)
            val names =
                databases.values
                    .filter { giver.id in it.hosts && (database == null || it.name == database) }
                    .map { it.name }
                    .sorted()
            for (name in names) {
                val to =
                    candidates(name, hosts(name)).firstOrNull { load(it.id) <= count - 2 }
                        ?: continue
                return apply(Move(name, giver.id, to.id))
            }
        }
        return null
    }

    /**
     * Moves the copies of [database], or of every database, that their servers cannot keep, at most
     * [limit] if given.
     */
    private fun conforming(database: String?, limit: Int?): List<Move> {
        val moves = mutableListOf<Move>()
        for (placed in databases.values.sortedBy { it.name }) {
            if (database != null && placed.name != database) continue
            val misplaced =
                placed.hosts
                    .filter { id -> servers[id]?.let { !canKeep(it, placed.name) } ?: true }
                    .sorted()
            for (id in misplaced) {
                if (limit != null && moves.size >= limit) return moves
                moves += move(placed.name, id, emptySet())
            }
        }
        return moves
    }

    /** Moves the copy of [database] on [from] to the best other server outside [avoid]. */
    private fun move(database: String, from: UUID, avoid: Set<UUID>): Move {
        val to =
            candidates(database, hosts(database) + avoid).firstOrNull()
                ?: throw AllocationException(
                    "no other server can host '$database'" +
                        (servers[from]?.let { " in place of ${it.record.name}" } ?: "")
                )
        return apply(Move(database, from, to.id))
    }

    private fun apply(move: Move): Move {
        val placed = placed(move.database)
        databases[move.database] = placed.copy(hosts = placed.hosts - move.from + move.to)
        return move
    }

    /** Runs [operation]; if it fails, the view is as before. */
    private fun <T> changing(operation: () -> T): T {
        val before = LinkedHashMap(databases)
        try {
            return operation()
        } catch (e: AllocationException) {
            databases.clear()
            databases.putAll(before)
            throw e
        }
    }

    /** The servers that can take a new copy of [database], outside [excluded], best first. */
    private fun candidates(database: String, excluded: Set<UUID>): List<Host> =
        servers.values
            .filter { it.id !in excluded && canHost(it, database) }
            .sortedWith(compareBy<Host> { !it.available }.thenBy { load(it.id) }.thenBy { it.id })

    /** How many copies [server] hosts. */
    private fun load(server: UUID): Int = databases.values.count { server in it.hosts }

    private fun placed(database: String): Placed =
        databases[database] ?: throw AllocationException("database '$database' is not placed")

    private fun unsatisfiable(database: String, primaries: Int): AllocationException {
        val able = servers.values.count { canHost(it, database) }
        return AllocationException(
            "'$database' needs $primaries ${servers(primaries)} that can host it as a primary, " +
                "and ${if (able == 0) "none" else "only $able"} can"
        )
    }

    private fun servers(count: Int) = if (count == 1) "server" else "servers"

    private companion object {
        /** Whether [server] can take a new copy of [database]. */
        fun canHost(server: Host, database: String): Boolean =
            server.record.status == InstanceStatus.ENABLED && refusal(server, database) == null

        /** Whether [server] can keep its copy of [database]: also while cordoned. */
        fun canKeep(server: Host, database: String): Boolean =
            server.record.status != InstanceStatus.DEALLOCATING && refusal(server, database) == null

        /** Why [server] cannot take a new copy of [database]. */
        fun why(server: Host, database: String): String =
            when (server.record.status) {
                InstanceStatus.DEALLOCATING -> "it is deallocating"
                InstanceStatus.CORDONED -> "it is cordoned"
                else -> refusal(server, database) ?: "it can"
            }

        /** Why [server]'s options keep it from hosting [database], or null if they do not. */
        fun refusal(server: Host, database: String): String? {
            val options = server.record.options
            return when {
                options.modeConstraint == InstanceModeConstraint.SECONDARY ->
                    "its mode constraint is SECONDARY"
                options.allowedDatabases.isNotEmpty() &&
                    options.allowedDatabases.none { it.equals(database, ignoreCase = true) } ->
                    "its allowed databases leave it out"
                options.deniedDatabases.any { it.equals(database, ignoreCase = true) } ->
                    "its denied databases name it"
                else -> null
            }
        }
    }
}
