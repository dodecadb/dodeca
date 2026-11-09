package org.dozendb.core.servers

import java.util.UUID
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus

/**
 * The mode of a copy of a database: a primary votes in the database's Raft group and may lead it, a
 * secondary pulls committed transactions from another copy. [hostedOn] is how the system graph
 * records it and [label] how commands show it, as in Neo4j Enterprise.
 */
enum class CopyMode(val hostedOn: HostedOnMode, val label: String) {
    PRIMARY(HostedOnMode.RAFT, "primary"),
    SECONDARY(HostedOnMode.REPLICA, "secondary");

    val other: CopyMode
        get() = if (this == PRIMARY) SECONDARY else PRIMARY
}

/**
 * A server as the [Allocator] sees it: its record, and whether it answers now. A Free server has no
 * record and hosts no copies, so it is not one.
 */
data class Host(val record: ServerRecord, val available: Boolean = true) {
    val id: UUID
        get() = record.id
}

/**
 * A database as the [Allocator] places it: [primaries] and [secondaries] copies requested, [copies]
 * on the servers it is allocated to, with the mode of each, and [online] if it is requested online.
 */
data class Placed(
    val name: String,
    val primaries: Int,
    val secondaries: Int = 0,
    val online: Boolean = true,
    val copies: Map<UUID, CopyMode> = emptyMap(),
) {
    /** The servers that host a copy in [mode]. */
    fun hosts(mode: CopyMode): Set<UUID> = copies.filterValues { it == mode }.keys
}

/** A copy of [database] in [mode] moved from server [from] to server [to]. */
data class Move(val database: String, val from: UUID, val to: UUID, val mode: CopyMode)

/** The servers cannot host the copies asked for; the message tells why. */
class AllocationException(message: String) : Exception(message)

/**
 * Decides which servers host the copies of databases, as the commands that place them ask: CREATE
 * and ALTER DATABASE, DEALLOCATE, REALLOCATE and ALTER SERVER. Neo4j Enterprise leaves its
 * algorithm undocumented; this one is deterministic, so that the same records place the same.
 *
 * A server can take a new copy of a database if it is enabled (not cordoned or deallocating), its
 * mode constraint allows the copy's mode, and its allowed and denied databases let it host the
 * database; one copy per server, in either mode. A cordoned server keeps the copies it has. New
 * copies go to servers that answer, those that host the fewest copies first, then by id; an
 * unavailable server is chosen only when no other can be, as in Enterprise, where only cordoning
 * keeps new copies off a server.
 *
 * A copy that changes mode stays on its server, which keeps its store: a server that holds a
 * database in the other mode is chosen before one that would need a copy, where that saves copies
 * overall. A server that may host either mode takes a primary only where the secondaries still fit,
 * so that a placement is found whenever one exists.
 *
 * An allocator starts from the current [servers] and [databases], and each operation changes its
 * view, so that operations can follow one another; one that fails with [AllocationException] leaves
 * it unchanged.
 */
class Allocator(servers: Collection<Host>, databases: Collection<Placed>) {
    private val servers: Map<UUID, Host> = servers.associateBy { it.id }
    private val databases: MutableMap<String, Placed> =
        databases.associateByTo(LinkedHashMap()) { it.name }

    /** The servers that host [database], with the mode of each; none if it is not placed. */
    fun copies(database: String): Map<UUID, CopyMode> = databases[database]?.copies.orEmpty()

    /** Every database with the servers that host it. */
    fun placements(): Map<String, Map<UUID, CopyMode>> = databases.mapValues { it.value.copies }

    /**
     * Places the new database [name] on [primaries] servers and [secondaries] others, [first] among
     * the primaries if given, as a server named to seed it, then those of [prefer] that can host
     * it, as servers that already hold its store. With [bestEffort], as many primaries as can be
     * placed, at least one, then as many secondaries as can be.
     */
    fun place(
        name: String,
        primaries: Int,
        secondaries: Int = 0,
        first: UUID? = null,
        prefer: List<UUID> = emptyList(),
        bestEffort: Boolean = false,
    ): Map<UUID, CopyMode> {
        require(primaries >= 1) { "a database needs at least one primary" }
        require(secondaries >= 0) { "a negative number of secondaries" }
        if (first != null) {
            val server =
                servers[first]
                    ?: throw AllocationException("server $first is not an enabled server")
            if (!canHost(server, name, CopyMode.PRIMARY)) {
                throw AllocationException(
                    "server ${server.record.name} cannot host '$name' " +
                        "(${why(server, name, CopyMode.PRIMARY)})"
                )
            }
        }
        val placed = Placed(name, primaries, secondaries)
        val copies =
            assign(
                placed,
                primaries,
                secondaries,
                first = first,
                prefer = prefer,
                bestEffort = bestEffort,
                short = { mode -> unsatisfiable(placed, mode, primaries, secondaries) },
            )
        databases[name] =
            Placed(name, count(copies, CopyMode.PRIMARY), count(copies, CopyMode.SECONDARY))
                .copy(copies = copies)
        return copies
    }

    /**
     * Changes the number of copies of [database] to [primaries] and [secondaries]. Fewer copies of
     * a mode leave cordoned servers first, then unavailable ones, then those that host the most
     * copies, and may stay in the other mode; more go where [place] puts them.
     */
    fun resize(database: String, primaries: Int, secondaries: Int = 0): Map<UUID, CopyMode> {
        require(primaries >= 1) { "a database needs at least one primary" }
        require(secondaries >= 0) { "a negative number of secondaries" }
        val placed = placed(database)
        val copies =
            assign(placed, primaries, secondaries) { mode ->
                unsatisfiable(placed, mode, primaries, secondaries)
            }
        databases[database] =
            placed.copy(primaries = primaries, secondaries = secondaries, copies = copies)
        return copies
    }

    /**
     * Moves the copies on the servers [from] to other servers: of every database they host, of
     * [database] only, or of [limit] of their databases, by name. Refused for a database requested
     * offline, which cannot be copied, and for one with a quorum of its primaries on cordoned
     * servers.
     */
    fun deallocate(from: Set<UUID>, database: String? = null, limit: Int? = null): List<Move> {
        val affected =
            databases.values
                .filter { placed -> placed.copies.keys.any { it in from } }
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
            val primaries = placed.hosts(CopyMode.PRIMARY)
            val cordoned = primaries.count {
                servers[it]?.record?.status == InstanceStatus.CORDONED
            }
            if (cordoned > primaries.size / 2) {
                throw AllocationException(
                    "a quorum of the servers that host '${placed.name}' is cordoned"
                )
            }
        }
        return changing {
            affected.flatMap { placed -> relocate(placed, banned = from) { id, _ -> id in from } }
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
     * until none helps. Only copies of [database] if given; at most [limit] moves if given, though
     * the copies of one database that change places together all move.
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
            val held =
                databases.values
                    .filter { giver.id in it.copies && (database == null || it.name == database) }
                    .sortedBy { it.name }
            for (placed in held) {
                val mode = placed.copies.getValue(giver.id)
                val to =
                    servers.values
                        .filter { it.id !in placed.copies && canHost(it, placed.name, mode) }
                        .map { it.id }
                        .sortedWith(newCopies)
                        .firstOrNull { load(it) <= count - 2 } ?: continue
                databases[placed.name] =
                    placed.copy(copies = placed.copies - giver.id + (to to mode))
                return Move(placed.name, giver.id, to, mode)
            }
        }
        return null
    }

    /**
     * Moves the copies of [database], or of every database, that their servers cannot keep; stops
     * once [limit] moves are made, if given.
     */
    private fun conforming(database: String?, limit: Int?): List<Move> {
        val moves = mutableListOf<Move>()
        for (placed in databases.values.sortedBy { it.name }) {
            if (database != null && placed.name != database) continue
            if (limit != null && moves.size >= limit) break
            val misplaced = { id: UUID, mode: CopyMode ->
                servers[id]?.let { !canKeep(it, placed.name, mode) } ?: true
            }
            if (placed.copies.none { (id, mode) -> misplaced(id, mode) }) continue
            moves += relocate(placed, drop = misplaced)
        }
        return moves
    }

    /**
     * Gives [placed] as many copies of each mode as it has, without those that [drop] lets go and
     * none on the servers [banned]; returns the moves.
     */
    private fun relocate(
        placed: Placed,
        banned: Set<UUID> = emptySet(),
        drop: (UUID, CopyMode) -> Boolean,
    ): List<Move> {
        val before = placed.copies
        val after =
            assign(
                placed,
                count(before, CopyMode.PRIMARY),
                count(before, CopyMode.SECONDARY),
                drop = drop,
                banned = banned,
            ) { mode ->
                val from =
                    before.filter { (id, copyMode) -> copyMode == mode && drop(id, copyMode) }.keys
                val name = from.minOrNull()?.let { servers[it]?.record?.name }
                AllocationException(
                    "no other server can host '${placed.name}'" +
                        (if (mode == CopyMode.SECONDARY) " as a secondary" else "") +
                        (name?.let { " in place of $it" } ?: "")
                )
            }
        databases[placed.name] = placed.copy(copies = after)
        return CopyMode.entries.flatMap { mode ->
            val left = before.filter { (id, it) -> it == mode && after[id] != mode }.keys.sorted()
            val arrived = after.filter { (id, it) -> it == mode && before[id] != mode }.keys
            left.zip(arrived) { from, to -> Move(placed.name, from, to, mode) }
        }
    }

    /**
     * The copies of [placed] for [primaries] and [secondaries], least costly first: a copy that
     * stays costs nothing, one that changes mode on its server little, a new one, which needs the
     * database copied there, most; [first] must be a primary, and [prefer] costs less than a new
     * copy. Within a cost, copies stay on servers by [keptCopies] and come to servers by
     * [newCopies], primaries before secondaries. Copies that [drop] lets go do not stay, and the
     * servers [banned] take none. [short] is thrown, with the mode that does not fit, if no
     * placement exists; with [bestEffort], the most primaries that fit, at least one, then the most
     * secondaries.
     *
     * A server at a time decides, over every number of copies of each mode placed so far, so that
     * the least costly placement is found exactly (a dynamic programme over the servers by id).
     */
    private fun assign(
        placed: Placed,
        primaries: Int,
        secondaries: Int,
        drop: (UUID, CopyMode) -> Boolean = { _, _ -> false },
        banned: Set<UUID> = emptySet(),
        first: UUID? = null,
        prefer: List<UUID> = emptyList(),
        bestEffort: Boolean = false,
        short: (CopyMode) -> AllocationException,
    ): Map<UUID, CopyMode> {
        val name = placed.name
        val current = placed.copies
        val ids = (servers.keys + current.keys).sorted()
        val n = ids.size.toLong() + 1
        fun keeps(id: UUID, mode: CopyMode) = current[id] == mode && !drop(id, mode)
        fun takes(id: UUID, mode: CopyMode) =
            id !in banned && servers[id]?.let { canHost(it, name, mode) } == true

        val kept =
            CopyMode.entries.associateWith { mode ->
                ranks(ids.filter { keeps(it, mode) }.sortedWith(keptCopies(name, mode)))
            }
        val fresh = ranks(ids.sortedWith(newCopies))
        val preferred = ranks(prefer.distinct())
        // Primaries' order outweighs secondaries', and the kind of change outweighs both: ranks
        // are below n, and secondaries' add up to less than primaryWeight.
        val primaryWeight = (secondaries + 1) * n
        val changeWeight = (primaries + 1) * primaryWeight * n
        fun cost(id: UUID, mode: CopyMode): Long? {
            val rank: Long
            val change: Long
            when {
                keeps(id, mode) -> {
                    change = STAY
                    rank = kept.getValue(mode).getValue(id)
                }
                !takes(id, mode) -> return null
                mode == CopyMode.PRIMARY && id == first -> {
                    change = STAY
                    rank = 0
                }
                current[id] == mode.other -> {
                    change = SWITCH
                    rank = fresh.getValue(id)
                }
                mode == CopyMode.PRIMARY && id in preferred -> {
                    change = PREFERRED
                    rank = preferred.getValue(id)
                }
                else -> {
                    change = COPY
                    rank = fresh.getValue(id)
                }
            }
            return change * changeWeight + rank * if (mode == CopyMode.PRIMARY) primaryWeight else 1
        }

        // least[i][a][b]: the least cost of a primaries and b secondaries on the first i servers.
        val unreached = Long.MAX_VALUE
        val least =
            Array(ids.size + 1) {
                Array(primaries + 1) { LongArray(secondaries + 1) { unreached } }
            }
        val chose =
            Array(ids.size + 1) { Array(primaries + 1) { arrayOfNulls<CopyMode>(secondaries + 1) } }
        least[0][0][0] = 0
        for ((i, id) in ids.withIndex()) {
            val primary = cost(id, CopyMode.PRIMARY)
            val secondary = cost(id, CopyMode.SECONDARY).takeIf { id != first }
            for (a in 0..primaries) {
                for (b in 0..secondaries) {
                    val sum = least[i][a][b]
                    if (sum == unreached) continue
                    fun reach(a2: Int, b2: Int, add: Long, mode: CopyMode?) {
                        if (sum + add < least[i + 1][a2][b2]) {
                            least[i + 1][a2][b2] = sum + add
                            chose[i + 1][a2][b2] = mode
                        }
                    }
                    if (id != first) reach(a, b, 0, null)
                    if (primary != null && a < primaries) reach(a + 1, b, primary, CopyMode.PRIMARY)
                    if (secondary != null && b < secondaries) {
                        reach(a, b + 1, secondary, CopyMode.SECONDARY)
                    }
                }
            }
        }
        val last = least[ids.size]
        val (a, b) =
            if (bestEffort) {
                val a =
                    (primaries downTo 1).firstOrNull { a -> last[a].any { it != unreached } }
                        ?: throw short(CopyMode.PRIMARY)
                a to (secondaries downTo 0).first { last[a][it] != unreached }
            } else {
                if (last[primaries].all { it == unreached }) throw short(CopyMode.PRIMARY)
                if (last[primaries][secondaries] == unreached) throw short(CopyMode.SECONDARY)
                primaries to secondaries
            }
        val chosen = mutableListOf<Pair<UUID, CopyMode>>()
        var (left, right) = a to b
        for (i in ids.size downTo 1) {
            val mode = chose[i][left][right] ?: continue
            chosen += ids[i - 1] to mode
            if (mode == CopyMode.PRIMARY) left-- else right--
        }
        // Primaries first, then secondaries, each the least costly first.
        return chosen
            .sortedWith(
                compareBy<Pair<UUID, CopyMode>> { it.second }.thenBy { cost(it.first, it.second) }
            )
            .associateTo(LinkedHashMap()) { it }
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

    /**
     * The order in which copies of [database] in [mode] stay: those their servers can keep first,
     * then on servers that are recorded, not cordoned, answer and host the fewest copies, then by
     * id. Fewer copies leave from the end.
     */
    private fun keptCopies(database: String, mode: CopyMode): Comparator<UUID> =
        compareBy<UUID> { id -> servers[id]?.let { canKeep(it, database, mode) } != true }
            .thenBy { servers[it]?.record?.status == InstanceStatus.CORDONED }
            .thenBy { servers[it]?.available != true }
            .thenBy { load(it) }
            .thenBy { it }

    /** The order in which servers take new copies: those that answer and host the fewest first. */
    private val newCopies: Comparator<UUID>
        get() = compareBy<UUID> { servers[it]?.available != true }.thenBy { load(it) }.thenBy { it }

    /** How many copies [server] hosts. */
    private fun load(server: UUID): Int = databases.values.count { server in it.copies }

    private fun placed(database: String): Placed =
        databases[database] ?: throw AllocationException("database '$database' is not placed")

    /**
     * Why [placed] cannot have [primaries] and [secondaries]: too few servers for the primaries,
     * or, with as many primaries as fit placed where they leave the most room, for the secondaries.
     */
    private fun unsatisfiable(
        placed: Placed,
        mode: CopyMode,
        primaries: Int,
        secondaries: Int,
    ): AllocationException {
        val name = placed.name
        val ids = servers.keys + placed.copies.keys
        fun able(id: UUID, mode: CopyMode) =
            placed.copies[id] == mode || servers[id]?.let { canHost(it, name, mode) } == true
        if (mode == CopyMode.PRIMARY) {
            val able = ids.count { able(it, CopyMode.PRIMARY) }
            return AllocationException(
                "'$name' needs $primaries ${servers(primaries)} that can host it as a primary, " +
                    "and ${if (able == 0) "none" else "only $able"} can"
            )
        }
        val onlyPrimary = ids.count { able(it, CopyMode.PRIMARY) && !able(it, CopyMode.SECONDARY) }
        val secondary = ids.count { able(it, CopyMode.SECONDARY) }
        val left = secondary - maxOf(0, primaries - onlyPrimary)
        return AllocationException(
            "'$name' needs $secondaries ${servers(secondaries)} that can host it as a secondary " +
                "besides those of its primaries, and ${if (left <= 0) "none" else "only $left"} can"
        )
    }

    private fun servers(count: Int) = if (count == 1) "server" else "servers"

    private companion object {
        /**
         * What changing a copy costs: staying, switching mode on its server, a preferred server.
         */
        const val STAY = 0L
        const val SWITCH = 1L
        const val PREFERRED = 2L

        /** A new copy costs more than two switches, which a swap of modes between servers takes. */
        const val COPY = 4L

        /** The number of copies in [mode] of [copies]. */
        fun count(copies: Map<UUID, CopyMode>, mode: CopyMode): Int =
            copies.values.count { it == mode }

        /** Each of [ids] with its position. */
        fun ranks(ids: List<UUID>): Map<UUID, Long> =
            ids.withIndex().associate { (index, id) -> id to index.toLong() }

        /** Whether [server] can take a new copy of [database] in [mode]. */
        fun canHost(server: Host, database: String, mode: CopyMode): Boolean =
            server.record.status == InstanceStatus.ENABLED &&
                refusal(server, database, mode) == null

        /** Whether [server] can keep its copy of [database] in [mode]: also while cordoned. */
        fun canKeep(server: Host, database: String, mode: CopyMode): Boolean =
            server.record.status != InstanceStatus.DEALLOCATING &&
                refusal(server, database, mode) == null

        /** Why [server] cannot take a new copy of [database] in [mode]. */
        fun why(server: Host, database: String, mode: CopyMode): String =
            when (server.record.status) {
                InstanceStatus.DEALLOCATING -> "it is deallocating"
                InstanceStatus.CORDONED -> "it is cordoned"
                else -> refusal(server, database, mode) ?: "it can"
            }

        /**
         * Why [server]'s options keep it from hosting [database] in [mode], or null if they do not.
         */
        fun refusal(server: Host, database: String, mode: CopyMode): String? {
            val options = server.record.options
            return when {
                mode == CopyMode.PRIMARY &&
                    options.modeConstraint == InstanceModeConstraint.SECONDARY ->
                    "its mode constraint is SECONDARY"
                mode == CopyMode.SECONDARY &&
                    options.modeConstraint == InstanceModeConstraint.PRIMARY ->
                    "its mode constraint is PRIMARY"
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
