package org.dozendb.cluster.internal

import java.time.ZonedDateTime
import java.util.UUID
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Transaction

/**
 * The servers that host `system` as secondaries, which pull it instead of voting in its group, as
 * the system graph records them: a DozenDB `DozenDbSystemSecondary` node for each, with its `id`,
 * the `raft_address` other servers reach it at, and when it was admitted, `added_at`. The system
 * leader records a server when it admits it as a system secondary; the record goes when the server
 * is dropped or turns primary. While the leader asks it to turn primary, its record has `promote`
 * set.
 */
internal object SystemSecondaryRecords {
    private val LABEL = Label.label("DozenDbSystemSecondary")
    private const val ID = "id"
    private const val ADDRESS = "raft_address"
    private const val ADDED_AT = "added_at"
    private const val PROMOTE = "promote"

    /** The recorded system secondaries, ordered by id. */
    fun all(tx: Transaction): List<Member> =
        tx.findNodes(LABEL).use { nodes ->
            nodes
                .asSequence()
                .mapNotNull { node ->
                    val id = (node.getProperty(ID, null) as? String)?.let(::parseId)
                    val address = (node.getProperty(ADDRESS, null) as? String)?.let(::parseAddress)
                    if (id == null || address == null) null else Member(MemberId(id), address)
                }
                .distinctBy { it.id }
                .sortedBy { it.id }
                .toList()
        }

    /** Records [member] a system secondary, at its current address; returns whether it was not. */
    fun add(tx: Transaction, member: Member): Boolean {
        val existing = tx.findNode(LABEL, ID, member.id.toString())
        if (existing != null) {
            existing.setProperty(ADDRESS, member.raftAddress.toString())
            return false
        }
        tx.createNode(LABEL).apply {
            setProperty(ID, member.id.toString())
            setProperty(ADDRESS, member.raftAddress.toString())
            setProperty(ADDED_AT, ZonedDateTime.now())
        }
        return true
    }

    /** Asks the system secondary [id] to turn primary; returns whether it has a record. */
    fun promote(tx: Transaction, id: MemberId): Boolean {
        val node = tx.findNode(LABEL, ID, id.toString()) ?: return false
        node.setProperty(PROMOTE, true)
        return true
    }

    /** Whether the system secondary [id] is asked to turn primary. */
    fun promoted(tx: Transaction, id: MemberId): Boolean =
        tx.findNode(LABEL, ID, id.toString())?.getProperty(PROMOTE, false) == true

    /** Deletes the record of the system secondary [id]; returns whether there was one. */
    fun remove(tx: Transaction, id: MemberId): Boolean {
        val nodes = tx.findNodes(LABEL, ID, id.toString()).use { it.asSequence().toList() }
        nodes.forEach { it.delete() }
        return nodes.isNotEmpty()
    }

    private fun parseId(text: String): UUID? = runCatching { UUID.fromString(text) }.getOrNull()

    private fun parseAddress(text: String): HostPort? = runCatching {
        HostPort.parse(text)
    }
        .getOrNull()
}
