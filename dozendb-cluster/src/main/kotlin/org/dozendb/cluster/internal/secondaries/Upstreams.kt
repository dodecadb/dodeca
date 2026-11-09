package org.dozendb.cluster.internal.secondaries

import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.core.servers.CopyMode
import org.dozendb.core.servers.UpstreamCandidate
import org.dozendb.core.servers.UpstreamSelection

/**
 * Where the secondaries of this member pull from: the other servers that run a database and answer,
 * its primaries as its group has them and its secondaries as the system graph places them, and each
 * of them as it reports the database online. [selection] chooses among them for every pull; the
 * others follow in random order, to copy from when the chosen one cannot serve a copy.
 *
 * @property group the voting members of a database's group, and its leader if known.
 * @property secondaries the servers a database's secondaries are placed on.
 * @property online whether a server reports a database online.
 * @property answering whether a server answers this one.
 * @property addresses the consensus address of each server.
 * @property tags the server tags of each server.
 */
internal class Upstreams(
    private val self: MemberId,
    private val selection: UpstreamSelection,
    private val group: (DatabaseRef) -> Pair<Set<MemberId>, MemberId?>,
    private val secondaries: (DatabaseRef) -> Set<MemberId>,
    private val online: (MemberId, DatabaseRef) -> Boolean,
    private val answering: (MemberId) -> Boolean,
    private val addresses: () -> Map<MemberId, HostPort>,
    private val tags: () -> Map<MemberId, Set<String>>,
) {
    /** The upstreams of [database], the one to pull from first; none if no other server runs it. */
    fun of(database: DatabaseRef): List<HostPort> {
        val (voters, leader) = group(database)
        val addresses = addresses()
        val tags = tags()
        val candidates =
            (voters.map { it to CopyMode.PRIMARY } +
                    (secondaries(database) - voters).map { it to CopyMode.SECONDARY })
                .filter { (id, _) ->
                    id != self && id in addresses && answering(id) && online(id, database)
                }
                .map { (id, mode) ->
                    UpstreamCandidate(id.uuid, mode, id == leader, tags[id].orEmpty())
                }
        val chosen =
            selection.choose(candidates, tags[self].orEmpty())?.let { MemberId(it.id) }
                ?: return emptyList()
        val others = candidates.map { MemberId(it.id) }.filter { it != chosen }.shuffled()
        return (listOf(chosen) + others).map { addresses.getValue(it) }
    }
}
