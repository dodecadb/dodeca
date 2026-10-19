package org.dozendb.cluster.core.internal

import java.util.stream.Stream
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.core.ClusterService
import org.dozendb.cluster.core.MembershipChangeException
import org.dozendb.cluster.core.displayName
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure

/**
 * Cluster procedures, callable on any database including `system`; those that change the cluster's
 * members need an administrator. Neo4j requires every instance field to be a `@Context` field.
 */
class ClusterProcedures {
    @JvmField @Context var cluster: ClusterService? = null

    @SystemProcedure
    @Procedure(name = "dozendb.cluster.status", mode = Mode.DBMS)
    @Description("The Raft status of every database group on this member.")
    fun status(): Stream<StatusRow> {
        val service = requireCluster()
        val self = service.self.id.toString()
        return service.status().stream().map {
            StatusRow(
                database = it.database.name,
                groupId = it.database.id.toString(),
                memberId = self,
                role = it.role.displayName,
                leader = it.leader?.toString(),
                term = it.term,
                commitIndex = it.commitIndex,
                appliedIndex = it.appliedIndex,
                snapshotIndex = service.latestSnapshot(it.database)?.position?.index,
                members =
                    try {
                        service.groupMembers(it.database).map { member -> member.id.toString() }
                    } catch (_: ConsensusUnavailableException) {
                        emptyList()
                    },
            )
        }
    }

    @SystemProcedure
    @Procedure(name = "dozendb.cluster.members", mode = Mode.DBMS)
    @Description("The voting members of the cluster, as seen from this member.")
    fun members(): Stream<MemberRow> =
        requireCluster().members().stream().map {
            MemberRow(
                memberId = it.member.id.toString(),
                raftAddress = it.member.raftAddress.toString(),
                boltAddress = it.boltAddress?.toString(),
                role = if (it.isLeader) "LEADER" else "FOLLOWER",
                self = it.isSelf,
                databases = it.databases,
            )
        }

    @Admin
    @SystemProcedure
    @Procedure(name = "dozendb.cluster.addMember", mode = Mode.DBMS)
    @Description(
        "Adds the member at the given Raft address to the cluster. Start it first with " +
            "dbms.dozendb.cluster.initial_members listing members of the cluster. Returns once it " +
            "votes in the group of the system database; it joins the group of every other " +
            "database once it has copied it, as dozendb.cluster.members() shows."
    )
    fun addMember(@Name("raftAddress") raftAddress: String): Stream<AddedMemberRow> {
        val address =
            try {
                HostPort.parse(raftAddress)
            } catch (e: IllegalArgumentException) {
                throw ProcedureFailure("Invalid Raft address: ${e.message}", e)
            }
        val member = changingMembers { requireCluster().addMember(address) }
        return Stream.of(AddedMemberRow(member.id.toString(), member.raftAddress.toString()))
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dozendb.cluster.removeMember", mode = Mode.DBMS)
    @Description(
        "Removes the member with the given id from the cluster. Returns once it no longer votes " +
            "in the group of the system database; the group of every other database follows. " +
            "The removed member stops its databases; to add it again, delete its data directory " +
            "first."
    )
    fun removeMember(@Name("memberId") memberId: String) {
        val id =
            try {
                MemberId.parse(memberId)
            } catch (e: IllegalArgumentException) {
                throw ProcedureFailure("Invalid member id '$memberId': ${e.message}", e)
            }
        changingMembers { requireCluster().removeMember(id) }
    }

    private fun <T> changingMembers(change: () -> T): T =
        try {
            change()
        } catch (e: MembershipChangeException) {
            throw ProcedureFailure(e.message.orEmpty(), e)
        }

    private fun requireCluster(): ClusterService =
        checkNotNull(cluster) { "DozenDB clustering is not running" }

    /** A failure whose message Neo4j reports as it is, as for any exception with a status. */
    private class ProcedureFailure(message: String, cause: Throwable) :
        RuntimeException(message, cause), Status.HasStatus {
        override fun status(): Status = Status.Procedure.ProcedureCallFailed
    }

    @JvmRecord
    data class StatusRow(
        val database: String,
        val groupId: String,
        val memberId: String,
        val role: String,
        val leader: String?,
        val term: Long,
        val commitIndex: Long,
        val appliedIndex: Long,
        /** The latest snapshot: the log no longer needs entries up to it. */
        val snapshotIndex: Long?,
        /** The ids of the members voting in the group, as this member knows them. */
        val members: List<String>,
    )

    @JvmRecord
    data class MemberRow(
        val memberId: String,
        val raftAddress: String,
        val boltAddress: String?,
        val role: String,
        val self: Boolean,
        /** The databases whose groups the member votes in, as this member knows them. */
        val databases: List<String>,
    )

    @JvmRecord data class AddedMemberRow(val memberId: String, val raftAddress: String)
}
