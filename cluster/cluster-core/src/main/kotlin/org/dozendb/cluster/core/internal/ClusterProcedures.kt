package org.dozendb.cluster.core.internal

import java.util.stream.Stream
import org.dozendb.cluster.core.ClusterService
import org.dozendb.cluster.core.displayName
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Procedure

/**
 * Read-only cluster procedures, callable on any database including `system`. Neo4j requires every
 * instance field to be a `@Context` field.
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
            )
        }

    private fun requireCluster(): ClusterService =
        checkNotNull(cluster) { "DozenDB clustering is not running" }

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
    )

    @JvmRecord
    data class MemberRow(
        val memberId: String,
        val raftAddress: String,
        val boltAddress: String?,
        val role: String,
        val self: Boolean,
    )
}
