package org.dozendb.cluster.internal

import java.util.stream.Stream
import org.dozendb.cluster.ClusterService
import org.dozendb.cluster.MembershipChangeException
import org.dozendb.cluster.UpgradeException
import org.dozendb.cluster.displayName
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.HostPort
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

    @JvmField @Context var servers: ServerReconciler? = null

    @JvmField @Context var administration: ServerAdministration? = null

    @JvmField @Context var secondaries: SecondaryReplication? = null

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
                routingAddress = it.routingAddress?.toString(),
                role = if (it.isLeader) "LEADER" else "FOLLOWER",
                self = it.isSelf,
                databases = it.databases,
                dozendbVersion = it.dozenDbVersion,
                formatLevels = it.formatLevels?.toString(),
            )
        }

    @SystemProcedure
    @Procedure(name = "dozendb.cluster.formatLevel", mode = Mode.DBMS)
    @Description(
        "The cluster's format level, which says what formats members write, as this member " +
            "knows it, and the format levels this member's DozenDB version supports."
    )
    fun formatLevel(): Stream<FormatLevelRow> {
        val service = requireCluster()
        service.refreshFormatLevel()
        return Stream.of(
            FormatLevelRow(service.formatLevel.toLong(), service.formatLevels.toString())
        )
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dozendb.cluster.finishUpgrade", mode = Mode.DBMS)
    @Description(
        "Finishes a rolling upgrade of DozenDB once every member runs the new version: raises " +
            "the cluster's format level to the newest a member supports, once every member " +
            "supports it, so that members write its formats. Returns once every member recorded " +
            "it; from then on no member starts with a DozenDB version that does not support it. " +
            "Runs on the leader of the system database."
    )
    fun finishUpgrade(): Stream<UpgradeRow> {
        val upgrade =
            try {
                requireCluster().finishUpgrade()
            } catch (e: UpgradeException) {
                throw ProcedureFailure(e.message.orEmpty(), e)
            }
        return Stream.of(UpgradeRow(upgrade.previous.toLong(), upgrade.current.toLong()))
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.setAutomaticallyEnableFreeServers", mode = Mode.DBMS)
    @Description(
        "Sets whether a server that joins the cluster is enabled at once, so that databases are " +
            "placed on it, instead of staying Free until ENABLE SERVER; true also enables the " +
            "servers that are Free now. Runs on the leader of the system database, as Neo4j " +
            "Enterprise's procedure of the same name."
    )
    fun setAutomaticallyEnableFreeServers(@Name("autoEnable") autoEnable: Boolean) {
        val reconciler = checkNotNull(servers) { "the servers are not known yet" }
        try {
            reconciler.setAutoEnable(autoEnable)
        } catch (e: IllegalStateException) {
            throw ProcedureFailure(e.message.orEmpty(), e)
        }
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.setDefaultAllocationNumbers", mode = Mode.DBMS)
    @Description(
        "Sets the number of primaries and secondaries that a new database has when CREATE " +
            "DATABASE gives no TOPOLOGY: 1 to 11 primaries, and 0 to 20 secondaries. Runs on the " +
            "leader of the system database, as Neo4j Enterprise's procedure of the same name."
    )
    fun setDefaultAllocationNumbers(
        @Name("primaries") primaries: Long,
        @Name("secondaries") secondaries: Long,
    ) {
        administering { it.setDefaults(primaries, secondaries) }
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.showTopologyGraphConfig", mode = Mode.DBMS)
    @Description(
        "The settings that place databases: the default numbers of primaries and secondaries, " +
            "the default database, and whether servers that join are enabled at once, as Neo4j " +
            "Enterprise's procedure of the same name shows them; allocator is always null."
    )
    fun showTopologyGraphConfig(): Stream<TopologyGraphConfigRow> {
        val config = administering { it.topologyGraphConfig() }
        return Stream.of(
            TopologyGraphConfigRow(
                null,
                config.defaults.primaries.toLong(),
                config.defaults.secondaries.toLong(),
                config.defaultDatabase,
                config.autoEnable,
            )
        )
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.cordonServer", mode = Mode.DBMS)
    @Description(
        "Cordons a server, named by its name or id: it keeps the databases it hosts and takes no " +
            "new ones, until dbms.cluster.uncordonServer or ENABLE SERVER enables it again. Runs " +
            "on the leader of the system database, as Neo4j Enterprise's procedure of the same " +
            "name."
    )
    fun cordonServer(@Name("server") server: String) {
        administering { it.cordon(server) }
    }

    @Admin
    @SystemProcedure
    @Procedure(
        name = "dbms.cluster.uncordonServer",
        mode = Mode.DBMS,
        deprecatedBy = "ENABLE SERVER",
    )
    @Description(
        "Enables a cordoned server, named by its name or id, again, as ENABLE SERVER does. Runs " +
            "on the leader of the system database, as Neo4j Enterprise's procedure of the same " +
            "name."
    )
    // Neo4j looks for Java's annotation with deprecatedBy, and warns without it.
    @Suppress("DEPRECATED_JAVA_ANNOTATION")
    @java.lang.Deprecated
    fun uncordonServer(@Name("server") server: String) {
        administering { it.uncordon(server) }
    }

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.deallocateDatabaseFromServer", mode = Mode.DBMS)
    @Description(
        "Moves the copy of a database on a server, named by its name or id, to another server; " +
            "the server stays as it is. With dryRun, only shows the move. Runs on the leader of " +
            "the system database, as Neo4j Enterprise's procedure of the same name."
    )
    fun deallocateDatabaseFromServer(
        @Name("server") server: String,
        @Name("database") database: String,
        @Name(value = "dryRun", defaultValue = "false") dryRun: Boolean,
    ): Stream<MoveRow> = deallocating(listOf(server), database, null, dryRun)

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.deallocateDatabaseFromServers", mode = Mode.DBMS)
    @Description(
        "Moves the copies of a database on servers, named by their names or ids, to other " +
            "servers; the servers stay as they are. With dryRun, only shows the moves. Runs on " +
            "the leader of the system database, as Neo4j Enterprise's procedure of the same name."
    )
    fun deallocateDatabaseFromServers(
        @Name("servers") servers: List<String>,
        @Name("database") database: String,
        @Name(value = "dryRun", defaultValue = "false") dryRun: Boolean,
    ): Stream<MoveRow> = deallocating(servers, database, null, dryRun)

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.deallocateNumberOfDatabases", mode = Mode.DBMS)
    @Description(
        "Moves the copies of a number of the databases on a server, named by its name or id, by " +
            "name, to other servers; the server stays as it is. With dryRun, only shows the " +
            "moves. Runs on the leader of the system database, as Neo4j Enterprise's procedure " +
            "of the same name."
    )
    fun deallocateNumberOfDatabases(
        @Name("server") server: String,
        @Name("number") number: Long,
        @Name(value = "dryRun", defaultValue = "false") dryRun: Boolean,
    ): Stream<MoveRow> =
        deallocating(
            listOf(server),
            null,
            number.coerceIn(0, Int.MAX_VALUE.toLong()).toInt(),
            dryRun,
        )

    private fun deallocating(
        servers: List<String>,
        database: String?,
        limit: Int?,
        dryRun: Boolean,
    ): Stream<MoveRow> = administering {
        it.deallocate(servers, database, limit, dryRun)
    }
        .stream()
        .map(::MoveRow)

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.reallocateDatabase", mode = Mode.DBMS)
    @Description(
        "Moves copies of a database to even out how many each server hosts. With dryRun, only " +
            "shows the moves. Runs on the leader of the system database, as Neo4j Enterprise's " +
            "procedure of the same name."
    )
    fun reallocateDatabase(
        @Name("database") database: String,
        @Name(value = "dryRun", defaultValue = "false") dryRun: Boolean,
    ): Stream<MoveRow> = reallocating(database, null, dryRun)

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.reallocateNumberOfDatabases", mode = Mode.DBMS)
    @Description(
        "Moves a number of copies of databases, one at a time, to even out how many each server " +
            "hosts. With dryRun, only shows the moves. Runs on the leader of the system " +
            "database, as Neo4j Enterprise's procedure of the same name."
    )
    fun reallocateNumberOfDatabases(
        @Name("number") number: Long,
        @Name(value = "dryRun", defaultValue = "false") dryRun: Boolean,
    ): Stream<MoveRow> =
        reallocating(null, number.coerceIn(0, Int.MAX_VALUE.toLong()).toInt(), dryRun)

    private fun reallocating(database: String?, limit: Int?, dryRun: Boolean): Stream<MoveRow> =
        administering {
            it.reallocate(database, limit, dryRun)
        }
        .stream()
        .map(::MoveRow)

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

    private fun <T> administering(change: (ServerAdministration) -> T): T {
        val administration = checkNotNull(administration) { "the servers are not known yet" }
        return try {
            change(administration)
        } catch (e: IllegalStateException) {
            throw ProcedureFailure(e.message.orEmpty(), e)
        }
    }

    private fun <T> changingMembers(change: () -> T): T =
        try {
            change()
        } catch (e: MembershipChangeException) {
            throw ProcedureFailure(e.message.orEmpty(), e)
        }

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.cluster.secondaryReplicationDisable", mode = Mode.DBMS)
    @Description(
        "The toggle can pause or resume the secondary replication process: with pause, the " +
            "secondary of the database on the connected server pulls no transactions until it is " +
            "resumed, or the database or the server starts again; routing tables leave it out " +
            "meanwhile. Returns its state, paused or running, as Neo4j Enterprise's procedure of " +
            "the same name."
    )
    fun secondaryReplicationDisable(
        @Name("databaseName") databaseName: String,
        @Name("pause") pause: Boolean,
    ): Stream<ReplicationStateRow> = toggled(databaseName, pause)

    @Admin
    @SystemProcedure
    @Procedure(
        name = "dbms.cluster.readReplicaToggle",
        mode = Mode.DBMS,
        deprecatedBy = "dbms.cluster.secondaryReplicationDisable",
    )
    @Description(
        "The toggle can pause or resume read replica (deprecated in favor of " +
            "dbms.cluster.secondaryReplicationDisable)."
    )
    // Neo4j looks for Java's annotation with deprecatedBy, and warns without it.
    @Suppress("DEPRECATED_JAVA_ANNOTATION")
    @java.lang.Deprecated
    fun readReplicaToggle(
        @Name("databaseName") databaseName: String,
        @Name("pause") pause: Boolean,
    ): Stream<ReplicationStateRow> = toggled(databaseName, pause)

    private fun toggled(databaseName: String, pause: Boolean): Stream<ReplicationStateRow> {
        val replication = checkNotNull(secondaries) { "DozenDB clustering is not running" }
        val state =
            try {
                replication.toggle(databaseName, pause)
            } catch (e: IllegalArgumentException) {
                throw ProcedureFailure(e.message.orEmpty(), e)
            }
        return Stream.of(ReplicationStateRow(state))
    }

    private fun requireCluster(): ClusterService =
        checkNotNull(cluster) { "DozenDB clustering is not running" }

    /** A failure whose message Neo4j reports as it is, as for any exception with a status. */
    private class ProcedureFailure(message: String, cause: Throwable) :
        RuntimeException(message, cause), Status.HasStatus {
        override fun status(): Status = Status.Procedure.ProcedureCallFailed
    }

    /** The settings that place databases, in Neo4j Enterprise's columns. */
    @JvmRecord
    data class TopologyGraphConfigRow(
        val allocator: String?,
        val defaultPrimariesCount: Long,
        val defaultSecondariesCount: Long,
        val defaultDatabase: String?,
        val autoEnableFreeServers: Boolean,
    )

    /** A copy moved, or to be moved, in Neo4j Enterprise's columns. */
    @JvmRecord
    data class MoveRow(
        val database: String,
        val fromServerName: String,
        val fromServerId: String,
        val toServerName: String,
        val toServerId: String,
        val mode: String,
    ) {
        constructor(
            named: ServerAdministration.NamedMove
        ) : this(
            named.move.database,
            named.fromName,
            named.move.from.toString(),
            named.toName,
            named.move.to.toString(),
            named.move.mode.label,
        )
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
        /** Where other members forward writes when it leads; null if it takes none. */
        val routingAddress: String?,
        val role: String,
        val self: Boolean,
        /** The databases whose groups the member votes in, as this member knows them. */
        val databases: List<String>,
        /** The member's DozenDB version, once it reported it. */
        val dozendbVersion: String?,
        /** The format levels the member's DozenDB version supports, once it reported them. */
        val formatLevels: String?,
    )

    @JvmRecord data class FormatLevelRow(val formatLevel: Long, val supportedFormatLevels: String)

    @JvmRecord data class UpgradeRow(val previousFormatLevel: Long, val formatLevel: Long)

    @JvmRecord data class AddedMemberRow(val memberId: String, val raftAddress: String)

    @JvmRecord data class ReplicationStateRow(val state: String)
}
