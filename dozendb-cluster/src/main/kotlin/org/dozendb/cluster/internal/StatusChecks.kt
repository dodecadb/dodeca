package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.cluster.ClusterService
import org.dozendb.cluster.internal.replication.TransactionEnvelope
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.dozendb.core.servers.ServerRecord
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * `dbms.cluster.statusCheck`: whether the leader of each database this member hosts as a primary
 * replicates a status check entry ([TransactionEnvelope.statusCheck]) to a majority within a
 * timeout, and which primaries apply it, as Neo4j Enterprise checks with a dummy transaction. The
 * leader replicates the entry and each primary waits until it applied it, both as this member asks
 * them with [StatusCheckProtocol], which this class also answers, as the bulk source of its name.
 *
 * @property replicate replicates a status check of a database on this member, its leader, or throws
 *   [ConsensusUnavailableException] if the database does not run here.
 */
class StatusChecks
internal constructor(
    private val cluster: ClusterService,
    private val consensus: ConsensusLog,
    private val replicate: suspend (DatabaseRef) -> CommitResult,
    private val records: () -> Map<UUID, ServerRecord>,
) : BulkSource {
    /** How a primary took part in a status check, as Neo4j Enterprise names it. */
    enum class MemberStatus {
        /** It applied the entry. */
        APPLYING,

        /** Its log holds the entry, but it did not apply it, as while its database is stopped. */
        REPLICATING,

        /** It did not answer, or its log lacks the entry. */
        UNAVAILABLE,
    }

    /** What a leader answered when asked to replicate a status check of a database. */
    internal sealed interface Replication {
        data class Replicated(val index: Long, val term: Long) : Replication

        data class Failed(val message: String) : Replication
    }

    /** What a primary answered of its part in a status check of a database. */
    internal sealed interface Report {
        /** Its [status], and the leader and term it knows. */
        data class Reported(val status: MemberStatus, val leader: MemberId?, val term: Long) :
            Report

        data class Failed(val message: String) : Report
    }

    /**
     * A row of `dbms.cluster.statusCheck`: the part that the primary [serverId] took in the check
     * of [database], in Neo4j Enterprise's columns. [memberStatus] is null for a database this
     * member does not host as a primary, whose only row, this member's, tells so in [error].
     */
    data class Result(
        val database: String,
        val serverId: UUID,
        val serverName: String,
        val address: String?,
        val replicationSuccessful: Boolean,
        val memberStatus: MemberStatus?,
        val recognisedLeader: MemberId?,
        val recognisedLeaderTerm: Long?,
        val requester: Boolean,
        val error: String?,
    )

    /**
     * Checks the databases [names], or every database this member hosts as a primary if empty,
     * waiting up to [timeout] for the entry to be replicated and applied. Each other member asked
     * may take [timeout] longer to answer.
     */
    fun check(names: List<String>, timeout: Duration): List<Result> {
        val deadline = System.nanoTime() + timeout.toNanos()
        val running = cluster.status().filter { it.role != Role.Inactive }
        val requested: List<Pair<String, GroupStatus?>> =
            if (names.isEmpty()) {
                running.sortedBy { it.database.name }.map { it.database.name to it }
            } else {
                names
                    .map { NormalizedDatabaseName(it).name() }
                    .distinct()
                    .map { name -> name to running.firstOrNull { it.database.name == name } }
            }
        val checked = requested.mapNotNull { it.second }
        val members = checked.associate { it.database.id to cluster.groupMembers(it.database) }
        val (replications, reports) =
            runBlocking(Dispatchers.IO) {
                val replications = replicateAll(checked, members, deadline, timeout)
                replications to reportAll(checked, members, replications, deadline, timeout)
            }
        val self = cluster.self.id
        val addresses = cluster.members().associate { it.member.id to it.boltAddress?.toString() }
        val records = records()
        fun serverName(id: MemberId) = records[id.uuid]?.name ?: id.toString()
        return requested.flatMap { (name, status) ->
            if (status == null) {
                return@flatMap listOf(
                    Result(
                        name,
                        self.uuid,
                        serverName(self),
                        addresses[self],
                        replicationSuccessful = false,
                        memberStatus = null,
                        recognisedLeader = null,
                        recognisedLeaderTerm = null,
                        requester = true,
                        error = "Database '$name' is not hosted on this server as a primary.",
                    )
                )
            }
            val replication = replications.getValue(status.database.id)
            members.getValue(status.database.id).map { member ->
                val report = reports.getValue(member.id to status.database.id)
                val reported = report as? Report.Reported
                Result(
                    status.database.name,
                    member.id.uuid,
                    serverName(member.id),
                    addresses[member.id],
                    replicationSuccessful = replication is Replication.Replicated,
                    memberStatus = reported?.status ?: MemberStatus.UNAVAILABLE,
                    recognisedLeader = reported?.leader,
                    recognisedLeaderTerm = reported?.term,
                    requester = member.id == self,
                    error =
                        listOfNotNull(
                                (replication as? Replication.Failed)?.message,
                                (report as? Report.Failed)?.message,
                            )
                            .joinToString(" ")
                            .ifEmpty { null },
                )
            }
        }
    }

    /** Asks the leader of each group in [groups] to replicate a status check, by database. */
    private suspend fun replicateAll(
        groups: List<GroupStatus>,
        members: Map<UUID, List<Member>>,
        deadline: Long,
        grace: Duration,
    ): Map<UUID, Replication> = coroutineScope {
        groups
            .groupBy { it.leader }
            .map { (leader, led) ->
                async {
                    val databases = led.map { it.database }
                    val leading = leader?.let { id ->
                        members.getValue(databases.first().id).firstOrNull { it.id == id }
                    }
                    val answers =
                        when {
                            leading == null ->
                                databases.map {
                                    Replication.Failed("Database '${it.name}' has no leader.")
                                }
                            leading.id == cluster.self.id ->
                                replicateHere(databases, remaining(deadline))
                            else -> {
                                val wait = remaining(deadline)
                                ask(
                                    leading,
                                    StatusCheckProtocol.encodeReplicate(databases, wait),
                                    wait + grace,
                                    databases.size,
                                    StatusCheckProtocol::readReplications,
                                ) {
                                    Replication.Failed(it)
                                }
                            }
                        }
                    databases.map { it.id }.zip(answers)
                }
            }
            .awaitAll()
            .flatten()
            .toMap()
    }

    /** Asks each primary of [groups] for its part in their checks, by member and database. */
    private suspend fun reportAll(
        groups: List<GroupStatus>,
        members: Map<UUID, List<Member>>,
        replications: Map<UUID, Replication>,
        deadline: Long,
        grace: Duration,
    ): Map<Pair<MemberId, UUID>, Report> = coroutineScope {
        groups
            .flatMap { group ->
                val replicated = replications.getValue(group.database.id) as? Replication.Replicated
                val entry =
                    StatusCheckProtocol.Entry(
                        group.database,
                        replicated?.index ?: NOT_REPLICATED,
                        replicated?.term ?: NOT_REPLICATED,
                    )
                members.getValue(group.database.id).map { it to entry }
            }
            .groupBy({ it.first.id }, { it })
            .map { (id, asked) ->
                val member = asked.first().first
                val entries = asked.map { it.second }
                async {
                    val wait = remaining(deadline)
                    val answers =
                        if (id == cluster.self.id) {
                            reportHere(entries, wait)
                        } else {
                            ask(
                                member,
                                StatusCheckProtocol.encodeReport(entries, wait),
                                wait + grace,
                                entries.size,
                                StatusCheckProtocol::readReports,
                            ) {
                                Report.Failed(it)
                            }
                        }
                    entries.zip(answers) { entry, answer -> (id to entry.database.id) to answer }
                }
            }
            .awaitAll()
            .flatten()
            .toMap()
    }

    /** Asks [member] with [request]; if that fails, [failed] with why, for each of [count]. */
    private fun <T> ask(
        member: Member,
        request: ByteArray,
        timeout: Duration,
        count: Int,
        read: (InputStream) -> List<T>,
        failed: (String) -> T,
    ): List<T> =
        try {
            consensus.fetchBulk(member.raftAddress, StatusCheckProtocol.NAME, request, timeout) {
                read(it).also { answers ->
                    if (answers.size != count) {
                        throw IOException("${answers.size} answers to $count questions")
                    }
                }
            }
        } catch (e: ConsensusUnavailableException) {
            val failure = failed("Server ${member.id} did not answer: ${e.message}")
            List(count) { failure }
        }

    override fun serve(request: ByteArray, out: OutputStream) {
        when (val asked = StatusCheckProtocol.decode(request)) {
            is StatusCheckProtocol.Request.Replicate ->
                StatusCheckProtocol.writeReplications(
                    out,
                    runBlocking { replicateHere(asked.databases, asked.wait) },
                )
            is StatusCheckProtocol.Request.Report ->
                StatusCheckProtocol.writeReports(
                    out,
                    runBlocking { reportHere(asked.entries, asked.wait) },
                )
        }
    }

    private suspend fun replicateHere(
        databases: List<DatabaseRef>,
        wait: Duration,
    ): List<Replication> = coroutineScope {
        databases.map { async { replicateHere(it, wait) } }.awaitAll()
    }

    /** Replicates a status check of [database], led here, waiting up to [wait] for its commit. */
    private suspend fun replicateHere(database: DatabaseRef, wait: Duration): Replication {
        val result =
            try {
                withTimeoutOrNull(wait.toMillis()) { replicate(database) }
            } catch (e: ConsensusUnavailableException) {
                return Replication.Failed(e.message ?: e.toString())
            }
        return when (result) {
            null ->
                Replication.Failed(
                    "The status check of database '${database.name}' did not commit within " +
                        "${wait.toMillis()} ms."
                )
            is CommitResult.Committed -> Replication.Replicated(result.index, result.term)
            is CommitResult.NotLeader ->
                Replication.Failed(
                    "Server ${cluster.self.id} no longer leads database '${database.name}'."
                )
            is CommitResult.OutcomeUnknown ->
                Replication.Failed(
                    "The status check of database '${database.name}' may not have committed: " +
                        (result.cause.message ?: result.cause.toString())
                )
        }
    }

    private suspend fun reportHere(
        entries: List<StatusCheckProtocol.Entry>,
        wait: Duration,
    ): List<Report> = coroutineScope { entries.map { async { reportHere(it, wait) } }.awaitAll() }

    /**
     * This member's part in the check of [entry]: whether it applies the entry within [wait], else
     * whether its log holds it, and the leader and term it knows then.
     */
    private suspend fun reportHere(entry: StatusCheckProtocol.Entry, wait: Duration): Report {
        val database = entry.database
        return try {
            if (consensus.status(database).role == Role.Inactive) {
                return Report.Failed(
                    "Server ${cluster.self.id} is not a primary of database '${database.name}'."
                )
            }
            val status =
                when {
                    entry.index == NOT_REPLICATED -> MemberStatus.UNAVAILABLE
                    withTimeoutOrNull(wait.toMillis()) {
                        consensus.awaitApplied(database, entry.index)
                    } != null -> MemberStatus.APPLYING
                    consensus.termOf(database, entry.index) == entry.term ->
                        MemberStatus.REPLICATING
                    else -> MemberStatus.UNAVAILABLE
                }
            val known = consensus.status(database)
            Report.Reported(status, known.leader, known.term)
        } catch (e: ConsensusUnavailableException) {
            Report.Failed(e.message ?: e.toString())
        }
    }

    private fun remaining(deadline: Long): Duration =
        Duration.ofNanos(maxOf(0, deadline - System.nanoTime()))

    private companion object {
        /** The index and term of a status check that was not replicated. */
        const val NOT_REPLICATED = -1L
    }
}

/**
 * What a member asks another during a status check (see [StatusChecks]), as one bulk transfer under
 * [NAME]: the leader of databases to replicate a status check of each, or a primary to tell its
 * part in the checks of databases.
 *
 * A request is `byte version, byte kind, long waitMillis, int count`, then for each database `UTF
 * name, long, long` (its id), and for a report `long index, long term` of the entry replicated, or
 * -1 for both if it was not. An answer is `byte version, int count`, then for each database `byte
 * status`, then if the status is 0, for a replication `long index, long term`, for a report `UTF
 * memberStatus, boolean`, the leader's id as `long, long` if the boolean is true, and `long term`;
 * else a UTF message.
 */
internal object StatusCheckProtocol {
    const val NAME = "dozendb.statusCheck"
    private const val VERSION = 1
    private const val REPLICATE = 0
    private const val REPORT = 1
    private const val ANSWERED = 0
    private const val FAILED = 1

    /** A message's most characters, so that its modified UTF-8 fits `writeUTF`'s 64 KB. */
    private const val MESSAGE_LIMIT = 8_000

    /** A database whose status check a primary reports on, and the entry replicated, if any. */
    data class Entry(val database: DatabaseRef, val index: Long, val term: Long)

    sealed interface Request {
        /** How long the member asked may wait for the entries to commit, or to be applied. */
        val wait: Duration

        data class Replicate(val databases: List<DatabaseRef>, override val wait: Duration) :
            Request

        data class Report(val entries: List<Entry>, override val wait: Duration) : Request
    }

    fun encodeReplicate(databases: List<DatabaseRef>, wait: Duration): ByteArray =
        encode(REPLICATE, wait, databases) {}

    fun encodeReport(entries: List<Entry>, wait: Duration): ByteArray =
        encode(REPORT, wait, entries.map { it.database }) { index ->
            writeLong(entries[index].index)
            writeLong(entries[index].term)
        }

    private fun encode(
        kind: Int,
        wait: Duration,
        databases: List<DatabaseRef>,
        writeRest: DataOutputStream.(Int) -> Unit,
    ): ByteArray =
        ByteArrayOutputStream().use { bytes ->
            DataOutputStream(bytes).use { data ->
                data.writeByte(VERSION)
                data.writeByte(kind)
                data.writeLong(wait.toMillis())
                data.writeInt(databases.size)
                databases.forEachIndexed { index, database ->
                    data.writeUTF(database.name)
                    data.writeLong(database.id.mostSignificantBits)
                    data.writeLong(database.id.leastSignificantBits)
                    data.writeRest(index)
                }
            }
            bytes.toByteArray()
        }

    fun decode(bytes: ByteArray): Request =
        DataInputStream(bytes.inputStream()).use { data ->
            checkVersion(data.readUnsignedByte())
            val kind = data.readUnsignedByte()
            val wait = Duration.ofMillis(data.readLong())
            val count = data.readInt()
            fun database() = DatabaseRef(data.readUTF(), UUID(data.readLong(), data.readLong()))
            when (kind) {
                REPLICATE -> Request.Replicate(List(count) { database() }, wait)
                REPORT ->
                    Request.Report(
                        List(count) { Entry(database(), data.readLong(), data.readLong()) },
                        wait,
                    )
                else -> throw IOException("unknown status check request $kind")
            }
        }

    fun writeReplications(out: OutputStream, answers: List<StatusChecks.Replication>) =
        write(out, answers) { answer ->
            when (answer) {
                is StatusChecks.Replication.Replicated -> {
                    writeByte(ANSWERED)
                    writeLong(answer.index)
                    writeLong(answer.term)
                }
                is StatusChecks.Replication.Failed -> writeFailure(answer.message)
            }
        }

    fun readReplications(input: InputStream): List<StatusChecks.Replication> =
        read(input) {
            when (val status = readUnsignedByte()) {
                ANSWERED -> StatusChecks.Replication.Replicated(readLong(), readLong())
                FAILED -> StatusChecks.Replication.Failed(readUTF())
                else -> throw IOException("unknown replication status $status")
            }
        }

    fun writeReports(out: OutputStream, answers: List<StatusChecks.Report>) =
        write(out, answers) { answer ->
            when (answer) {
                is StatusChecks.Report.Reported -> {
                    writeByte(ANSWERED)
                    writeUTF(answer.status.name)
                    writeBoolean(answer.leader != null)
                    answer.leader?.let {
                        writeLong(it.uuid.mostSignificantBits)
                        writeLong(it.uuid.leastSignificantBits)
                    }
                    writeLong(answer.term)
                }
                is StatusChecks.Report.Failed -> writeFailure(answer.message)
            }
        }

    fun readReports(input: InputStream): List<StatusChecks.Report> =
        read(input) {
            when (val status = readUnsignedByte()) {
                ANSWERED -> {
                    val memberStatus = readUTF()
                    val leader = if (readBoolean()) MemberId(UUID(readLong(), readLong())) else null
                    StatusChecks.Report.Reported(
                        StatusChecks.MemberStatus.entries.firstOrNull { it.name == memberStatus }
                            ?: throw IOException("unknown member status $memberStatus"),
                        leader,
                        readLong(),
                    )
                }
                FAILED -> StatusChecks.Report.Failed(readUTF())
                else -> throw IOException("unknown report status $status")
            }
        }

    private fun DataOutputStream.writeFailure(message: String) {
        writeByte(FAILED)
        writeUTF(message.take(MESSAGE_LIMIT))
    }

    private fun <T> write(
        out: OutputStream,
        answers: List<T>,
        writeOne: DataOutputStream.(T) -> Unit,
    ) {
        val data = DataOutputStream(out)
        data.writeByte(VERSION)
        data.writeInt(answers.size)
        answers.forEach { data.writeOne(it) }
        data.flush()
    }

    private fun <T> read(input: InputStream, readOne: DataInputStream.() -> T): List<T> {
        val data = DataInputStream(input)
        checkVersion(data.readUnsignedByte())
        return List(data.readInt()) { data.readOne() }
    }

    private fun checkVersion(version: Int) {
        if (version != VERSION) {
            throw IOException("unsupported status check protocol version $version, not $VERSION")
        }
    }
}
