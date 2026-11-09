package org.dozendb.cluster

import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.PeerInfo
import org.dozendb.consensus.Role
import org.dozendb.consensus.Snapshot
import org.dozendb.core.servers.CopyMode
import org.neo4j.logging.Log

/**
 * A server of the cluster as seen from this member; [boltAddress] is null while it is not known,
 * and [routingAddress] also if the member takes no forwarded writes.
 *
 * @property databases the databases whose groups the member votes in, as this member knows them,
 *   and `system`.
 * @property isSecondary whether it hosts `system` as a secondary instead of voting in its group.
 */
data class MemberView(
    val member: Member,
    val isLeader: Boolean,
    val isSelf: Boolean,
    val boltAddress: HostPort? = null,
    val routingAddress: HostPort? = null,
    val databases: List<String> = emptyList(),
    val dozenDbVersion: String? = null,
    val formatLevels: FormatLevels? = null,
    val isSecondary: Boolean = false,
)

/** The cluster's format level before and after [ClusterService.finishUpgrade]. */
data class FormatUpgrade(val previous: Int, val current: Int)

/** A change of the cluster's members that cannot be made; the message tells the operator why. */
class MembershipChangeException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

/**
 * Runs this member's part of the cluster: starts consensus, forms, joins or rejoins the `system`
 * group, makes sure this member starts from the cluster's databases, follows and changes the
 * cluster's members, and reports status. It knows nothing about Neo4j internals.
 *
 * The cluster's servers are the members of the `system` group and the system secondaries
 * ([servers]), which pull `system` instead of voting in its group ([systemHosting]). A member that
 * starts without Raft state and finds the cluster formed without it joins it: it copies the
 * databases it needs to start, runs the `system` group without voting in it, and waits until a
 * member of the cluster adds it with [addMember]. A member removed with [removeMember] stops its
 * databases and takes no part in the cluster any more; to be added again it needs its data
 * directory deleted, after which it joins as a new member.
 *
 * A member runs one DozenDB version, which supports [formatLevels]; the cluster's format level says
 * which formats members write. It rises when an administrator finishes an upgrade with
 * [finishUpgrade]. A member refuses to start, to rejoin or to be added when it does not support the
 * cluster's format level, or runs another Neo4j version than the members it reaches.
 *
 * @property metadata what this member reports when probed, compared with a member being added.
 * @property replicatedDatabases the replicated databases running on this member.
 * @property directory the members' Bolt addresses, if this member serves Bolt.
 * @property formatLevelStore where the cluster records its format level, once `system` runs.
 * @property upgradeConfirmation how long [finishUpgrade] waits for every member to record the
 *   cluster's format level.
 * @property servers the cluster's servers.
 * @property systemMode the mode this member is configured to host `system` in,
 *   `server.cluster.system_database_mode`.
 * @property systemHosting how this member hosts `system` as a secondary, if it can.
 */
class ClusterService(
    private val consensus: ConsensusLog,
    val systemDatabase: DatabaseRef,
    private val formation: ClusterFormation,
    private val stores: LocalStores,
    private val membershipStore: MembershipStore,
    private val userLog: Log,
    private val metadata: () -> Map<String, String> = { emptyMap() },
    private val replicatedDatabases: () -> List<DatabaseRef> = { emptyList() },
    val directory: MemberDirectory? = null,
    val formatLevels: FormatLevels = FormatLevels.SUPPORTED,
    private val formatLevelStore: FormatLevelStore? = null,
    private val upgradeConfirmation: Duration = Duration.ofSeconds(30),
    private val servers: ClusterServers = ClusterServers(consensus, systemDatabase),
    private val systemMode: CopyMode = CopyMode.PRIMARY,
    private val systemHosting: SystemHosting? = null,
) {
    private var statusListener: AutoCloseable? = null
    private val levelRefresh: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "dozendb-format-level").apply { isDaemon = true }
    }
    private val levelRefreshRequested = AtomicBoolean(false)
    private val followRequested = AtomicBoolean(false)

    /** Turns `system` here between primary and secondary, one change at a time. */
    private val modeChanges: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-system-mode").apply { isDaemon = true }
        }
    private val turning = AtomicBoolean(false)

    /** Whether this member hosts `system` as a secondary, which pulls it. */
    @Volatile
    var pullsSystem: Boolean = false
        private set

    @Volatile private var record: MembershipRecord? = null

    /**
     * The members this member formed the cluster with, if it is one of them: the initial members of
     * the groups of `system` and the default database. Null for a member that joined later.
     */
    val foundingMembers: List<Member>?
        get() = record?.formation?.takeIf { members -> members.any { it.id == self.id } }

    /** This member's place in the cluster. */
    val state: MemberState
        get() = record?.state ?: MemberState.JOINING

    /**
     * Whether this member brought the store of [database] to the cluster it formed, and has not yet
     * detached it from the Raft group it may have been replicated in before: a dump or copy of a
     * clustered database holds the Raft indexes of that group.
     */
    fun mustDetach(database: String): Boolean = database in record?.storesToDetach.orEmpty()

    /** Records that the store of [database] no longer refers to a Raft group it came from. */
    fun detached(database: String) {
        remember { it.copy(storesToDetach = it.storesToDetach - database) }
    }

    /** The cluster's format level, as this member last saw it. */
    val formatLevel: Int
        get() = record?.formatLevel ?: FormatLevels.INITIAL

    /** Runs once this member learns that it was removed from the cluster, on an internal thread. */
    @Volatile var onRemoved: () -> Unit = {}

    /**
     * Starts consensus and blocks until this member takes part in the `system` group, with the
     * databases it lacks copied from another member; a system secondary, until it has a copy of
     * `system` to pull (see [startPulling]). Runs before any database starts.
     */
    fun start() {
        record = membershipStore.read()
        val firstStart = record == null
        requireSupported(formatLevel, "as this member last saw it")
        if (systemMode == CopyMode.SECONDARY) {
            formation.refuseInitialSecondary(SYSTEM_DATABASE_MODE_SETTING)
        }
        consensus.start()
        val hosting = systemHosting
        if (hosting != null && hosting.pulls) {
            startPulling(hosting)
            return
        }
        val joined =
            if (systemDatabase.id in consensus.localGroups()) {
                userLog.info("Rejoining the cluster from local Raft state")
                checkRejoin()
                if (systemMode == CopyMode.SECONDARY) logOtherMode(CopyMode.PRIMARY)
                null
            } else {
                formation.awaitFormation(systemDatabase.id).also { found ->
                    remember {
                        if (found.members.any { member -> member.id == self.id }) {
                            it.copy(
                                state = MemberState.MEMBER,
                                formation = found.members,
                                // Only the member whose databases the cluster starts from, all of
                                // them, and only on its first start: decided before any copy, and
                                // kept if it crashes before detaching them.
                                storesToDetach =
                                    if (firstStart && stores.existed) stores.present().toSet()
                                    else it.storesToDetach,
                            )
                        } else {
                            it.copy(state = MemberState.JOINING)
                        }
                    }
                }
            }
        // Only `system`: the other databases are copied by the servers they are placed on, once
        // the system leader placed them.
        val missing = stores.missing().filter { it == systemDatabase.name }
        if (missing.isNotEmpty()) {
            val source = joined?.storeSource
            if (source != null && source.id == consensus.self.id) {
                userLog.info("This member creates the databases of the new cluster")
            } else {
                copyStores(source, "databases ${missing.joinToString()}") {
                    stores.copyFrom(it, missing)
                }
            }
        }
        val roleLogger = RoleChangeLogger(userLog)
        statusListener =
            consensus.addStatusListener(systemDatabase) {
                roleLogger(it)
                followMembership()
            }
        // The system database attaches its state machine when it starts. A joining member's
        // bootstrap members leave it out: it votes once it is added.
        consensus.joinGroup(systemDatabase, joined?.members ?: emptyList())
        followMembership()
        if (state == MemberState.REMOVED) userLog.error(REMOVED_MESSAGE)
        if (state == MemberState.JOINING) {
            userLog.info(
                "This member is not part of the cluster yet. To add it, run " +
                    "CALL dozendb.cluster.addMember('%s') on a member of the cluster.",
                self.raftAddress,
            )
        }
        directory?.start()
    }

    /**
     * Starts this member as a system secondary, which pulls `system` instead of voting in its
     * group. Without a secondary's store of `system`, it first waits until the initial members have
     * formed the cluster, which a secondary never does, and copies `system` from a member that runs
     * it. It is a member once the system graph it pulls records it (see [addMember]).
     */
    private fun startPulling(hosting: SystemHosting) {
        if (!hosting.pulled()) {
            formation.awaitFormation(systemDatabase.id, form = false)
            if (record == null) remember { it }
            copyStores(null, "the system database, to pull it as a secondary") {
                hosting.copyPulled(it)
            }
        }
        pullsSystem = true
        if (state == MemberState.REMOVED) userLog.error(REMOVED_MESSAGE)
        if (state == MemberState.MEMBER && systemMode == CopyMode.PRIMARY) {
            logOtherMode(CopyMode.SECONDARY)
        }
        if (state == MemberState.JOINING) {
            userLog.info(
                "This server hosts the system database as a secondary and is not part of the " +
                    "cluster yet. To add it, run CALL dozendb.cluster.addMember('%s') on the " +
                    "leader of the system database.",
                self.raftAddress,
            )
        }
        directory?.start()
    }

    /**
     * Looks again, in the background, whether this member was admitted or dropped, as `system`
     * changed; only on a system secondary, which learns it from the system graph it pulls.
     */
    fun systemChanged() {
        if (!pullsSystem || !followRequested.compareAndSet(false, true)) return
        try {
            levelRefresh.execute {
                followRequested.set(false)
                try {
                    followMembership()
                } catch (e: Exception) {
                    userLog.debug("Reading this member's membership failed: %s", e.toString())
                }
            }
        } catch (_: RejectedExecutionException) {
            // Stopping.
        }
    }

    /**
     * Tells why this member, which hosts `system` in [mode], does not host it as configured: the
     * mode changes once `addMember` admits it again.
     */
    private fun logOtherMode(mode: CopyMode) {
        userLog.info(
            "This server is configured to host the system database as a %s " +
                "(%s=%s), but hosts it as a %s: it does so until CALL " +
                "dozendb.cluster.addMember('%s') on the leader of the system database changes it.",
            systemMode.label,
            SYSTEM_DATABASE_MODE_SETTING,
            systemMode.name,
            mode.label,
            self.raftAddress,
        )
    }

    /**
     * Turns `system` here into a secondary, in the background, as this member, configured as one,
     * left its group: the leader of `system` admitted it as a secondary, or DROP SERVER removed it,
     * which the system graph it then pulls tells.
     */
    private fun turnSecondary(hosting: SystemHosting) {
        changeMode {
            userLog.info(
                "This server left the group of the system database: it turns into a secondary of " +
                    "it, as configured, from its own store"
            )
            statusListener?.close()
            statusListener = null
            hosting.turnSecondary()
            pullsSystem = true
            userLog.info("This server pulls the system database as a secondary now")
            true
        }
    }

    /**
     * Turns `system` here into a primary, in the background, as the leader of `system` asks this
     * system secondary, configured as a primary, to: it runs the group from its own store and waits
     * to be added. Tried again while it cannot be.
     */
    private fun turnPrimary(hosting: SystemHosting) {
        changeMode {
            if (!hosting.turnPrimary()) return@changeMode false
            pullsSystem = false
            val roleLogger = RoleChangeLogger(userLog)
            statusListener =
                consensus.addStatusListener(systemDatabase) {
                    roleLogger(it)
                    followMembership()
                }
            userLog.info(
                "This server runs the group of the system database from its own store, as the " +
                    "leader of the system database asked: it votes in it once the leader adds it"
            )
            true
        }
    }

    /**
     * Runs [change] on the mode changes' thread unless one runs, again after a while if it returns
     * false.
     */
    private fun changeMode(change: () -> Boolean) {
        if (!turning.compareAndSet(false, true)) return
        try {
            modeChanges.execute {
                var done = false
                try {
                    done = change()
                } catch (e: Exception) {
                    userLog.error("Changing how this server hosts the system database failed", e)
                } finally {
                    turning.set(false)
                }
                if (done) systemChanged() else retryLater()
            }
        } catch (_: RejectedExecutionException) {
            // Stopping.
            turning.set(false)
        }
    }

    private fun retryLater() {
        try {
            modeChanges.schedule(
                {
                    followRequested.set(false)
                    systemChanged()
                },
                MODE_CHANGE_RETRY_MILLIS,
                TimeUnit.MILLISECONDS,
            )
        } catch (_: RejectedExecutionException) {
            // Stopping.
        }
    }

    /**
     * A system secondary is a member once the system graph records it, and removed once it records
     * it dropped. Configured as a primary, it turns primary once the system graph asks it to.
     */
    @Synchronized
    private fun followPulled(hosting: SystemHosting) {
        if (state == MemberState.JOINING && servers.secondaries().any { it.id == self.id }) {
            remember { it.copy(state = MemberState.MEMBER) }
            userLog.info(
                "This server is now one of the cluster's servers, a secondary of the system " +
                    "database"
            )
        }
        if (state == MemberState.MEMBER && hosting.dropped()) {
            remember { it.copy(state = MemberState.REMOVED) }
            userLog.error(REMOVED_MESSAGE)
            onRemoved()
            return
        }
        if (state == MemberState.MEMBER && systemMode == CopyMode.PRIMARY && hosting.promoted()) {
            turnPrimary(hosting)
        }
    }

    /**
     * Records that this member became a voting member, once the change of members settled, and that
     * it was removed, which only the group stopping tells: a follower that installs a snapshot
     * briefly shows an older configuration, which may leave it out.
     */
    @Synchronized
    private fun followMembership() {
        val hosting = systemHosting
        if (turning.get()) return
        if (pullsSystem && hosting != null) return followPulled(hosting)
        val membership =
            try {
                consensus.membership(systemDatabase)
            } catch (_: Exception) {
                // Not running yet, or stopping.
                return
            }
        val included = membership.members.any { it.id == self.id }
        if (state == MemberState.JOINING && membership.settled && included) {
            remember { it.copy(state = MemberState.MEMBER) }
            userLog.info(
                "This member is now one of the cluster's members; it joins the group of each " +
                    "database once it has copied it"
            )
        }
        if (state == MemberState.MEMBER && membership.removed) {
            // Configured as a secondary, it pulls on: the system graph then tells whether it was
            // admitted as a secondary or dropped.
            if (systemMode == CopyMode.SECONDARY && hosting != null) return turnSecondary(hosting)
            remember { it.copy(state = MemberState.REMOVED) }
            userLog.error(REMOVED_MESSAGE)
            onRemoved()
        }
    }

    /** Changes this member's record with [change] and keeps the result. */
    @Synchronized
    private fun remember(change: (MembershipRecord) -> MembershipRecord) {
        val update = change(record ?: MembershipRecord(MemberState.JOINING))
        membershipStore.write(update)
        record = update
    }

    /**
     * Before a member that has Raft state rejoins: refuses if a server it reaches runs another
     * Neo4j version, or says that the cluster uses a format level this member does not support, as
     * after an upgrade was finished without it. Servers it does not reach are not checked.
     */
    private fun checkRejoin() {
        val others =
            try {
                servers.all().filter { it.id != self.id }
            } catch (_: ConsensusUnavailableException) {
                return
            }
        val neo4jVersion = metadata()[MemberMetadata.NEO4J_VERSION]
        for ((member, answer) in probeAll(others)) {
            val peer = answer.getOrNull()?.takeIf { it.member.id == member.id } ?: continue
            val version = peer.metadata[MemberMetadata.NEO4J_VERSION]
            if (version != neo4jVersion) {
                throw IncompatibleVersionException(
                    "Member ${peer.member} runs Neo4j $version, but this member runs Neo4j " +
                        "$neo4jVersion. All members must run the same Neo4j version: to upgrade " +
                        "Neo4j, shut down every member first."
                )
            }
            val level = peer.metadata[MemberMetadata.FORMAT_LEVEL]?.toIntOrNull() ?: continue
            requireSupported(level, "as member ${peer.member} reports")
        }
    }

    private fun requireSupported(level: Int, source: String) {
        if (level !in formatLevels) throw IncompatibleVersionException(unsupported(level, source))
    }

    private fun unsupported(level: Int, source: String): String =
        "The cluster uses format level $level, $source, but this member's DozenDB version " +
            "${metadata()[MemberMetadata.DOZENDB_VERSION]} supports format levels $formatLevels. " +
            "Run a DozenDB version that supports format level $level."

    /** The format levels [peer] supports; [invalid] tells why they cannot be read. */
    private fun reportedLevels(
        peer: PeerInfo,
        invalid: (String) -> RuntimeException,
    ): FormatLevels =
        try {
            FormatLevels.parse(peer.metadata[MemberMetadata.FORMAT_LEVELS])
        } catch (e: IllegalArgumentException) {
            throw invalid(e.message.orEmpty())
        }

    private fun probeAll(members: List<Member>): List<Pair<Member, Result<PeerInfo>>> =
        runBlocking {
            members
                .map { member ->
                    async { member to runCatching { consensus.probe(member.raftAddress) } }
                }
                .awaitAll()
        }

    /**
     * Asks for [refreshFormatLevel] in the background, as when `system` starts or applies a
     * transaction; requests made while one waits are merged.
     */
    fun requestFormatLevelRefresh() {
        if (!levelRefreshRequested.compareAndSet(false, true)) return
        try {
            levelRefresh.execute {
                levelRefreshRequested.set(false)
                try {
                    refreshFormatLevel()
                } catch (e: Exception) {
                    // `system` stopped meanwhile; it asks again when it runs.
                    userLog.debug("Reading the cluster's format level failed: %s", e.toString())
                }
            }
        } catch (_: RejectedExecutionException) {
            // Stopping.
        }
    }

    /**
     * Reads the cluster's format level from the system graph and keeps it in this member's record,
     * so that this member refuses to start, later, with a version that does not support it.
     */
    fun refreshFormatLevel() {
        val level = formatLevelStore?.formatLevel() ?: return
        if (level == formatLevel) return
        remember { it.copy(formatLevel = level) }
        if (level in formatLevels) {
            userLog.info("The cluster uses format level %d", level)
        } else {
            userLog.error(unsupported(level, "as this member's system database records it"))
        }
    }

    /**
     * Finishes a rolling upgrade: raises the cluster's format level to the newest level a server
     * supports, so that servers write the formats it names, once every server supports it. Returns
     * once every server recorded the level: from then on none starts with a DozenDB version that
     * does not support it. Runs on the leader of `system`.
     *
     * @throws UpgradeException if a member does not answer, does not support that level yet, or
     *   does not record it in time.
     */
    fun finishUpgrade(): FormatUpgrade {
        val store = checkNotNull(formatLevelStore) { "the cluster's format level is not known" }
        if (consensus.status(systemDatabase).role != Role.Leader) {
            val leader = consensus.status(systemDatabase).leader
            throw UpgradeException(
                "This member is not the leader of the system database; finish the upgrade on " +
                    "the leader${leader?.let { ", member $it" }.orEmpty()}."
            )
        }
        val current = store.formatLevel()
        val others = servers.all().filter { it.id != self.id }
        val supported =
            mapOf(self to formatLevels) +
                probeAll(others).associate { (member, answer) ->
                    val peer = answer.getOrElse { e ->
                        throw UpgradeException(
                            "Member $member did not tell the format levels it supports " +
                                "(${e.message}). Every member must answer to finish an upgrade.",
                            e,
                        )
                    }
                    member to
                        reportedLevels(peer) { UpgradeException("Member $member reports $it.") }
                }
        val target = maxOf(current, supported.values.maxOf { it.newest })
        val behind = supported.filterValues { current !in it || target !in it }
        if (behind.isNotEmpty()) {
            throw UpgradeException(
                "Upgrade every member to a DozenDB version that supports format level $target " +
                    "first: " +
                    behind.entries.joinToString { (member, levels) ->
                        "member $member supports format levels $levels"
                    } +
                    "."
            )
        }
        if (target <= current) {
            awaitRecorded(current, others)
            userLog.info(
                "The cluster already uses format level %d, the newest any member supports",
                current,
            )
            return FormatUpgrade(current, current)
        }
        try {
            store.recordFormatLevel(target)
        } catch (e: Exception) {
            throw UpgradeException("Recording format level $target failed: ${e.message}", e)
        }
        awaitRecorded(target, others)
        userLog.info(
            "Finished the upgrade: the cluster uses format level %d, after %d. Members can no " +
                "longer return to DozenDB versions that do not support it.",
            target,
            current,
        )
        return FormatUpgrade(current, target)
    }

    /**
     * Waits until this member and [others] recorded format [level], which they do once they applied
     * the transaction that raised it; they report the level they recorded when probed.
     */
    private fun awaitRecorded(level: Int, others: List<Member>) {
        refreshFormatLevel()
        val deadline = System.nanoTime() + upgradeConfirmation.toNanos()
        while (true) {
            val pending =
                probeAll(others).mapNotNull { (member, answer) ->
                    val reported =
                        answer
                            .getOrNull()
                            ?.metadata
                            ?.get(MemberMetadata.FORMAT_LEVEL)
                            ?.toIntOrNull()
                    member.takeIf { reported == null || reported < level }
                }
            if (pending.isEmpty()) return
            if (System.nanoTime() - deadline >= 0) {
                val waited =
                    if (upgradeConfirmation.toMillis() % 1000 == 0L) {
                        "${upgradeConfirmation.toSeconds()} seconds"
                    } else {
                        "${upgradeConfirmation.toMillis()} ms"
                    }
                throw UpgradeException(
                    "The cluster uses format level $level, but " +
                        pending.joinToString { "member $it" } +
                        " did not record it within $waited: until they do, they may still start " +
                        "with a DozenDB version that does not support it. Call finishUpgrade " +
                        "again once they answer."
                )
            }
            Thread.sleep(CONFIRMATION_POLL_MILLIS)
        }
    }

    /**
     * Copies [what] from [source], or any initial member that serves its databases, with [copy],
     * until a copy succeeds.
     */
    private fun copyStores(source: Member?, what: String, copy: (HostPort) -> Unit) {
        while (true) {
            val address = formation.awaitStoreSource(source)
            userLog.info("Copying %s from %s", what, address)
            try {
                copy(address)
                return
            } catch (e: ConsensusUnavailableException) {
                userLog.warn("Copying %s from %s failed, retrying: %s", what, address, e.message)
            } catch (e: java.io.IOException) {
                userLog.warn("Copying %s from %s failed, retrying: %s", what, address, e.message)
            }
            formation.pause()
        }
    }

    fun stop() {
        modeChanges.shutdownNow()
        levelRefresh.shutdownNow()
        directory?.stop()
        statusListener?.close()
        statusListener = null
        consensus.close()
    }

    val self: Member
        get() = consensus.self

    /** The local status of every group this member runs. */
    fun status(): List<GroupStatus> =
        (listOf(systemDatabase) + replicatedDatabases()).distinct().map(consensus::status)

    /** The voting members of the group of [database], as this member knows them. */
    fun groupMembers(database: DatabaseRef): List<Member> = consensus.members(database)

    /** The latest snapshot of [database] on this member: its log may lack entries up to it. */
    fun latestSnapshot(database: DatabaseRef): Snapshot? = consensus.latestSnapshot(database)

    /** The servers of the cluster: the voting members of `system`, then its secondaries. */
    fun members(): List<MemberView> {
        val leader = servers.leader()
        val groups =
            replicatedDatabases()
                .filter { it.id != systemDatabase.id }
                .associateWith { database ->
                    try {
                        consensus.members(database).map { it.id }
                    } catch (_: ConsensusUnavailableException) {
                        emptyList()
                    }
                }
        val voters = servers.voters().members
        val secondaries = servers.secondaries().filter { s -> voters.none { it.id == s.id } }
        return (voters + secondaries).map { member ->
            val reported =
                if (member.id == consensus.self.id) metadata()
                else directory?.reported(member.id).orEmpty()
            MemberView(
                member,
                isLeader = member.id == leader,
                isSelf = member.id == consensus.self.id,
                boltAddress = directory?.boltAddress(member.id),
                routingAddress = directory?.routingAddress(member.id),
                dozenDbVersion = reported[MemberMetadata.DOZENDB_VERSION],
                formatLevels =
                    reported[MemberMetadata.FORMAT_LEVELS]?.let {
                        runCatching { FormatLevels.parse(it) }.getOrNull()
                    },
                databases =
                    listOf(systemDatabase.name) +
                        groups.filterValues { member.id in it }.keys.map { it.name }.sorted(),
                isSecondary = member in secondaries,
            )
        }
    }

    /**
     * Adds the member at [address] to the cluster, in the mode it reports
     * (`server.cluster.system_database_mode`), and returns it. It must wait to join this cluster:
     * started with initial members of this cluster, without Raft state of its own, and on the same
     * Neo4j version. A primary is first brought up to date with the `system` group; this returns
     * once it votes in it. A system secondary, which pulls `system`, is admitted by the leader of
     * `system`, which records it ([SystemHosting.admit]); this returns once it is recorded. It
     * joins the group of each database once it has copied it.
     *
     * A server of the cluster already changes its mode, on the leader of `system`, if it reports
     * the other one, as after it restarted with the other setting: a primary is recorded as a
     * secondary and removed from the `system` group, which makes it turn secondary from its own
     * store; refused if no voter would be left. A secondary is asked, in its record, to turn
     * primary, which it does from its own store once it pulled that, and is added to the group, and
     * its record goes; this returns once that is done. Otherwise this returns at once.
     *
     * @throws MembershipChangeException if the member cannot be added.
     */
    fun addMember(address: HostPort): Member {
        requireMember()
        val peer =
            try {
                runBlocking { consensus.probe(address) }
            } catch (e: ConsensusUnavailableException) {
                throw MembershipChangeException(
                    "No DozenDB member answers at $address: ${e.message}",
                    e,
                )
            }
        val candidate = peer.member
        val voter = servers.voters().members.any { it.id == candidate.id }
        val recorded = servers.secondaries().any { it.id == candidate.id }
        val hosting = systemHosting
        if (voter || recorded) {
            if (hosting != null && secondary(peer) && voter) turnIntoSecondary(hosting, candidate)
            if (hosting != null && !secondary(peer) && recorded) {
                turnIntoPrimary(hosting, address, candidate, voter)
            }
            return candidate
        }
        checkCanJoin(address, peer)
        if (secondary(peer)) {
            val hosting =
                systemHosting
                    ?: throw MembershipChangeException("This cluster takes no system secondaries.")
            userLog.info("Admitting server %s as a secondary of the system database", candidate)
            hosting.admit(candidate)
            userLog.info("Admitted server %s as a secondary of the system database", candidate)
            return candidate
        }
        if (pullsSystem) {
            throw MembershipChangeException(
                "This server hosts the system database as a secondary: add the member on a " +
                    "primary of the system database."
            )
        }
        userLog.info("Adding member %s to the cluster", candidate)
        try {
            runBlocking { consensus.addMember(systemDatabase, candidate, CHANGE_TIMEOUT) }
        } catch (e: ConsensusUnavailableException) {
            throw MembershipChangeException("Adding member $candidate failed: ${e.message}", e)
        }
        userLog.info("Added member %s to the cluster", candidate)
        return candidate
    }

    /**
     * Removes the member [id] from the cluster: from the `system` group, which it then stops
     * running, or, for a system secondary, its record ([SystemHosting.forget]), which it learns of
     * from the system graph it pulls. Returns once the change is committed; the group of each
     * database follows, and the removed member stops its databases.
     *
     * @throws MembershipChangeException if [id] is not a member, is the only one, or the change
     *   failed.
     */
    fun removeMember(id: MemberId) {
        requireMember()
        val hosting = systemHosting
        val secondary = servers.secondaries().firstOrNull { it.id == id }
        if (hosting != null && secondary != null && servers.voters().members.none { it.id == id }) {
            userLog.info("Removing system secondary %s from the cluster", secondary)
            hosting.forget(id)
            userLog.info("Removed system secondary %s from the cluster", secondary)
            return
        }
        val members = consensus.members(systemDatabase)
        val member =
            members.firstOrNull { it.id == id }
                ?: throw MembershipChangeException("$id is not a member of the cluster.")
        if (members.size == 1) {
            throw MembershipChangeException(
                "$member is the only member of the cluster and cannot be removed."
            )
        }
        userLog.info("Removing member %s from the cluster", member)
        try {
            runBlocking { consensus.removeMember(systemDatabase, id, CHANGE_TIMEOUT) }
        } catch (e: ConsensusUnavailableException) {
            throw MembershipChangeException("Removing member $member failed: ${e.message}", e)
        }
        userLog.info("Removed member %s from the cluster", member)
    }

    /**
     * Records the voter [candidate] a system secondary, then removes it from the `system` group.
     */
    private fun turnIntoSecondary(hosting: SystemHosting, candidate: Member) {
        if (servers.voters().members.size <= 1) {
            throw MembershipChangeException(
                "Member $candidate is the only voting member of the system database: it stays a " +
                    "primary of it until another server is one."
            )
        }
        userLog.info("Turning server %s into a secondary of the system database", candidate)
        hosting.admit(candidate)
        try {
            runBlocking { consensus.removeMember(systemDatabase, candidate.id, CHANGE_TIMEOUT) }
        } catch (e: ConsensusUnavailableException) {
            throw MembershipChangeException(
                "Removing server $candidate from the group of the system database failed: " +
                    "${e.message}",
                e,
            )
        }
        userLog.info(
            "Server %s left the group of the system database: it pulls it as a secondary",
            candidate,
        )
    }

    /**
     * Asks the system secondary [candidate] to turn primary, waits until it runs the `system`
     * group, adds it unless it is a [voter] already, and deletes its record.
     */
    private fun turnIntoPrimary(
        hosting: SystemHosting,
        address: HostPort,
        candidate: Member,
        voter: Boolean,
    ) {
        userLog.info("Turning server %s into a primary of the system database", candidate)
        if (!voter) {
            hosting.promote(candidate.id)
            awaitRunsSystem(address, candidate)
            try {
                runBlocking { consensus.addMember(systemDatabase, candidate, CHANGE_TIMEOUT) }
            } catch (e: ConsensusUnavailableException) {
                throw MembershipChangeException(
                    "Adding server $candidate to the group of the system database failed: " +
                        "${e.message}",
                    e,
                )
            }
        }
        hosting.forget(candidate.id)
        userLog.info("Server %s is a primary of the system database", candidate)
    }

    /** Waits until [candidate] at [address] runs the `system` group, as it turned primary. */
    private fun awaitRunsSystem(address: HostPort, candidate: Member) {
        val deadline = System.nanoTime() + PROMOTION_TIMEOUT.toNanos()
        while (true) {
            val peer = runCatching { runBlocking { consensus.probe(address) } }.getOrNull()
            if (peer?.member?.id == candidate.id && systemDatabase.id in peer.groups) return
            if (System.nanoTime() - deadline >= 0) {
                throw MembershipChangeException(
                    "Server $candidate did not turn primary within " +
                        "${PROMOTION_TIMEOUT.toSeconds()} seconds: it does once it has pulled the " +
                        "request and the leader of the system database is known. Run " +
                        "dozendb.cluster.addMember again to finish."
                )
            }
            Thread.sleep(CONFIRMATION_POLL_MILLIS)
        }
    }

    private fun requireMember() {
        if (state != MemberState.MEMBER) {
            throw MembershipChangeException(
                "This member is not one of the cluster's members; change members on one that is."
            )
        }
    }

    private fun checkCanJoin(address: HostPort, peer: PeerInfo) {
        val own = metadata()
        val candidate = "Member ${peer.member.id} at $address"
        val version = peer.metadata[MemberMetadata.NEO4J_VERSION]
        if (version != own[MemberMetadata.NEO4J_VERSION]) {
            throw MembershipChangeException(
                "$candidate runs Neo4j $version, but this cluster runs Neo4j " +
                    "${own[MemberMetadata.NEO4J_VERSION]}. All members must run the same version."
            )
        }
        val level = formatLevelStore?.formatLevel() ?: formatLevel
        val levels = reportedLevels(peer) { MembershipChangeException("$candidate reports $it.") }
        if (level !in levels) {
            throw MembershipChangeException(
                "$candidate runs DozenDB ${peer.metadata[MemberMetadata.DOZENDB_VERSION]}, which " +
                    "supports format levels $levels, but this cluster uses format level $level. " +
                    "Run a DozenDB version that supports format level $level on it."
            )
        }
        when (peer.metadata[MemberMetadata.MEMBER_STATE]) {
            MemberState.JOINING.name -> {}
            MemberState.REMOVED.name ->
                throw MembershipChangeException(
                    "$candidate was removed from the cluster and cannot be added again. Delete " +
                        "its data directory and start it again: it then joins as a new member."
                )
            else -> throw MembershipChangeException("$candidate is a member of another cluster.")
        }
        val clusterId = peer.metadata[MemberMetadata.CLUSTER_ID]
        val started =
            if (secondary(peer)) peer.metadata[MemberMetadata.STORES_READY] == "true"
            else systemDatabase.id in peer.groups
        if (clusterId == null || !started) {
            throw MembershipChangeException(
                "$candidate is still starting; add it once it has started."
            )
        }
        if (clusterId != own[MemberMetadata.CLUSTER_ID]) {
            throw MembershipChangeException(
                "$candidate waits to join another cluster: its databases come from there. Check " +
                    "its ${MemberMetadata.INITIAL_MEMBERS_SETTING}, delete its data directory " +
                    "and start it again."
            )
        }
    }

    /** Whether [peer] asks to host `system` as a secondary. */
    private fun secondary(peer: PeerInfo): Boolean =
        peer.metadata[MemberMetadata.SYSTEM_DATABASE_MODE] == CopyMode.SECONDARY.name

    private companion object {
        /** How long a change of members may take: bringing a new member up to date included. */
        val CHANGE_TIMEOUT: Duration = Duration.ofMinutes(10)

        const val CONFIRMATION_POLL_MILLIS = 200L

        const val MODE_CHANGE_RETRY_MILLIS = 2_000L

        /** How long a system secondary may take to turn primary once asked. */
        val PROMOTION_TIMEOUT: Duration = Duration.ofMinutes(2)

        const val SYSTEM_DATABASE_MODE_SETTING = "server.cluster.system_database_mode"

        const val REMOVED_MESSAGE =
            "This member was removed from the cluster: it takes no part in it any more and its " +
                "databases are stopped. Shut it down. To add it again, delete its data directory " +
                "first, then start it: it joins as a new member."
    }

    private class RoleChangeLogger(private val log: Log) : (GroupStatus) -> Unit {
        private var last: Pair<Role, Any?>? = null

        @Synchronized
        override fun invoke(status: GroupStatus) {
            val current = status.role to status.leader
            if (current == last) return
            last = current
            log.info(
                "Raft group %s: this member is %s, leader %s, term %d",
                status.database.name,
                status.role.displayName,
                status.leader ?: "unknown",
                status.term,
            )
        }
    }
}

/** The name of a role in logs and procedure output. */
val Role.displayName: String
    get() =
        when (this) {
            Role.Leader -> "LEADER"
            Role.Follower -> "FOLLOWER"
            Role.Candidate -> "CANDIDATE"
            Role.Inactive -> "INACTIVE"
        }
