package dev.dodeca.cluster.internal.replication

import dev.dodeca.cluster.internal.recreation.Seed
import dev.dodeca.consensus.HostPort
import dev.dodeca.core.servers.CopyMode
import java.io.IOException
import java.util.UUID
import org.neo4j.kernel.database.NamedDatabaseId

/** Databases as the database manager and the replication keep them: starting starts the group. */
internal class FakeLocalDatabases : LocalDatabases {
    val registered = mutableMapOf<NamedDatabaseId, LocalDatabase>()
    val left = mutableListOf<NamedDatabaseId>()
    /** Database files by name; true if complete. */
    val stored = mutableMapOf<String, Boolean>()
    val groups = mutableSetOf<UUID>()
    val catchingUp = mutableSetOf<NamedDatabaseId>()
    val unreachable = mutableSetOf<HostPort>()
    val copyAttempts = mutableListOf<HostPort>()
    val copies = mutableListOf<Pair<HostPort, NamedDatabaseId>>()
    val dropped = mutableListOf<NamedDatabaseId>()
    val deletedStores = mutableListOf<String>()

    /** The databases whose stores are secondaries', by id. */
    val marks = mutableSetOf<UUID>()

    /** The databases whose copies here are in quarantine, by id. */
    val quarantines = mutableSetOf<UUID>()

    /** Puts the copy of [id], registered, in quarantine, stopping it. */
    fun quarantine(id: NamedDatabaseId) {
        quarantines += id.databaseId().uuid()
        registered[id] = registered.getValue(id).copy(started = false, quarantined = true)
    }

    override fun quarantined(): Set<UUID> = quarantines.toSet()

    override fun forgetQuarantine(id: UUID) {
        quarantines -= id
    }

    fun add(id: NamedDatabaseId, started: Boolean) {
        registered[id] = LocalDatabase(started, false)
        stored[id.name()] = true
        groups += id.databaseId().uuid()
    }

    /** A running secondary of [id]. */
    fun addSecondary(id: NamedDatabaseId) {
        registered[id] = LocalDatabase(true, false, CopyMode.SECONDARY)
        stored[id.name()] = true
        marks += id.databaseId().uuid()
    }

    override fun secondaries(): Set<UUID> = marks.toSet()

    override fun unmark(id: UUID) {
        marks -= id
    }

    override fun registered(): Map<NamedDatabaseId, LocalDatabase> = registered.toMap()

    override fun stored(): Set<String> = stored.keys.toSet()

    override fun complete(name: String): Boolean = stored[name] == true

    override fun groups(): Set<UUID> = groups.toSet()

    override fun catchingUp(id: NamedDatabaseId): Boolean = id in catchingUp

    /** Databases whose group this member was removed from. */
    val notVoting = mutableSetOf<NamedDatabaseId>()

    override fun votes(id: NamedDatabaseId): Boolean? =
        if (id.databaseId().uuid() in groups) id !in notVoting else null

    /** Databases whose group stopped here, as this member is not one of its members. */
    val notMember = mutableSetOf<NamedDatabaseId>()

    override fun left(id: NamedDatabaseId): Boolean = id in notMember

    override fun rejoinFromStore(id: NamedDatabaseId): ModeSwitch =
        throw UnsupportedOperationException("not reconciled")

    override fun create(id: NamedDatabaseId, mode: CopyMode) {
        check(id !in registered) { "$id is registered" }
        registered[id] = LocalDatabase(started = false, failed = false, mode = mode)
    }

    /** The seeds each database was started with. */
    val seeds = mutableMapOf<NamedDatabaseId, StoreSeed?>()

    /** Whether starts fail, as the start of a broken store does. */
    var failStarts = false

    // As DatabaseManager, a start of a failed copy tries anew.
    override fun start(id: NamedDatabaseId, seed: StoreSeed?) {
        val state = registered.getValue(id)
        check(!state.started)
        seeds[id] = seed
        if (failStarts) {
            registered[id] = state.copy(failed = true)
            return
        }
        registered[id] = state.copy(started = true, failed = false)
        stored[id.name()] = true
        if (state.mode == CopyMode.PRIMARY) groups += id.databaseId().uuid()
    }

    override fun stop(id: NamedDatabaseId) {
        registered[id] = registered.getValue(id).copy(started = false, failed = false)
    }

    override fun leave(id: NamedDatabaseId) {
        registered[id] = registered.getValue(id).copy(started = false, failed = false)
        left += id
    }

    override fun drop(id: NamedDatabaseId, keepQuarantine: Boolean) {
        registered.remove(id)
        stored.remove(id.name())
        groups.remove(id.databaseId().uuid())
        marks.remove(id.databaseId().uuid())
        if (!keepQuarantine) quarantines.remove(id.databaseId().uuid())
        dropped += id
    }

    /** The kept seeds, by name, with the ids they seed. */
    val keptSeeds = mutableMapOf<String, UUID>()

    override fun seeds(): Map<String, UUID> = keptSeeds.toMap()

    override fun keepAsSeed(id: NamedDatabaseId, recreated: UUID) {
        registered.remove(id)
        groups.remove(id.databaseId().uuid())
        marks.remove(id.databaseId().uuid())
        keptSeeds[id.name()] = recreated
    }

    override fun markSeed(name: String, recreated: UUID) {
        check(stored[name] == true) { "$name has no complete store" }
        keptSeeds[name] = recreated
    }

    override fun unmarkSeed(name: String) {
        keptSeeds.remove(name)
    }

    override fun seedSecondary(id: NamedDatabaseId) {
        check(keptSeeds[id.name()] == id.databaseId().uuid()) { "$id has no seed here" }
        marks += id.databaseId().uuid()
        keptSeeds.remove(id.name())
    }

    /** What the seeds kept hold, by name; [KEPT] if not set. */
    val seedStates = mutableMapOf<String, Seed?>()

    override fun seedState(name: String): Seed? {
        check(name in keptSeeds) { "$name is no seed" }
        return if (name in seedStates) seedStates[name] else KEPT
    }

    override fun deleteStore(name: String) {
        check(registered.keys.none { it.name() == name }) { "$name is registered" }
        stored.remove(name)
        keptSeeds.remove(name)
        deletedStores += name
    }

    override fun removeGroup(id: UUID) {
        groups.remove(id)
    }

    /** What a switch to a primary answers; a switch to a secondary always happens. */
    var toPrimary = ModeSwitch.SWITCHED
    val switched = mutableListOf<Pair<NamedDatabaseId, CopyMode>>()

    /** The append index of the last transaction of each secondary, if set. */
    val pulled = mutableMapOf<NamedDatabaseId, Long>()

    override fun switch(id: NamedDatabaseId, mode: CopyMode, pulledBeyond: Long?): ModeSwitch {
        val state = registered.getValue(id)
        check(state.mode != mode) { "$id is a ${state.mode} already" }
        if (mode == CopyMode.PRIMARY && toPrimary != ModeSwitch.SWITCHED) return toPrimary
        if (mode == CopyMode.PRIMARY && pulledBeyond != null) {
            if (pulled.getValue(id) <= pulledBeyond) return ModeSwitch.LATER
        }
        switched += id to mode
        registered[id] = LocalDatabase(started = false, failed = false, mode = mode)
        val uuid = id.databaseId().uuid()
        when (mode) {
            CopyMode.SECONDARY -> {
                groups -= uuid
                marks += uuid
            }
            CopyMode.PRIMARY -> marks -= uuid
        }
        return ModeSwitch.SWITCHED
    }

    override fun copy(source: HostPort, id: NamedDatabaseId, mode: CopyMode) {
        copyAttempts += source
        if (source in unreachable) throw IOException("$source is unreachable")
        check(id.name() !in stored || id.name() in keptSeeds) { "${id.name()} has files" }
        stored[id.name()] = true
        copies += source to id
        if (mode == CopyMode.SECONDARY) marks += id.databaseId().uuid()
    }

    companion object {
        /** What a kept seed holds unless a test says otherwise. */
        val KEPT = Seed(appendIndex = 10, lastTransaction = 7, checksum = 111, storeId = "1/2")
    }
}
