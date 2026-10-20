package org.dozendb.consensus.fake

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.dozendb.consensus.LogEntry
import org.dozendb.consensus.Member
import org.dozendb.consensus.ReplicatedStateMachine

/**
 * A state machine that records applied entries in memory. Thread-safe, because real consensus
 * implementations apply on their own threads while tests inspect from the test thread.
 */
class RecordingStateMachine : ReplicatedStateMachine {
    private val applied = mutableListOf<LogEntry>()

    /** Answers `applied <index>`, so that tests can check that responses reach the submitter. */
    @Synchronized
    override fun apply(entry: LogEntry): ByteArray {
        check(entry.index > lastAppliedIndex()) {
            "entry ${entry.index} applied twice or out of order, last applied ${lastAppliedIndex()}"
        }
        applied += entry
        return "applied ${entry.index}".encodeToByteArray()
    }

    @Synchronized override fun lastAppliedIndex(): Long = applied.lastOrNull()?.index ?: -1

    /** Everything applied is durable, unless a test sets this lower. */
    @Volatile var durable: Long? = null

    /** How often [durableIndex] was called, so tests can wait until a value was observed. */
    val durableReads = AtomicInteger()

    override fun durableIndex(): Long {
        durableReads.incrementAndGet()
        return durable ?: lastAppliedIndex()
    }

    /** Sets [durable] and waits until it was read at least once. */
    fun makeDurable(index: Long) {
        durable = index
        val reads = durableReads.get()
        // The read in progress may have returned the previous value; wait for one more.
        while (durableReads.get() < reads + 2) Thread.sleep(10)
    }

    /** The snapshot requests received, as (leader, minimum index). */
    val snapshotRequests: MutableList<Pair<Member?, Long>> = CopyOnWriteArrayList()

    override fun onSnapshotNeeded(leader: Member?, minimumIndex: Long) {
        snapshotRequests += leader to minimumIndex
    }

    /** Replaces the state with a copy of [other]'s, as a store copy would. */
    @Synchronized
    fun replaceWith(other: RecordingStateMachine) {
        val copy = other.entries()
        applied.clear()
        applied += copy
    }

    /** The payloads applied so far, decoded as UTF-8. */
    @Synchronized fun payloads(): List<String> = applied.map { it.payload.decodeToString() }

    @Synchronized fun entries(): List<LogEntry> = applied.toList()
}
