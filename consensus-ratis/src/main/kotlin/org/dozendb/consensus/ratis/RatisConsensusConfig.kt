package org.dozendb.consensus.ratis

import java.nio.file.Path
import java.time.Duration
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId

/**
 * Configuration of a [RatisConsensusLog].
 *
 * @property listenHost the interface to bind, empty for all interfaces.
 * @property listenPort the port to bind; 0 picks a free port, which tests use.
 * @property advertisedAddress the address other members dial; null means the listen host and the
 *   bound port, which is only sensible in tests.
 * @property storageDirectory the root of the Raft logs, one subdirectory per group.
 * @property electionTimeoutMin a follower that hears nothing from a leader for a random time
 *   between [electionTimeoutMin] and [electionTimeoutMax] starts an election.
 * @property requestTimeout the deadline for probes and membership changes.
 * @property metadata facts reported to probing members, such as the Neo4j version. Read at every
 *   probe, so it may change.
 * @property maxEntrySize the largest payload a group accepts. Ratis sizes per-group buffers from
 *   it, and members must agree on it.
 * @property snapshotInterval how often the state machines' durable indexes are checked for a new
 *   snapshot.
 * @property logSegmentSize the size at which a log segment is closed, raised to twice the largest
 *   entry if smaller. Logs are discarded a whole closed segment at a time.
 * @property tls TLS for all traffic between members, whose certificates may change while the member
 *   runs; null sends it in plaintext. Members must agree on it.
 */
data class RatisConsensusConfig(
    val memberId: MemberId,
    val listenHost: String,
    val listenPort: Int,
    val advertisedAddress: HostPort?,
    val storageDirectory: Path,
    val electionTimeoutMin: Duration = Duration.ofSeconds(1),
    val electionTimeoutMax: Duration = Duration.ofSeconds(2),
    val requestTimeout: Duration = Duration.ofSeconds(5),
    val metadata: () -> Map<String, String> = { emptyMap() },
    val maxEntrySize: Int = DEFAULT_MAX_ENTRY_SIZE,
    val snapshotInterval: Duration = Duration.ofSeconds(10),
    val logSegmentSize: Long = DEFAULT_LOG_SEGMENT_SIZE,
    val tls: ReloadableTls? = null,
) {
    init {
        require(listenPort in 0..65535) { "listen port must be in 0..65535, was $listenPort" }
        require(!electionTimeoutMin.isNegative && !electionTimeoutMin.isZero) {
            "election timeout min must be positive, was $electionTimeoutMin"
        }
        require(electionTimeoutMax > electionTimeoutMin) {
            "election timeout max ($electionTimeoutMax) must exceed min ($electionTimeoutMin)"
        }
        require(!requestTimeout.isNegative && !requestTimeout.isZero) {
            "request timeout must be positive, was $requestTimeout"
        }
        require(!snapshotInterval.isNegative && !snapshotInterval.isZero) {
            "snapshot interval must be positive, was $snapshotInterval"
        }
        require(logSegmentSize > 0) { "log segment size must be positive, was $logSegmentSize" }
        require(maxEntrySize in MIN_MAX_ENTRY_SIZE..MAX_MAX_ENTRY_SIZE) {
            "max entry size must be in $MIN_MAX_ENTRY_SIZE..$MAX_MAX_ENTRY_SIZE, was $maxEntrySize"
        }
    }

    companion object {
        const val DEFAULT_MAX_ENTRY_SIZE: Int = 16 shl 20
        const val DEFAULT_LOG_SEGMENT_SIZE: Long = 32L shl 20
        const val MIN_MAX_ENTRY_SIZE: Int = 64 shl 10
        const val MAX_MAX_ENTRY_SIZE: Int = 256 shl 20
    }
}
