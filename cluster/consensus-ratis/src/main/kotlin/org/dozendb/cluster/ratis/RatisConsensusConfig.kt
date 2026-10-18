package org.dozendb.cluster.ratis

import java.nio.file.Path
import java.time.Duration
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.MemberId

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
 * @property metadata facts reported to probing members, such as the Neo4j version.
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
    val metadata: Map<String, String> = emptyMap(),
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
    }
}
