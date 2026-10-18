package org.dozendb.cluster.core.internal

import org.dozendb.cluster.ratis.RatisConsensusConfig

/**
 * Adjusts the consensus configuration of a member, for tests that need small log segments or fast
 * snapshots. Supplied as an external dependency of the DBMS; production members have none.
 */
internal fun interface ConsensusTuning {
    fun adjust(config: RatisConsensusConfig): RatisConsensusConfig
}
