package org.dozendb.cluster.internal

import org.dozendb.consensus.ratis.RatisConsensusConfig

/**
 * Adjusts the consensus configuration of a member, for tests that need small log segments or fast
 * snapshots. Supplied as an external dependency of the DBMS; production members have none.
 */
internal fun interface ConsensusTuning {
    fun adjust(config: RatisConsensusConfig): RatisConsensusConfig
}
