package org.dozendb.core.servers

import java.util.UUID
import kotlin.random.Random

/**
 * A server that runs a database and answers, as a secondary of it chooses where to pull from:
 * hosting it in [mode], leading it if [leader], with its server [tags].
 */
data class UpstreamCandidate(
    val id: UUID,
    val mode: CopyMode,
    val leader: Boolean,
    val tags: Set<String>,
)

/**
 * How a secondary chooses its upstream, as Neo4j Enterprise documents
 * `server.cluster.catchup.upstream_strategy`: [strategies] are asked in order, and the first that
 * finds a server wins; without any, `typically-connect-to-random-secondary`; a random primary is
 * the last resort. The choice is made again for every pull, at random among the servers a strategy
 * finds ([random]).
 *
 * The tag strategies take [toTags], `server.cluster.catchup.connect_randomly_to_server_tags`, or
 * this server's own tags; the deprecated group strategies take tags too, [toGroups] for the one
 * that names them. `user-defined` follows [rules].
 */
class UpstreamSelection(
    private val strategies: List<String>,
    private val toTags: () -> Collection<String>,
    private val toGroups: () -> Collection<String>,
    private val rules: ServerRules,
    private val random: Random = Random.Default,
) {
    /**
     * The upstream that a secondary with [ownTags] pulls from among [candidates], the other servers
     * that run its database and answer; null if none can be.
     */
    fun choose(candidates: List<UpstreamCandidate>, ownTags: Set<String>): UpstreamCandidate? {
        for (strategy in strategies.ifEmpty { listOf(TYPICALLY_SECONDARY) }) {
            choose(strategy, candidates, ownTags)?.let {
                return it
            }
        }
        return candidates.filter { it.mode == CopyMode.PRIMARY }.randomOrNull(random)
    }

    private fun choose(
        strategy: String,
        candidates: List<UpstreamCandidate>,
        ownTags: Set<String>,
    ): UpstreamCandidate? {
        val primaries = candidates.filter { it.mode == CopyMode.PRIMARY }
        val secondaries = candidates.filter { it.mode == CopyMode.SECONDARY }
        fun tagged(tags: Collection<String>) =
            secondaries
                .filter { candidate -> candidate.tags.any { it in tags } }
                .randomOrNull(random)
        return when (strategy) {
            RANDOM_PRIMARY -> primaries.randomOrNull(random)
            TYPICALLY_SECONDARY ->
                if (random.nextInt(10) == 0) primaries.randomOrNull(random)
                else secondaries.randomOrNull(random)
            TO_TAGS,
            TO_TAGS_ALIAS -> tagged(toTags())
            WITHIN_TAGS,
            WITHIN_GROUP -> tagged(ownTags)
            TO_GROUP -> tagged(toGroups())
            LEADER_ONLY -> candidates.firstOrNull { it.leader }
            USER_DEFINED,
            USER_DEFINED_ALIAS -> rules.select(candidates) { it.tags }.randomOrNull(random)
            else -> throw IllegalArgumentException("unknown upstream strategy $strategy")
        }
    }

    companion object {
        const val RANDOM_PRIMARY = "connect-to-random-primary-server"
        const val TYPICALLY_SECONDARY = "typically-connect-to-random-secondary"
        const val TO_TAGS = "connect-randomly-to-server-tags"

        /** The name the description of the tags setting gives the strategy. */
        const val TO_TAGS_ALIAS = "connect-randomly-to-server-with-tag"
        const val WITHIN_TAGS = "connect-randomly-within-server-tags"
        const val LEADER_ONLY = "leader-only"
        const val USER_DEFINED = "user-defined"

        /** The name the description of the rules setting gives the strategy. */
        const val USER_DEFINED_ALIAS = "user_defined"

        /** Deprecated in Neo4j 5.5, as server groups became server tags. */
        const val TO_GROUP = "connect-randomly-to-server-group"
        const val WITHIN_GROUP = "connect-randomly-within-server-group"

        /** Every strategy name taken, in the order Enterprise lists them. */
        val NAMES: List<String> =
            listOf(
                RANDOM_PRIMARY,
                TYPICALLY_SECONDARY,
                TO_TAGS,
                TO_TAGS_ALIAS,
                WITHIN_TAGS,
                LEADER_ONLY,
                USER_DEFINED,
                USER_DEFINED_ALIAS,
                TO_GROUP,
                WITHIN_GROUP,
            )

        /** The deprecated strategy names. */
        val DEPRECATED: Set<String> = setOf(TO_GROUP, WITHIN_GROUP)

        /** The names among [strategies] that are no strategy. */
        fun unknown(strategies: List<String>): List<String> = strategies.filter { it !in NAMES }
    }
}
