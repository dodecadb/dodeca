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
    private val rules: UpstreamRules,
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
            USER_DEFINED_ALIAS -> rules.select(candidates).randomOrNull(random)
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

/**
 * Neo4j Enterprise's rules for a user-defined upstream strategy,
 * `server.cluster.catchup.user_defined_upstream_strategy`: rules separated by `;`, asked in order,
 * the first that passes any server wins. A rule is a chain of filters joined by `->`, which starts
 * from every server: `tags(a, b)` passes the servers with any of the tags (`groups(...)` is its
 * deprecated name), `min(n)` passes the servers it is given if there are at least n, else none,
 * `all()` passes every server, and `halt()`, which may only end the last rule, passes none and ends
 * the rules. Unless they end so, `all()` follows the last rule.
 *
 * A comma between rules, which one of Enterprise's examples has, is refused: its meaning is not
 * documented.
 */
class UpstreamRules
private constructor(
    private val rules: List<List<Filter>>,
    private val halted: Boolean,
) {
    private sealed interface Filter {
        fun pass(servers: List<UpstreamCandidate>): List<UpstreamCandidate>

        data class Tags(val tags: Set<String>) : Filter {
            override fun pass(servers: List<UpstreamCandidate>) = servers.filter { server ->
                server.tags.any { it in tags }
            }
        }

        data class Min(val count: Int) : Filter {
            override fun pass(servers: List<UpstreamCandidate>) =
                if (servers.size >= count) servers else emptyList()
        }

        data object All : Filter {
            override fun pass(servers: List<UpstreamCandidate>) = servers
        }

        data object Halt : Filter {
            override fun pass(servers: List<UpstreamCandidate>) = emptyList<UpstreamCandidate>()
        }
    }

    /** The servers of [candidates] that the first rule with a result passes, in their order. */
    fun select(candidates: List<UpstreamCandidate>): List<UpstreamCandidate> {
        for (rule in rules) {
            val passed = rule.fold(candidates) { servers, filter -> filter.pass(servers) }
            if (passed.isNotEmpty()) return passed
        }
        return if (halted) emptyList() else candidates
    }

    companion object {
        /** The rules [text] writes. */
        fun parse(text: String): UpstreamRules {
            require(text.count { it == '(' } == text.count { it == ')' }) {
                "unbalanced parentheses in upstream rules '$text'"
            }
            var depth = 0
            for (character in text) {
                when (character) {
                    '(' -> depth++
                    ')' -> depth--
                    ',' ->
                        require(depth > 0) {
                            "a comma separates rules in '$text'; separate them with ';'"
                        }
                }
                require(depth in 0..1) { "unbalanced parentheses in upstream rules '$text'" }
            }
            val rules =
                text
                    .split(';')
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
                    .map { rule ->
                        rule.split("->").map { filter(it.trim(), text) }
                    }
            for ((index, rule) in rules.withIndex()) {
                for ((position, filter) in rule.withIndex()) {
                    require(
                        filter != Filter.Halt ||
                            (index == rules.lastIndex && position == rule.lastIndex)
                    ) {
                        "halt() ends the last rule only, in '$text'"
                    }
                }
            }
            return UpstreamRules(rules, rules.lastOrNull()?.lastOrNull() == Filter.Halt)
        }

        private fun filter(text: String, rules: String): Filter {
            val match =
                Regex("""([A-Za-z]+)\s*\((.*)\)""").matchEntire(text)
                    ?: throw IllegalArgumentException("'$text' is no filter, in '$rules'")
            val name = match.groupValues[1]
            val arguments =
                match.groupValues[2].split(',').map { it.trim() }.filter { it.isNotEmpty() }
            fun none(filter: Filter): Filter {
                require(arguments.isEmpty()) { "$name() takes no arguments, in '$rules'" }
                return filter
            }
            return when (name) {
                "tags",
                "groups" -> {
                    require(arguments.isNotEmpty()) { "$name() needs a tag, in '$rules'" }
                    Filter.Tags(arguments.toSet())
                }
                "min" -> {
                    val count = arguments.singleOrNull()?.toIntOrNull()
                    require(count != null && count > 0) {
                        "min() needs a number of servers above 0, in '$rules'"
                    }
                    Filter.Min(count)
                }
                "all" -> none(Filter.All)
                "halt" -> none(Filter.Halt)
                else -> throw IllegalArgumentException("unknown filter $name, in '$rules'")
            }
        }
    }
}
