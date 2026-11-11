package org.dozendb.core.servers

/**
 * Neo4j Enterprise's rules that choose servers by their tags, in which both a routing policy
 * (`dbms.routing.load_balancing.config.server_policies.<name>`) and the user-defined upstream
 * strategy of secondaries (`server.cluster.catchup.user_defined_upstream_strategy`) are written:
 * rules separated by `;`, asked in order, the first that passes any server wins. A rule is a chain
 * of filters joined by `->`, which starts from every server: `tags(a, b)` passes the servers with
 * any of the tags (`groups(...)` is its deprecated name), so that two in a chain pass those with a
 * tag of each, `min(n)` passes the servers it is given if there are at least n, else none, `all()`
 * passes every server, and `halt()`, which may only end the last rule, passes none and ends the
 * rules. Unless they end so, `all()` follows the last rule.
 *
 * A comma between rules, which one of Enterprise's upstream examples has, is refused: its meaning
 * is not documented.
 */
class ServerRules private constructor(private val rules: List<List<Filter>>) {
    private val halted = rules.lastOrNull()?.lastOrNull() == Filter.Halt

    private sealed interface Filter {
        fun <T> pass(servers: List<T>, tagsOf: (T) -> Set<String>): List<T>

        data class Tags(val tags: Set<String>) : Filter {
            override fun <T> pass(servers: List<T>, tagsOf: (T) -> Set<String>) =
                servers.filter { server ->
                    tagsOf(server).any { it in tags }
                }

            override fun toString() = "tags(${tags.sorted().joinToString(", ")})"
        }

        data class Min(val count: Int) : Filter {
            override fun <T> pass(servers: List<T>, tagsOf: (T) -> Set<String>) =
                if (servers.size >= count) servers else emptyList()

            override fun toString() = "min($count)"
        }

        data object All : Filter {
            override fun <T> pass(servers: List<T>, tagsOf: (T) -> Set<String>) = servers

            override fun toString() = "all()"
        }

        data object Halt : Filter {
            override fun <T> pass(servers: List<T>, tagsOf: (T) -> Set<String>) = emptyList<T>()

            override fun toString() = "halt()"
        }
    }

    /**
     * The servers of [servers], with the [tags] of each, that the first rule with a result passes,
     * in their order.
     */
    fun <T> select(servers: List<T>, tags: (T) -> Set<String>): List<T> {
        for (rule in rules) {
            val passed = rule.fold(servers) { passing, filter -> filter.pass(passing, tags) }
            if (passed.isNotEmpty()) return passed
        }
        return if (halted) emptyList() else servers
    }

    /**
     * The rules in one spelling: filters joined by ` -> `, rules by `; `, `groups` as `tags` with
     * the tags sorted. Rules that read alike choose alike.
     */
    override fun toString() = rules.joinToString("; ") { it.joinToString(" -> ") }

    override fun equals(other: Any?) = other is ServerRules && other.rules == rules

    override fun hashCode() = rules.hashCode()

    companion object {
        /** No rules: every server, as `all()`. */
        val ALL = ServerRules(emptyList())

        /** The rules [text] writes. */
        fun parse(text: String): ServerRules {
            require(text.count { it == '(' } == text.count { it == ')' }) {
                "unbalanced parentheses in '$text'"
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
                require(depth in 0..1) { "unbalanced parentheses in '$text'" }
            }
            val rules =
                text
                    .split(';')
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
                    .map { rule -> rule.split("->").map { filter(it.trim(), text) } }
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
            // all() passes what it is given, and a rule of all() alone every server, as the
            // all() that follows the last rule does: rules that differ in them choose alike.
            val chains = rules.map { rule -> rule.filter { it != Filter.All } }
            return ServerRules(chains.takeWhile { it.isNotEmpty() })
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
