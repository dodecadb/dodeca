package org.dozendb.core.servers

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

class ServerRulesTest {
    private data class Server(val name: String, val tags: Set<String>)

    // Two data centres in the north region, and the south region; app1 runs in some.
    private val north1a = Server("north1a", setOf("north1", "north", "app1"))
    private val north1b = Server("north1b", setOf("north1", "north"))
    private val north2 = Server("north2", setOf("north2", "north", "app1"))
    private val south = Server("south", setOf("south"))
    private val southApp = Server("southApp", setOf("south", "app1"))
    private val all = listOf(north1a, north1b, north2, south, southApp)

    private fun ServerRules.select(servers: List<Server>) = select(servers) { it.tags }

    private fun select(rules: String, servers: List<Server> = all) =
        ServerRules.parse(rules).select(servers)

    @Test
    fun `a specific data centre only, as Enterprise's north1_only example`() {
        val rules = "tags(north1)->min(2); halt();"

        assertThat(select(rules)).containsExactly(north1a, north1b)
        assertThat(select(rules, all - north1b)).describedAs("no other data centre").isEmpty()
    }

    @Test
    fun `a specific data centre preferably, as Enterprise's north1 example`() {
        val rules = "tags(north1)->min(2);"

        assertThat(select(rules)).containsExactly(north1a, north1b)
        assertThat(select(rules, all - north1b))
            .describedAs("all() follows the last rule")
            .containsExactly(north1a, north2, south, southApp)
    }

    @Test
    fun `falling back to neighbours, as Enterprise's north_app1 example`() {
        // As neo4j.conf reads the example's continued lines.
        val rules = "tags(north1,north2)->min(2);tags(north);all();"

        assertThat(select(rules)).containsExactly(north1a, north1b, north2)
        assertThat(select(rules, listOf(north1a, south, southApp))).containsExactly(north1a)
        assertThat(select(rules, listOf(south, southApp))).containsExactly(south, southApp)
    }

    @Test
    fun `tags pass servers with any of them, and chained ones those with one of each`() {
        assertThat(select("tags(north2, south)")).containsExactly(north2, south, southApp)
        assertThat(select("tags(north) -> tags(app1)")).containsExactly(north1a, north2)
        assertThat(select("groups(south)"))
            .describedAs("deprecated")
            .containsExactly(south, southApp)
        assertThat(select("tags(west); halt()")).isEmpty()
        assertThat(select("tags(west)")).isEqualTo(all)
    }

    @Test
    fun `min passes the servers it is given if there are enough, else the next rule is asked`() {
        assertThat(select("tags(app1) -> min(3); tags(north1)"))
            .containsExactly(north1a, north2, southApp)
        assertThat(select("tags(app1) -> min(4); tags(north1)")).containsExactly(north1a, north1b)
        assertThat(select("min(5); halt()")).isEqualTo(all)
        assertThat(select("min(6); halt()")).isEmpty()
    }

    @Test
    fun `no rules, or all, pass every server`() {
        assertThat(select("")).isEqualTo(all)
        assertThat(select("all()")).isEqualTo(all)
        assertThat(ServerRules.ALL.select(all)).isEqualTo(all)
        assertThat(ServerRules.ALL.select(emptyList())).isEmpty()
    }

    @Test
    fun `rules have one spelling, so that rules that read alike are equal`() {
        val rules = ServerRules.parse("groups(b,a)->min(2);halt();")

        assertThat(rules.toString()).isEqualTo("tags(a, b) -> min(2); halt()")
        assertThat(rules).isEqualTo(ServerRules.parse(" tags(a, b) ->  min(2) ; halt() "))
        assertThat(rules).isEqualTo(ServerRules.parse(rules.toString()))
        assertThat(rules).isNotEqualTo(ServerRules.parse("tags(a, b) -> min(2)"))
        assertThat(ServerRules.parse("").toString()).isEmpty()
        assertThat(ServerRules.parse("all()")).isEqualTo(ServerRules.ALL)
        assertThat(ServerRules.parse("all() -> tags(a); all(); tags(b); halt()"))
            .describedAs("a rule of all() alone passes every server, as the last one does")
            .isEqualTo(ServerRules.parse("tags(a)"))
        assertThat(ServerRules.parse("tags(a) -> all() -> halt()").toString())
            .isEqualTo("tags(a) -> halt()")
    }

    @Test
    fun `rules that are unclear are refused`() {
        for ((text, error) in
            listOf(
                "tags(north)->min(3), tags(east)" to "a comma separates rules",
                "tags()" to "tags() needs a tag",
                "min(0)" to "min() needs a number of servers above 0",
                "min(x)" to "min() needs a number of servers above 0",
                "halt(); tags(east)" to "halt() ends the last rule only",
                "halt()->tags(east)" to "halt() ends the last rule only",
                "near(east)" to "unknown filter near",
                "tags(east" to "unbalanced parentheses",
                "tags(east))(" to "unbalanced parentheses",
                "all(east)" to "all() takes no arguments",
                "north" to "'north' is no filter",
            )) {
            assertThat(catchThrowable { ServerRules.parse(text) })
                .describedAs(text)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(error)
        }
    }
}
