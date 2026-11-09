package org.dozendb.core.servers

import java.util.UUID
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.servers.CopyMode.PRIMARY
import org.dozendb.core.servers.CopyMode.SECONDARY
import org.junit.jupiter.api.Test

class UpstreamSelectionTest {
    private val leader = candidate(1, PRIMARY, leader = true, tags = setOf("east"))
    private val follower = candidate(2, PRIMARY, tags = setOf("west"))
    private val north = candidate(3, SECONDARY, tags = setOf("north"))
    private val north2 = candidate(4, SECONDARY, tags = setOf("north2", "north"))
    private val south = candidate(5, SECONDARY, tags = setOf("south"))
    private val all = listOf(leader, follower, north, north2, south)

    private fun candidate(
        index: Long,
        mode: CopyMode,
        leader: Boolean = false,
        tags: Set<String> = emptySet(),
    ) = UpstreamCandidate(UUID(0, index), mode, leader, tags)

    /** Picks the first of what it is given, and never connects to a primary by chance. */
    private val firstPick =
        object : Random() {
            override fun nextBits(bitCount: Int): Int = 0

            override fun nextInt(until: Int): Int = if (until == 10) 9 else 0
        }

    private fun selection(
        vararg strategies: String,
        toTags: List<String> = emptyList(),
        rules: String = "",
        random: Random = firstPick,
    ) =
        UpstreamSelection(
            strategies.toList(),
            toTags = { toTags },
            toGroups = { emptyList() },
            rules = UpstreamRules.parse(rules),
            random = random,
        )

    @Test
    fun `the strategies are asked in order, a random primary is the last resort`() {
        assertThat(selection("leader-only").choose(all, emptySet())).isEqualTo(leader)
        assertThat(
                selection("connect-randomly-to-server-tags", "leader-only", toTags = listOf("x"))
                    .choose(all, emptySet())
            )
            .describedAs("no secondary has tag x")
            .isEqualTo(leader)
        assertThat(selection("leader-only").choose(listOf(follower, north), emptySet()))
            .describedAs("no leader known")
            .isEqualTo(follower)
        assertThat(selection("leader-only").choose(listOf(north), emptySet())).isNull()
    }

    @Test
    fun `typically connects to a secondary, and to a primary one time in ten`() {
        assertThat(selection().choose(all, emptySet()))
            .describedAs("the default strategy")
            .isEqualTo(north)
        val primaryTurn =
            object : Random() {
                override fun nextBits(bitCount: Int): Int = 0
            }
        assertThat(
                selection("typically-connect-to-random-secondary", random = primaryTurn)
                    .choose(all, emptySet())
            )
            .isEqualTo(leader)
        val chosen =
            List(1000) {
                    selection("typically-connect-to-random-secondary", random = Random(it))
                        .choose(all, emptySet())!!
                        .mode
                }
                .count { it == PRIMARY }
        assertThat(chosen).isBetween(50, 150)
        assertThat(
                selection("connect-to-random-primary-server", random = Random(1))
                    .choose(all, emptySet())!!
                    .mode
            )
            .isEqualTo(PRIMARY)
    }

    @Test
    fun `tag strategies connect to secondaries with the tags configured, or this server's`() {
        assertThat(
                selection("connect-randomly-to-server-tags", toTags = listOf("south", "other"))
                    .choose(all, emptySet())
            )
            .isEqualTo(south)
        assertThat(
                selection("connect-randomly-to-server-with-tag", toTags = listOf("north2"))
                    .choose(all, emptySet())
            )
            .isEqualTo(north2)
        assertThat(
                selection("connect-randomly-within-server-tags").choose(all, setOf("south", "east"))
            )
            .describedAs("a primary with this server's tag does not count")
            .isEqualTo(south)
    }

    @Test
    fun `user-defined rules pick from the first rule with a result`() {
        val rules = "tags(north2); tags(north); halt()"
        assertThat(selection("user-defined", rules = rules).choose(all, emptySet()))
            .isEqualTo(north2)
        assertThat(selection("user_defined", rules = rules).choose(all - north2, emptySet()))
            .isEqualTo(north)
        assertThat(selection("user-defined", rules = rules).choose(listOf(south), emptySet()))
            .describedAs("halted, and no primary for the last resort")
            .isNull()
        assertThat(
                selection("user-defined", rules = "tags(north)->min(3)")
                    .choose(listOf(south, north, north2), emptySet())
            )
            .describedAs("all() follows the rules unless halt() ends them")
            .isEqualTo(south)
    }

    @Test
    fun `rules filter, pass at least a number of servers or none, and refuse what is unclear`() {
        val rules = UpstreamRules.parse("tags(north, west) -> min(3); tags(east); halt()")
        assertThat(rules.select(all)).containsExactly(follower, north, north2)
        assertThat(rules.select(all - north)).containsExactly(leader)
        assertThat(rules.select(listOf(south))).isEmpty()
        assertThat(UpstreamRules.parse("groups(south);").select(all)).containsExactly(south)
        assertThat(UpstreamRules.parse("").select(all)).isEqualTo(all)
        assertThat(UpstreamRules.parse("all()").select(all)).isEqualTo(all)

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
                "all(east)" to "all() takes no arguments",
            )) {
            assertThat(catchThrowable { UpstreamRules.parse(text) })
                .describedAs(text)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(error)
        }
    }

    @Test
    fun `unknown strategy names are told apart from known ones`() {
        assertThat(
                UpstreamSelection.unknown(
                    listOf("leader-only", "connect-randomly-to-server-group", "closest")
                )
            )
            .containsExactly("closest")
    }
}
