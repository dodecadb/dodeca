package org.dozendb.cluster.internal.leadership

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.junit.jupiter.api.Test

class LeaderMovesTest {
    private val a = member(1)
    private val b = member(2)
    private val c = member(3)
    private val members = setOf(a, b, c)
    private val databases =
        (listOf("system") + (1..7).map { "db$it" }).map { DatabaseRef(it, UUID.randomUUID()) }

    private fun member(n: Int) = MemberId(UUID(0, n.toLong()))

    /** Every member can lead every database it does not lead. */
    private fun view(leaders: Map<DatabaseRef, MemberId?>, up: Set<MemberId> = members) =
        LeadershipView(
            leaders.map { (database, leader) ->
                Leadership(database, leader, up - setOfNotNull(leader))
            }
        )

    @Test
    fun `a database read-only on its leader goes to an eligible member, fewest led first`() {
        val leaders = databases.associateWith { if (it == databases[3]) c else a }
        val view =
            LeadershipView(
                view(leaders).databases.map {
                    when (it.database) {
                        // Read-only on a, which leads it, and on c.
                        databases[1] -> it.copy(writableOnLeader = false, eligible = setOf(b))
                        // Read-only on a, and nowhere else can take it.
                        databases[2] -> it.copy(writableOnLeader = false, eligible = emptySet())
                        else -> it
                    }
                }
            )

        assertThat(LeaderMoves.readOnly(view, a))
            .isEqualTo(mapOf(databases[1] to listOf(b), databases[2] to emptyList()))
        assertThat(LeaderMoves.readOnly(view, b)).isEmpty()
    }

    @Test
    fun `ranking puts the members that lead the fewest first, then by id`() {
        val leaders = mapOf(databases[0] to c, databases[1] to c, databases[2] to a)

        assertThat(LeaderMoves.rank(view(leaders), listOf(c, a, b))).containsExactly(b, a, c)
    }
}
