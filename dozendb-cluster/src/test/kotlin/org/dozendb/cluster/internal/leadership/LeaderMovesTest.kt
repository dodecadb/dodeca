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

    private fun led(view: LeadershipView) = members.associateWith(view::led)

    /** Applies balancing moves until none helps; returns the final view and the moves. */
    private fun settle(start: LeadershipView): Pair<LeadershipView, List<Move>> {
        var view = start
        val moves = mutableListOf<Move>()
        while (true) {
            val move = LeaderMoves.balancing(view) ?: return view to moves
            check(moves.size < 100) { "no end to moves: $moves" }
            moves += move
            view =
                LeadershipView(
                    view.databases.map {
                        if (it.database != move.database) it
                        else it.copy(leader = move.to, eligible = it.eligible + move.from - move.to)
                    }
                )
        }
    }

    @Test
    fun `a member that leads every database spreads them, one move at a time`() {
        val (end, moves) = settle(view(databases.associateWith { a }))

        assertThat(led(end).values).containsExactlyInAnyOrder(3, 3, 2)
        assertThat(moves).hasSize(5).allMatch { it.from == a && it.reason == MoveReason.BALANCING }
        // The lowest id first on ties, and system last.
        assertThat(moves.first()).isEqualTo(Move(databases[1], a, b, MoveReason.BALANCING))
        assertThat(moves.map { it.database.name }).doesNotContain("system")
    }

    @Test
    fun `a member that came back leading nothing gets its share`() {
        val leaders =
            databases.withIndex().associate { (i, database) ->
                database to if (i % 2 == 0) a else b
            }

        val (end, moves) = settle(view(leaders))

        assertThat(led(end)).isEqualTo(mapOf(a to 3, b to 3, c to 2))
        // One from each member leading four.
        assertThat(moves.map { it.from }).containsExactly(a, b)
        assertThat(moves).allMatch { it.to == c }
    }

    @Test
    fun `counts one apart are balanced`() {
        val leaders =
            databases.withIndex().associate { (i, database) ->
                database to listOf(a, b, c)[i % 3]
            }

        assertThat(LeaderMoves.balancing(view(leaders))).isNull()
    }

    @Test
    fun `a member that is down takes nothing over`() {
        val leaders =
            databases.withIndex().associate { (i, database) ->
                database to if (i % 2 == 0) a else b
            }

        assertThat(LeaderMoves.balancing(view(leaders, up = setOf(a, b)))).isNull()
    }

    @Test
    fun `databases without a known leader are left alone`() {
        val leaders = databases.associateWith<DatabaseRef, MemberId?> { null } + (databases[1] to a)

        assertThat(LeaderMoves.balancing(view(leaders))).isNull()
    }

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
        // Balancing leaves databases that are read-only on their leader to the read-only rule.
        assertThat(LeaderMoves.balancing(view)?.database).isNotIn(databases[1], databases[2])
    }

    @Test
    fun `ranking puts the members that lead the fewest first, then by id`() {
        val leaders = mapOf(databases[0] to c, databases[1] to c, databases[2] to a)

        assertThat(LeaderMoves.rank(view(leaders), listOf(c, a, b))).containsExactly(b, a, c)
    }
}
