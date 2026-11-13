package dev.dodeca.cluster.internal.leadership

import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.GroupProgress
import dev.dodeca.consensus.MemberId
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
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
                view.copy(
                    databases =
                        view.databases.map {
                            if (it.database != move.database) it
                            else
                                it.copy(
                                    leader = move.to,
                                    eligible = it.eligible + move.from - move.to,
                                )
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

    /** [view] where [prioritized] have the priority tag `eu`, which [carriers] carry. */
    private fun prioritized(
        view: LeadershipView,
        prioritized: Collection<DatabaseRef>,
        carriers: Set<MemberId>,
    ) =
        LeadershipView(
            view.databases.map {
                if (it.database in prioritized) it.copy(priorityTag = EU) else it
            },
            tags =
                members.associateWith {
                    if (it in carriers) setOf("other", EU) else setOf("other")
                },
        )

    @Test
    fun `a database with a priority tag goes to an eligible member that carries it, fewest led first`() {
        val leaders = databases.associateWith { if (it == databases[4]) b else a }
        val view = prioritized(view(leaders), listOf(databases[1], databases[2]), setOf(b, c))

        assertThat(LeaderMoves.priority(view, a))
            .isEqualTo(mapOf(databases[1] to listOf(c, b), databases[2] to listOf(c, b)))
        assertThat(LeaderMoves.priority(view, b)).isEmpty()
    }

    @Test
    fun `a leader that carries the priority tag keeps it, as does one no carrier can relieve`() {
        val leaders = databases.associateWith { if (it == databases[1]) c else a }
        // c, the only carrier, leads databases[1]; it is down for databases[2].
        val view =
            prioritized(view(leaders), listOf(databases[1], databases[2]), setOf(c)).let { view ->
                view.copy(
                    databases =
                        view.databases.map {
                            if (it.database == databases[2]) it.copy(eligible = setOf(b)) else it
                        }
                )
            }

        assertThat(LeaderMoves.priority(view, c)).isEmpty()
        assertThat(LeaderMoves.priority(view, a))
            .isEqualTo(mapOf(databases[2] to emptyList<MemberId>()))
    }

    @Test
    fun `balancing never moves a database with a priority tag, but counts it`() {
        // b leads the four with a priority tag, which it carries; a leads the other four.
        val tagged = databases.subList(1, 5)
        val leaders = databases.associateWith { if (it in tagged) b else a }

        val (end, moves) = settle(prioritized(view(leaders), tagged, setOf(b)))

        assertThat(moves).isNotEmpty.allMatch { it.from == a && it.to == c }
        assertThat(led(end)).isEqualTo(mapOf(a to 2, b to 4, c to 2))
        assertThat(end.databases.filter { it.database in tagged }).allMatch { it.leader == b }
    }

    @Test
    fun `members that carry the priority tag take a database read-only on its leader over first`() {
        val leaders = databases.associateWith { if (it == databases[2]) c else a }
        val view =
            prioritized(view(leaders), listOf(databases[1]), setOf(c)).let { view ->
                view.copy(
                    databases =
                        view.databases.map {
                            if (it.database == databases[1]) it.copy(writableOnLeader = false)
                            else it
                        }
                )
            }

        // c leads one more than b, but carries the tag.
        assertThat(LeaderMoves.readOnly(view, a)).isEqualTo(mapOf(databases[1] to listOf(c, b)))
        // The read-only rule moves it.
        assertThat(LeaderMoves.priority(view, a)).isEmpty()
        assertThat(LeaderMoves.rank(view, listOf(a, b, c), EU)).containsExactly(c, b, a)
    }

    @Test
    fun `a database that moves off its leader goes to an eligible member, its carriers first`() {
        val leaders = databases.associateWith { if (it == databases[2]) c else a }
        val view =
            prioritized(view(leaders), listOf(databases[1]), setOf(c)).let { view ->
                view.copy(
                    databases =
                        view.databases.map {
                            if (it.database == databases[1]) it.copy(placed = setOf(b, c)) else it
                        }
                )
            }

        // c leads one more than b, but carries the tag.
        assertThat(LeaderMoves.leaving(view, a)).isEqualTo(mapOf(databases[1] to listOf(c, b)))
        assertThat(LeaderMoves.leaving(view, c)).isEmpty()
    }

    @Test
    fun `the other rules leave a database that moves off its leader to that move`() {
        val leaders = databases.associateWith { a }
        // databases[1] has a priority tag that only c carries, which cannot lead it yet.
        val view =
            prioritized(view(leaders), listOf(databases[1]), setOf(c)).let { view ->
                view.copy(
                    databases =
                        view.databases.map {
                            when (it.database) {
                                databases[1] -> it.copy(eligible = setOf(b), placed = setOf(b, c))
                                databases[2] -> it.copy(placed = setOf(b))
                                else -> it
                            }
                        }
                )
            }

        assertThat(LeaderMoves.priority(view, a)).isEmpty()
        val (_, moves) = settle(view)
        assertThat(moves).isNotEmpty
        assertThat(moves.map { it.database }).doesNotContain(databases[1], databases[2])
    }

    /** What a leader knows of [b], its follower, which last answered [contact] ms ago. */
    private fun progress(commit: Long, match: Long, contact: Long = 50) =
        GroupProgress(
            appendIndex = commit + 10,
            commitIndex = commit,
            appliedIndex = commit,
            pruneIndex = 0,
            term = 3,
            leader = true,
            sinceLeaderContactMillis = 0,
            followerMatchIndexes = mapOf(b to match, c to commit),
            followerContactMillis = mapOf(b to contact, c to 10),
            snapshotsTaken = 0,
            leaderChanges = 1,
            leadershipTransfers = 0,
        )

    private val contactLimit = Duration.ofSeconds(1)

    @Test
    fun `a follower behind the commit index by the entries in flight keeps up`() {
        // Under steady writes the other follower acknowledges first and moves the commit index on.
        assertThat(LeaderMoves.keepsUp(progress(7737, 7729), b, 7400, contactLimit)).isTrue()
    }

    @Test
    fun `a follower without what was committed at the previous look does not keep up`() {
        // Catching up after a restart.
        assertThat(LeaderMoves.keepsUp(progress(15086, 8453), b, 13315, contactLimit)).isFalse()
    }

    @Test
    fun `a follower that has not answered lately does not keep up`() {
        assertThat(LeaderMoves.keepsUp(progress(7737, 7737, contact = 2030), b, 7400, contactLimit))
            .isFalse()
    }

    @Test
    fun `no follower keeps up before the leader has looked once, nor one it knows nothing of`() {
        assertThat(LeaderMoves.keepsUp(progress(7737, 7737), b, null, contactLimit)).isFalse()
        assertThat(LeaderMoves.keepsUp(progress(7737, 7737), a, 7400, contactLimit)).isFalse()
    }

    @Test
    fun `ranking puts the members that lead the fewest first, then by id`() {
        val leaders = mapOf(databases[0] to c, databases[1] to c, databases[2] to a)

        assertThat(LeaderMoves.rank(view(leaders), listOf(c, a, b))).containsExactly(b, a, c)
    }

    private companion object {
        const val EU = "eu"
    }
}
