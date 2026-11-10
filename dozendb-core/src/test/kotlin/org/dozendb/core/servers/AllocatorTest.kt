package org.dozendb.core.servers

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.servers.CopyMode.PRIMARY
import org.dozendb.core.servers.CopyMode.SECONDARY
import org.junit.jupiter.api.Test
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus

class AllocatorTest {
    private val ids = List(6) { UUID(0, it + 1L) }

    private fun host(
        index: Int,
        status: InstanceStatus = InstanceStatus.ENABLED,
        options: ServerOptions = ServerOptions(),
        available: Boolean = true,
    ) = Host(ServerRecord(ids[index], "server-$index", options, status), available)

    private fun hosts(count: Int) = List(count) { host(it) }

    private fun on(vararg indexes: Int): Set<UUID> = indexes.map { ids[it] }.toSet()

    private fun primaries(vararg indexes: Int): Map<UUID, CopyMode> = indexes.associate {
        ids[it] to PRIMARY
    }

    private fun secondaries(vararg indexes: Int): Map<UUID, CopyMode> = indexes.associate {
        ids[it] to SECONDARY
    }

    private fun constrained(index: Int, mode: InstanceModeConstraint) =
        host(index, options = ServerOptions(modeConstraint = mode))

    @Test
    fun `new copies go to the servers that host the fewest, then by id`() {
        val allocator = Allocator(hosts(4), listOf(Placed("films", 2, copies = primaries(0, 1))))

        assertThat(allocator.place("books", 2)).isEqualTo(primaries(2, 3))
        assertThat(allocator.place("music", 3)).isEqualTo(primaries(0, 1, 2))
    }

    @Test
    fun `servers that answer go first, but one that does not may be chosen`() {
        val servers = listOf(host(0, available = false), host(1), host(2))
        val allocator = Allocator(servers, emptyList())

        assertThat(allocator.place("films", 2)).isEqualTo(primaries(1, 2))
        assertThat(allocator.place("books", 2))
            .describedAs("hosting less")
            .isEqualTo(primaries(1, 2))
        assertThat(allocator.place("music", 3).keys).containsExactly(ids[1], ids[2], ids[0])
    }

    @Test
    fun `copies go only to enabled servers whose options let them host the database`() {
        val servers =
            listOf(
                host(0, InstanceStatus.CORDONED),
                host(1, InstanceStatus.DEALLOCATING),
                host(2, options = ServerOptions(modeConstraint = InstanceModeConstraint.SECONDARY)),
                host(3, options = ServerOptions(allowedDatabases = listOf("books"))),
                host(4, options = ServerOptions(deniedDatabases = listOf("FILMS"))),
            )
        val allocator = Allocator(servers, emptyList())

        val error = catchThrowable { allocator.place("films", 1) }
        assertThat(error)
            .isInstanceOf(AllocationException::class.java)
            .hasMessage("'films' needs 1 server that can host it as a primary, and none can")
        assertThat(allocator.place("books", 1)).isEqualTo(primaries(3))
        assertThat(allocator.placements()).containsOnlyKeys("books")
    }

    @Test
    fun `a topology the servers cannot satisfy fails, unless best effort`() {
        val allocator = Allocator(hosts(2), emptyList())

        assertThat(catchThrowable { allocator.place("films", 3) })
            .hasMessage("'films' needs 3 servers that can host it as a primary, and only 2 can")
        assertThat(allocator.place("neo4j", 3, bestEffort = true)).isEqualTo(primaries(0, 1))
        assertThat(allocator.placements()).containsOnlyKeys("neo4j")
    }

    @Test
    fun `a seed server is placed first, and servers that hold the store are preferred`() {
        val allocator = Allocator(hosts(4), listOf(Placed("films", 1, copies = primaries(3))))

        assertThat(allocator.place("books", 2, first = ids[3]).keys).containsExactly(ids[3], ids[0])
        assertThat(allocator.place("neo4j", 2, prefer = listOf(ids[2], ids[3])).keys)
            .containsExactly(ids[2], ids[3])
        val denied = host(0, options = ServerOptions(deniedDatabases = listOf("music")))
        assertThat(
                catchThrowable {
                    Allocator(listOf(denied), emptyList()).place("music", 1, first = ids[0])
                }
            )
            .hasMessage("server server-0 cannot host 'music' (its denied databases name it)")
    }

    @Test
    fun `required servers each host a copy, primaries first by preference`() {
        // Server 0 hosts the fewest, so it would take a copy first.
        val allocator = Allocator(hosts(5), listOf(Placed("films", 1, copies = primaries(4))))
        val seeders = listOf(ids[4], ids[3], ids[2])

        val copies = allocator.place("books", 2, 1, prefer = seeders, required = seeders.toSet())

        assertThat(copies).isEqualTo(primaries(4, 3) + secondaries(2))
        val secondaryOnly = constrained(3, InstanceModeConstraint.SECONDARY)
        assertThat(
                Allocator(hosts(3) + secondaryOnly, emptyList())
                    .place("music", 1, 1, prefer = listOf(ids[3]), required = on(3))
            )
            .isEqualTo(primaries(0) + secondaries(3))
        assertThat(catchThrowable { allocator.place("music", 1, 1, required = on(0, 1, 2)) })
            .isInstanceOf(AllocationException::class.java)
            .hasMessage("3 servers cannot all host 'music', which has 2 copies")
        val cordoned = host(1, status = InstanceStatus.CORDONED)
        assertThat(
                catchThrowable {
                    Allocator(listOf(host(0), cordoned), emptyList())
                        .place("music", 2, required = on(1))
                }
            )
            .hasMessage("server server-1 cannot host 'music' (it is cordoned)")
        assertThat(allocator.copies("music")).describedAs("failed, unchanged").isEmpty()
    }

    @Test
    fun `promoting a secondary turns a primary secondary, every copy staying`() {
        val allocator =
            Allocator(
                hosts(5),
                listOf(Placed("films", 2, 2, copies = primaries(0, 1) + secondaries(2, 3))),
            )

        val copies = allocator.promote("films", ids[3])

        assertThat(copies.keys).containsExactlyInAnyOrder(ids[0], ids[1], ids[2], ids[3])
        assertThat(copies[ids[3]]).isEqualTo(PRIMARY)
        assertThat(copies[ids[2]]).isEqualTo(SECONDARY)
        assertThat(copies.values.count { it == PRIMARY }).isEqualTo(2)
        assertThat(allocator.copies("films")).isEqualTo(copies)
        assertThat(allocator.promote("films", ids[3]))
            .describedAs("a primary already")
            .isEqualTo(copies)
    }

    @Test
    fun `promoting is refused where the server or the primaries cannot change mode`() {
        val secondaryOnly = constrained(1, InstanceModeConstraint.SECONDARY)
        val placed = listOf(Placed("films", 1, 1, copies = primaries(0) + secondaries(1)))

        assertThat(
                catchThrowable {
                    Allocator(listOf(host(0), secondaryOnly), placed).promote("films", ids[1])
                }
            )
            .isInstanceOf(AllocationException::class.java)
            .hasMessageStartingWith("server server-1 cannot host a primary of 'films'")
        val primaryOnly = constrained(0, InstanceModeConstraint.PRIMARY)
        val allocator = Allocator(listOf(primaryOnly, host(1)), placed)
        assertThat(catchThrowable { allocator.promote("films", ids[1]) })
            .hasMessage(
                "no primary of 'films' can turn secondary for server server-1 to host a primary"
            )
        assertThat(allocator.copies("films")).describedAs("unchanged").isEqualTo(placed[0].copies)
        assertThat(catchThrowable { allocator.promote("films", ids[2]) })
            .hasMessageContaining("does not host 'films'")
    }

    @Test
    fun `more copies go where new ones go, fewer leave cordoned servers first`() {
        val servers =
            listOf(host(0), host(1, InstanceStatus.CORDONED), host(2), host(3, available = false))
        val allocator =
            Allocator(servers, listOf(Placed("films", 4, copies = primaries(0, 1, 2, 3))))

        assertThat(allocator.resize("films", 3)).isEqualTo(primaries(0, 2, 3))
        assertThat(allocator.resize("films", 2))
            .describedAs("then unavailable ones")
            .isEqualTo(primaries(0, 2))
        assertThat(allocator.resize("films", 1)).isEqualTo(primaries(0))
        assertThat(allocator.resize("films", 3))
            .describedAs("cordoned servers take none")
            .isEqualTo(primaries(0, 2, 3))
        assertThat(catchThrowable { allocator.resize("films", 4) })
            .isInstanceOf(AllocationException::class.java)
        assertThat(allocator.copies("films")).isEqualTo(primaries(0, 2, 3))
    }

    @Test
    fun `fewer copies leave the servers that host the most`() {
        val allocator =
            Allocator(
                hosts(3),
                listOf(
                    Placed("films", 3, copies = primaries(0, 1, 2)),
                    Placed("books", 1, copies = primaries(1)),
                ),
            )

        assertThat(allocator.resize("films", 2)).isEqualTo(primaries(0, 2))
    }

    @Test
    fun `deallocating moves every copy off the servers, single primaries too`() {
        val allocator =
            Allocator(
                hosts(4),
                listOf(
                    Placed("films", 2, copies = primaries(0, 1)),
                    Placed("books", 1, copies = primaries(0)),
                    Placed("music", 1, copies = primaries(2)),
                ),
            )

        val moves = allocator.deallocate(on(0))

        assertThat(moves)
            .containsExactly(
                Move("books", ids[0], ids[3], PRIMARY),
                Move("films", ids[0], ids[2], PRIMARY),
            )
        assertThat(allocator.copies("films")).isEqualTo(primaries(1, 2))
        assertThat(allocator.copies("books")).isEqualTo(primaries(3))
    }

    @Test
    fun `deallocating one database, or a number of them, moves only those`() {
        val placed =
            listOf(
                Placed("films", 1, copies = primaries(0)),
                Placed("books", 1, copies = primaries(0)),
                Placed("music", 1, copies = primaries(0)),
            )

        assertThat(Allocator(hosts(2), placed).deallocate(on(0), database = "music"))
            .containsExactly(Move("music", ids[0], ids[1], PRIMARY))
        assertThat(Allocator(hosts(2), placed).deallocate(on(0), limit = 2))
            .containsExactly(
                Move("books", ids[0], ids[1], PRIMARY),
                Move("films", ids[0], ids[1], PRIMARY),
            )
    }

    @Test
    fun `deallocating is refused for an offline database or a cordoned quorum, changing nothing`() {
        val offline =
            Allocator(hosts(3), listOf(Placed("films", 1, online = false, copies = primaries(0))))
        assertThat(catchThrowable { offline.deallocate(on(0)) })
            .hasMessageContaining("'films' is offline")

        val servers =
            listOf(
                host(0),
                host(1, InstanceStatus.CORDONED),
                host(2, InstanceStatus.CORDONED),
                host(3),
            )
        val cordoned = Allocator(servers, listOf(Placed("films", 3, copies = primaries(0, 1, 2))))
        assertThat(catchThrowable { cordoned.deallocate(on(0)) })
            .hasMessage("a quorum of the servers that host 'films' is cordoned")

        val full =
            Allocator(
                hosts(2),
                listOf(
                    Placed("books", 1, copies = primaries(0)),
                    Placed("films", 2, copies = primaries(0, 1)),
                ),
            )
        assertThat(catchThrowable { full.deallocate(on(0)) })
            .hasMessage("no other server can host 'films' in place of server-0")
        assertThat(full.copies("books")).describedAs("not moved either").isEqualTo(primaries(0))
    }

    @Test
    fun `conforming moves the copies servers may no longer host`() {
        val servers =
            listOf(
                host(0, options = ServerOptions(deniedDatabases = listOf("films"))),
                host(1, InstanceStatus.DEALLOCATING),
                host(2, InstanceStatus.CORDONED),
                host(3),
                host(4),
            )
        val allocator =
            Allocator(
                servers,
                listOf(
                    Placed("films", 2, copies = primaries(0, 2)),
                    Placed("books", 1, copies = primaries(1)),
                ),
            )

        assertThat(allocator.conform())
            .containsExactly(
                Move("books", ids[1], ids[3], PRIMARY),
                Move("films", ids[0], ids[4], PRIMARY),
            )
        assertThat(allocator.copies("films"))
            .describedAs("a cordoned server keeps its copy")
            .isEqualTo(primaries(2, 4))
    }

    @Test
    fun `reallocating evens out the copies, one move at a time`() {
        val allocator =
            Allocator(
                hosts(4),
                listOf(
                    Placed("a", 2, copies = primaries(0, 1)),
                    Placed("b", 2, copies = primaries(0, 1)),
                    Placed("c", 2, copies = primaries(0, 1)),
                ),
            )

        val moves = allocator.reallocate()

        assertThat(moves)
            .containsExactly(Move("a", ids[0], ids[2], PRIMARY), Move("a", ids[1], ids[3], PRIMARY))
        val loads = ids.take(4).map { id -> allocator.placements().values.count { id in it } }
        assertThat(loads.max() - loads.min()).isLessThanOrEqualTo(1)
        assertThat(allocator.reallocate()).describedAs("balanced").isEmpty()
    }

    @Test
    fun `reallocating can be limited to a database or a number of moves`() {
        val placed =
            listOf(
                Placed("a", 2, copies = primaries(0, 1)),
                Placed("b", 2, copies = primaries(0, 1)),
            )

        assertThat(Allocator(hosts(4), placed).reallocate(database = "b"))
            .containsExactly(Move("b", ids[0], ids[2], PRIMARY), Move("b", ids[1], ids[3], PRIMARY))
        assertThat(Allocator(hosts(4), placed).reallocate(limit = 1))
            .containsExactly(Move("a", ids[0], ids[2], PRIMARY))
    }

    @Test
    fun `reallocating also moves the copies servers may no longer host`() {
        val servers = listOf(host(0, InstanceStatus.DEALLOCATING), host(1))
        val allocator = Allocator(servers, listOf(Placed("films", 1, copies = primaries(0))))

        assertThat(allocator.reallocate()).containsExactly(Move("films", ids[0], ids[1], PRIMARY))
    }

    @Test
    fun `the same records place the same`() {
        val placed =
            listOf(Placed("a", 1, copies = primaries(0)), Placed("b", 1, copies = primaries(0)))

        val first = Allocator(hosts(3).reversed(), placed.reversed()).reallocate()
        val second = Allocator(hosts(3), placed).reallocate()

        assertThat(first).isEqualTo(second)
    }

    @Test
    fun `secondaries go to servers whose mode constraint lets them`() {
        val servers =
            listOf(
                constrained(0, InstanceModeConstraint.PRIMARY),
                host(1),
                constrained(2, InstanceModeConstraint.SECONDARY),
            )

        assertThat(Allocator(servers, emptyList()).place("films", 1, 2))
            .isEqualTo(primaries(0) + secondaries(1, 2))
        assertThat(Allocator(servers, emptyList()).place("books", 2, 1))
            .isEqualTo(primaries(0, 1) + secondaries(2))
        assertThat(catchThrowable { Allocator(servers, emptyList()).place("music", 2, 2) })
            .hasMessage(
                "'music' needs 2 servers that can host it as a secondary besides those of its " +
                    "primaries, and only 1 can"
            )
        assertThat(catchThrowable { Allocator(servers, emptyList()).place("music", 3) })
            .hasMessage("'music' needs 3 servers that can host it as a primary, and only 2 can")
    }

    @Test
    fun `a server that takes either mode hosts a primary only where no other one can`() {
        val servers = listOf(host(0), constrained(1, InstanceModeConstraint.PRIMARY))
        val busy =
            listOf(Placed("a", 1, copies = primaries(1)), Placed("b", 1, copies = primaries(1)))

        assertThat(Allocator(servers, busy).place("films", 1, 1))
            .describedAs("though it hosts fewer copies")
            .isEqualTo(primaries(1) + secondaries(0))
    }

    @Test
    fun `trading primaries for secondaries keeps the copies where they are, in the other mode`() {
        val allocator = Allocator(hosts(4), listOf(Placed("films", 3, copies = primaries(0, 1, 2))))

        assertThat(allocator.resize("films", 1, 2)).isEqualTo(primaries(0) + secondaries(1, 2))
        assertThat(allocator.resize("films", 3, 0)).isEqualTo(primaries(0, 1, 2))
        assertThat(allocator.resize("films", 2, 1)).isEqualTo(primaries(0, 1) + secondaries(2))
    }

    @Test
    fun `a server that may no longer host its primary swaps modes with a secondary`() {
        val servers =
            listOf(constrained(0, InstanceModeConstraint.SECONDARY), host(1), host(2), host(3))
        val allocator =
            Allocator(
                servers,
                listOf(Placed("films", 2, 1, copies = primaries(0, 1) + secondaries(2))),
            )

        assertThat(allocator.conform())
            .containsExactly(
                Move("films", ids[0], ids[2], PRIMARY),
                Move("films", ids[2], ids[0], SECONDARY),
            )
        assertThat(allocator.copies("films")).isEqualTo(primaries(1, 2) + secondaries(0))
    }

    @Test
    fun `a copy that moves off a server leaves the other copies in their modes`() {
        val allocator =
            Allocator(
                hosts(3),
                listOf(Placed("films", 1, 1, copies = primaries(0) + secondaries(1))),
            )

        assertThat(allocator.deallocate(on(0)))
            .containsExactly(Move("films", ids[0], ids[2], PRIMARY))
        assertThat(allocator.copies("films")).isEqualTo(primaries(2) + secondaries(1))
    }

    @Test
    fun `secondaries count as copies when balancing, and keep their mode`() {
        val placed =
            listOf(
                Placed("a", 1, 1, copies = primaries(0) + secondaries(1)),
                Placed("b", 1, 1, copies = primaries(0) + secondaries(1)),
            )
        val allocator = Allocator(hosts(3), placed)

        assertThat(allocator.reallocate()).containsExactly(Move("a", ids[0], ids[2], PRIMARY))
        assertThat(allocator.copies("a")).isEqualTo(primaries(2) + secondaries(1))
    }

    @Test
    fun `best effort places as many secondaries as remain after the primaries`() {
        assertThat(Allocator(hosts(2), emptyList()).place("neo4j", 1, 3, bestEffort = true))
            .isEqualTo(primaries(0) + secondaries(1))
        val secondaryOnly = listOf(constrained(0, InstanceModeConstraint.SECONDARY))
        assertThat(
                catchThrowable {
                    Allocator(secondaryOnly, emptyList()).place("neo4j", 1, 1, bestEffort = true)
                }
            )
            .hasMessage("'neo4j' needs 1 server that can host it as a primary, and none can")
    }

    @Test
    fun `cordoned servers keep their secondaries, take no new ones, and count for no quorum`() {
        val servers =
            listOf(
                host(0),
                host(1, InstanceStatus.CORDONED),
                host(2, InstanceStatus.CORDONED),
                host(3),
            )
        val placed = listOf(Placed("films", 1, 2, copies = primaries(0) + secondaries(1, 2)))
        val allocator = Allocator(servers, placed)

        assertThat(allocator.resize("films", 1, 1)).isEqualTo(primaries(0) + secondaries(1))
        assertThat(allocator.resize("films", 1, 2)).isEqualTo(primaries(0) + secondaries(1, 3))
        assertThat(Allocator(servers, placed).deallocate(on(0)))
            .containsExactly(Move("films", ids[0], ids[3], PRIMARY))
    }
}
