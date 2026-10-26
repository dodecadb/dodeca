package org.dozendb.core.servers

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus

class AllocatorTest {
    private val ids = List(5) { UUID(0, it + 1L) }

    private fun host(
        index: Int,
        status: InstanceStatus = InstanceStatus.ENABLED,
        options: ServerOptions = ServerOptions(),
        available: Boolean = true,
    ) = Host(ServerRecord(ids[index], "server-$index", options, status), available)

    private fun hosts(count: Int) = List(count) { host(it) }

    private fun on(vararg indexes: Int): Set<UUID> = indexes.map { ids[it] }.toSet()

    @Test
    fun `new copies go to the servers that host the fewest, then by id`() {
        val allocator = Allocator(hosts(4), listOf(Placed("films", 2, hosts = on(0, 1))))

        assertThat(allocator.place("books", 2)).isEqualTo(on(2, 3))
        assertThat(allocator.place("music", 3)).containsExactlyInAnyOrderElementsOf(on(0, 1, 2))
    }

    @Test
    fun `servers that answer go first, but one that does not may be chosen`() {
        val servers = listOf(host(0, available = false), host(1), host(2))
        val allocator = Allocator(servers, emptyList())

        assertThat(allocator.place("films", 2)).isEqualTo(on(1, 2))
        assertThat(allocator.place("books", 2)).describedAs("hosting less").isEqualTo(on(1, 2))
        assertThat(allocator.place("music", 3)).containsExactly(ids[1], ids[2], ids[0])
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
        assertThat(allocator.place("books", 1)).isEqualTo(on(3))
        assertThat(allocator.placements()).containsOnlyKeys("books")
    }

    @Test
    fun `a topology the servers cannot satisfy fails, unless best effort`() {
        val allocator = Allocator(hosts(2), emptyList())

        assertThat(catchThrowable { allocator.place("films", 3) })
            .hasMessage("'films' needs 3 servers that can host it as a primary, and only 2 can")
        assertThat(allocator.place("neo4j", 3, bestEffort = true)).isEqualTo(on(0, 1))
        assertThat(allocator.placements()).containsOnlyKeys("neo4j")
    }

    @Test
    fun `a seed server is placed first, and servers that hold the store are preferred`() {
        val allocator = Allocator(hosts(4), listOf(Placed("films", 1, hosts = on(3))))

        assertThat(allocator.place("books", 2, first = ids[3])).containsExactly(ids[3], ids[0])
        assertThat(allocator.place("neo4j", 2, prefer = listOf(ids[2], ids[3])))
            .containsExactly(ids[2], ids[3])
        val denied = host(0, options = ServerOptions(deniedDatabases = listOf("music")))
        assertThat(
                catchThrowable { Allocator(listOf(denied), emptyList()).place("music", 1, ids[0]) }
            )
            .hasMessage("server server-0 cannot host 'music' (its denied databases name it)")
    }

    @Test
    fun `more copies go where new ones go, fewer leave cordoned servers first`() {
        val servers =
            listOf(host(0), host(1, InstanceStatus.CORDONED), host(2), host(3, available = false))
        val allocator = Allocator(servers, listOf(Placed("films", 4, hosts = on(0, 1, 2, 3))))

        assertThat(allocator.resize("films", 3)).isEqualTo(on(0, 2, 3))
        assertThat(allocator.resize("films", 2))
            .describedAs("then unavailable ones")
            .isEqualTo(on(0, 2))
        assertThat(allocator.resize("films", 1)).isEqualTo(on(0))
        assertThat(allocator.resize("films", 3))
            .describedAs("cordoned servers take none")
            .isEqualTo(on(0, 2, 3))
        assertThat(catchThrowable { allocator.resize("films", 4) })
            .isInstanceOf(AllocationException::class.java)
        assertThat(allocator.hosts("films")).isEqualTo(on(0, 2, 3))
    }

    @Test
    fun `fewer copies leave the servers that host the most`() {
        val allocator =
            Allocator(
                hosts(3),
                listOf(Placed("films", 3, hosts = on(0, 1, 2)), Placed("books", 1, hosts = on(1))),
            )

        assertThat(allocator.resize("films", 2)).isEqualTo(on(0, 2))
    }

    @Test
    fun `deallocating moves every copy off the servers, single primaries too`() {
        val allocator =
            Allocator(
                hosts(4),
                listOf(
                    Placed("films", 2, hosts = on(0, 1)),
                    Placed("books", 1, hosts = on(0)),
                    Placed("music", 1, hosts = on(2)),
                ),
            )

        val moves = allocator.deallocate(on(0))

        assertThat(moves)
            .containsExactly(Move("books", ids[0], ids[3]), Move("films", ids[0], ids[2]))
        assertThat(allocator.hosts("films")).isEqualTo(on(1, 2))
        assertThat(allocator.hosts("books")).isEqualTo(on(3))
    }

    @Test
    fun `deallocating one database, or a number of them, moves only those`() {
        val placed =
            listOf(
                Placed("films", 1, hosts = on(0)),
                Placed("books", 1, hosts = on(0)),
                Placed("music", 1, hosts = on(0)),
            )

        assertThat(Allocator(hosts(2), placed).deallocate(on(0), database = "music"))
            .containsExactly(Move("music", ids[0], ids[1]))
        assertThat(Allocator(hosts(2), placed).deallocate(on(0), limit = 2))
            .containsExactly(Move("books", ids[0], ids[1]), Move("films", ids[0], ids[1]))
    }

    @Test
    fun `deallocating is refused for an offline database or a cordoned quorum, changing nothing`() {
        val offline = Allocator(hosts(3), listOf(Placed("films", 1, online = false, hosts = on(0))))
        assertThat(catchThrowable { offline.deallocate(on(0)) })
            .hasMessageContaining("'films' is offline")

        val servers =
            listOf(
                host(0),
                host(1, InstanceStatus.CORDONED),
                host(2, InstanceStatus.CORDONED),
                host(3),
            )
        val cordoned = Allocator(servers, listOf(Placed("films", 3, hosts = on(0, 1, 2))))
        assertThat(catchThrowable { cordoned.deallocate(on(0)) })
            .hasMessage("a quorum of the servers that host 'films' is cordoned")

        val full =
            Allocator(
                hosts(2),
                listOf(Placed("books", 1, hosts = on(0)), Placed("films", 2, hosts = on(0, 1))),
            )
        assertThat(catchThrowable { full.deallocate(on(0)) })
            .hasMessage("no other server can host 'films' in place of server-0")
        assertThat(full.hosts("books")).describedAs("not moved either").isEqualTo(on(0))
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
                listOf(Placed("films", 2, hosts = on(0, 2)), Placed("books", 1, hosts = on(1))),
            )

        assertThat(allocator.conform())
            .containsExactly(Move("books", ids[1], ids[3]), Move("films", ids[0], ids[4]))
        assertThat(allocator.hosts("films"))
            .describedAs("a cordoned server keeps its copy")
            .isEqualTo(on(2, 4))
    }

    @Test
    fun `reallocating evens out the copies, one move at a time`() {
        val allocator =
            Allocator(
                hosts(4),
                listOf(
                    Placed("a", 2, hosts = on(0, 1)),
                    Placed("b", 2, hosts = on(0, 1)),
                    Placed("c", 2, hosts = on(0, 1)),
                ),
            )

        val moves = allocator.reallocate()

        assertThat(moves).containsExactly(Move("a", ids[0], ids[2]), Move("a", ids[1], ids[3]))
        val loads = ids.take(4).map { id -> allocator.placements().values.count { id in it } }
        assertThat(loads.max() - loads.min()).isLessThanOrEqualTo(1)
        assertThat(allocator.reallocate()).describedAs("balanced").isEmpty()
    }

    @Test
    fun `reallocating can be limited to a database or a number of moves`() {
        val placed = listOf(Placed("a", 2, hosts = on(0, 1)), Placed("b", 2, hosts = on(0, 1)))

        assertThat(Allocator(hosts(4), placed).reallocate(database = "b"))
            .containsExactly(Move("b", ids[0], ids[2]), Move("b", ids[1], ids[3]))
        assertThat(Allocator(hosts(4), placed).reallocate(limit = 1))
            .containsExactly(Move("a", ids[0], ids[2]))
    }

    @Test
    fun `reallocating also moves the copies servers may no longer host`() {
        val servers = listOf(host(0, InstanceStatus.DEALLOCATING), host(1))
        val allocator = Allocator(servers, listOf(Placed("films", 1, hosts = on(0))))

        assertThat(allocator.reallocate()).containsExactly(Move("films", ids[0], ids[1]))
    }

    @Test
    fun `the same records place the same`() {
        val placed = listOf(Placed("a", 1, hosts = on(0)), Placed("b", 1, hosts = on(0)))

        val first = Allocator(hosts(3).reversed(), placed.reversed()).reallocate()
        val second = Allocator(hosts(3), placed).reallocate()

        assertThat(first).isEqualTo(second)
    }
}
