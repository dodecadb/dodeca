package org.dozendb.cluster.internal.replication

import java.io.IOException
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.junit.jupiter.api.Test
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.NullLog

class DatabaseReconcilerTest {
    private val self = MemberId(UUID.randomUUID())
    private val creator = MemberId(UUID.randomUUID())
    private val creatorAddress = HostPort("creator", 7000)
    private val otherAddress = HostPort("other", 7000)
    private val graph = FakeSystemGraph()
    private val local = FakeLocalDatabases()
    private val members =
        listOf(Member(self, HostPort("self", 7000)), Member(creator, creatorAddress))
    /** The settled members of the system group, null while they change. */
    private var systemMembers: List<Member>? = members
    private var removed = false
    private var answering: (MemberId) -> Boolean = { true }
    private val reconciler =
        DatabaseReconciler(
            graph,
            local,
            self,
            DEFAULT,
            { database ->
                listOf(
                        if (database.creator == creator) creatorAddress else otherAddress,
                        otherAddress,
                    )
                    .distinct()
            },
            { systemMembers },
            { answering(it) },
            { removed },
            NullLog.getInstance(),
        )

    private val films = database("films")

    private fun database(name: String): NamedDatabaseId =
        DatabaseIdFactory.from(name, UUID.randomUUID())

    /** A database placed, by default, on every member but the Free ones. */
    private fun desired(
        id: NamedDatabaseId,
        online: Boolean = true,
        creator: MemberId? = null,
        seedUri: String? = null,
        designatedSeeder: MemberId? = null,
        hosts: Set<MemberId> = members.map { it.id }.filter { it !in graph.free }.toSet(),
        created: Boolean = false,
    ) = DesiredDatabase(id, online, creator, seedUri, designatedSeeder, hosts, created)

    @Test
    fun `the creator of a seeded database creates its store from the seed, the others copy it`() {
        val seed = "file:///seeds/films.backup"
        graph.databases = listOf(desired(films, creator = self, seedUri = seed))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.seeds).containsExactly(entry(films, StoreSeed.Uri(seed)))

        val series = database("series")
        graph.databases = listOf(desired(series, creator = creator, seedUri = seed))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.copies).containsExactly(creatorAddress to series)
        assertThat(local.seeds).containsEntry(series, null)
    }

    @Test
    fun `the system leader names the designated seeder the creator, which uses its restored store`() {
        graph.leading = true
        graph.databases = listOf(desired(films, designatedSeeder = creator))

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(graph.named).containsExactly(films to creator)

        val series = database("series")
        graph.databases = listOf(desired(series, creator = self, designatedSeeder = self))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.seeds).containsExactly(entry(series, StoreSeed.Restored))
    }

    @Test
    fun `a designated seeder that is not a member is not named the creator`() {
        graph.leading = true
        graph.databases = listOf(desired(films, designatedSeeder = MemberId(UUID.randomUUID())))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).isEmpty()
        assertThat(local.registered).isEmpty()
    }

    @Test
    fun `the system leader names itself the creator of a new database, then creates it`() {
        graph.leading = true
        graph.databases = listOf(desired(films))

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(graph.named).containsExactly(films to self)
        assertThat(graph.initialMembers).containsExactly(members)
        assertThat(local.registered).isEmpty()

        graph.databases = listOf(desired(films, creator = self))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered).containsEntry(films, LocalDatabase(true, false))
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `a member removed from the cluster stops its databases and creates none`() {
        local.add(films, started = true)
        local.add(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID, started = true)
        graph.leading = true
        graph.databases = listOf(desired(films), desired(database("series")))
        removed = true

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.left).containsExactly(films)
        assertThat(local.registered)
            .containsEntry(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID, LocalDatabase(true, false))
            .hasSize(2)
        assertThat(graph.named).isEmpty()
    }

    @Test
    fun `no creator is named while the members change`() {
        graph.leading = true
        graph.databases = listOf(desired(films))
        systemMembers = null

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(graph.named).isEmpty()

        systemMembers = members

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(graph.named).containsExactly(films to self)
    }

    @Test
    fun `a member without a creator to follow creates nothing`() {
        graph.databases = listOf(desired(films))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).isEmpty()
        assertThat(local.registered).isEmpty()
        assertThat(local.stored).isEmpty()
    }

    @Test
    fun `other members copy a new database, from its creator first`() {
        graph.databases = listOf(desired(films, creator = creator))
        local.unreachable += creatorAddress

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copyAttempts).containsExactly(creatorAddress, otherAddress)
        assertThat(local.copies).containsExactly(otherAddress to films)
        assertThat(local.registered).containsEntry(films, LocalDatabase(true, false))
    }

    @Test
    fun `a copy that fails everywhere is tried again`() {
        graph.databases = listOf(desired(films, creator = creator))
        local.unreachable += listOf(creatorAddress, otherAddress)

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(local.registered).isEmpty()

        local.unreachable.clear()

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered).containsKey(films)
    }

    @Test
    fun `a copy placed here whose group stopped it as a non-member is copied again`() {
        local.add(films, started = true)
        local.notVoting += films
        local.notMember += films
        graph.databases = listOf(desired(films, creator = creator, created = true))

        assertThat(reconciler.reconcile()).describedAs("copied at the next look").isTrue()

        assertThat(local.dropped).containsExactly(films)
        local.notMember.clear()
        local.notVoting.clear()

        reconciler.reconcile()

        assertThat(local.copies).containsExactly(creatorAddress to films)
        assertThat(local.registered).containsEntry(films, LocalDatabase(true, false))
    }

    @Test
    fun `such a copy is kept while the system graph cannot be confirmed current`() {
        local.add(films, started = true)
        local.notVoting += films
        local.notMember += films
        graph.databases = listOf(desired(films, creator = creator, created = true))
        // As on a member removed from the cluster, which has not learnt it yet.
        graph.reachable = false

        assertThat(reconciler.reconcile()).describedAs("tried again").isTrue()

        assertThat(local.dropped).isEmpty()
        assertThat(local.registered).containsKey(films)
    }

    @Test
    fun `a creator copies a database that was created, as when it moved off it and back`() {
        graph.databases = listOf(desired(films, creator = self, created = true))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copies).containsExactly(otherAddress to films)
        assertThat(graph.named).isEmpty()
    }

    @Test
    fun `a creator that already has the database's Raft group copies the store instead`() {
        graph.databases = listOf(desired(films, creator = self))
        local.groups += films.databaseId().uuid()

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copies).containsExactly(otherAddress to films)
    }

    @Test
    fun `databases the system graph lacks are dropped only once it is confirmed current`() {
        local.add(films, started = true)
        graph.reachable = false

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(local.registered).containsKey(films)

        graph.reachable = true

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered).isEmpty()
        assertThat(local.stored).isEmpty()
        assertThat(local.groups).isEmpty()
    }

    @Test
    fun `a system graph that is behind after a restart deletes nothing`() {
        local.stored["films"] = true
        local.groups += films.databaseId().uuid()
        graph.databases = emptyList()
        graph.whenConfirmed = listOf(desired(films, creator = creator))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copies).isEmpty()
        assertThat(local.registered).containsEntry(films, LocalDatabase(true, false))
    }

    @Test
    fun `the leftovers of a dropped namesake are replaced by a copy of the new database`() {
        val dropped = database("films")
        local.stored["films"] = true
        local.groups += dropped.databaseId().uuid()
        graph.databases = listOf(desired(films, creator = creator))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.deletedStores).containsExactly("films")
        assertThat(local.groups).containsExactly(films.databaseId().uuid())
        assertThat(local.copies).containsExactly(creatorAddress to films)
        assertThat(local.registered.keys).containsExactly(films)
    }

    @Test
    fun `a registered namesake is dropped before the new database is created`() {
        val dropped = database("films")
        local.add(dropped, started = true)
        graph.databases = listOf(desired(films, creator = creator))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.dropped).containsExactly(dropped)
        assertThat(local.registered.keys).containsExactly(films)
    }

    @Test
    fun `leftovers wait while the system graph cannot be confirmed`() {
        local.stored["films"] = true
        graph.reachable = false
        graph.databases = listOf(desired(films, creator = creator))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(local.deletedStores).isEmpty()
        assertThat(local.copies).isEmpty()
        assertThat(local.registered).isEmpty()
    }

    @Test
    fun `databases follow their status in the system graph`() {
        local.add(films, started = true)
        graph.databases = listOf(desired(films, online = false))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered).containsEntry(films, LocalDatabase(false, false))

        graph.databases = listOf(desired(films, online = true))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered).containsEntry(films, LocalDatabase(true, false))
    }

    @Test
    fun `a failed database starts again only after it was taken offline`() {
        local.registered[films] = LocalDatabase(started = false, failed = true)
        graph.databases = listOf(desired(films, online = true))

        reconciler.reconcile()
        assertThat(local.registered).containsEntry(films, LocalDatabase(false, true))

        graph.databases = listOf(desired(films, online = false))
        reconciler.reconcile()
        graph.databases = listOf(desired(films, online = true))
        reconciler.reconcile()

        assertThat(local.registered).containsEntry(films, LocalDatabase(true, false))
    }

    @Test
    fun `an offline database is registered from a local store and copied only once online`() {
        val local2 = database("archive")
        local.stored["archive"] = true
        local.groups += local2.databaseId().uuid()
        graph.databases =
            listOf(
                desired(films, online = false, creator = creator),
                desired(local2, online = false, creator = creator),
            )

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copies).isEmpty()
        assertThat(local.registered).containsOnlyKeys(local2)
        assertThat(local.registered).containsEntry(local2, LocalDatabase(false, false))
    }

    @Test
    fun `databases being caught up are left alone`() {
        val caughtUp = database("caught")
        local.add(caughtUp, started = false)
        local.add(films, started = true)
        local.catchingUp += listOf(caughtUp, films)
        graph.databases = listOf(desired(films, online = false))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(local.registered)
            .containsEntry(caughtUp, LocalDatabase(false, false))
            .containsEntry(films, LocalDatabase(true, false))
    }

    @Test
    fun `the default database is never dropped or replaced, but follows its status`() {
        val running = database(DEFAULT)
        local.add(running, started = true)
        graph.databases = listOf(desired(database(DEFAULT), creator = self))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered.keys).containsExactly(running)

        graph.databases = listOf(desired(running, online = false))

        reconciler.reconcile()
        assertThat(local.registered).containsEntry(running, LocalDatabase(false, false))
    }

    @Test
    fun `a member the default database is placed on copies it from its creator`() {
        val default = database(DEFAULT)
        graph.databases = listOf(desired(default, creator = creator))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copies.map { it.second }).containsExactly(default)
        assertThat(local.registered).containsEntry(default, LocalDatabase(true, false))
    }

    @Test
    fun `a new database's group starts with the members that host databases`() {
        val free = Member(MemberId(UUID.randomUUID()), HostPort("free", 7000))
        systemMembers = members + free
        graph.free = setOf(free.id)
        graph.leading = true
        graph.databases = listOf(desired(films))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).containsExactly(films to self)
        assertThat(graph.initialMembers).containsExactly(members)
    }

    @Test
    fun `a Free system leader names a member that hosts databases and answers the creator`() {
        // Ahead of the creator by id, but it does not answer.
        val silent =
            Member(MemberId(UUID(Long.MIN_VALUE, Long.MIN_VALUE)), HostPort("silent", 7000))
        systemMembers = members + silent
        answering = { it != silent.id }
        graph.free = setOf(self)
        graph.leading = true
        graph.databases = listOf(desired(films, hosts = setOf(creator, silent.id)))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).containsExactly(films to creator)
        assertThat(graph.initialMembers).containsExactly(listOf(members[1], silent))
        assertThat(local.registered).isEmpty()
    }

    @Test
    fun `a member creates and copies only the databases placed on it`() {
        graph.leading = true
        graph.databases =
            listOf(
                desired(films, creator = creator, hosts = setOf(creator)),
                desired(database(DEFAULT), hosts = setOf(creator)),
            )

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(local.registered).isEmpty()
        assertThat(local.copies).isEmpty()
        assertThat(graph.named.map { it.second })
            .describedAs("named among its servers")
            .containsOnly(creator)
    }

    @Test
    fun `a database not placed yet is left as it is`() {
        val held = database("held")
        local.add(held, started = true)
        graph.leading = true
        graph.databases =
            listOf(
                desired(films, hosts = emptySet()),
                desired(held, online = false, hosts = emptySet()),
            )

        reconciler.reconcile()

        assertThat(local.registered).containsOnlyKeys(held)
        assertThat(local.registered[held]).isEqualTo(LocalDatabase(false, false))
        assertThat(graph.named).isEmpty()
    }

    @Test
    fun `a copy no longer placed here is kept while this member votes, then deleted`() {
        local.add(films, started = true)
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator)))

        assertThat(reconciler.reconcile()).describedAs("tried again").isTrue()

        assertThat(local.registered).containsKey(films)
        assertThat(local.dropped).isEmpty()

        local.notVoting += films
        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.dropped).containsExactly(films)
    }

    @Test
    fun `a designated seeder that is Free is not named the creator`() {
        val free = Member(MemberId(UUID.randomUUID()), HostPort("free", 7000))
        systemMembers = members + free
        graph.free = setOf(free.id)
        graph.leading = true
        graph.databases = listOf(desired(films, designatedSeeder = free.id))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).isEmpty()
    }

    @Test
    fun `a Free server creates and copies no database`() {
        graph.free = setOf(self)
        graph.databases = listOf(desired(database(DEFAULT)), desired(films, creator = self))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.registered).isEmpty()
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `Raft groups of databases the system graph lacks are deleted`() {
        val orphan = UUID.randomUUID()
        local.groups += orphan
        local.add(films, started = true)
        graph.databases = listOf(desired(films))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.groups).containsExactly(films.databaseId().uuid())
    }

    private class FakeSystemGraph : SystemGraph {
        var databases: List<DesiredDatabase> = emptyList()
        /** What this member's system graph holds once confirmed current, if it was behind. */
        var whenConfirmed: List<DesiredDatabase>? = null
        var reachable = true
        var leading = false
        /** The members that host only `system`. */
        var free = emptySet<MemberId>()
        val named = mutableListOf<Pair<NamedDatabaseId, MemberId>>()
        val initialMembers = mutableListOf<List<Member>>()

        override fun databases(): List<DesiredDatabase> = databases

        override fun awaitCurrent(): Boolean {
            if (!reachable) return false
            whenConfirmed?.let { databases = it }
            whenConfirmed = null
            return true
        }

        override fun leading(): Boolean = leading

        override fun nameCreator(
            database: NamedDatabaseId,
            creator: MemberId,
            initialMembers: List<Member>,
        ) {
            named += database to creator
            this.initialMembers += initialMembers
        }
    }

    /**
     * Databases as the database manager and the replication keep them: starting starts the group.
     */
    private class FakeLocalDatabases : LocalDatabases {
        val registered = mutableMapOf<NamedDatabaseId, LocalDatabase>()
        val left = mutableListOf<NamedDatabaseId>()
        /** Database files by name; true if complete. */
        val stored = mutableMapOf<String, Boolean>()
        val groups = mutableSetOf<UUID>()
        val catchingUp = mutableSetOf<NamedDatabaseId>()
        val unreachable = mutableSetOf<HostPort>()
        val copyAttempts = mutableListOf<HostPort>()
        val copies = mutableListOf<Pair<HostPort, NamedDatabaseId>>()
        val dropped = mutableListOf<NamedDatabaseId>()
        val deletedStores = mutableListOf<String>()

        fun add(id: NamedDatabaseId, started: Boolean) {
            registered[id] = LocalDatabase(started, false)
            stored[id.name()] = true
            groups += id.databaseId().uuid()
        }

        override fun registered(): Map<NamedDatabaseId, LocalDatabase> = registered.toMap()

        override fun stored(): Set<String> = stored.keys.toSet()

        override fun complete(name: String): Boolean = stored[name] == true

        override fun groups(): Set<UUID> = groups.toSet()

        override fun catchingUp(id: NamedDatabaseId): Boolean = id in catchingUp

        /** Databases whose group this member was removed from. */
        val notVoting = mutableSetOf<NamedDatabaseId>()

        override fun votes(id: NamedDatabaseId): Boolean? =
            if (id.databaseId().uuid() in groups) id !in notVoting else null

        /** Databases whose group stopped here, as this member is not one of its members. */
        val notMember = mutableSetOf<NamedDatabaseId>()

        override fun left(id: NamedDatabaseId): Boolean = id in notMember

        override fun create(id: NamedDatabaseId) {
            check(id !in registered) { "$id is registered" }
            registered[id] = LocalDatabase(started = false, failed = false)
        }

        /** The seeds each database was started with. */
        val seeds = mutableMapOf<NamedDatabaseId, StoreSeed?>()

        override fun start(id: NamedDatabaseId, seed: StoreSeed?) {
            check(registered.getValue(id) == LocalDatabase(started = false, failed = false))
            seeds[id] = seed
            registered[id] = LocalDatabase(started = true, failed = false)
            stored[id.name()] = true
            groups += id.databaseId().uuid()
        }

        override fun stop(id: NamedDatabaseId) {
            registered[id] = LocalDatabase(started = false, failed = false)
        }

        override fun leave(id: NamedDatabaseId) {
            registered[id] = LocalDatabase(started = false, failed = false)
            left += id
        }

        override fun drop(id: NamedDatabaseId) {
            registered.remove(id)
            stored.remove(id.name())
            groups.remove(id.databaseId().uuid())
            dropped += id
        }

        override fun deleteStore(name: String) {
            check(registered.keys.none { it.name() == name }) { "$name is registered" }
            stored.remove(name)
            deletedStores += name
        }

        override fun removeGroup(id: UUID) {
            groups.remove(id)
        }

        override fun copy(source: HostPort, id: NamedDatabaseId) {
            copyAttempts += source
            if (source in unreachable) throw IOException("$source is unreachable")
            check(id.name() !in stored) { "${id.name()} has files" }
            stored[id.name()] = true
            copies += source to id
        }
    }

    private companion object {
        const val DEFAULT = "neo4j"
    }
}
