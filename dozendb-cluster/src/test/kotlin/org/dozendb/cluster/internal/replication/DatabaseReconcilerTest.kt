package org.dozendb.cluster.internal.replication

import java.io.IOException
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.dozendb.cluster.internal.recreation.RecreationSeeds
import org.dozendb.cluster.internal.recreation.Seed
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.UriSeed
import org.dozendb.core.servers.AllocationException
import org.dozendb.core.servers.CopyMode
import org.junit.jupiter.api.Test
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.NullLog

class DatabaseReconcilerTest {
    private val self = MemberId(UUID.randomUUID())
    private val creator = MemberId(UUID.randomUUID())
    /** A server that is no member here, which hosts secondaries. */
    private val other = MemberId(UUID.randomUUID())
    private val creatorAddress = HostPort("creator", 7000)
    private val otherAddress = HostPort("other", 7000)
    private val graph = FakeSystemGraph()
    private val local = FakeLocalDatabases()
    private val seeds = FakeSeeds()
    private val members =
        listOf(Member(self, HostPort("self", 7000)), Member(creator, creatorAddress))
    /** The settled members of the system group, null while they change. */
    private var systemMembers: List<Member>? = members
    private var removed = false
    private var answering: (MemberId) -> Boolean = { true }
    /** The members that run each database, as far as this member knows; all of them by default. */
    private var runs: (MemberId, NamedDatabaseId) -> Boolean = { _, _ -> true }
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
            { member, id -> runs(member, id) },
            seeds,
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
        seed: UriSeed? = null,
        designatedSeeder: MemberId? = null,
        hosts: Set<MemberId> = members.map { it.id }.filter { it !in graph.free }.toSet(),
        created: Boolean = false,
        secondaries: Set<MemberId> = emptySet(),
        startedAt: Any? = null,
    ) =
        DesiredDatabase(
            id,
            online,
            creator,
            seed,
            designatedSeeder,
            hosts = hosts,
            secondaries = secondaries,
            created = created,
            startedAt = startedAt,
        )

    @Test
    fun `the creator of a seeded database creates its store from the seed, the others copy it`() {
        val seed = UriSeed("file:///seeds/films.backup")
        graph.databases = listOf(desired(films, creator = self, seed = seed))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.seeds).containsExactly(entry(films, StoreSeed.Uri(seed)))

        val series = database("series")
        graph.databases = listOf(desired(series, creator = creator, seed = seed))

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

    // As after dbms.unquarantineDatabase's replace operations deleted the copy: it must not run
    // the group before the group removed it.
    @Test
    fun `a copy in quarantine whose store and group are gone is copied only once lifted`() {
        local.quarantines += films.databaseId().uuid()
        graph.databases = listOf(desired(films, creator = self, created = true))

        reconciler.reconcile()

        assertThat(local.copies).isEmpty()
        assertThat(local.registered).doesNotContainKey(films)
        local.quarantines.clear()

        reconciler.reconcile()

        assertThat(local.copies).containsExactly(otherAddress to films)
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
    fun `a copy in quarantine is left as it is, started, stopped or moved by none`() {
        local.add(films, started = true)
        local.quarantine(films)
        graph.databases = listOf(desired(films, creator = creator, created = true))

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered[films]?.started).isFalse()

        graph.databases = listOf(desired(films, online = false, creator = creator, created = true))
        assertThat(reconciler.reconcile()).isFalse()

        // Placed elsewhere, and this member left the group.
        local.notVoting += films
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator)))
        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.registered)
            .containsEntry(films, LocalDatabase(false, false, quarantined = true))
        assertThat(local.dropped).isEmpty()
        assertThat(local.seeds).isEmpty()
    }

    @Test
    fun `a dropped database goes with its quarantine, and those of databases gone are forgotten`() {
        local.add(films, started = true)
        local.quarantine(films)
        val gone = UUID.randomUUID()
        local.quarantines += gone
        graph.reachable = false

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(local.quarantines).contains(gone)

        graph.reachable = true
        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.dropped).containsExactly(films)
        assertThat(local.quarantines).isEmpty()
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

    // Enterprise's documented retry of a failed start: START DATABASE, which records a new start.
    @Test
    fun `a copy whose start failed starts again when START DATABASE asks again, once`() {
        local.add(films, started = false)
        graph.databases = listOf(desired(films, online = true, startedAt = 1))
        local.failStarts = true
        reconciler.reconcile()
        assertThat(local.registered).containsEntry(films, LocalDatabase(false, true))
        local.failStarts = false

        reconciler.reconcile()
        assertThat(local.registered)
            .describedAs("the start it failed for is not asked again")
            .containsEntry(films, LocalDatabase(false, true))

        graph.databases = listOf(desired(films, online = true, startedAt = 2))
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
    fun `the default database is dropped once recreated under another id`() {
        val running = database(DEFAULT)
        local.add(running, started = true)
        val recreated = database(DEFAULT)
        graph.databases =
            listOf(
                desired(recreated, creator = creator)
                    .copy(recreatedFrom = running.databaseId().uuid())
            )

        reconciler.reconcile()

        assertThat(local.registered.keys).doesNotContain(running)
        assertThat(local.dropped).contains(running)
    }

    /** [films] recreated from [old], with [seeders]. */
    private fun recreated(
        old: NamedDatabaseId,
        seeders: Set<MemberId> = setOf(self),
        creator: MemberId? = null,
        created: Boolean = false,
    ): DesiredDatabase =
        desired(database(old.name()), creator = creator, created = created)
            .copy(
                recreatedFrom = old.databaseId().uuid(),
                seeders = seeders,
                designatedSeeder = seeders.firstOrNull(),
            )

    @Test
    fun `a seeder keeps the store of the old copy of a recreated database as its seed`() {
        local.add(films, started = true)
        val new = recreated(films)
        graph.databases = listOf(new)

        reconciler.reconcile()

        assertThat(local.dropped).isEmpty()
        assertThat(local.registered.keys).noneMatch { it.name() == films.name() }
        assertThat(local.keptSeeds).containsExactly(entry(films.name(), new.id.databaseId().uuid()))
        assertThat(local.groups).doesNotContain(films.databaseId().uuid())
    }

    @Test
    fun `a member that seeds no recreated database drops its old copy`() {
        local.add(films, started = true)
        graph.databases = listOf(recreated(films, seeders = setOf(creator), creator = creator))

        reconciler.reconcile()

        assertThat(local.dropped).containsExactly(films)
        assertThat(local.keptSeeds).isEmpty()
    }

    @Test
    fun `the creator of a recreated database creates it from its kept seed`() {
        local.add(films, started = true)
        val new = recreated(films)
        graph.databases = listOf(new)
        reconciler.reconcile()

        graph.databases = listOf(new.copy(creator = self))
        reconciler.reconcile()

        assertThat(local.registered).containsEntry(new.id, LocalDatabase(true, false))
        assertThat(local.seeds).containsEntry(new.id, StoreSeed.Kept)
        assertThat(local.copies).isEmpty()
        assertThat(local.deletedStores).isEmpty()
    }

    @Test
    fun `a seeder that leads system names the creator of the database recreated from its seed`() {
        local.add(films, started = true)
        graph.leading = true
        val new = recreated(films)
        graph.databases = listOf(new)

        reconciler.reconcile()

        assertThat(local.keptSeeds).containsKey(films.name())
        assertThat(graph.named).containsExactly(new.id to self)
    }

    @Test
    fun `a store the old copy left becomes a seed of the recreated database`() {
        // As after a restart: the old database is not registered any more.
        local.stored[films.name()] = true
        val new = recreated(films)
        graph.databases = listOf(new)

        reconciler.reconcile()

        assertThat(local.keptSeeds).containsExactly(entry(films.name(), new.id.databaseId().uuid()))
        assertThat(local.deletedStores).isEmpty()
    }

    @Test
    fun `a seeder that is not the creator runs its seed as a secondary, then turns primary once it pulled beyond the creator's seed`() {
        local.add(films, started = true)
        val new = recreated(films, seeders = setOf(creator, self))
        graph.databases = listOf(new)

        reconciler.reconcile()

        assertThat(local.keptSeeds).containsKey(films.name())

        graph.databases = listOf(new.copy(creator = creator, seedAppendIndex = 10))
        reconciler.reconcile()

        assertThat(local.registered)
            .containsEntry(new.id, LocalDatabase(true, false, CopyMode.SECONDARY))
        assertThat(local.keptSeeds).isEmpty()
        assertThat(local.copies).isEmpty()
        assertThat(local.deletedStores).isEmpty()

        // It holds the creator's seed, not yet the transaction that detached it.
        local.pulled[new.id] = 10
        assertThat(reconciler.reconcile()).isTrue()
        assertThat(local.switched).isEmpty()

        local.pulled[new.id] = 11
        reconciler.reconcile()

        assertThat(local.switched).containsExactly(new.id to CopyMode.PRIMARY)
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `a seeder placed as a secondary runs its seed as one`() {
        local.add(films, started = true)
        val new =
            recreated(films, seeders = setOf(creator, self))
                .copy(hosts = setOf(creator), secondaries = setOf(self))
        graph.databases = listOf(new)
        reconciler.reconcile()

        graph.databases = listOf(new.copy(creator = creator, seedAppendIndex = 10))
        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.registered)
            .containsEntry(new.id, LocalDatabase(true, false, CopyMode.SECONDARY))
        assertThat(local.copies).isEmpty()
        assertThat(local.switched).isEmpty()
    }

    @Test
    fun `a seeder reports the seed it keeps once kept, and none without a store`() {
        local.add(films, started = true)
        val new = recreated(films, seeders = setOf(self, creator))
        val series = database("series")
        val newSeries = recreated(series, seeders = setOf(self, creator))
        graph.databases = listOf(new, newSeries)
        graph.reachable = false

        reconciler.reconcile()

        // The old copy is kept once the system leader confirmed the system graph current.
        assertThat(reconciler.seedReports)
            .containsExactly(entry(newSeries.id.databaseId().uuid(), null))

        graph.reachable = true
        reconciler.reconcile()

        assertThat(reconciler.seedReports)
            .containsExactlyInAnyOrderEntriesOf(
                mapOf(new.id.databaseId().uuid() to KEPT, newSeries.id.databaseId().uuid() to null)
            )

        graph.databases = listOf(new.copy(creator = creator, created = true))
        reconciler.reconcile()

        assertThat(reconciler.seedReports).describedAs("created").isEmpty()
    }

    /** [films] recreated from the stores of [creator] and [other], this member leading system. */
    private fun recreatedFromOthers(
        secondaries: Set<MemberId> = emptySet(),
        seed: UriSeed? = null,
    ): DesiredDatabase {
        systemMembers = members + Member(other, otherAddress)
        graph.leading = true
        return recreated(films, seeders = setOf(creator, other))
            .copy(
                hosts = setOf(self, creator, other) - secondaries,
                secondaries = secondaries,
                seed = seed,
            )
            .also { graph.databases = listOf(it) }
    }

    private fun DesiredDatabase.reported(member: MemberId, seed: Seed?) {
        seeds.reports[member] = mapOf(id.databaseId().uuid() to seed)
    }

    @Test
    fun `the system leader names the seeder with the latest store the creator, once checked`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(appendIndex = 12, lastTransaction = 9, checksum = 222))
        seeds.checksums[other] = mapOf(10L to KEPT.checksum)

        reconciler.reconcile()

        assertThat(seeds.asked).containsExactly(other to listOf(10L))
        assertThat(graph.named).containsExactly(new.id to other)
        assertThat(graph.seedAppendIndexes).containsExactly(12L)
        assertThat(graph.recorded).isEmpty()
    }

    @Test
    fun `seeders with the same append index need no checksums asked, and the lowest id creates`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT)

        reconciler.reconcile()

        assertThat(seeds.asked).isEmpty()
        assertThat(graph.named).containsExactly(new.id to minOf(creator, other))
    }

    @Test
    fun `naming the creator waits for every seeder to report, as recorded`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).isEmpty()
        assertThat(seeds.refreshes).isEqualTo(1)
        assertThat(graph.recorded)
            .containsExactly(
                Triple(new.id, "Waiting for seeder $other to report its store.", false)
            )

        // Recorded already.
        graph.databases =
            listOf(new.copy(recreationStatus = "Waiting for seeder $other to report its store."))
        reconciler.reconcile()
        assertThat(graph.recorded).hasSize(1)
    }

    @Test
    fun `a seeder whose last transaction differs from the latest store's fails the recreation`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(appendIndex = 12, lastTransaction = 9, checksum = 222))
        seeds.checksums[other] = mapOf(10L to 999)
        val message =
            "Seeders $other and $creator have different checksums for transaction 7. All " +
                "seeders must have the same checksum for the same append index."

        reconciler.reconcile()

        assertThat(graph.named).isEmpty()
        assertThat(graph.recorded).containsExactly(Triple(new.id, message, true))

        // Failed: nothing is tried again until it is recreated anew.
        graph.databases = listOf(new.copy(recreationStatus = message, recreationFailed = true))
        seeds.asked.clear()

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(seeds.asked).isEmpty()
        assertThat(graph.recorded).hasSize(1)
        assertThat(local.registered).isEmpty()
    }

    @Test
    fun `seeders with the same append index and different checksums fail too`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(checksum = 222))
        val (first, second) = listOf(creator, other).sorted()

        reconciler.reconcile()

        assertThat(graph.recorded.single().second)
            .startsWith("Seeders $first and $second have different checksums for transaction 7.")
    }

    @Test
    fun `seeders with different store ids fail the recreation`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(storeId = "3/4"))
        val (first, second) = listOf(creator, other).sorted()

        reconciler.reconcile()

        assertThat(graph.named).isEmpty()
        assertThat(graph.recorded)
            .containsExactly(
                Triple(
                    new.id,
                    "Seeders $first and $second have incompatible storeIds. All seeders must " +
                        "have compatible storeIds.",
                    true,
                )
            )
    }

    @Test
    fun `a seeder whose transaction the latest store's log no longer holds passes`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(appendIndex = 12, lastTransaction = 9, checksum = 222))
        seeds.checksums[other] = mapOf(10L to null)

        reconciler.reconcile()

        assertThat(graph.named).containsExactly(new.id to other)
    }

    @Test
    fun `naming the creator waits while the seeder with the latest store cannot tell checksums`() {
        val new = recreatedFromOthers()
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(appendIndex = 12, lastTransaction = 9, checksum = 222))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).isEmpty()
        assertThat(graph.recorded.single().second)
            .startsWith("Waiting for seeder $other, whose store is the latest")
        assertThat(graph.recorded.single().third).isFalse()
    }

    @Test
    fun `without a store on any seeder the recreation fails, unless it has a seed URI`() {
        val new = recreatedFromOthers()
        new.reported(creator, null)
        new.reported(other, null)

        reconciler.reconcile()

        assertThat(graph.named).isEmpty()
        assertThat(graph.recorded)
            .containsExactly(
                Triple(
                    new.id,
                    "No store found on any of the seeders ${listOf(creator, other).sorted().joinToString()}.",
                    true,
                )
            )

        graph.recorded.clear()
        val seeded = recreatedFromOthers(seed = UriSeed("file:///seeds/films.backup"))
        seeded.reported(creator, null)
        seeded.reported(other, null)

        reconciler.reconcile()

        // As for a new database: this member, as it is placed on it.
        assertThat(graph.named).containsExactly(seeded.id to self)
        assertThat(graph.recorded).isEmpty()

        // A server: seed is read by the server it names, which creates it.
        graph.named.clear()
        val onServer = recreatedFromOthers(seed = UriSeed("server://${other.uuid}/films.backup"))
        onServer.reported(creator, null)
        onServer.reported(other, null)

        reconciler.reconcile()

        assertThat(graph.named).containsExactly(onServer.id to other)
    }

    @Test
    fun `the system leader names the server that reads a server seed the creator`() {
        graph.leading = true
        graph.databases =
            listOf(desired(films, seed = UriSeed("server://${creator.uuid}/films.backup")))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).containsExactly(films to creator)
    }

    @Test
    fun `the seeder with the latest store hosts a primary before it creates the database`() {
        val new = recreatedFromOthers(secondaries = setOf(other))
        new.reported(creator, KEPT)
        new.reported(other, KEPT.copy(appendIndex = 12, lastTransaction = 9))
        seeds.checksums[other] = mapOf(10L to KEPT.checksum)

        reconciler.reconcile()

        assertThat(graph.promoted).containsExactly(new.id to other)
        assertThat(graph.named).isEmpty()

        graph.promoted.clear()
        graph.promoteRefusal = "server other cannot host a primary of 'films'"
        reconciler.reconcile()

        assertThat(graph.recorded)
            .containsExactly(
                Triple(
                    new.id,
                    "Seeder $other holds the latest store, but cannot host a primary: server " +
                        "other cannot host a primary of 'films'.",
                    true,
                )
            )
    }

    @Test
    fun `a seed kept for a recreation that is recreated again seeds the new one`() {
        local.add(films, started = true)
        val first = recreated(films, seeders = setOf(self, creator))
        graph.databases = listOf(first)
        reconciler.reconcile()

        val second = recreated(first.id, seeders = setOf(self))
        graph.databases = listOf(second)
        reconciler.reconcile()

        assertThat(local.keptSeeds)
            .containsExactly(entry(films.name(), second.id.databaseId().uuid()))
        assertThat(local.deletedStores).isEmpty()
        assertThat(reconciler.seedReports)
            .containsExactly(entry(second.id.databaseId().uuid(), KEPT))
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

    @Test
    fun `a secondary copies the database from the members that hold it, marks it, and starts it`() {
        graph.databases =
            listOf(
                desired(films, creator = creator, hosts = setOf(creator), secondaries = setOf(self))
            )

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.copies).containsExactly(creatorAddress to films)
        assertThat(local.marks).containsExactly(films.databaseId().uuid())
        assertThat(local.registered[films])
            .isEqualTo(LocalDatabase(true, false, CopyMode.SECONDARY))
        assertThat(local.groups).describedAs("it runs no group").isEmpty()
        assertThat(local.seeds).containsEntry(films, null)
    }

    @Test
    fun `a system leader that hosts a new database as a secondary names its creator first`() {
        graph.leading = true
        graph.databases = listOf(desired(films, hosts = setOf(creator), secondaries = setOf(self)))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(graph.named).containsExactly(films to creator)
        assertThat(local.copyAttempts).describedAs("nothing to copy yet").isEmpty()
    }

    @Test
    fun `a primary that moved off this member while it was down starts, and goes once it left`() {
        local.stored[films.name()] = true
        local.groups += films.databaseId().uuid()
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator, other)))

        reconciler.reconcile()

        assertThat(local.registered[films])
            .describedAs("its group counts it in until the system leader removes it")
            .isEqualTo(LocalDatabase(true, false, CopyMode.PRIMARY))
        local.notVoting += films
        local.notMember += films
        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.dropped).containsExactly(films)
    }

    @Test
    fun `a secondary that moved off this member while it was down goes once its replacement runs`() {
        local.stored[films.name()] = true
        local.marks += films.databaseId().uuid()
        graph.databases =
            listOf(
                desired(
                    films,
                    creator = creator,
                    hosts = setOf(creator),
                    secondaries = setOf(other),
                )
            )
        runs = { member, _ -> member == other }

        reconciler.reconcile()
        reconciler.reconcile()

        assertThat(local.dropped).containsExactly(films)
        assertThat(local.marks).isEmpty()
    }

    @Test
    fun `a secondary goes as soon as it is no longer placed here`() {
        local.addSecondary(films)
        graph.databases =
            listOf(
                desired(films, creator = creator, hosts = setOf(creator), secondaries = emptySet())
            )

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.dropped).containsExactly(films)
        assertThat(local.marks).isEmpty()
    }

    @Test
    fun `a secondary placed elsewhere goes once its replacement runs`() {
        local.addSecondary(films)
        graph.databases =
            listOf(
                desired(
                    films,
                    creator = creator,
                    hosts = setOf(creator),
                    secondaries = setOf(other),
                )
            )
        runs = { _, _ -> false }

        assertThat(reconciler.reconcile()).describedAs("waits for its replacement").isTrue()
        assertThat(local.dropped).isEmpty()

        runs = { member, _ -> member == other }
        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.dropped).containsExactly(films)
    }

    @Test
    fun `a secondary of an offline database placed elsewhere goes at once`() {
        local.addSecondary(films)
        graph.databases =
            listOf(
                desired(
                    films,
                    online = false,
                    creator = creator,
                    hosts = setOf(creator),
                    secondaries = setOf(other),
                )
            )
        runs = { _, _ -> false }

        reconciler.reconcile()

        assertThat(local.dropped).containsExactly(films)
    }

    @Test
    fun `a primary placed here as a secondary leaves its group first, then turns secondary`() {
        local.add(films, started = true)
        graph.databases =
            listOf(
                desired(films, creator = creator, hosts = setOf(creator), secondaries = setOf(self))
            )

        assertThat(reconciler.reconcile()).describedAs("waits to leave its group").isTrue()
        assertThat(local.switched).isEmpty()

        local.notVoting += films
        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.switched).containsExactly(films to CopyMode.SECONDARY)
        assertThat(local.registered[films])
            .isEqualTo(LocalDatabase(true, false, CopyMode.SECONDARY))
        assertThat(local.marks).containsExactly(films.databaseId().uuid())
        assertThat(local.groups).isEmpty()
        assertThat(local.dropped).isEmpty()
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `a secondary placed here as a primary turns primary and joins the group`() {
        local.addSecondary(films)
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator, self)))

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.switched).containsExactly(films to CopyMode.PRIMARY)
        assertThat(local.registered[films]).isEqualTo(LocalDatabase(true, false, CopyMode.PRIMARY))
        assertThat(local.groups).containsExactly(films.databaseId().uuid())
        assertThat(local.marks).isEmpty()
        assertThat(local.dropped).isEmpty()
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `a secondary placed here as a primary tries again while its group cannot tell`() {
        local.addSecondary(films)
        local.toPrimary = ModeSwitch.LATER
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator, self)))

        assertThat(reconciler.reconcile()).isTrue()

        assertThat(local.registered[films])
            .isEqualTo(LocalDatabase(true, false, CopyMode.SECONDARY))
        assertThat(local.dropped).isEmpty()
    }

    @Test
    fun `a secondary's store placed here as a primary while this member was down turns primary`() {
        local.stored[films.name()] = true
        local.marks += films.databaseId().uuid()
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator, self)))

        reconciler.reconcile()
        reconciler.reconcile()

        assertThat(local.switched).containsExactly(films to CopyMode.PRIMARY)
        assertThat(local.registered[films]).isEqualTo(LocalDatabase(true, false, CopyMode.PRIMARY))
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `a secondary placed here as a primary whose group moved on is replaced by a copy`() {
        local.addSecondary(films)
        local.toPrimary = ModeSwitch.NEEDS_COPY
        graph.databases = listOf(desired(films, creator = creator, hosts = setOf(creator, self)))

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(local.dropped).containsExactly(films)
        assertThat(local.marks).isEmpty()

        assertThat(reconciler.reconcile()).isFalse()
        assertThat(local.registered[films]).isEqualTo(LocalDatabase(true, false, CopyMode.PRIMARY))
        assertThat(local.groups).containsExactly(films.databaseId().uuid())
    }

    @Test
    fun `a primary's store placed here as a secondary while this member was down runs its group first`() {
        local.stored[films.name()] = true
        local.groups += films.databaseId().uuid()
        graph.databases =
            listOf(
                desired(films, creator = creator, hosts = setOf(creator), secondaries = setOf(self))
            )

        reconciler.reconcile()

        assertThat(local.registered[films]).isEqualTo(LocalDatabase(true, false, CopyMode.PRIMARY))
        assertThat(local.copies).isEmpty()

        local.notVoting += films
        reconciler.reconcile()

        assertThat(local.registered[films])
            .isEqualTo(LocalDatabase(true, false, CopyMode.SECONDARY))
        assertThat(local.copies).isEmpty()
    }

    @Test
    fun `a secondary's mark whose database the system graph lacks is forgotten`() {
        val orphan = UUID.randomUUID()
        local.marks += orphan
        local.addSecondary(films)
        graph.databases =
            listOf(
                desired(films, creator = creator, hosts = setOf(creator), secondaries = setOf(self))
            )

        assertThat(reconciler.reconcile()).isFalse()

        assertThat(local.marks).containsExactly(films.databaseId().uuid())
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

        /** The seed append indexes named with the creators. */
        val seedAppendIndexes = mutableListOf<Long?>()

        override fun nameCreator(
            database: NamedDatabaseId,
            creator: MemberId,
            initialMembers: List<Member>,
            seedAppendIndex: Long?,
        ) {
            named += database to creator
            this.initialMembers += initialMembers
            seedAppendIndexes += seedAppendIndex
        }

        /** What was recorded of recreations: the database, the message, and whether it failed. */
        val recorded = mutableListOf<Triple<NamedDatabaseId, String, Boolean>>()
        val promoted = mutableListOf<Pair<NamedDatabaseId, MemberId>>()
        /** Why promoting is refused, if it is. */
        var promoteRefusal: String? = null

        override fun recordRecreation(database: NamedDatabaseId, message: String, failed: Boolean) {
            recorded += Triple(database, message, failed)
        }

        override fun promote(database: NamedDatabaseId, server: MemberId) {
            promoteRefusal?.let { throw AllocationException(it) }
            promoted += database to server
        }
    }

    /**
     * What the seeders report: this member's own reports, the others' as set, and the checksums
     * their seeds tell.
     */
    private inner class FakeSeeds : RecreationSeeds {
        val reports = mutableMapOf<MemberId, Map<UUID, Seed?>>()
        val checksums = mutableMapOf<MemberId, Map<Long, Int?>>()
        val asked = mutableListOf<Pair<MemberId, List<Long>>>()
        var refreshes = 0

        override fun reported(member: MemberId): Map<UUID, Seed?>? =
            if (member == self) reconciler.seedReports else reports[member]

        override fun refresh() {
            refreshes++
        }

        override fun checksums(
            member: MemberId,
            database: NamedDatabaseId,
            appendIndexes: List<Long>,
        ): Map<Long, Int?> {
            asked += member to appendIndexes
            val known = checksums[member] ?: throw IOException("$member does not answer")
            return appendIndexes.associateWith { known[it] }
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

        /** The databases whose stores are secondaries', by id. */
        val marks = mutableSetOf<UUID>()

        /** The databases whose copies here are in quarantine, by id. */
        val quarantines = mutableSetOf<UUID>()

        /** Puts the copy of [id], registered, in quarantine, stopping it. */
        fun quarantine(id: NamedDatabaseId) {
            quarantines += id.databaseId().uuid()
            registered[id] = registered.getValue(id).copy(started = false, quarantined = true)
        }

        override fun quarantined(): Set<UUID> = quarantines.toSet()

        override fun forgetQuarantine(id: UUID) {
            quarantines -= id
        }

        fun add(id: NamedDatabaseId, started: Boolean) {
            registered[id] = LocalDatabase(started, false)
            stored[id.name()] = true
            groups += id.databaseId().uuid()
        }

        /** A running secondary of [id]. */
        fun addSecondary(id: NamedDatabaseId) {
            registered[id] = LocalDatabase(true, false, CopyMode.SECONDARY)
            stored[id.name()] = true
            marks += id.databaseId().uuid()
        }

        override fun secondaries(): Set<UUID> = marks.toSet()

        override fun unmark(id: UUID) {
            marks -= id
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

        override fun rejoinFromStore(id: NamedDatabaseId): ModeSwitch =
            throw UnsupportedOperationException("not reconciled")

        override fun create(id: NamedDatabaseId, mode: CopyMode) {
            check(id !in registered) { "$id is registered" }
            registered[id] = LocalDatabase(started = false, failed = false, mode = mode)
        }

        /** The seeds each database was started with. */
        val seeds = mutableMapOf<NamedDatabaseId, StoreSeed?>()

        /** Whether starts fail, as the start of a broken store does. */
        var failStarts = false

        // As DatabaseManager, a start of a failed copy tries anew.
        override fun start(id: NamedDatabaseId, seed: StoreSeed?) {
            val state = registered.getValue(id)
            check(!state.started)
            seeds[id] = seed
            if (failStarts) {
                registered[id] = state.copy(failed = true)
                return
            }
            registered[id] = state.copy(started = true, failed = false)
            stored[id.name()] = true
            if (state.mode == CopyMode.PRIMARY) groups += id.databaseId().uuid()
        }

        override fun stop(id: NamedDatabaseId) {
            registered[id] = registered.getValue(id).copy(started = false, failed = false)
        }

        override fun leave(id: NamedDatabaseId) {
            registered[id] = registered.getValue(id).copy(started = false, failed = false)
            left += id
        }

        override fun drop(id: NamedDatabaseId) {
            registered.remove(id)
            stored.remove(id.name())
            groups.remove(id.databaseId().uuid())
            marks.remove(id.databaseId().uuid())
            quarantines.remove(id.databaseId().uuid())
            dropped += id
        }

        /** The kept seeds, by name, with the ids they seed. */
        val keptSeeds = mutableMapOf<String, UUID>()

        override fun seeds(): Map<String, UUID> = keptSeeds.toMap()

        override fun keepAsSeed(id: NamedDatabaseId, recreated: UUID) {
            registered.remove(id)
            groups.remove(id.databaseId().uuid())
            marks.remove(id.databaseId().uuid())
            keptSeeds[id.name()] = recreated
        }

        override fun markSeed(name: String, recreated: UUID) {
            check(stored[name] == true) { "$name has no complete store" }
            keptSeeds[name] = recreated
        }

        override fun unmarkSeed(name: String) {
            keptSeeds.remove(name)
        }

        override fun seedSecondary(id: NamedDatabaseId) {
            check(keptSeeds[id.name()] == id.databaseId().uuid()) { "$id has no seed here" }
            marks += id.databaseId().uuid()
            keptSeeds.remove(id.name())
        }

        /** What the seeds kept hold, by name; [KEPT] if not set. */
        val seedStates = mutableMapOf<String, Seed?>()

        override fun seedState(name: String): Seed? {
            check(name in keptSeeds) { "$name is no seed" }
            return if (name in seedStates) seedStates[name] else KEPT
        }

        override fun deleteStore(name: String) {
            check(registered.keys.none { it.name() == name }) { "$name is registered" }
            stored.remove(name)
            keptSeeds.remove(name)
            deletedStores += name
        }

        override fun removeGroup(id: UUID) {
            groups.remove(id)
        }

        /** What a switch to a primary answers; a switch to a secondary always happens. */
        var toPrimary = ModeSwitch.SWITCHED
        val switched = mutableListOf<Pair<NamedDatabaseId, CopyMode>>()

        /** The append index of the last transaction of each secondary, if set. */
        val pulled = mutableMapOf<NamedDatabaseId, Long>()

        override fun switch(id: NamedDatabaseId, mode: CopyMode, pulledBeyond: Long?): ModeSwitch {
            val state = registered.getValue(id)
            check(state.mode != mode) { "$id is a ${state.mode} already" }
            if (mode == CopyMode.PRIMARY && toPrimary != ModeSwitch.SWITCHED) return toPrimary
            if (mode == CopyMode.PRIMARY && pulledBeyond != null) {
                if (pulled.getValue(id) <= pulledBeyond) return ModeSwitch.LATER
            }
            switched += id to mode
            registered[id] = LocalDatabase(started = false, failed = false, mode = mode)
            val uuid = id.databaseId().uuid()
            when (mode) {
                CopyMode.SECONDARY -> {
                    groups -= uuid
                    marks += uuid
                }
                CopyMode.PRIMARY -> marks -= uuid
            }
            return ModeSwitch.SWITCHED
        }

        override fun copy(source: HostPort, id: NamedDatabaseId, mode: CopyMode) {
            copyAttempts += source
            if (source in unreachable) throw IOException("$source is unreachable")
            check(id.name() !in stored || id.name() in keptSeeds) { "${id.name()} has files" }
            stored[id.name()] = true
            copies += source to id
            if (mode == CopyMode.SECONDARY) marks += id.databaseId().uuid()
        }
    }

    private companion object {
        const val DEFAULT = "neo4j"

        /** What a kept seed holds unless a test says otherwise. */
        val KEPT = Seed(appendIndex = 10, lastTransaction = 7, checksum = 111, storeId = "1/2")
    }
}
