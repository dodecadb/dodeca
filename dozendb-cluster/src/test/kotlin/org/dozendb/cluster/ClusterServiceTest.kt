package org.dozendb.cluster

import java.io.IOException
import java.time.Duration
import java.util.UUID
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.dozendb.consensus.fake.InMemoryConsensus
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class ClusterServiceTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val metadata =
        mapOf(MemberMetadata.NEO4J_VERSION to "5.26.27", MemberMetadata.STORES_READY to "true")
    private val logs = List(3) { consensus.newMember(metadata = metadata) }
    private val addresses = logs.map { it.self.raftAddress }
    private val stores = List(3) { FakeStores() }
    private val memberships = List(3) { InMemoryMembershipStore() }
    private val ownMetadata =
        mapOf(
            MemberMetadata.NEO4J_VERSION to "5.26.27",
            MemberMetadata.DOZENDB_VERSION to "0.9.0",
            MemberMetadata.CLUSTER_ID to CLUSTER,
        )

    /** The format level in the system graph, which every member shares. */
    private class SharedFormatLevel(@Volatile var level: Int = FormatLevels.INITIAL) :
        FormatLevelStore {
        override fun formatLevel(): Int = level

        override fun recordFormatLevel(level: Int) {
            this.level = level
        }
    }

    private val clusterLevel = SharedFormatLevel()

    /** What a member that waits to join this cluster reports. */
    private val joiningMetadata =
        ownMetadata +
            mapOf(
                MemberMetadata.MEMBER_STATE to MemberState.JOINING.name,
                MemberMetadata.STORES_READY to "true",
            )
    private val log = NullLog.getInstance()

    /** The member every other one copies from when no member has databases. */
    private val source = logs.indices.minBy { logs[it].self.id }

    /** [others] are databases besides those the cluster needs to form. */
    private class FakeStores(
        override val existed: Boolean = false,
        var failures: Int = 0,
        private val missing: List<String> = if (existed) emptyList() else ALL_DATABASES,
        private val others: List<String> = emptyList(),
    ) : LocalStores {
        val copies = mutableListOf<HostPort>()
        val copied = mutableListOf<String>()

        override fun missing(): List<String> = missing - copied.toSet()

        override fun present(): List<String> = ALL_DATABASES - missing().toSet() + others

        override fun copyFrom(source: HostPort, databases: List<String>) {
            if (failures > 0) {
                failures--
                throw IOException("connection reset")
            }
            copies += source
            copied += databases
        }

        companion object {
            val ALL_DATABASES = listOf("system", "neo4j")
        }
    }

    private fun service(
        index: Int,
        localStores: LocalStores = stores[index],
        formatLevels: FormatLevels = FormatLevels.SUPPORTED,
    ) =
        ClusterService(
            logs[index],
            system,
            ClusterFormation(logs[index], addresses, "5.26.27", log, sleep = {}),
            localStores,
            memberships[index],
            log,
            metadata = { ownMetadata },
            formatLevels = formatLevels,
            formatLevelStore = clusterLevel,
            upgradeConfirmation = Duration.ofMillis(500),
        )

    @Test
    fun `members form the system group and report the elected leader`() {
        val services = logs.indices.map(::service)
        logs.forEach { it.start() }
        services.forEach { it.start() }

        consensus.electLeader(system, logs[1].self.id)

        val statuses = services.map { it.status().single() }
        assertThat(statuses.map { it.leader }.distinct()).containsExactly(logs[1].self.id)
        assertThat(statuses.map { it.role })
            .containsExactly(Role.Follower, Role.Leader, Role.Follower)
        val view = services[0].members()
        assertThat(view.map { it.member }).containsExactlyInAnyOrderElementsOf(logs.map { it.self })
        assertThat(view.single { it.isLeader }.member).isEqualTo(logs[1].self)
        assertThat(view.single { it.isSelf }.member).isEqualTo(logs[0].self)
    }

    @Test
    fun `members that form the cluster remember the formation`() {
        val services = logs.indices.map(::service)
        logs.forEach { it.start() }
        services.forEach { it.start() }

        for (index in logs.indices) {
            assertThat(memberships[index].record)
                .isEqualTo(
                    MembershipRecord(MemberState.MEMBER, logs.map { it.self }.sortedBy { it.id })
                )
            assertThat(services[index].foundingMembers)
                .isEqualTo(memberships[index].record!!.formation)
        }
    }

    @Test
    fun `the member whose databases a new cluster starts from detaches them`() {
        // Not the first member to form the group, which then names no store source to the others.
        val owner = logs.indices.first { it != source }
        val services =
            logs.indices.map { index ->
                service(
                    index,
                    if (index == owner) FakeStores(existed = true, others = listOf("films"))
                    else FakeStores(others = listOf("leftover")),
                )
            }
        logs.forEach { it.start() }
        services.forEach { it.start() }

        // Every database it holds, as from a restore of another cluster's databases.
        assertThat(memberships[owner].record!!.storesToDetach)
            .containsExactlyInAnyOrder("system", "neo4j", "films")
        for (index in logs.indices - owner) {
            assertThat(memberships[index].record!!.storesToDetach)
                .describedAs("member $index, which copied the databases")
                .isEmpty()
        }
        services[owner].detached("neo4j")
        assertThat(services[owner].mustDetach("neo4j")).isFalse
        assertThat(services[owner].mustDetach("system")).isTrue
        assertThat(memberships[owner].record!!.storesToDetach)
            .containsExactlyInAnyOrder("system", "films")
    }

    @Test
    fun `a member that copied the databases and restarts before joining does not detach them`() {
        val copier = logs.indices.last()
        val others = (logs.indices - copier).map(::service)
        logs.forEach { it.start() }
        others.forEach { it.start() }
        // Its first start recorded the formation; it then copied the databases and stopped before
        // taking part in the system group.
        memberships[copier].write(
            MembershipRecord(MemberState.MEMBER, memberships[0].record!!.formation)
        )
        val restarted = service(copier, FakeStores(existed = true))

        restarted.start()

        assertThat(restarted.mustDetach("neo4j")).isFalse
        assertThat(memberships[copier].record!!.storesToDetach).isEmpty()
    }

    /** Forms the cluster of the three initial members, with the first one leading. */
    private fun formCluster(): List<ClusterService> {
        val services = logs.indices.map(::service)
        logs.forEach { it.start() }
        services.forEach { it.start() }
        consensus.electLeader(system, logs[0].self.id)
        return services
    }

    @Test
    fun `a member started after the cluster formed joins it once it is added`() {
        val services = formCluster()
        val joiningLog = consensus.newMember(metadata = joiningMetadata)
        val store = InMemoryMembershipStore()
        val stores = FakeStores()
        val joining =
            ClusterService(
                joiningLog,
                system,
                ClusterFormation(joiningLog, addresses, "5.26.27", log, sleep = {}),
                stores,
                store,
                log,
                metadata = { ownMetadata },
            )

        joining.start()

        assertThat(joining.state).isEqualTo(MemberState.JOINING)
        assertThat(store.record).isEqualTo(MembershipRecord(MemberState.JOINING))
        // A Free server: the other databases follow once it is enabled.
        assertThat(stores.copied).containsExactly("system")
        assertThat(joiningLog.status(system).role).isEqualTo(Role.Inactive)
        assertThat(joining.foundingMembers).isNull()

        assertThat(services[1].addMember(joiningLog.self.raftAddress)).isEqualTo(joiningLog.self)

        assertThat(joining.state).isEqualTo(MemberState.MEMBER)
        assertThat(store.record).isEqualTo(MembershipRecord(MemberState.MEMBER))
        assertThat(joiningLog.status(system).role).isEqualTo(Role.Follower)
        assertThat(services[0].members().map { it.member })
            .containsExactlyInAnyOrderElementsOf(logs.map { it.self } + joiningLog.self)
        // Adding it again changes nothing.
        assertThat(services[2].addMember(joiningLog.self.raftAddress)).isEqualTo(joiningLog.self)
    }

    @Test
    fun `members that cannot join this cluster are refused`() {
        val services = formCluster()
        fun candidate(metadata: Map<String, String>, runsSystem: Boolean = true): HostPort {
            val candidate = consensus.newMember(metadata = metadata).apply { start() }
            if (runsSystem) candidate.joinGroup(system, logs.map { it.self })
            return candidate.self.raftAddress
        }
        fun refusal(address: HostPort) = assertThatThrownBy { services[0].addMember(address) }

        refusal(candidate(joiningMetadata + (MemberMetadata.NEO4J_VERSION to "5.26.32")))
            .isInstanceOf(MembershipChangeException::class.java)
            .hasMessageContaining("runs Neo4j 5.26.32")
        refusal(candidate(joiningMetadata + (MemberMetadata.MEMBER_STATE to "REMOVED")))
            .hasMessageContaining("was removed from the cluster")
        refusal(candidate(joiningMetadata + (MemberMetadata.MEMBER_STATE to "MEMBER")))
            .hasMessageContaining("is a member of another cluster")
        refusal(candidate(joiningMetadata + (MemberMetadata.CLUSTER_ID to "elsewhere")))
            .hasMessageContaining("waits to join another cluster")
        refusal(candidate(joiningMetadata - MemberMetadata.CLUSTER_ID))
            .hasMessageContaining("is still starting")
        refusal(candidate(joiningMetadata, runsSystem = false))
            .hasMessageContaining("is still starting")
        refusal(HostPort("nowhere", 7700)).hasMessageContaining("No DozenDB member answers")
        assertThat(services[0].members()).hasSize(3)
    }

    @Test
    fun `a removed member stops taking part`() {
        val services = formCluster()
        var removals = 0
        services[2].onRemoved = { removals++ }

        services[1].removeMember(logs[2].self.id)

        assertThat(services[0].members().map { it.member })
            .containsExactlyInAnyOrder(logs[0].self, logs[1].self)
        assertThat(services[2].state).isEqualTo(MemberState.REMOVED)
        assertThat(memberships[2].record?.state).isEqualTo(MemberState.REMOVED)
        assertThat(removals).isEqualTo(1)
        assertThatThrownBy { services[2].addMember(logs[0].self.raftAddress) }
            .hasMessageContaining("not one of the cluster's members")
    }

    @Test
    fun `a member that briefly sees a configuration without itself stays a member`() {
        val services = formCluster()
        var removals = 0
        services[2].onRemoved = { removals++ }

        logs[2].showPassingConfiguration(system, listOf(logs[0].self, logs[1].self))

        assertThat(services[2].state).isEqualTo(MemberState.MEMBER)
        assertThat(memberships[2].record?.state).isEqualTo(MemberState.MEMBER)
        assertThat(removals).isZero()
    }

    @Test
    fun `members that are not in the cluster, and the last member, cannot be removed`() {
        val services = formCluster()

        assertThatThrownBy { services[0].removeMember(MemberId(UUID.randomUUID())) }
            .isInstanceOf(MembershipChangeException::class.java)
            .hasMessageContaining("is not a member of the cluster")
        services[0].removeMember(logs[2].self.id)
        services[0].removeMember(logs[1].self.id)
        assertThatThrownBy { services[0].removeMember(logs[0].self.id) }
            .hasMessageContaining("is the only member of the cluster")
    }

    @Test
    fun `only members add members`() {
        formCluster()
        val joiningLog = consensus.newMember(metadata = joiningMetadata)
        val joining =
            ClusterService(
                joiningLog,
                system,
                ClusterFormation(joiningLog, addresses, "5.26.27", log, sleep = {}),
                FakeStores(),
                InMemoryMembershipStore(),
                log,
                metadata = { ownMetadata },
            )
        joining.start()

        assertThatThrownBy { joining.addMember(logs[0].self.raftAddress) }
            .isInstanceOf(MembershipChangeException::class.java)
            .hasMessageContaining("not one of the cluster's members")
    }

    @Test
    fun `members without databases copy them from the store source`() {
        val version = mapOf(MemberMetadata.NEO4J_VERSION to "5.26.27")
        logs.forEach {
            it.metadata = version
            it.start()
        }
        val services =
            logs.indices.map { index ->
                ClusterService(
                    logs[index],
                    system,
                    ClusterFormation(
                        logs[index],
                        addresses,
                        "5.26.27",
                        log,
                        sleep = { Thread.sleep(1) },
                    ),
                    stores[index],
                    memberships[index],
                    log,
                )
            }

        // As in a real cluster, members start concurrently and serve their databases once started.
        val threads =
            logs.indices.map { index ->
                thread {
                    services[index].start()
                    logs[index].metadata = version + (MemberMetadata.STORES_READY to "true")
                }
            }
        threads.forEach { it.join(10_000) }

        assertThat(threads).noneMatch { it.isAlive }
        assertThat(stores[source].copies).describedAs("the source creates them").isEmpty()
        // A member that finds the cluster already formed copies from any member serving them.
        for (index in logs.indices - source) {
            assertThat(stores[index].copies).hasSize(1).doesNotContain(addresses[index])
        }
        assertThat(stores.flatMap { it.copies })
            .describedAs("at first only the source serves them")
            .contains(addresses[source])
    }

    @Test
    fun `a failed copy is retried`() {
        logs.forEach { it.start() }
        val other = logs.indices.first { it != source }
        val flaky = FakeStores(failures = 2)

        service(other, flaky).start()

        assertThat(flaky.copies).containsExactly(addresses[source])
    }

    @Test
    fun `a member with local Raft state rejoins without probing`() {
        val services = logs.indices.map { service(it, FakeStores(existed = true)) }
        logs.forEach { it.start() }
        services.forEach { it.start() }
        services[2].stop()
        logs.take(2).forEach { it.close() }

        service(2, FakeStores(existed = true)).start()

        assertThat(logs[2].status(system).role).isEqualTo(Role.Follower)
    }

    @Test
    fun `a rejoining member without databases copies them from a member that serves them`() {
        val services = logs.indices.map { service(it, FakeStores(existed = true)) }
        logs.forEach { it.start() }
        services.forEach { it.start() }
        services[2].stop()
        logs[0].close()
        val wiped = FakeStores()

        service(2, wiped).start()

        assertThat(wiped.copies).containsExactly(addresses[1])
        assertThat(wiped.copied).containsExactlyElementsOf(FakeStores.ALL_DATABASES)
    }

    @Test
    fun `a rejoining member copies only the databases it lacks`() {
        val services = logs.indices.map { service(it, FakeStores(existed = true)) }
        logs.forEach { it.start() }
        services.forEach { it.start() }
        services[2].stop()
        val partly = FakeStores(existed = true, missing = listOf("neo4j"))

        service(2, partly).start()

        assertThat(partly.copied).containsExactly("neo4j")
        assertThat(logs[2].status(system).role).isEqualTo(Role.Follower)
    }

    @Test
    fun `a rejoining member refuses members that run another Neo4j version`() {
        val services = logs.indices.map { service(it, FakeStores(existed = true)) }
        logs.forEach { it.start() }
        services.forEach { it.start() }
        services[2].stop()
        logs[0].metadata = metadata + (MemberMetadata.NEO4J_VERSION to "5.26.32")

        assertThatThrownBy { service(2, FakeStores(existed = true)).start() }
            .isInstanceOf(IncompatibleVersionException::class.java)
            .hasMessageContaining("Member ${logs[0].self} runs Neo4j 5.26.32")
            .hasMessageContaining("shut down every member first")
    }

    @Test
    fun `a member does not start with a version that lacks the cluster's format level`() {
        val services = logs.indices.map { service(it, FakeStores(existed = true)) }
        logs.forEach { it.start() }
        services.forEach { it.start() }
        services[2].stop()
        val record = checkNotNull(memberships[2].record)

        memberships[2].record = record.copy(formatLevel = 2)
        assertThatThrownBy { service(2, FakeStores(existed = true)).start() }
            .isInstanceOf(IncompatibleVersionException::class.java)
            .hasMessageContaining("format level 2, as this member last saw it")
            .hasMessageContaining("DozenDB version 0.9.0 supports format levels 1.")
        memberships[2].record = record
        logs[0].metadata = metadata + (MemberMetadata.FORMAT_LEVEL to "2")
        assertThatThrownBy { service(2, FakeStores(existed = true)).start() }
            .isInstanceOf(IncompatibleVersionException::class.java)
            .hasMessageContaining("format level 2, as member ${logs[0].self} reports")

        service(2, FakeStores(existed = true), FormatLevels(1, 2)).start()
        assertThat(logs[2].status(system).role).isEqualTo(Role.Follower)
    }

    @Test
    fun `a member is added only if it supports the cluster's format level`() {
        val services = formCluster()
        clusterLevel.level = 2
        fun candidate(levels: String?): HostPort {
            val reported =
                joiningMetadata + listOfNotNull(levels?.let { MemberMetadata.FORMAT_LEVELS to it })
            val candidate = consensus.newMember(metadata = reported).apply { start() }
            candidate.joinGroup(system, logs.map { it.self })
            return candidate.self.raftAddress
        }

        assertThatThrownBy { services[0].addMember(candidate("1")) }
            .isInstanceOf(MembershipChangeException::class.java)
            .hasMessageContaining("supports format levels 1, but this cluster uses format level 2")
        // A version older than format levels.
        assertThatThrownBy { services[0].addMember(candidate(null)) }
            .hasMessageContaining("supports format levels 1, but this cluster uses format level 2")
        services[0].addMember(candidate("1-2"))
        assertThat(services[0].members()).hasSize(4)
    }

    @Test
    fun `an upgrade finishes on the leader once every member supports the new format level`() {
        val services = logs.indices.map { service(it, formatLevels = FormatLevels(1, 2)) }
        logs.forEach { it.start() }
        services.forEach { it.start() }
        consensus.electLeader(system, logs[0].self.id)
        logs[1].metadata = metadata + (MemberMetadata.FORMAT_LEVELS to "1-2")

        // The last member still runs a version older than format levels.
        assertThatThrownBy { services[0].finishUpgrade() }
            .isInstanceOf(UpgradeException::class.java)
            .hasMessageContaining("member ${logs[2].self} supports format levels 1.")
        assertThat(clusterLevel.level).isEqualTo(1)
        logs[2].metadata = metadata + (MemberMetadata.FORMAT_LEVELS to "1-2")
        assertThatThrownBy { services[1].finishUpgrade() }
            .isInstanceOf(UpgradeException::class.java)
            .hasMessageContaining("not the leader of the system database")

        // Recorded, but the others do not report that they recorded it yet.
        assertThatThrownBy { services[0].finishUpgrade() }
            .isInstanceOf(UpgradeException::class.java)
            .hasMessageContaining("The cluster uses format level 2, but member")
            .hasMessageContaining("member ${logs[1].self}")
            .hasMessageContaining("member ${logs[2].self}")
            .hasMessageContaining("within 500 ms")
        assertThat(clusterLevel.level).isEqualTo(2)
        for (index in 1..2) {
            logs[index].metadata = logs[index].metadata + (MemberMetadata.FORMAT_LEVEL to "2")
        }

        assertThat(services[0].finishUpgrade()).isEqualTo(FormatUpgrade(2, 2))
        services.forEach { it.refreshFormatLevel() }
        assertThat(services.map { it.formatLevel }).containsOnly(2)
        assertThat(memberships.map { it.record?.formatLevel }).containsOnly(2)
    }

    @Test
    fun `an upgrade does not finish while a member does not answer`() {
        val services = formCluster()
        logs[2].close()

        assertThatThrownBy { services[0].finishUpgrade() }
            .isInstanceOf(UpgradeException::class.java)
            .hasMessageContaining("Member ${logs[2].self} did not tell")
        assertThat(clusterLevel.level).isEqualTo(1)
    }

    @Test
    fun `the status covers the replicated databases`() {
        val neo4j = DatabaseRef("neo4j", UUID(0, 2))
        val service =
            ClusterService(
                logs[0],
                system,
                ClusterFormation(logs[0], addresses, "5.26.27", log, sleep = {}),
                FakeStores(existed = true),
                InMemoryMembershipStore(),
                log,
                replicatedDatabases = { listOf(neo4j) },
            )

        assertThat(service.status().map { it.database }).containsExactly(system, neo4j)
    }

    private companion object {
        const val CLUSTER = "8a1c0f3e-0000-4000-8000-000000000001"
    }
}
