package org.dozendb.cluster.core

import java.io.IOException
import java.util.UUID
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.fake.InMemoryConsensus
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class ClusterServiceTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val metadata =
        mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.27", DozenDbMetadata.STORES_READY to "true")
    private val logs = List(3) { consensus.newMember(metadata = metadata) }
    private val addresses = logs.map { it.self.raftAddress }
    private val stores = List(3) { FakeStores() }
    private val memberships = List(3) { InMemoryMembershipStore() }
    private val log = NullLog.getInstance()

    /** The member every other one copies from when no member has databases. */
    private val source = logs.indices.minBy { logs[it].self.id }

    private class FakeStores(
        override val existed: Boolean = false,
        var failures: Int = 0,
        private val missing: List<String> = if (existed) emptyList() else ALL_DATABASES,
    ) : LocalStores {
        val copies = mutableListOf<HostPort>()
        val copied = mutableListOf<String>()

        override fun missing(): List<String> = missing - copied.toSet()

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

    private fun service(index: Int, localStores: LocalStores = stores[index]) =
        ClusterService(
            logs[index],
            system,
            ClusterFormation(logs[index], addresses, "5.26.27", log, sleep = {}),
            localStores,
            memberships[index],
            log,
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
    fun `members without databases copy them from the store source`() {
        val version = mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.27")
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
                    logs[index].metadata = version + (DozenDbMetadata.STORES_READY to "true")
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
}
