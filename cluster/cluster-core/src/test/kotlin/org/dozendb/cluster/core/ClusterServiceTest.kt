package org.dozendb.cluster.core

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.fake.InMemoryConsensus
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class ClusterServiceTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val version = mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.27")
    private val logs = List(3) { consensus.newMember(metadata = version) }
    private val addresses = logs.map { it.self.raftAddress }
    private val log = NullLog.getInstance()

    private fun service(index: Int) =
        ClusterService(
            logs[index],
            system,
            ClusterFormation(logs[index], addresses, "5.26.27", log, sleep = {}),
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
    fun `a member with local Raft state rejoins without probing`() {
        val services = logs.indices.map(::service)
        logs.forEach { it.start() }
        services.forEach { it.start() }
        services[2].stop()
        logs.take(2).forEach { it.close() }

        service(2).start()

        assertThat(logs[2].status(system).role).isEqualTo(Role.Follower)
    }
}
