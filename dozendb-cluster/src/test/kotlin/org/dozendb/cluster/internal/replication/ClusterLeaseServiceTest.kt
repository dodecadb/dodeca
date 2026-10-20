package org.dozendb.cluster.internal.replication

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.api.LeaseException
import org.neo4j.kernel.impl.api.LeaseService

class ClusterLeaseServiceTest {
    private var bootstrapping = false
    private val leases = ClusterLeaseService { bootstrapping }

    @Test
    fun `no lease is granted while the member is not writable`() {
        val client = leases.newClient()

        assertThatThrownBy { client.ensureValid() }
            .isInstanceOfSatisfying(LeaseException::class.java) {
                assertThat(it.status()).isEqualTo(Status.Cluster.NotALeader)
            }
        assertThat(client.leaseId()).isEqualTo(LeaseService.NO_LEASE)
    }

    @Test
    fun `a client keeps the lease of the term it got and loses it with the term`() {
        leases.grant(4)
        val client = leases.newClient()
        client.ensureValid()
        client.ensureValid()
        assertThat(client.leaseId()).isEqualTo(4)

        leases.revoke()
        leases.grant(6)

        assertThatThrownBy { client.ensureValid() }.isInstanceOf(LeaseException::class.java)
        assertThat(client.leaseId()).isEqualTo(4)
        val next = leases.newClient().apply { ensureValid() }
        assertThat(next.leaseId()).isEqualTo(6)
    }

    @Test
    fun `while a new store is initialised writes need no lease`() {
        bootstrapping = true
        val client = leases.newClient()

        client.ensureValid()

        assertThat(client.leaseId()).isEqualTo(LeaseService.NO_LEASE)
        bootstrapping = false
        assertThatThrownBy { client.ensureValid() }.isInstanceOf(LeaseException::class.java)
    }
}
