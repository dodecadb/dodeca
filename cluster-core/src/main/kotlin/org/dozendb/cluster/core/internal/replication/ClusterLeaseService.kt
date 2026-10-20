package org.dozendb.cluster.core.internal.replication

import org.neo4j.kernel.impl.api.LeaseClient
import org.neo4j.kernel.impl.api.LeaseException
import org.neo4j.kernel.impl.api.LeaseService

/**
 * Leases for writing to a replicated database: the lease is the Raft term in which this member is
 * the writable leader.
 *
 * The kernel asks for a lease when a transaction first writes, and again if its commit fails. A
 * transaction keeps the lease it got; once the member is no longer writable leader of that term the
 * lease is invalid. The state machine applies a transaction only if its lease equals the term of
 * its log entry, which every member decides identically.
 *
 * While [bootstrapping] holds, transactions may write without a lease. That covers only the
 * transaction in which Neo4j initialises a store it has just created, before the Raft group exists.
 */
internal class ClusterLeaseService(private val bootstrapping: () -> Boolean) : LeaseService {
    @Volatile private var current: Int = LeaseService.NO_LEASE

    /** The lease new transactions get, or [LeaseService.NO_LEASE] when not writable. */
    val currentLease: Int
        get() = current

    fun grant(term: Long) {
        // Kernel leases are ints; a term beyond that would take billions of elections.
        current = Math.toIntExact(term)
    }

    fun revoke() {
        current = LeaseService.NO_LEASE
    }

    override fun newClient(): LeaseClient = Client()

    private inner class Client : LeaseClient {
        private var leaseId = LeaseService.NO_LEASE

        override fun leaseId(): Int = leaseId

        override fun ensureValid() {
            val lease = current
            if (leaseId == LeaseService.NO_LEASE) {
                if (lease == LeaseService.NO_LEASE) {
                    if (bootstrapping()) return
                    throw LeaseException.notOnALeader()
                }
                leaseId = lease
            } else if (leaseId != lease) {
                throw LeaseException.localInstanceLostLease()
            }
        }
    }
}
