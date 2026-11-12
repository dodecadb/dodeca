package org.dozendb.cluster.internal.replication

import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlRuntimeException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.kernel.api.exceptions.Status
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
 * While [local] holds, transactions may write without a lease. That covers only transactions that
 * commit locally before the Raft group starts: Neo4j initialising a store it has just created, and
 * adapting a store this member brought to the cluster it formed. Otherwise a transaction that
 * writes while this member is not the writable leader fails with [refusal].
 */
internal class ClusterLeaseService(
    private val local: () -> Boolean,
    private val refusal: () -> RuntimeException,
) : LeaseService {
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
                    if (local()) return
                    throw refusal()
                }
                leaseId = lease
            } else if (leaseId != lease) {
                throw LeaseException.localInstanceLostLease()
            }
        }
    }
}

/**
 * A write on a member that does not lead the database, with the status and GQL status of Neo4j's
 * own ([LeaseException.notOnALeader]) and a [message] that tells where to write instead.
 */
internal class NotLeaderException(message: String) :
    GqlRuntimeException(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N07).build(),
        message,
    ),
    Status.HasStatus {
    override fun status(): Status = Status.Cluster.NotALeader
}
