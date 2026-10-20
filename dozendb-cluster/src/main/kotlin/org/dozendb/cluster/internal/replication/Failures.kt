package org.dozendb.cluster.internal.replication

import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException
import org.neo4j.kernel.api.exceptions.Status

/** Why a replicated commit failed, as Neo4j status codes that drivers act on. */
internal enum class CommitFailure(private val gql: GqlStatusInfoCodes, private val status: Status) {
    /** This member cannot write; drivers retry on another member. */
    NOT_LEADER(GqlStatusInfoCodes.STATUS_08N07, Status.Cluster.NotALeader),

    /** The transaction ran under a lease of an earlier term; transient, so drivers retry it. */
    LEASE_EXPIRED(GqlStatusInfoCodes.STATUS_25N08, Status.Transaction.LeaseExpired),

    /** Not retried by drivers: the transaction may have committed, or can never commit. */
    COMMIT_FAILED(GqlStatusInfoCodes.STATUS_2DN01, Status.Transaction.TransactionCommitFailed);

    fun exception(message: String, cause: Throwable? = null): TransactionFailureException =
        ReplicationFailure(
            ErrorGqlStatusObjectImplementation.from(gql).build(),
            status,
            cause,
            message,
        )
}

/**
 * A failed replicated commit. Neo4j's public constructors that take a GQL status set the cause, to
 * null, so that it cannot be set afterwards; the one that takes a cause is protected.
 */
private class ReplicationFailure(
    gql: ErrorGqlStatusObject,
    status: Status,
    cause: Throwable?,
    message: String,
) : TransactionFailureException(gql, status, cause, "%s", message)
