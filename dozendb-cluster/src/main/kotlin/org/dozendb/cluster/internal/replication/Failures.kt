package org.dozendb.cluster.internal.replication

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

    fun exception(message: String, cause: Throwable? = null): TransactionFailureException {
        val exception =
            TransactionFailureException(
                ErrorGqlStatusObjectImplementation.from(gql).build(),
                status,
                "%s",
                message,
            )
        if (cause != null) exception.initCause(cause)
        return exception
    }
}
