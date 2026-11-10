package org.dozendb.cluster.internal.routing

import java.time.Duration
import java.util.UUID
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.DatabaseRef
import org.dozendb.core.edition.DatabaseState
import org.neo4j.dbms.routing.DatabaseAvailabilityChecker
import org.neo4j.dbms.routing.LocalRoutingTableServiceValidator
import org.neo4j.dbms.routing.RoutingTableServiceHelpers
import org.neo4j.dbms.routing.RoutingTableServiceValidator
import org.neo4j.kernel.database.DatabaseReferenceImpl

/**
 * Whether a database can be routed: as Community checks it where it is hosted here, else whether a
 * member that votes in its group runs it ([DatabaseLocations]), asking the members again, for at
 * most [refreshWait], before refusing it. Community's check alone refuses every database this
 * member does not host. A copy in quarantine here, which stays stopped, counts as none
 * ([quarantined], by database id).
 */
internal class ClusterRoutingValidator(
    private val local: DatabaseAvailabilityChecker,
    private val locations: DatabaseLocations,
    private val directory: MemberDirectory,
    private val refreshWait: Duration,
    private val quarantined: (UUID) -> Boolean = { false },
) : RoutingTableServiceValidator {
    private val community = LocalRoutingTableServiceValidator(local)

    override fun isValidForServerSideRouting(databaseReference: DatabaseReferenceImpl.Internal) =
        check(databaseReference) { community.isValidForServerSideRouting(it) }

    override fun isValidForClientSideRouting(databaseReference: DatabaseReferenceImpl.Internal) =
        check(databaseReference) { community.isValidForClientSideRouting(it) }

    private fun check(
        reference: DatabaseReferenceImpl.Internal,
        here: (DatabaseReferenceImpl.Internal) -> Unit,
    ) {
        val id = reference.databaseId()
        if (local.isPresent(reference) && !quarantined(id.databaseId().uuid())) {
            return here(reference)
        }
        val database = DatabaseRef(id.name(), id.databaseId().uuid())
        if (running(database)) return
        // What the members reported last may be from before it started, as right after CREATE
        // DATABASE ... WAIT returned.
        directory.awaitRefresh(refreshWait)
        if (!running(database)) {
            throw RoutingTableServiceHelpers.databaseNotAvailableException(reference.alias().name())
        }
    }

    /** Whether a member that votes in the group of [database] reports running it. */
    private fun running(database: DatabaseRef): Boolean =
        locations.of(database)?.voters.orEmpty().any { member ->
            directory.databaseStates(member.id).orEmpty().any {
                it.id == database.id && it.status == DatabaseState.Status.ONLINE
            }
        }
}
