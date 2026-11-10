package org.dozendb.core.edition

import java.util.stream.Stream
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure

/**
 * `dbms.quarantineDatabase`, Neo4j Enterprise's procedure that puts the copy of a database on the
 * server it runs on in quarantine, or lifts its quarantine (see [DatabaseQuarantine]). Neo4j
 * requires every instance field to be a `@Context` field.
 */
class QuarantineProcedures {
    @JvmField @Context var quarantine: DatabaseQuarantine? = null

    @JvmField @Context var securityContext: SecurityContext? = null

    @JvmField @Context var transaction: Transaction? = null

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.quarantineDatabase", mode = Mode.DBMS)
    @Description(
        "Place a database into quarantine or remove it from quarantine, on the server it runs " +
            "on: a database in quarantine is stopped there, and stays stopped until its " +
            "quarantine is lifted."
    )
    fun quarantineDatabase(
        @Name("databaseName") databaseName: String,
        @Name("setStatus") setStatus: Boolean,
        @Name(value = "reason", defaultValue = "") reason: String,
    ): Stream<QuarantineRow> {
        val user = securityContext?.subject()?.executingUser().orEmpty().ifEmpty { ANONYMOUS }
        val caller =
            (transaction as? InternalTransaction)?.let {
                DatabaseQuarantine.Caller(it.databaseName, it::isOpen)
            }
        val outcome = checkNotNull(quarantine).set(databaseName, setStatus, user, reason, caller)
        return Stream.of(QuarantineRow(outcome.databaseName, outcome.quarantined, outcome.result))
    }

    @JvmRecord
    data class QuarantineRow(
        val databaseName: String,
        val quarantined: Boolean,
        val result: String,
    )

    private companion object {
        /** The user of a call without authentication, as with `dbms.security.auth_enabled` off. */
        const val ANONYMOUS = "anonymous"
    }
}
