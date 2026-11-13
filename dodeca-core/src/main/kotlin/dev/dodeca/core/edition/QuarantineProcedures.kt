package dev.dodeca.core.edition

import java.util.stream.Stream
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.QueryLanguage
import org.neo4j.kernel.api.procedure.QueryLanguageScope
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure

/**
 * Neo4j Enterprise's quarantine procedures: `dbms.unquarantineDatabase`, which lifts the quarantine
 * of a database's copy on any server (see [Unquarantines]), and `dbms.quarantineDatabase`, which
 * puts the copy on the server it runs on in quarantine, or lifts its quarantine (see
 * [DatabaseQuarantine]), deprecated since Enterprise 2025.01 and gone from its Cypher 25. Neo4j
 * requires every instance field to be a `@Context` field.
 */
class QuarantineProcedures {
    @JvmField @Context var quarantine: DatabaseQuarantine? = null

    @JvmField @Context var unquarantines: Unquarantines? = null

    @JvmField @Context var securityContext: SecurityContext? = null

    @JvmField @Context var transaction: Transaction? = null

    @Admin
    @SystemProcedure
    @Procedure(name = "dbms.unquarantineDatabase", mode = Mode.DBMS)
    @Description(
        "Lift quarantine from a database on a given server, named by its name or id. The " +
            "operation keepStateKeepStore, the default, leaves its store and cluster state as " +
            "they are; replaceStateKeepStore joins it as a new member, clearing its cluster " +
            "state but keeping its store; replaceStateReplaceStore joins it as a new member, " +
            "clearing both. A new member joins only if a majority of the database's primaries " +
            "lets it in."
    )
    fun unquarantineDatabase(
        @Name("server") server: String,
        @Name("databaseName") databaseName: String,
        @Name(value = "operation", defaultValue = "keepStateKeepStore") operation: String,
    ) {
        try {
            checkNotNull(unquarantines).lift(server, databaseName, operation, user())
        } catch (e: RuntimeException) {
            throw ProcedureException.quarantineChangeFailed(UNQUARANTINE, e, false)
        }
    }

    @Admin
    @SystemProcedure
    @Procedure(
        name = "dbms.quarantineDatabase",
        mode = Mode.DBMS,
        deprecatedBy = "dbms.unquarantineDatabase",
    )
    @QueryLanguageScope(scope = [QueryLanguage.CYPHER_5])
    @Description(
        "Place a database into quarantine or remove it from quarantine, on the server it runs " +
            "on: a database in quarantine is stopped there, and stays stopped until its " +
            "quarantine is lifted. Deprecated in favor of dbms.unquarantineDatabase, which lifts " +
            "it; Cypher 25 does not have it."
    )
    // Neo4j looks for Java's annotation with deprecatedBy, and warns without it.
    @Suppress("DEPRECATED_JAVA_ANNOTATION")
    @java.lang.Deprecated
    fun quarantineDatabase(
        @Name("databaseName") databaseName: String,
        @Name("setStatus") setStatus: Boolean,
        @Name(value = "reason", defaultValue = "") reason: String,
    ): Stream<QuarantineRow> {
        val user = user()
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

    /** The user who calls. */
    private fun user(): String =
        securityContext?.subject()?.executingUser().orEmpty().ifEmpty { ANONYMOUS }

    private companion object {
        /** The user of a call without authentication, as with `dbms.security.auth_enabled` off. */
        const val ANONYMOUS = "anonymous"

        const val UNQUARANTINE = "dbms.unquarantineDatabase"
    }
}
