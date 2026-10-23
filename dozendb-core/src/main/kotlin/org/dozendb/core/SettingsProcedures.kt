package org.dozendb.core

import org.neo4j.configuration.Config
import org.neo4j.configuration.SettingImpl
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure

/**
 * `dbms.setConfigValue`, Neo4j Enterprise's procedure that changes a dynamic setting while the
 * server runs: for administrators, on this server only (in a cluster, on the member it runs on),
 * and lost when the server restarts.
 */
class SettingsProcedures {
    @JvmField @Context var graph: GraphDatabaseAPI? = null

    @JvmField @Context var securityContext: SecurityContext? = null

    @Admin
    @SystemProcedure
    @Description(
        "Changes a dynamic setting on this server until it restarts; an empty value restores the " +
            "default. In a cluster, only the member the procedure runs on changes."
    )
    @Procedure(name = "dbms.setConfigValue", mode = Mode.DBMS)
    fun setConfigValue(@Name("setting") setting: String, @Name("value") value: String) {
        val config = checkNotNull(graph).dependencyResolver.resolveDependency(Config::class.java)
        val declared =
            config.declaredSettings[setting]
                ?: throw IllegalArgumentException("Unknown setting: $setting")
        val parsed: Any? =
            if (value.isEmpty()) {
                null
            } else {
                try {
                    (declared as SettingImpl<Any>).parse(value)
                } catch (e: IllegalArgumentException) {
                    // Without the cause: Neo4j reports a procedure's root cause.
                    throw IllegalArgumentException(
                        "Bad value '$value' for setting '$setting': ${e.message}"
                    )
                }
            }
        config.setDynamicByUser(
            declared,
            parsed,
            checkNotNull(securityContext).subject().executingUser(),
        )
    }
}
