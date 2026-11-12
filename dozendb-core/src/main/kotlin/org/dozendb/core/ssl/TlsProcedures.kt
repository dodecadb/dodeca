package org.dozendb.core.ssl

import org.dozendb.core.DozenDbSettings
import org.neo4j.configuration.Config
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Procedure
import org.neo4j.ssl.config.SslPolicyProvider

/**
 * `dbms.security.reloadTLS()`, as Neo4j Enterprise documents it: reloads the SSL policies of this
 * server, whose new certificates and configuration connections made afterwards use. Neo4j's Bolt,
 * routing connector and HTTPS take every change; DozenDB's channels between members and for backups
 * take the certificates and trusted authorities, and the rest at the next restart.
 */
class TlsProcedures {
    @JvmField @Context var graph: GraphDatabaseAPI? = null

    @Admin
    @SystemProcedure
    @Description("Trigger the dynamic reloading of all TLS certificates and configuration.")
    @Procedure(name = "dbms.security.reloadTLS", mode = Mode.DBMS)
    fun reloadTLS() {
        val dependencies = checkNotNull(graph).dependencyResolver
        val enabled = DozenDbSettings.tlsReloadEnabled
        check(dependencies.resolveDependency(Config::class.java).get(enabled)) {
            "Reloading TLS is turned off: set ${enabled.name()}=true and restart the server."
        }
        try {
            dependencies.resolveDependency(SslPolicyProvider::class.java).reloadPolicies()
        } catch (e: RuntimeException) {
            // Without the cause: Neo4j reports a procedure's root cause.
            throw IllegalStateException(
                "Unable to reload TLS, the certificates in use stay: ${reason(e)}"
            )
        }
    }

    private fun reason(e: Throwable): String =
        generateSequence(e) { it.cause }.mapNotNull { it.message }.joinToString(": ")
}
