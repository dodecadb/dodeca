package dev.dodeca.core

import java.util.stream.Stream
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
 * and lost when the server restarts. Only the settings `server.dynamic.setting.allowlist` names
 * change. `dbms.checkConfigValue` tells whether a value is valid for any setting.
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
        val allowlist = DodecaSettings.dynamicSettingAllowlist
        // A setting that is not dynamic is refused below, saying so.
        if (declared.dynamic() && config.get(allowlist).none { matches(it, setting) }) {
            throw IllegalArgumentException(
                "Setting '$setting' may not be changed while the server runs: " +
                    "${allowlist.name()} does not name it."
            )
        }
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

    @Admin
    @SystemProcedure
    @Description(
        "Checks whether a value is valid for a setting, dynamic or not, as the configuration " +
            "would take it, and changes nothing."
    )
    @Procedure(name = "dbms.checkConfigValue", mode = Mode.DBMS)
    fun checkConfigValue(
        @Name("setting") setting: String,
        @Name("value") value: String,
    ): Stream<ConfigValueCheck> {
        val config = checkNotNull(graph).dependencyResolver.resolveDependency(Config::class.java)
        val declared =
            config.declaredSettings[setting]
                ?: return Stream.of(ConfigValueCheck(false, "Unknown setting: $setting"))
        return try {
            val impl = declared as SettingImpl<Any>
            val parsed = impl.parse(value)
            // As the configuration takes it: a relative path from its directory, for instance.
            val solved =
                impl.dependency()?.let { impl.parser().solveDependency(parsed, config.get(it)) }
                    ?: parsed
            impl.validate(solved, config)
            Stream.of(ConfigValueCheck(true, ""))
        } catch (e: IllegalArgumentException) {
            Stream.of(
                ConfigValueCheck(false, "Bad value '$value' for setting '$setting': ${e.message}")
            )
        }
    }

    /** A row of `dbms.checkConfigValue`. */
    class ConfigValueCheck(@JvmField val valid: Boolean, @JvmField val message: String)

    /** Whether [pattern], a setting name in which `*` stands for any text, names [setting]. */
    private fun matches(pattern: String, setting: String): Boolean =
        Regex(pattern.split("*").joinToString(".*") { Regex.escape(it) }).matches(setting)
}
