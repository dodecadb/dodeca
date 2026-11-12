package org.dozendb.core

import org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.logging.InternalLog

/**
 * Settings that Neo4j Enterprise removed in [SINCE], as server groups became server tags.
 * Enterprise treats them as settings it does not know: under strict validation, its default, it
 * refuses to start, and otherwise it ignores them with a warning. DozenDB does the same, but names
 * the setting that replaced each one, from the setting migrators, which see the configuration
 * before Neo4j validates it.
 */
object RemovedSettings {
    /** The Neo4j version that removed the settings. */
    const val SINCE = "2025.01"

    /**
     * Refuses [removed] in [values], naming [replacement]; with [ofGroup], also `removed.<name>`,
     * naming `replacement.<name>`. Without strict validation, it removes them from [values] and
     * warns on [log] instead.
     */
    fun refuse(
        values: MutableMap<String, String>,
        log: InternalLog,
        removed: String,
        replacement: String,
        ofGroup: Boolean = false,
    ) {
        val keys = values.keys.filter { it == removed || (ofGroup && it.startsWith("$removed.")) }
        for (key in keys) {
            val message =
                "Setting '$key' was removed in Neo4j $SINCE; use " +
                    "'$replacement${key.removePrefix(removed)}' instead."
            require(!strict(values)) {
                "$message Cleanup the config or disable '${strict_config_validation.name()}' " +
                    "to continue."
            }
            log.warn("%s It is ignored.", message)
            values.remove(key)
        }
    }

    /**
     * Whether [values] ask for strict validation. A value that is no boolean counts as the default;
     * Neo4j refuses it when it validates the configuration.
     */
    private fun strict(values: Map<String, String>): Boolean =
        values[strict_config_validation.name()]?.let {
            runCatching { BOOL.parse(it.trim()) }.getOrNull()
        } ?: strict_config_validation.defaultValue()
}
