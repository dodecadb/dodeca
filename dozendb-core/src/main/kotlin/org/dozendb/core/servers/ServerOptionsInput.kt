package org.dozendb.core.servers

import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

/**
 * The options of ENABLE SERVER and ALTER SERVER, as Neo4j Enterprise takes them: `modeConstraint`,
 * `allowedDatabases`, `deniedDatabases` and `tags`, named in any case.
 */
@Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for most of these refusals.
object ServerOptionsInput {
    private const val MODE_CONSTRAINT = "modeConstraint"
    private const val ALLOWED = "allowedDatabases"
    private const val DENIED = "deniedDatabases"
    private const val TAGS = "tags"
    private val NAMES = listOf(MODE_CONSTRAINT, ALLOWED, DENIED, TAGS)

    /**
     * The options [given] sets, each one it leaves out taken from [defaults]. [failure] starts
     * every message, such as "Could not alter server 'a'"; [noDefaults] tells why an option left
     * out cannot be taken while [defaults] is null.
     */
    fun parse(
        given: MapValue,
        defaults: ServerOptions?,
        failure: String,
        noDefaults: String = "",
    ): ServerOptions {
        val values = mutableMapOf<String, AnyValue>()
        for (key in given.keySet()) {
            val name =
                NAMES.firstOrNull { it.equals(key, ignoreCase = true) }
                    ?: throw InvalidArgumentException(
                        GqlHelper.getGql42001_22N04(key, "option", NAMES),
                        "$failure: unrecognised option '$key'. Expected " +
                            NAMES.joinToString { "'$it'" } +
                            ".",
                    )
            values[name] = given.get(key)
        }
        if (defaults == null && values.keys != NAMES.toSet()) {
            val missing = NAMES.filter { it !in values }
            throw InvalidArgumentException(
                "$failure: ${missing.joinToString { "'$it'" }} not given$noDefaults."
            )
        }
        val base = defaults ?: ServerOptions()
        val options =
            ServerOptions(
                tags = values[TAGS]?.let { tags(it, failure) } ?: base.tags,
                modeConstraint =
                    values[MODE_CONSTRAINT]?.let { mode(it, failure) } ?: base.modeConstraint,
                allowedDatabases =
                    values[ALLOWED]?.let { strings(it, ALLOWED, failure) } ?: base.allowedDatabases,
                deniedDatabases =
                    values[DENIED]?.let { strings(it, DENIED, failure) } ?: base.deniedDatabases,
            )
        if (options.allowedDatabases.isNotEmpty() && options.deniedDatabases.isNotEmpty()) {
            throw InvalidArgumentException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N85).build(),
                "$failure: '$ALLOWED' and '$DENIED' cannot both be set.",
            )
        }
        return options
    }

    private fun mode(value: AnyValue, failure: String): InstanceModeConstraint {
        val text = (value as? TextValue)?.stringValue()
        return InstanceModeConstraint.entries.firstOrNull {
            it.name.equals(text, ignoreCase = true)
        }
            ?: throw InvalidArgumentException(
                "$failure: '$MODE_CONSTRAINT' is ${show(value)}, not one of " +
                    InstanceModeConstraint.entries.joinToString { "'${it.name}'" } +
                    "."
            )
    }

    private fun strings(value: AnyValue, option: String, failure: String): List<String> {
        val list = value as? ListValue
        val strings = list?.map { (it as? TextValue)?.stringValue() }
        if (strings == null || strings.any { it == null }) {
            throw InvalidArgumentException(
                "$failure: '$option' is ${show(value)}, not a list of strings."
            )
        }
        return strings.filterNotNull()
    }

    /** [value] as Cypher writes it. */
    private fun show(value: AnyValue): String =
        when (value) {
            is TextValue -> "'${value.stringValue()}'"
            is ListValue -> value.joinToString(", ", "[", "]") { show(it) }
            is Value -> value.prettyPrint()
            else -> value.toString()
        }

    private fun tags(value: AnyValue, failure: String): List<String> {
        val tags = strings(value, TAGS, failure)
        tags
            .firstOrNull { it.isEmpty() || ',' in it }
            ?.let {
                throw InvalidArgumentException(
                    "$failure: the tag '$it' is empty or has a comma, which tags cannot have."
                )
            }
        if (tags.toSet().size < tags.size) {
            throw InvalidArgumentException("$failure: the tags $tags have duplicates.")
        }
        return tags
    }
}
