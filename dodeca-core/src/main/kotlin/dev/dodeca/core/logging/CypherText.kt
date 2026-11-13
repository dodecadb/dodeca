package dev.dodeca.core.logging

import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

/**
 * Values as Cypher literals, as the query log writes parameters and annotation data: `{name: 'Tom',
 * born: 1956, roles: ['Forrest']}`. Strings are single-quoted, with `\`, `'` and line breaks
 * escaped, so that a value never breaks a log line.
 */
internal object CypherText {
    /** [parameters] as a Cypher map, each value cut to [maxLength] characters followed by `...`. */
    fun parameters(parameters: MapValue, maxLength: Int): String {
        val text = StringBuilder("{")
        for ((index, key) in parameters.keySet().withIndex()) {
            if (index > 0) text.append(", ")
            val formatted = StringBuilder().also { append(parameters.get(key), it) }
            text.append(key(key)).append(": ")
            if (formatted.length > maxLength) {
                text.append(formatted, 0, maxLength).append("...")
            } else {
                text.append(formatted)
            }
        }
        return text.append('}').toString()
    }

    /** [map]'s plain Java values, such as transaction metadata, as a Cypher map. */
    fun map(map: Map<String, Any?>): String =
        StringBuilder().also { append(ValueUtils.asMapValue(map), it) }.toString()

    private fun append(value: AnyValue, text: StringBuilder) {
        when (value) {
            is NoValue -> text.append("null")
            is TextValue -> string(value.stringValue(), text)
            is BooleanValue -> text.append(value.booleanValue())
            is NumberValue -> text.append(value.prettyPrint())
            is ListValue -> {
                text.append('[')
                value.forEachIndexed { index, element ->
                    if (index > 0) text.append(", ")
                    append(element, text)
                }
                text.append(']')
            }
            is MapValue -> {
                text.append('{')
                for ((index, key) in value.keySet().withIndex()) {
                    if (index > 0) text.append(", ")
                    text.append(key(key)).append(": ")
                    append(value.get(key), text)
                }
                text.append('}')
            }
            is VirtualNodeValue -> text.append("(").append(value.id()).append(")")
            is VirtualRelationshipValue -> text.append("[").append(value.id()).append("]")
            is Value -> text.append(value.prettyPrint())
            else -> text.append(value)
        }
    }

    private fun key(key: String): String =
        if (
            key.isNotEmpty() && key.all { it.isLetterOrDigit() || it == '_' } && !key[0].isDigit()
        ) {
            key
        } else {
            "`" + key.replace("`", "``") + "`"
        }

    private fun string(value: String, text: StringBuilder) {
        text.append('\'')
        for (c in value) {
            when (c) {
                '\\' -> text.append("\\\\")
                '\'' -> text.append("\\'")
                '\n' -> text.append("\\n")
                '\r' -> text.append("\\r")
                else -> text.append(c)
            }
        }
        text.append('\'')
    }
}
