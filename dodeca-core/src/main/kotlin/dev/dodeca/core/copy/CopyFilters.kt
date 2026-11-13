package dev.dodeca.core.copy

/**
 * What a store copy leaves out, as the options of `neo4j-admin database copy` give it: nodes by
 * their labels, relationships by their types, labels, and properties, everywhere or of a label or
 * type. Names are matched exactly; a node is left out if [nodesWithLabels] leaves it out, and with
 * it its relationships.
 */
internal data class CopyFilters(
    /** Only the nodes with any of these labels, if not null. */
    val onlyNodesWithLabels: Set<String>? = null,
    /** Not the nodes with any of these labels. */
    val ignoreNodesWithLabels: Set<String> = emptySet(),
    /** Only the relationships of these types, if not null. */
    val onlyRelationshipsWithTypes: Set<String>? = null,
    /** Not the relationships of these types. */
    val ignoreRelationshipsWithTypes: Set<String> = emptySet(),
    /** Labels left off the nodes. */
    val skipLabels: Set<String> = emptySet(),
    /** Properties left off every node and relationship. */
    val skipProperties: Set<String> = emptySet(),
    /** Properties left off the nodes with a label, by label. */
    val skipNodeProperties: Map<String, Set<String>> = emptyMap(),
    /** Properties left off the relationships of a type, by type. */
    val skipRelationshipProperties: Map<String, Set<String>> = emptyMap(),
    /** The only properties kept of the nodes with a label, by label, if not null. */
    val onlyNodeProperties: Map<String, Set<String>>? = null,
    /** The only properties kept of the relationships of a type, by type, if not null. */
    val onlyRelationshipProperties: Map<String, Set<String>>? = null,
) {
    /** Whether a node with [labels] is copied. */
    fun copiesNode(labels: Collection<String>): Boolean =
        (onlyNodesWithLabels == null || labels.any { it in onlyNodesWithLabels }) &&
            labels.none { it in ignoreNodesWithLabels }

    /** The labels a copied node with [labels] keeps. */
    fun keptLabels(labels: List<String>): List<String> =
        if (skipLabels.isEmpty()) labels else labels.filter { it !in skipLabels }

    /** Whether a relationship of [type] is copied, its ends being. */
    fun copiesRelationship(type: String): Boolean =
        (onlyRelationshipsWithTypes == null || type in onlyRelationshipsWithTypes) &&
            type !in ignoreRelationshipsWithTypes

    /**
     * Whether a node with [labels], as it had them in the source, keeps its property [key]: the
     * label-specific filters apply for each of its labels.
     */
    fun keepsNodeProperty(labels: Collection<String>, key: String): Boolean {
        if (key in skipProperties) return false
        if (labels.any { key in skipNodeProperties[it].orEmpty() }) return false
        val only = onlyNodeProperties ?: return true
        // Labels without a list keep all their properties.
        val listed = labels.filter { it in only }
        return listed.isEmpty() || listed.any { key in only.getValue(it) }
    }

    /** Whether a relationship of [type] keeps its property [key]. */
    fun keepsRelationshipProperty(type: String, key: String): Boolean {
        if (key in skipProperties) return false
        if (key in skipRelationshipProperties[type].orEmpty()) return false
        val only = onlyRelationshipProperties?.get(type) ?: return true
        return key in only
    }

    /** Whether nodes or their properties may be left out, which a full copy does not. */
    val filtersNodes: Boolean
        get() = onlyNodesWithLabels != null || ignoreNodesWithLabels.isNotEmpty()

    /** Whether schema on the label [label] is left out of the copy: no copied node has it. */
    fun dropsLabel(label: String): Boolean = label in skipLabels || label in ignoreNodesWithLabels

    /** Whether schema on the relationship type [type] is left out of the copy. */
    fun dropsType(type: String): Boolean = !copiesRelationship(type)

    /** Whether schema on [key] of nodes with [label] is left out: no copied node has it. */
    fun dropsNodeProperty(label: String, key: String): Boolean =
        !keepsNodeProperty(listOf(label), key)

    /** Whether schema on [key] of relationships of [type] is left out. */
    fun dropsRelationshipProperty(type: String, key: String): Boolean =
        !keepsRelationshipProperty(type, key)

    companion object {
        /**
         * The names in [list], separated by commas; a name may be quoted with backticks, as in ``
         * `My,label` ``, a backtick in it doubled.
         */
        fun names(list: String): List<String> = split(list, ',').map(::unquote)

        /**
         * The `<label>.<property>` or `<type>.<property>` pairs in [list], each part of which may
         * be quoted with backticks, as in `` `My.label`.`my.property` ``.
         */
        fun pairs(list: String, option: String): Map<String, Set<String>> =
            split(list, ',')
                .map { pair ->
                    val parts = split(pair, '.')
                    if (parts.size != 2 || parts.any { it.isEmpty() }) {
                        throw IllegalArgumentException(
                            "$option takes <label or type>.<property> pairs, not '$pair'. Quote " +
                                "a name with a dot in it with backticks."
                        )
                    }
                    unquote(parts[0]) to unquote(parts[1])
                }
                .groupBy({ it.first }, { it.second })
                .mapValues { it.value.toSet() }

        /** [text] split at the [separator]s outside backticks, each part trimmed. */
        private fun split(text: String, separator: Char): List<String> {
            val parts = mutableListOf<String>()
            val part = StringBuilder()
            var quoted = false
            var i = 0
            while (i < text.length) {
                val c = text[i]
                when {
                    c == '`' && quoted && i + 1 < text.length && text[i + 1] == '`' -> {
                        part.append("``")
                        i++
                    }
                    c == '`' -> {
                        quoted = !quoted
                        part.append(c)
                    }
                    c == separator && !quoted -> {
                        parts += part.toString().trim()
                        part.clear()
                    }
                    else -> part.append(c)
                }
                i++
            }
            if (quoted) throw IllegalArgumentException("unmatched backtick in '$text'")
            parts += part.toString().trim()
            return parts.filter { it.isNotEmpty() || parts.size == 1 }
        }

        private fun unquote(name: String): String =
            if (name.length >= 2 && name.startsWith('`') && name.endsWith('`')) {
                name.substring(1, name.length - 1).replace("``", "`")
            } else {
                name
            }
    }
}
