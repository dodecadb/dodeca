package org.dozendb.core.copy

import org.neo4j.common.EntityType
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.token.TokenHolders
import org.neo4j.values.storable.ArrayValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value

/**
 * The Cypher statements that create, on a copy, the indexes and constraints of its source, by their
 * names, as `SHOW INDEXES` and `SHOW CONSTRAINTS` show them: one statement per index that no
 * constraint owns, and per constraint, with the index configuration of its index. Token lookup
 * indexes are left out: the copy builds its own. So is the schema of what [filters] leave out of
 * the copy, which [leftOut] describes.
 *
 * The index providers are left to the database that runs the statements, so that a copy gets the
 * current ones.
 */
internal class SchemaStatements(
    indexes: List<IndexDescriptor>,
    constraints: List<ConstraintDescriptor>,
    private val tokens: TokenHolders,
    private val filters: CopyFilters,
) {
    /** The statements, constraints after indexes, each in the order of its names. */
    val statements: List<String>

    /** The schema left out of the copy, described for its output. */
    val leftOut: List<String>

    init {
        val statements = mutableListOf<String>()
        val leftOut = mutableListOf<String>()
        val owned = indexes.filter { it.isUnique || it.owningConstraintId.isPresent }
        for (index in indexes.sortedBy { it.name }) {
            if (index.indexType == IndexType.LOOKUP || index in owned) continue
            if (dropped(index.schema())) {
                leftOut += "index ${index.name}"
            } else {
                statements += index(index)
            }
        }
        val ownedById = owned.associateBy { it.id }
        for (constraint in constraints.sortedBy { it.name }) {
            when {
                constraint.type() == ConstraintType.RELATIONSHIP_ENDPOINT_LABEL ->
                    leftOut += "constraint ${constraint.name}, a graph type constraint"
                dropped(constraint.schema()) -> leftOut += "constraint ${constraint.name}"
                else -> {
                    val index =
                        if (constraint.isIndexBackedConstraint) {
                            ownedById[constraint.asIndexBackedConstraint().ownedIndexId()]
                        } else {
                            null
                        }
                    statements += constraint(constraint, index)
                }
            }
        }
        this.statements = statements
        this.leftOut = leftOut
    }

    /** Whether no entity of the copy can have what [schema] covers. */
    private fun dropped(schema: SchemaDescriptor): Boolean {
        val names = entityNames(schema)
        val keys = schema.propertyIds.map(::keyName)
        return when (schema.entityType()) {
            EntityType.NODE ->
                names.any { label ->
                    filters.dropsLabel(label) || keys.any { filters.dropsNodeProperty(label, it) }
                }
            EntityType.RELATIONSHIP ->
                names.any { type ->
                    filters.dropsType(type) ||
                        keys.any { filters.dropsRelationshipProperty(type, it) }
                }
        }
    }

    private fun index(index: IndexDescriptor): String {
        val schema = index.schema()
        val keys = schema.propertyIds.map(::keyName)
        val type =
            when (index.indexType) {
                IndexType.RANGE -> "RANGE"
                IndexType.TEXT -> "TEXT"
                IndexType.POINT -> "POINT"
                IndexType.FULLTEXT -> "FULLTEXT"
                IndexType.VECTOR -> "VECTOR"
                else -> throw IllegalArgumentException("unexpected index type ${index.indexType}")
            }
        val pattern = pattern(schema, fulltext = index.indexType == IndexType.FULLTEXT)
        val properties =
            if (index.indexType == IndexType.FULLTEXT) {
                keys.joinToString(", ", "EACH [", "]") { "${variable(schema)}.${quote(it)}" }
            } else {
                keys.joinToString(", ", "(", ")") { "${variable(schema)}.${quote(it)}" }
            }
        return "CREATE $type INDEX ${quote(index.name)} FOR $pattern ON $properties" +
            options(index)
    }

    private fun constraint(constraint: ConstraintDescriptor, index: IndexDescriptor?): String {
        val schema = constraint.schema()
        val keys = schema.propertyIds.map(::keyName)
        val variable = variable(schema)
        val properties = keys.joinToString(", ", "(", ")") { "$variable.${quote(it)}" }
        val node = schema.entityType() == EntityType.NODE
        val requirement =
            when (constraint.type()) {
                ConstraintType.UNIQUE -> "$properties IS UNIQUE"
                ConstraintType.UNIQUE_EXISTS ->
                    "$properties IS ${if (node) "NODE" else "RELATIONSHIP"} KEY"
                ConstraintType.EXISTS -> "$variable.${quote(keys.single())} IS NOT NULL"
                ConstraintType.PROPERTY_TYPE ->
                    "$variable.${quote(keys.single())} IS :: " +
                        constraint.asPropertyTypeConstraint().propertyType().userDescription()
                else -> throw IllegalArgumentException("unexpected constraint ${constraint.type()}")
            }
        return "CREATE CONSTRAINT ${quote(constraint.name)} FOR ${pattern(schema, false)} " +
            "REQUIRE $requirement" +
            (index?.let(::options) ?: "")
    }

    private fun pattern(schema: SchemaDescriptor, fulltext: Boolean): String {
        val names = entityNames(schema).map(::quote)
        val tokens = if (fulltext) names.joinToString("|") else names.single()
        return when (schema.entityType()) {
            EntityType.NODE -> "(n:$tokens)"
            EntityType.RELATIONSHIP -> "()-[r:$tokens]-()"
        }
    }

    private fun variable(schema: SchemaDescriptor) =
        if (schema.entityType() == EntityType.NODE) "n" else "r"

    private fun options(index: IndexDescriptor): String {
        val config = index.indexConfig.asMap()
        if (config.isEmpty()) return ""
        return config.entries.joinToString(", ", " OPTIONS {indexConfig: {", "}}") { (key, value) ->
            "${quote(key)}: ${literal(value)}"
        }
    }

    /** The names of the labels or relationship types of [schema]. */
    private fun entityNames(schema: SchemaDescriptor): List<String> {
        val holder =
            when (schema.entityType()) {
                EntityType.NODE -> tokens.labelTokens()
                EntityType.RELATIONSHIP -> tokens.relationshipTypeTokens()
            }
        return schema.entityTokenIds.map { holder.getTokenById(it).name() }
    }

    private fun keyName(id: Int): String = tokens.propertyKeyTokens().getTokenById(id).name()

    companion object {
        /** [name] as a Cypher identifier, in backticks, which it may itself hold doubled. */
        fun quote(name: String): String = "`" + name.replace("`", "``") + "`"

        /** [value], an index setting, as a Cypher literal. */
        fun literal(value: Value): String =
            when (value) {
                is TextValue ->
                    "'" + value.stringValue().replace("\\", "\\\\").replace("'", "\\'") + "'"
                is BooleanValue -> value.booleanValue().toString()
                is NumberValue -> value.prettyPrint()
                is ArrayValue ->
                    (0 until value.intSize()).joinToString(", ", "[", "]") {
                        literal(value.value(it) as Value)
                    }
                else -> throw IllegalArgumentException("unexpected index setting $value")
            }
    }
}
