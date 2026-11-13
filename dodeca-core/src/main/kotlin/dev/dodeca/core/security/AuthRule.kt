package dev.dodeca.core.security

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

/**
 * An auth rule, as Neo4j Enterprise 2026.03 has them: while [enabled], it gives its [roles] to the
 * users for whom its [condition], a Cypher expression in [version], holds when a transaction
 * starts. Kept in the system graph as an `AuthRule` node with a [GIVES_ROLE] relationship to each
 * role.
 */
class AuthRule(
    val name: String,
    val condition: String,
    val version: CypherVersion,
    val enabled: Boolean,
    val roles: Set<String>,
) {
    override fun toString(): String = "auth rule '$name'"

    companion object {
        val LABEL: Label = SecurityGraphDbmsModel.AUTH_RULE_LABEL
        const val NAME = SecurityGraphDbmsModel.AUTH_RULE_NAME_PROPERTY
        const val CONDITION = SecurityGraphDbmsModel.AUTH_RULE_CONDITION_PROPERTY
        const val ENABLED = SecurityGraphDbmsModel.AUTH_RULE_ENABLED_PROPERTY
        const val VERSION = SecurityGraphDbmsModel.AUTH_RULE_CYPHER_VERSION_PROPERTY

        /**
         * From an auth rule to a role it gives: not `HAS_ROLE`, so that the members of roles stay
         * their users.
         */
        val GIVES_ROLE: RelationshipType = RelationshipType.withName("GIVES_ROLE")

        /** The rule of [node]. */
        fun read(node: Node): AuthRule =
            AuthRule(
                node.getProperty(NAME) as String,
                node.getProperty(CONDITION) as String,
                CypherVersion.fromStoredValue(node.getProperty(VERSION) as String),
                node.getProperty(ENABLED, true) as Boolean,
                node.getRelationships(Direction.OUTGOING, GIVES_ROLE).use { given ->
                    given
                        .map { it.endNode.getProperty(PrivilegeRecords.ROLE_NAME) as String }
                        .toSet()
                },
            )
    }
}
