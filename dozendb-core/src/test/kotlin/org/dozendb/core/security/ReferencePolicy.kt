package org.dozendb.core.security

import inet.ipaddr.IPAddressString
import java.net.InetAddress
import java.util.UUID
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.string.Globbing
import org.neo4j.values.storable.Value

/**
 * The rules of Neo4j's privileges written as plainly as possible, without Neo4j's
 * `PrivilegeAction.satisfies`, to check [PrivilegePolicy] and what is built on it against: every
 * privilege is broken down into the single actions it covers, from [COVERS], and each question
 * collects the ones that answer it. Only single actions may be asked about.
 */
class ReferencePolicy(privileges: Collection<Privilege>, private val home: UUID?) {
    /** One single action that a privilege grants or denies. */
    internal class Fact(val action: PrivilegeAction, val privilege: Privilege) {
        val access = privilege.access
        val segment = privilege.segment
        val resource = privilege.resource
    }

    private val facts = privileges.flatMap { privilege ->
        COVERS.getValue(privilege.action)
            // T4: a DENY of some properties' reads does not hide the elements.
            .filterNot {
                it == PrivilegeAction.TRAVERSE &&
                    privilege.access == Access.DENIED &&
                    privilege.resource is PrivilegeResource.Property
            }
            .map { Fact(it, privilege) }
    }

    fun admin(
        action: PrivilegeAction,
        database: UUID?,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ): PermissionState {
        require(action in SINGLE) { "$action is not a single action" }
        return answer(
            facts.filter {
                it.action == action && applies(it.privilege.scope, database) && covers(it, segment)
            }
        )
    }

    fun procedure(name: String, admin: Boolean): Execution {
        val execute = executable(PrivilegeAction.EXECUTE, name, procedures = true)
        val boost = executable(PrivilegeAction.EXECUTE_BOOSTED, name, procedures = true)
        val anyAdmin = answer(facts.filter { it.action == PrivilegeAction.EXECUTE_ADMIN })
        val asAdmin =
            when {
                execute == PermissionState.EXPLICIT_DENY ||
                    boost == PermissionState.EXPLICIT_DENY ||
                    anyAdmin == PermissionState.EXPLICIT_DENY -> PermissionState.EXPLICIT_DENY
                anyAdmin == PermissionState.EXPLICIT_GRANT -> PermissionState.EXPLICIT_GRANT
                execute == PermissionState.EXPLICIT_GRANT &&
                    boost == PermissionState.EXPLICIT_GRANT -> PermissionState.EXPLICIT_GRANT
                else -> PermissionState.NOT_GRANTED
            }
        if (!admin) return Execution(execute, boost, asAdmin)
        val runs =
            when {
                execute == PermissionState.EXPLICIT_DENY -> PermissionState.EXPLICIT_DENY
                execute == PermissionState.EXPLICIT_GRANT ||
                    asAdmin == PermissionState.EXPLICIT_GRANT -> PermissionState.EXPLICIT_GRANT
                else -> PermissionState.NOT_GRANTED
            }
        return Execution(runs, asAdmin, asAdmin)
    }

    fun function(name: String): Execution =
        Execution(
            executable(PrivilegeAction.EXECUTE, name, procedures = false),
            executable(PrivilegeAction.EXECUTE_BOOSTED, name, procedures = false),
        )

    fun setting(name: String): PermissionState =
        answer(
            facts.filter { fact ->
                fact.privilege.action == PrivilegeAction.DBMS_ACTIONS ||
                    (fact.action == PrivilegeAction.SHOW_SETTING &&
                        (fact.segment as? PrivilegeSegment.Settings)?.glob?.matches(name) == true)
            }
        )

    fun loadAllData(): PermissionState =
        answer(facts.filter { it.privilege.action == PrivilegeAction.LOAD })

    fun loadFrom(address: InetAddress?): PermissionState =
        if (address == null) {
            PermissionState.NOT_GRANTED
        } else {
            answer(
                facts.filter { fact ->
                    val segment = fact.segment
                    fact.action == PrivilegeAction.LOAD_CIDR &&
                        segment is PrivilegeSegment.Cidr &&
                        IPAddressString(segment.range)
                            .contains(IPAddressString(address.hostAddress))
                }
            )
        }

    fun graph(id: UUID): Graph =
        Graph(facts.filter { it.action in GRAPH && applies(it.privilege.scope, id) })

    class Graph internal constructor(private val facts: List<Fact>) {
        fun traverseNode(labels: Set<String>, properties: (String) -> Value): Boolean =
            allowed(facts.filter { it.action == TRAVERSE && it.findsNode(labels, properties) })

        fun traverseRelationship(type: String): Boolean =
            allowed(facts.filter { it.action == TRAVERSE && it.findsRelationship(type) })

        fun readNodeProperty(
            labels: Set<String>,
            key: String,
            properties: (String) -> Value,
        ): Boolean =
            allowed(
                facts.filter {
                    it.action == READ && it.hasProperty(key) && it.findsNode(labels, properties)
                }
            )

        fun readRelationshipProperty(type: String, key: String): Boolean =
            allowed(
                facts.filter {
                    it.action == READ && it.hasProperty(key) && it.findsRelationship(type)
                }
            )

        fun createNode(labels: Set<String>): Boolean = everyLabel(CREATE, labels) { true }

        fun deleteNode(labels: Set<String>): Boolean = everyLabel(DELETE, labels) { true }

        fun setNodeProperty(labels: Set<String>, key: String): Boolean =
            everyLabel(SET_PROPERTY, labels) { it.hasProperty(key) }

        fun createRelationship(type: String): Boolean =
            allowed(facts.filter { it.action == CREATE && it.findsRelationship(type) })

        fun deleteRelationship(type: String): Boolean =
            allowed(facts.filter { it.action == DELETE && it.findsRelationship(type) })

        fun setRelationshipProperty(type: String, key: String): Boolean =
            allowed(
                facts.filter {
                    it.action == SET_PROPERTY && it.hasProperty(key) && it.findsRelationship(type)
                }
            )

        fun setLabel(label: String): Boolean =
            allowed(facts.filter { it.action == SET_LABEL && it.hasLabel(label) && it.onNodes() })

        fun removeLabel(label: String): Boolean =
            allowed(
                facts.filter { it.action == REMOVE_LABEL && it.hasLabel(label) && it.onNodes() }
            )

        /** No DENY on any label, and a GRANT on every node or on each label. */
        private fun everyLabel(
            action: PrivilegeAction,
            labels: Set<String>,
            about: (Fact) -> Boolean,
        ): Boolean {
            val relevant = facts.filter { it.action == action && about(it) && it.onNodes() }
            val denied = relevant.filter { it.access == Access.DENIED }
            val granted = relevant.filter { it.access == Access.GRANTED }
            if (denied.any { it.onAllNodes() || it.label() in labels }) return false
            if (granted.any { it.onAllNodes() }) return true
            return labels.isNotEmpty() &&
                labels.all { label -> granted.any { it.label() == label } }
        }

        private fun Fact.onNodes(): Boolean =
            segment == PrivilegeSegment.Database || segment is PrivilegeSegment.Nodes

        private fun Fact.onAllNodes(): Boolean =
            segment == PrivilegeSegment.Database || segment == PrivilegeSegment.Nodes(null)

        private fun Fact.label(): String? = (segment as? PrivilegeSegment.Nodes)?.label

        private fun Fact.findsNode(labels: Set<String>, properties: (String) -> Value): Boolean =
            when (val segment = segment) {
                PrivilegeSegment.Database -> true
                is PrivilegeSegment.Nodes -> segment.label == null || segment.label in labels
                is PrivilegeSegment.NodePattern -> {
                    val pattern = segment.pattern
                    val labelled =
                        pattern.labels().isEmpty() ||
                            pattern.labels().intersect(labels).isNotEmpty()
                    labelled && pattern.holdsFor(properties(pattern.property()))
                }
                else -> false
            }

        private fun Fact.findsRelationship(type: String): Boolean =
            segment == PrivilegeSegment.Database ||
                segment == PrivilegeSegment.Relationships(null) ||
                segment == PrivilegeSegment.Relationships(type)

        private fun Fact.hasProperty(key: String): Boolean =
            resource == PrivilegeResource.Graph ||
                resource == PrivilegeResource.AllProperties ||
                resource == PrivilegeResource.Property(key)

        private fun Fact.hasLabel(label: String): Boolean =
            resource == PrivilegeResource.Graph ||
                resource == PrivilegeResource.AllLabels ||
                resource == PrivilegeResource.Label(label)

        private fun PatternSegment.holdsFor(value: Value): Boolean =
            when (this) {
                is PatternSegment.ValuePatternSegment -> operator().test(value, value())
                is PatternSegment.NullPatternSegment -> operator().test(value)
                else -> error("Unknown rule $this")
            }

        private fun allowed(facts: List<Fact>): Boolean = answer(facts).allowsAccess()
    }

    private fun executable(
        action: PrivilegeAction,
        name: String,
        procedures: Boolean,
    ): PermissionState =
        answer(
            facts.filter { fact ->
                // X4: ALL DBMS PRIVILEGES executes and boosts everything.
                fact.privilege.action == PrivilegeAction.DBMS_ACTIONS ||
                    (fact.action == action &&
                        when (val segment = fact.segment) {
                            is PrivilegeSegment.Procedures ->
                                procedures && segment.glob.matches(name)
                            is PrivilegeSegment.Functions ->
                                !procedures && segment.glob.matches(name)
                            else -> false
                        })
            }
        )

    private fun applies(scope: PrivilegeScope, database: UUID?): Boolean =
        when (scope) {
            PrivilegeScope.Dbms,
            PrivilegeScope.AllDatabases -> true
            PrivilegeScope.Home -> database != null && database == home
            is PrivilegeScope.Database -> database == scope.id
        }

    private fun covers(fact: Fact, segment: PrivilegeSegment): Boolean =
        fact.segment == PrivilegeSegment.Database ||
            fact.segment == segment ||
            (fact.segment == PrivilegeSegment.Users(null) && segment is PrivilegeSegment.Users)

    private fun String.matches(name: String): Boolean = Globbing.globPredicate(this).test(name)

    companion object {
        /**
         * The answer of the [facts] that apply: DENIED if one denies, else GRANTED if one grants.
         */
        private fun answer(facts: List<Fact>): PermissionState =
            when {
                facts.any { it.access == Access.DENIED } -> PermissionState.EXPLICIT_DENY
                facts.isNotEmpty() -> PermissionState.EXPLICIT_GRANT
                else -> PermissionState.NOT_GRANTED
            }

        private val TRAVERSE = PrivilegeAction.TRAVERSE
        private val READ = PrivilegeAction.READ
        private val CREATE = PrivilegeAction.CREATE_ELEMENT
        private val DELETE = PrivilegeAction.DELETE_ELEMENT
        private val SET_LABEL = PrivilegeAction.SET_LABEL
        private val REMOVE_LABEL = PrivilegeAction.REMOVE_LABEL
        private val SET_PROPERTY = PrivilegeAction.SET_PROPERTY
        private val GRAPH =
            setOf(TRAVERSE, READ, CREATE, DELETE, SET_LABEL, REMOVE_LABEL, SET_PROPERTY)

        /**
         * The single actions each action covers, from the docs: the management groups of the DBMS
         * and database privilege pages, and the read and write privilege pages. Every action of
         * Neo4j is here, so that one Neo4j adds fails a test until it is placed.
         */
        val COVERS: Map<PrivilegeAction, Set<PrivilegeAction>> = buildMap {
            fun covers(action: PrivilegeAction, vararg covered: PrivilegeAction) {
                put(action, covered.flatMap { getOrElse(it) { setOf(it) } }.toSet())
            }
            val single =
                listOf(
                    PrivilegeAction.ACCESS,
                    TRAVERSE,
                    READ,
                    SET_LABEL,
                    REMOVE_LABEL,
                    CREATE,
                    DELETE,
                    SET_PROPERTY,
                    PrivilegeAction.CREATE_LABEL,
                    PrivilegeAction.CREATE_RELTYPE,
                    PrivilegeAction.CREATE_PROPERTYKEY,
                    PrivilegeAction.CREATE_INDEX,
                    PrivilegeAction.DROP_INDEX,
                    PrivilegeAction.SHOW_INDEX,
                    PrivilegeAction.CREATE_CONSTRAINT,
                    PrivilegeAction.DROP_CONSTRAINT,
                    PrivilegeAction.SHOW_CONSTRAINT,
                    PrivilegeAction.START_DATABASE,
                    PrivilegeAction.STOP_DATABASE,
                    PrivilegeAction.SHOW_TRANSACTION,
                    PrivilegeAction.TERMINATE_TRANSACTION,
                    PrivilegeAction.SHOW_CONNECTION,
                    PrivilegeAction.TERMINATE_CONNECTION,
                    PrivilegeAction.CREATE_DATABASE,
                    PrivilegeAction.DROP_DATABASE,
                    PrivilegeAction.SET_DATABASE_ACCESS,
                    PrivilegeAction.CREATE_COMPOSITE_DATABASE,
                    PrivilegeAction.DROP_COMPOSITE_DATABASE,
                    PrivilegeAction.CREATE_ALIAS,
                    PrivilegeAction.DROP_ALIAS,
                    PrivilegeAction.ALTER_ALIAS,
                    PrivilegeAction.SHOW_ALIAS,
                    PrivilegeAction.SHOW_USER,
                    PrivilegeAction.CREATE_USER,
                    PrivilegeAction.RENAME_USER,
                    PrivilegeAction.SET_USER_STATUS,
                    PrivilegeAction.SET_PASSWORDS,
                    PrivilegeAction.SET_USER_HOME_DATABASE,
                    PrivilegeAction.DROP_USER,
                    PrivilegeAction.IMPERSONATE,
                    PrivilegeAction.SHOW_ROLE,
                    PrivilegeAction.CREATE_ROLE,
                    PrivilegeAction.RENAME_ROLE,
                    PrivilegeAction.DROP_ROLE,
                    PrivilegeAction.ASSIGN_ROLE,
                    PrivilegeAction.REMOVE_ROLE,
                    PrivilegeAction.SHOW_PRIVILEGE,
                    PrivilegeAction.ASSIGN_PRIVILEGE,
                    PrivilegeAction.REMOVE_PRIVILEGE,
                    PrivilegeAction.ASSIGN_IMMUTABLE_PRIVILEGE,
                    PrivilegeAction.REMOVE_IMMUTABLE_PRIVILEGE,
                    PrivilegeAction.EXECUTE,
                    PrivilegeAction.EXECUTE_BOOSTED,
                    PrivilegeAction.EXECUTE_ADMIN,
                    PrivilegeAction.SHOW_SERVER,
                    PrivilegeAction.SHOW_SETTING,
                    PrivilegeAction.LOAD_CIDR,
                    PrivilegeAction.LOAD_URL,
                )
            single.forEach { put(it, setOf(it)) }
            covers(
                PrivilegeAction.TOKEN,
                PrivilegeAction.CREATE_LABEL,
                PrivilegeAction.CREATE_RELTYPE,
                PrivilegeAction.CREATE_PROPERTYKEY,
            )
            covers(
                PrivilegeAction.INDEX,
                PrivilegeAction.CREATE_INDEX,
                PrivilegeAction.DROP_INDEX,
                PrivilegeAction.SHOW_INDEX,
            )
            covers(
                PrivilegeAction.CONSTRAINT,
                PrivilegeAction.CREATE_CONSTRAINT,
                PrivilegeAction.DROP_CONSTRAINT,
                PrivilegeAction.SHOW_CONSTRAINT,
            )
            covers(
                PrivilegeAction.TRANSACTION_MANAGEMENT,
                PrivilegeAction.SHOW_TRANSACTION,
                PrivilegeAction.TERMINATE_TRANSACTION,
                PrivilegeAction.SHOW_CONNECTION,
                PrivilegeAction.TERMINATE_CONNECTION,
            )
            covers(
                PrivilegeAction.COMPOSITE_DATABASE_MANAGEMENT,
                PrivilegeAction.CREATE_COMPOSITE_DATABASE,
                PrivilegeAction.DROP_COMPOSITE_DATABASE,
            )
            covers(PrivilegeAction.ALTER_DATABASE, PrivilegeAction.SET_DATABASE_ACCESS)
            covers(
                PrivilegeAction.DATABASE_MANAGEMENT,
                PrivilegeAction.CREATE_DATABASE,
                PrivilegeAction.DROP_DATABASE,
                PrivilegeAction.ALTER_DATABASE,
                PrivilegeAction.COMPOSITE_DATABASE_MANAGEMENT,
            )
            covers(
                PrivilegeAction.ALIAS_MANAGEMENT,
                PrivilegeAction.CREATE_ALIAS,
                PrivilegeAction.DROP_ALIAS,
                PrivilegeAction.ALTER_ALIAS,
                PrivilegeAction.SHOW_ALIAS,
            )
            covers(PrivilegeAction.SET_AUTH, PrivilegeAction.SET_PASSWORDS)
            covers(
                PrivilegeAction.ALTER_USER,
                PrivilegeAction.SET_USER_STATUS,
                PrivilegeAction.SET_AUTH,
                PrivilegeAction.SET_USER_HOME_DATABASE,
            )
            covers(
                PrivilegeAction.USER_MANAGEMENT,
                PrivilegeAction.SHOW_USER,
                PrivilegeAction.CREATE_USER,
                PrivilegeAction.RENAME_USER,
                PrivilegeAction.DROP_USER,
                PrivilegeAction.ALTER_USER,
            )
            covers(
                PrivilegeAction.ROLE_MANAGEMENT,
                PrivilegeAction.SHOW_ROLE,
                PrivilegeAction.CREATE_ROLE,
                PrivilegeAction.RENAME_ROLE,
                PrivilegeAction.DROP_ROLE,
                PrivilegeAction.ASSIGN_ROLE,
                PrivilegeAction.REMOVE_ROLE,
            )
            covers(
                PrivilegeAction.PRIVILEGE_MANAGEMENT,
                PrivilegeAction.SHOW_PRIVILEGE,
                PrivilegeAction.ASSIGN_PRIVILEGE,
                PrivilegeAction.REMOVE_PRIVILEGE,
            )
            covers(PrivilegeAction.SERVER_MANAGEMENT, PrivilegeAction.SHOW_SERVER)
            covers(
                PrivilegeAction.DBMS_ACTIONS,
                PrivilegeAction.ROLE_MANAGEMENT,
                PrivilegeAction.USER_MANAGEMENT,
                PrivilegeAction.DATABASE_MANAGEMENT,
                PrivilegeAction.ALIAS_MANAGEMENT,
                PrivilegeAction.PRIVILEGE_MANAGEMENT,
                PrivilegeAction.EXECUTE_ADMIN,
                PrivilegeAction.IMPERSONATE,
                PrivilegeAction.SERVER_MANAGEMENT,
            )
            covers(
                PrivilegeAction.ADMIN,
                PrivilegeAction.DBMS_ACTIONS,
                PrivilegeAction.TRANSACTION_MANAGEMENT,
                PrivilegeAction.START_DATABASE,
                PrivilegeAction.STOP_DATABASE,
            )
            covers(
                PrivilegeAction.DATABASE_ACTIONS,
                PrivilegeAction.ACCESS,
                PrivilegeAction.INDEX,
                PrivilegeAction.CONSTRAINT,
                PrivilegeAction.TOKEN,
            )
            covers(PrivilegeAction.MATCH, TRAVERSE, READ)
            covers(PrivilegeAction.WRITE, SET_LABEL, REMOVE_LABEL, CREATE, DELETE, SET_PROPERTY)
            covers(PrivilegeAction.MERGE, TRAVERSE, READ, CREATE, SET_PROPERTY)
            covers(PrivilegeAction.GRAPH_ACTIONS, TRAVERSE, READ, PrivilegeAction.WRITE)
            covers(PrivilegeAction.LOAD, PrivilegeAction.LOAD_CIDR, PrivilegeAction.LOAD_URL)
        }

        /** The actions that cover only themselves. */
        val SINGLE: Set<PrivilegeAction> =
            COVERS.filter { (action, covered) -> covered == setOf(action) }.keys
    }
}
