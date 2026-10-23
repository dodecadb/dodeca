package org.dozendb.core.security

import java.net.InetAddress
import java.net.URI
import java.util.UUID
import java.util.function.Supplier
import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.eclipse.collections.impl.factory.primitive.IntSets
import org.neo4j.internal.kernel.api.RelTypeSupplier
import org.neo4j.internal.kernel.api.TokenSet
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.LoginContext.IdLookup
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.ReadSecurityPropertyProvider
import org.neo4j.messages.MessageUtil
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.token.api.TokenConstants
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

/**
 * What a user may do in the database [database], from [policy], for Neo4j's kernel, which names
 * labels, relationship types, properties, procedures and functions by the ids [lookup] gives them.
 * Names that do not exist yet are left out, as nothing has them; [stale] tells when they do.
 *
 * Property rules decide with the values of the properties they ask about, which Neo4j reads for the
 * nodes they may apply to; without the values, a DENY rule that may apply denies and a GRANT rule
 * grants nothing.
 */
class DozenDbAccessMode(
    private val policy: PrivilegePolicy,
    private val database: UUID,
    private val lookup: IdLookup,
    private val roles: Set<String>,
) : AccessMode {
    private val labels = Names(lookup::getLabelId)
    private val types = Names(lookup::getRelTypeId)
    private val keys = Names(lookup::getPropertyKeyId)

    private val graph = policy.graphPrivileges(database)

    private val traverseNodes = nodeRule(graph.filter { it.traverses() })
    private val traverseRelationships = relationshipRule(graph.filter { it.traverses() })
    private val readNodes = propertyRules(PrivilegeAction.READ, ::nodeRule)
    private val readRelationships = propertyRules(PrivilegeAction.READ, ::relationshipRule)
    private val createNodes = nodeRule(graph.filter { it.does(PrivilegeAction.CREATE_ELEMENT) })
    private val deleteNodes = nodeRule(graph.filter { it.does(PrivilegeAction.DELETE_ELEMENT) })
    private val createRelationships =
        relationshipRule(graph.filter { it.does(PrivilegeAction.CREATE_ELEMENT) })
    private val deleteRelationships =
        relationshipRule(graph.filter { it.does(PrivilegeAction.DELETE_ELEMENT) })
    private val setNodeProperties = propertyRules(PrivilegeAction.SET_PROPERTY, ::nodeRule)
    private val setRelationshipProperties =
        propertyRules(PrivilegeAction.SET_PROPERTY, ::relationshipRule)
    private val setLabels = labelRule(PrivilegeAction.SET_LABEL)
    private val removeLabels = labelRule(PrivilegeAction.REMOVE_LABEL)

    private val procedures = policy.procedures(lookup::getProcedureIds, lookup.adminProcedureIds)
    private val functions = policy.functions(lookup::getFunctionIds)
    private val aggregatingFunctions = policy.functions(lookup::getAggregatingFunctionIds)

    /**
     * Whether this mode is out of date for a transaction with [current]: procedures were loaded, or
     * a label, relationship type or property it left out now exists.
     */
    fun stale(current: IdLookup): Boolean =
        lookup.isStale ||
            labels.resolveIn(current::getLabelId) ||
            types.resolveIn(current::getRelTypeId) ||
            keys.resolveIn(current::getPropertyKeyId)

    override fun allowsWrites(): Boolean = graph.any { privilege ->
        privilege.access == Access.GRANTED && WRITES.any { privilege.action.satisfies(it) }
    }

    override fun allowsTokenCreates(action: PrivilegeAction): PermissionState =
        policy.admin(action, database)

    override fun allowsSchemaWrites(): Boolean = SCHEMA_WRITES.any {
        policy.admin(it, database).allowsAccess()
    }

    override fun allowsSchemaWrites(action: PrivilegeAction): PermissionState =
        policy.admin(action, database)

    override fun allowsShowIndex(): Boolean =
        policy.admin(PrivilegeAction.SHOW_INDEX, database).allowsAccess()

    override fun allowsShowConstraint(): Boolean =
        policy.admin(PrivilegeAction.SHOW_CONSTRAINT, database).allowsAccess()

    override fun allowsTraverseAllLabels(): Boolean = traverseNodes.everything()

    override fun allowsTraverseAllNodesWithLabel(label: Int): Boolean =
        if (label == TokenConstants.ANY_LABEL) allowsTraverseAllLabels()
        else traverseNodes.everythingWith(label)

    override fun disallowsTraverseLabel(label: Int): Boolean =
        if (label == TokenConstants.ANY_LABEL) traverseNodes.denied.all
        else traverseNodes.denied.reaches(label)

    override fun allowsTraverseNode(vararg labels: Int): Boolean =
        if (labels.size == 1 && labels[0] == TokenConstants.ANY_LABEL) allowsTraverseAllLabels()
        else traverseNodes.allowsAny(labels)

    override fun getTraverseSecurityProperties(labels: IntArray): IntSet =
        traverseNodes.keysFor(labels)

    override fun hasApplicableTraverseAllowPropertyRules(label: Int): Boolean =
        !traverseNodes.denied.reaches(label) &&
            traverseNodes.patterns.granted.any { it.appliesTo(intArrayOf(label)) }

    // Neo4j passes no property provider when no property rule applies.
    override fun allowsTraverseNodeWithPropertyRules(
        propertyProvider: ReadSecurityPropertyProvider?,
        vararg labels: Int,
    ): Boolean =
        if (propertyProvider == null) allowsTraverseNode(*labels)
        else traverseNodes.allowsAny(labels, propertyProvider.securityProperties)

    override fun hasTraversePropertyRules(): Boolean = traverseNodes.patterns.any()

    override fun allowsTraverseAllRelTypes(): Boolean = traverseRelationships.everything()

    override fun allowsTraverseRelType(relType: Int): Boolean =
        if (relType == TokenConstants.ANY_RELATIONSHIP_TYPE) allowsTraverseAllRelTypes()
        else traverseRelationships.allowsAny(intArrayOf(relType))

    override fun disallowsTraverseRelType(relType: Int): Boolean =
        if (relType == TokenConstants.ANY_RELATIONSHIP_TYPE) traverseRelationships.denied.all
        else traverseRelationships.denied.reaches(relType)

    override fun allowsReadPropertyAllLabels(propertyKey: Int): Boolean =
        readNodes.of(propertyKey).everything()

    override fun disallowsReadPropertyForSomeLabel(propertyKey: Int): Boolean =
        readNodes.of(propertyKey).let { it.denied.any() || it.patterns.denied.isNotEmpty() }

    override fun allowsReadNodeProperties(
        labels: Supplier<TokenSet>,
        propertyKeys: IntArray,
        propertyProvider: ReadSecurityPropertyProvider?,
    ): Boolean = propertyKeys.all { allowsReadNodeProperty(labels, it, propertyProvider) }

    override fun allowsReadNodeProperties(
        labels: Supplier<TokenSet>,
        propertyKeys: IntArray,
    ): Boolean {
        val tokens by lazy { labels.get().all() }
        return propertyKeys.all { readNodes.of(it).allowsAny { tokens } }
    }

    override fun allowsReadNodeProperty(
        labels: Supplier<TokenSet>,
        propertyKey: Int,
        propertyProvider: ReadSecurityPropertyProvider?,
    ): Boolean {
        val rule = readNodes.of(propertyKey)
        return if (propertyProvider == null || !rule.patterns.any()) {
            rule.allowsAny { labels.get().all() }
        } else {
            rule.allowsAny(labels.get().all(), propertyProvider.securityProperties)
        }
    }

    override fun allowsReadNodeProperty(labels: Supplier<TokenSet>, propertyKey: Int): Boolean =
        readNodes.of(propertyKey).allowsAny { labels.get().all() }

    override fun allowsReadPropertyAllRelTypes(propertyKey: Int): Boolean =
        readRelationships.of(propertyKey).everything()

    override fun allowsReadRelationshipProperty(
        relType: RelTypeSupplier,
        propertyKey: Int,
    ): Boolean = readRelationships.of(propertyKey).allowsAny { intArrayOf(relType.relType) }

    /** Lists a property key that some element could have read. */
    override fun allowsSeePropertyKeyToken(propertyKey: Int): Boolean =
        listOf(readNodes.of(propertyKey), readRelationships.of(propertyKey)).any {
            (it.granted.any() || it.patterns.granted.isNotEmpty()) && !it.denied.all
        }

    override fun hasPropertyReadRules(): Boolean = readNodes.all().any { it.patterns.any() }

    override fun hasPropertyReadRules(vararg propertyKeys: Int): Boolean = propertyKeys.any {
        readNodes.of(it).patterns.any()
    }

    override fun getReadSecurityProperties(propertyKey: Int): IntSet =
        readNodes.of(propertyKey).patterns.keys()

    override fun getAllReadSecurityProperties(): IntSet =
        IntSets.mutable.empty().also { keys ->
            readNodes.all().forEach { keys.addAll(it.patterns.keys()) }
        }

    /**
     * The properties whose values decide whether those of [selection] can be read: none without
     * property rules, null if the rules are about other properties. A selection may not know its
     * keys, as one of all properties but some: the rules' properties are asked about instead.
     */
    override fun getSecurityPropertySelection(selection: PropertySelection): PropertySelection? {
        if (!hasPropertyReadRules()) return PropertySelection.NO_PROPERTIES
        val keys = readNodes.securityKeys(selection::test)
        return if (keys.isEmpty) null else PropertySelection.selection(*keys.toArray())
    }

    override fun allowsExecuteProcedure(procedureId: Int): PermissionState =
        procedures(procedureId).execute

    override fun allowExecuteAdminProcedures(): PermissionState = policy.executeAdmin()

    /** Whether the user is an admin with the procedure [procedureId]. */
    fun allowExecuteAdminProcedure(procedureId: Int): PermissionState =
        procedures(procedureId).admin

    override fun shouldBoostProcedure(procedureId: Int): PermissionState =
        procedures(procedureId).boost

    override fun allowsExecuteFunction(id: Int): PermissionState =
        functions[id]?.execute ?: PermissionState.NOT_GRANTED

    override fun shouldBoostFunction(id: Int): PermissionState =
        functions[id]?.boost ?: PermissionState.NOT_GRANTED

    override fun allowsExecuteAggregatingFunction(id: Int): PermissionState =
        aggregatingFunctions[id]?.execute ?: PermissionState.NOT_GRANTED

    override fun shouldBoostAggregatingFunction(id: Int): PermissionState =
        aggregatingFunctions[id]?.boost ?: PermissionState.NOT_GRANTED

    override fun allowsShowSetting(setting: String): PermissionState = policy.setting(setting)

    override fun allowsSetLabel(labelId: Int): Boolean = setLabels.allowsAny(intArrayOf(labelId))

    override fun allowsRemoveLabel(labelId: Int): Boolean =
        removeLabels.allowsAny(intArrayOf(labelId))

    /** Creating a node with [labelIds], none if null, as Neo4j asks for a node without labels. */
    override fun allowsCreateNode(labelIds: IntArray?): Boolean =
        createNodes.allowsEvery(labelIds ?: IntArray(0))

    override fun allowsDeleteNode(labelSupplier: Supplier<TokenSet>): Boolean =
        deleteNodes.allowsEvery(labelSupplier.get().all())

    override fun allowsCreateRelationship(relType: Int): Boolean =
        createRelationships.allowsAny(intArrayOf(relType))

    override fun allowsDeleteRelationship(relType: Int): Boolean =
        deleteRelationships.allowsAny(intArrayOf(relType))

    override fun allowsSetProperty(labels: Supplier<TokenSet>, propertyKey: Int): Boolean =
        setNodeProperties.of(propertyKey).allowsEvery(labels.get().all())

    override fun allowsSetProperty(relType: RelTypeSupplier, propertyKey: Int): Boolean =
        setRelationshipProperties.of(propertyKey).allowsAny(intArrayOf(relType.relType))

    override fun allowsLoadAllData(): PermissionState = policy.loadAllData()

    override fun allowsLoadUri(url: URI, inetAddress: InetAddress?): PermissionState =
        policy.loadFrom(inetAddress)

    override fun name(): String = MessageUtil.standardMode(roles)

    override fun roles(): Set<String> = roles

    override fun isOverridden(): Boolean = false

    override fun isCacheable(): Boolean = lookup.isCachableLookup

    override fun toString(): String = "DozenDbAccessMode{${name()}}"

    /**
     * Nodes by their labels, from privileges about nodes or every element, and by the value of a
     * property for property rules.
     */
    private fun nodeRule(privileges: List<Privilege>): Rule =
        rule(privileges) { privilege ->
            when (val segment = privilege.segment) {
                PrivilegeSegment.Database -> ALL
                is PrivilegeSegment.Nodes ->
                    if (segment.label == null) ALL else labels.id(segment.label)
                is PrivilegeSegment.NodePattern -> pattern(segment.pattern)
                else -> null
            }
        }

    /**
     * The rule [pattern] compiled for nodes with the labels that exist. A property that does not
     * exist has no value on any node: the rule then applies by labels alone, or not at all.
     */
    private fun pattern(pattern: PatternSegment): Any? {
        val ids =
            if (pattern.labels().isEmpty()) null
            else
                pattern
                    .labels()
                    .mapNotNull { labels.id(it) }
                    .ifEmpty {
                        return null
                    }
        val holds: (Value) -> Boolean =
            when (pattern) {
                is PatternSegment.ValuePatternSegment -> { value ->
                    pattern.operator().test(value, pattern.value())
                }
                is PatternSegment.NullPatternSegment -> pattern.operator()::test
                else -> return null
            }
        val key = keys.id(pattern.property())
        return when {
            key != null -> Pattern(ids?.let { IntSets.immutable.of(*it.toIntArray()) }, key, holds)
            !holds(Values.NO_VALUE) -> null
            else -> ids ?: ALL
        }
    }

    /** Relationships by their type, from privileges about them or every element. */
    private fun relationshipRule(privileges: List<Privilege>): Rule =
        rule(privileges) { privilege ->
            when (val segment = privilege.segment) {
                PrivilegeSegment.Database -> ALL
                is PrivilegeSegment.Relationships ->
                    if (segment.type == null) ALL else types.id(segment.type)
                else -> null
            }
        }

    /** Labels set or removed, from privileges about them on nodes. */
    private fun labelRule(action: PrivilegeAction): Rule =
        rule(
            graph.filter {
                it.does(action) &&
                    (it.segment == PrivilegeSegment.Database ||
                        it.segment is PrivilegeSegment.Nodes)
            }
        ) { privilege ->
            when (val resource = privilege.resource) {
                PrivilegeResource.Graph,
                PrivilegeResource.AllLabels -> ALL
                is PrivilegeResource.Label -> labels.id(resource.name)
                else -> null
            }
        }

    /**
     * The rule of each property for [action], READ or SET_PROPERTY, from privileges about it or
     * every property, compiled by [rule].
     */
    private fun propertyRules(
        action: PrivilegeAction,
        rule: (List<Privilege>) -> Rule,
    ): PropertyRules {
        val relevant = graph.filter {
            it.action.satisfies(action) && it.resource != PrivilegeResource.Database
        }
        val general = relevant.filter {
            it.resource == PrivilegeResource.Graph || it.resource == PrivilegeResource.AllProperties
        }
        val named =
            relevant
                .mapNotNull { privilege ->
                    val resource = privilege.resource as? PrivilegeResource.Property
                    resource?.let { keys.id(it.name) }?.let { it to privilege }
                }
                .groupBy({ it.first }, { it.second })
        return PropertyRules(rule(general), named.mapValues { (_, own) -> rule(general + own) })
    }

    /**
     * [Rule] of [privileges], each reaching what [reach] says: [ALL], some ids, or nothing (null or
     * names that do not exist).
     */
    private fun rule(privileges: List<Privilege>, reach: (Privilege) -> Any?): Rule {
        val granted = Builder()
        val denied = Builder()
        for (privilege in privileges) {
            val builder = if (privilege.access == Access.DENIED) denied else granted
            when (val reached = reach(privilege)) {
                ALL -> builder.all = true
                is Int -> builder.ids.add(reached)
                is Collection<*> -> reached.forEach { builder.ids.add(it as Int) }
                is Pattern -> builder.patterns += reached
            }
        }
        return Rule(
            granted.build(),
            denied.build(),
            Patterns(granted.patterns.toList(), denied.patterns.toList()),
        )
    }

    private fun Privilege.traverses(): Boolean =
        action.satisfies(PrivilegeAction.TRAVERSE) &&
            (access == Access.GRANTED || resource !is PrivilegeResource.Property)

    private fun Privilege.does(action: PrivilegeAction): Boolean = this.action.satisfies(action)

    private class Builder {
        var all = false
        val ids = IntSets.mutable.empty()
        val patterns = mutableListOf<Pattern>()

        fun build() = Reach(all, ids.toImmutable())
    }

    /** The labels or relationship types some privileges reach: all, or some by id. */
    class Reach(val all: Boolean, val ids: IntSet) {
        fun reaches(id: Int): Boolean = all || ids.contains(id)

        fun reachesAny(ids: IntArray): Boolean = all || ids.any(this.ids::contains)

        fun any(): Boolean = all || ids.notEmpty()
    }

    /**
     * A property rule compiled: it applies to nodes with one of [labels], or to every node for
     * null, whose value of the property [key] [holds].
     */
    class Pattern(
        private val labels: IntSet?,
        val key: Int,
        private val holds: (Value) -> Boolean,
    ) {
        fun appliesTo(ids: IntArray): Boolean = labels == null || ids.any(labels::contains)

        /** Whether it applies to a node with [ids] and the property values [values]. */
        fun holdsFor(ids: IntArray, values: IntObjectMap<Value>): Boolean =
            appliesTo(ids) && holds(values.get(key) ?: Values.NO_VALUE)
    }

    /** The property rules of one action that grant and that deny. */
    class Patterns(val granted: List<Pattern>, val denied: List<Pattern>) {
        fun any(): Boolean = granted.isNotEmpty() || denied.isNotEmpty()

        /** The properties all the rules ask about. */
        fun keys(): IntSet = IntSets.immutable.of(*(granted + denied).map { it.key }.toIntArray())
    }

    /** What the privileges about one action grant and deny, by name and by property rules. */
    class Rule(val granted: Reach, val denied: Reach, val patterns: Patterns) {
        /** Every element, whatever its labels or type. */
        fun everything(): Boolean = granted.all && !denied.any() && patterns.denied.isEmpty()

        /** Every node with [label], whatever its other labels. */
        fun everythingWith(label: Int): Boolean =
            granted.reaches(label) && !denied.any() && patterns.denied.isEmpty()

        /**
         * An element with [ids]: granted for one of them, and denied for none. Without the values
         * of properties, a DENY rule that may apply denies and GRANT rules grant nothing.
         */
        fun allowsAny(ids: IntArray): Boolean =
            !denied.reachesAny(ids) &&
                patterns.denied.none { it.appliesTo(ids) } &&
                granted.reachesAny(ids)

        /** [allowsAny], asking for the ids only when they matter. */
        fun allowsAny(ids: () -> IntArray): Boolean =
            when {
                denied.all -> false
                granted.all && denied.ids.isEmpty && patterns.denied.isEmpty() -> true
                else -> allowsAny(ids())
            }

        /** A node with [ids] whose properties have [values], those its rules ask about. */
        fun allowsAny(ids: IntArray, values: IntObjectMap<Value>): Boolean =
            !denied.reachesAny(ids) &&
                patterns.denied.none { it.holdsFor(ids, values) } &&
                (granted.reachesAny(ids) || patterns.granted.any { it.holdsFor(ids, values) })

        /**
         * The properties whose values decide for a node with [ids]: those of the rules that may
         * apply to it, unless its labels decide.
         */
        fun keysFor(ids: IntArray): IntSet {
            if (!patterns.any() || denied.reachesAny(ids)) return IntSets.immutable.empty()
            val deciding =
                patterns.denied.filter { it.appliesTo(ids) } +
                    if (granted.reachesAny(ids)) emptyList()
                    else patterns.granted.filter { it.appliesTo(ids) }
            return IntSets.immutable.of(*deciding.map { it.key }.toIntArray())
        }

        /** A node with [labels]: granted for every node or each label, denied for none. */
        fun allowsEvery(labels: IntArray): Boolean =
            !denied.reachesAny(labels) &&
                (granted.all || (labels.isNotEmpty() && labels.all(granted.ids::contains)))
    }

    /** The rules of each property, and of those no privilege names. */
    private class PropertyRules(private val other: Rule, private val named: Map<Int, Rule>) {
        fun of(key: Int): Rule = named[key] ?: other

        fun all(): List<Rule> = named.values + other

        /** The properties the rules about those [selected] ask about, every property's included. */
        fun securityKeys(selected: (Int) -> Boolean): IntSet =
            IntSets.mutable.empty().also { keys ->
                keys.addAll(other.patterns.keys())
                named.forEach { (key, rule) ->
                    if (selected(key)) keys.addAll(rule.patterns.keys())
                }
            }
    }

    /** Names resolved to ids, remembering those that did not exist. */
    private class Names(private val resolve: (String) -> Int) {
        private val missing = mutableSetOf<String>()

        fun id(name: String): Int? {
            val id = resolve(name)
            if (id != TokenConstants.NO_TOKEN) return id
            missing += name
            return null
        }

        fun resolveIn(current: (String) -> Int): Boolean = missing.any {
            current(it) != TokenConstants.NO_TOKEN
        }
    }

    private companion object {
        /** What a privilege about every node, relationship, label or property reaches. */
        val ALL = Any()

        val WRITES =
            listOf(
                PrivilegeAction.CREATE_ELEMENT,
                PrivilegeAction.DELETE_ELEMENT,
                PrivilegeAction.SET_LABEL,
                PrivilegeAction.REMOVE_LABEL,
                PrivilegeAction.SET_PROPERTY,
            )
        val SCHEMA_WRITES =
            listOf(
                PrivilegeAction.CREATE_INDEX,
                PrivilegeAction.DROP_INDEX,
                PrivilegeAction.CREATE_CONSTRAINT,
                PrivilegeAction.DROP_CONSTRAINT,
            )
    }
}
