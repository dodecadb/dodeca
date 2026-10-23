package org.dozendb.core.security

import java.util.UUID
import java.util.function.Supplier
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.internal.kernel.api.RelTypeSupplier
import org.neo4j.internal.kernel.api.TokenSet
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.LoginContext.IdLookup
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.string.Globbing
import org.neo4j.token.api.TokenConstants
import org.neo4j.values.storable.Values

/** [DozenDbAccessMode] answers as [PrivilegePolicy] for Neo4j's kernel, which knows ids. */
class DozenDbAccessModeTest {
    private val database = UUID(0, 1)

    /** Ids as a database would give them: [labels] and the like exist, others not yet. */
    private class Lookup(
        val labels: List<String> = LABELS,
        val types: List<String> = TYPES,
        val keys: List<String> = KEYS,
    ) : IdLookup {
        var procedureViewChanged = false

        override fun getPropertyKeyId(name: String) = keys.indexOf(name)

        override fun getLabelId(name: String) = labels.indexOf(name)

        override fun getRelTypeId(name: String) = types.indexOf(name)

        override fun getProcedureIds(procedureGlobbing: String) =
            matching(procedureGlobbing, PROCEDURES)

        override fun getAdminProcedureIds() =
            PROCEDURES.indices.filter { PROCEDURES[it] in ADMIN }.toIntArray()

        override fun getFunctionIds(functionGlobbing: String) =
            matching(functionGlobbing, FUNCTIONS)

        override fun getAggregatingFunctionIds(functionGlobbing: String) =
            matching(functionGlobbing, AGGREGATING)

        override fun isCachableLookup() = true

        override fun isStale() = procedureViewChanged

        private fun matching(glob: String, names: List<String>): IntArray {
            val predicate = Globbing.globPredicate(glob)
            return names.indices.filter { predicate.test(names[it]) }.toIntArray()
        }
    }

    private fun tokens(names: Set<String>, lookup: Lookup = Lookup()) =
        names.map(lookup::getLabelId).toIntArray()

    private fun tokenSet(ids: IntArray): Supplier<TokenSet> = Supplier {
        object : TokenSet {
            override fun numberOfTokens() = ids.size

            override fun token(offset: Int) = ids[offset]

            override fun contains(token: Int) = token in ids

            override fun all() = ids
        }
    }

    private fun type(id: Int) = RelTypeSupplier { id }

    @Test
    fun `answers as the policy on random privileges`() {
        val random = Random(20261023)
        val generator = PrivilegeGenerator(random, listOf(database, UUID(0, 2)))
        repeat(2000) { round ->
            val privileges = List(random.nextInt(1, 7)) { generator.next(patterns = false) }
            val policy = PrivilegePolicy(privileges, database)
            val mode = DozenDbAccessMode(policy, database, Lookup(), setOf("PUBLIC"))
            check(mode, policy, "round $round: $privileges")
        }
    }

    private fun check(mode: DozenDbAccessMode, policy: PrivilegePolicy, context: String) {
        val graph = policy.graph(database)
        val none = { _: String -> Values.NO_VALUE }
        fun expect(question: String, actual: Boolean, expected: Boolean) =
            assertThat(actual).describedAs("$question, $context").isEqualTo(expected)
        fun soundly(question: String, shortcut: Boolean, truth: Boolean) {
            if (shortcut) assertThat(truth).describedAs("$question, $context").isTrue
        }

        // Neo4j asks about a node without labels with null.
        expect("create without labels", mode.allowsCreateNode(null), graph.createNode(emptySet()))
        for (labels in LABEL_SETS) {
            val ids = tokens(labels)
            expect(
                "traverse $labels",
                mode.allowsTraverseNode(*ids),
                graph.traverseNode(labels, none),
            )
            expect("create $labels", mode.allowsCreateNode(ids), graph.createNode(labels))
            expect("delete $labels", mode.allowsDeleteNode(tokenSet(ids)), graph.deleteNode(labels))
            for (key in KEYS) {
                val keyId = Lookup().getPropertyKeyId(key)
                expect(
                    "read $key of $labels",
                    mode.allowsReadNodeProperty(tokenSet(ids), keyId),
                    graph.readNodeProperty(labels, key, none),
                )
                expect(
                    "set $key of $labels",
                    mode.allowsSetProperty(tokenSet(ids), keyId),
                    graph.setNodeProperty(labels, key),
                )
            }
        }
        val everyNode = LABEL_SETS.all { graph.traverseNode(it, none) }
        soundly("traverse all", mode.allowsTraverseAllLabels(), everyNode)
        soundly("traverse any", mode.allowsTraverseNode(TokenConstants.ANY_LABEL), everyNode)
        soundly(
            "count all",
            mode.allowsTraverseAllNodesWithLabel(TokenConstants.ANY_LABEL),
            everyNode,
        )
        soundly(
            "none at all",
            mode.disallowsTraverseLabel(TokenConstants.ANY_LABEL),
            LABEL_SETS.none { graph.traverseNode(it, none) },
        )
        for (label in LABELS) {
            val id = Lookup().getLabelId(label)
            val withLabel = LABEL_SETS.filter { label in it }
            soundly(
                "all with $label",
                mode.allowsTraverseAllNodesWithLabel(id),
                withLabel.all { graph.traverseNode(it, none) },
            )
            soundly(
                "none with $label",
                mode.disallowsTraverseLabel(id),
                withLabel.none { graph.traverseNode(it, none) },
            )
            expect("set label $label", mode.allowsSetLabel(id), graph.setLabel(label))
            expect("remove label $label", mode.allowsRemoveLabel(id), graph.removeLabel(label))
        }
        for (key in KEYS) {
            val id = Lookup().getPropertyKeyId(key)
            soundly(
                "read $key everywhere",
                mode.allowsReadPropertyAllLabels(id),
                LABEL_SETS.all { graph.readNodeProperty(it, key, none) },
            )
            soundly(
                "read $key of every type",
                mode.allowsReadPropertyAllRelTypes(id),
                TYPES.all { graph.readRelationshipProperty(it, key) },
            )
            soundly(
                "list $key",
                !mode.allowsSeePropertyKeyToken(id),
                LABEL_SETS.none { graph.readNodeProperty(it, key, none) } &&
                    TYPES.none { graph.readRelationshipProperty(it, key) },
            )
        }
        val anyType = TYPES.all { graph.traverseRelationship(it) }
        soundly("traverse all types", mode.allowsTraverseAllRelTypes(), anyType)
        soundly(
            "traverse any type",
            mode.allowsTraverseRelType(TokenConstants.ANY_RELATIONSHIP_TYPE),
            anyType,
        )
        for (type in TYPES) {
            val id = Lookup().getRelTypeId(type)
            expect(
                "traverse $type",
                mode.allowsTraverseRelType(id),
                graph.traverseRelationship(type),
            )
            soundly(
                "none of $type",
                mode.disallowsTraverseRelType(id),
                !graph.traverseRelationship(type),
            )
            expect(
                "create $type",
                mode.allowsCreateRelationship(id),
                graph.createRelationship(type),
            )
            expect(
                "delete $type",
                mode.allowsDeleteRelationship(id),
                graph.deleteRelationship(type),
            )
            for (key in KEYS) {
                val keyId = Lookup().getPropertyKeyId(key)
                expect(
                    "read $key of $type",
                    mode.allowsReadRelationshipProperty(type(id), keyId),
                    graph.readRelationshipProperty(type, key),
                )
                expect(
                    "set $key of $type",
                    mode.allowsSetProperty(type(id), keyId),
                    graph.setRelationshipProperty(type, key),
                )
            }
        }
        for ((id, name) in PROCEDURES.withIndex()) {
            val expected = policy.procedure(name, name in ADMIN)
            assertThat(
                    Execution(
                        mode.allowsExecuteProcedure(id),
                        mode.shouldBoostProcedure(id),
                        mode.allowExecuteAdminProcedure(id),
                    )
                )
                .describedAs("procedure $name, $context")
                .isEqualTo(expected)
        }
        for ((id, name) in FUNCTIONS.withIndex()) {
            assertThat(Execution(mode.allowsExecuteFunction(id), mode.shouldBoostFunction(id)))
                .describedAs("function $name, $context")
                .isEqualTo(policy.function(name))
        }
        for ((id, name) in AGGREGATING.withIndex()) {
            assertThat(
                    Execution(
                        mode.allowsExecuteAggregatingFunction(id),
                        mode.shouldBoostAggregatingFunction(id),
                    )
                )
                .describedAs("aggregating function $name, $context")
                .isEqualTo(policy.function(name))
        }
        assertThat(mode.allowsLoadAllData()).describedAs(context).isEqualTo(policy.loadAllData())
        assertThat(mode.allowsTokenCreates(PrivilegeAction.CREATE_LABEL))
            .describedAs(context)
            .isEqualTo(policy.admin(PrivilegeAction.CREATE_LABEL, database))
    }

    @Test
    fun `privileges on names that do not exist yet apply once they do`() {
        val privileges =
            listOf(
                Privilege(
                    Access.GRANTED,
                    PrivilegeAction.MATCH,
                    PrivilegeScope.AllDatabases,
                    PrivilegeResource.AllProperties,
                    PrivilegeSegment.Nodes("Later"),
                )
            )
        val before = Lookup()
        val after = Lookup(labels = LABELS + "Later")
        val mode = DozenDbAccessMode(PrivilegePolicy(privileges, null), database, before, setOf())

        assertThat(mode.stale(before)).isFalse
        assertThat(mode.stale(after)).isTrue
        val later = after.getLabelId("Later")
        assertThat(mode.allowsTraverseNode(later)).isFalse
        assertThat(
                DozenDbAccessMode(PrivilegePolicy(privileges, null), database, after, setOf())
                    .allowsTraverseNode(later)
            )
            .isTrue
        before.procedureViewChanged = true
        assertThat(mode.stale(before)).isTrue
    }

    @Test
    fun `names the roles as Neo4j's messages do`() {
        val mode =
            DozenDbAccessMode(
                PrivilegePolicy(emptyList(), null),
                database,
                Lookup(),
                setOf("reader", "PUBLIC"),
            )

        assertThat(mode.name()).isEqualTo("roles [PUBLIC, reader]")
        assertThat(
                DozenDbAccessMode(PrivilegePolicy(emptyList(), null), database, Lookup(), setOf())
                    .name()
            )
            .isEqualTo("no roles")
    }

    @Test
    fun `implements every method of AccessMode itself`() {
        val declared =
            DozenDbAccessMode::class.java.declaredMethods.map {
                it.name to it.parameterTypes.toList()
            }
        for (method in AccessMode::class.java.methods) {
            assertThat(declared)
                .describedAs("AccessMode.${method.name}")
                .contains(method.name to method.parameterTypes.toList())
        }
    }

    private companion object {
        val LABELS = listOf("A", "B", "C")
        val LABEL_SETS =
            listOf(
                emptySet(),
                setOf("A"),
                setOf("B"),
                setOf("A", "B"),
                setOf("C"),
                setOf("A", "B", "C"),
            )
        val TYPES = listOf("R", "S")
        val KEYS = listOf("p", "q")
        val PROCEDURES =
            listOf("db.labels", "db.schema.visualization", "apoc.load.json", "apoc.trigger.add")
        val ADMIN = setOf("apoc.trigger.add")
        val FUNCTIONS = listOf("apoc.coll.sum", "apoc.text.join")
        val AGGREGATING = listOf("apoc.agg.first")
    }
}
