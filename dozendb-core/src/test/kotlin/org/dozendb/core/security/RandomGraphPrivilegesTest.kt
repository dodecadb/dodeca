package org.dozendb.core.security

import java.nio.file.Path
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.logging.NullLog
import org.neo4j.values.storable.Values

/**
 * Random graph privileges granted and denied with Cypher, against what [ReferencePolicy] makes of
 * the privileges they stored: what a user finds, reads, counts and writes through Bolt, the count
 * store and node degrees included, which Neo4j answers without looking at each element.
 */
class RandomGraphPrivilegesTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private lateinit var alice: Driver

    /** A node or relationship as the admin sees it. */
    private data class Element(
        val id: String,
        val labels: Set<String>,
        val properties: Map<String, Any?>,
        val type: String? = null,
        val start: String? = null,
        val end: String? = null,
    )

    private lateinit var nodes: List<Element>
    private lateinit var relationships: List<Element>

    @BeforeEach
    fun start() {
        server =
            StandaloneServer(
                    root,
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                    ),
                )
                .also { it.start() }
        server.administer("CREATE DATABASE films")
        server.rows(
            "CREATE (a:A {p: 1, q: 2, l: [1, 2]}), (b:B {p: 3, l: [2]}), (ab:A:B {q: 4}), " +
                "(c:C {p: 5, q: 6, l: []}), (none {p: 7}), (abc:A:B:C), " +
                "(a)-[:R {p: 8, l: [8, 9]}]->(b), (a)-[:S {q: 9, l: [9]}]->(ab), (ab)-[:R]->(c), " +
                "(c)-[:S {p: 10, q: 11, l: []}]->(none), (none)-[:R]->(a), (abc)-[:S]->(abc), " +
                "(b)-[:R {q: 12}]->(abc), (:B {q: 13}), (:C), ()"
        )
        nodes =
            server
                .rows(
                    "MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS p"
                )
                .map {
                    @Suppress("UNCHECKED_CAST")
                    Element(
                        it["id"] as String,
                        (it["labels"] as List<String>).toSet(),
                        lists(it["p"] as Map<String, Any?>),
                    )
                }
        relationships =
            server
                .rows(
                    "MATCH (s)-[r]->(e) RETURN elementId(r) AS id, type(r) AS type, " +
                        "properties(r) AS p, elementId(s) AS start, elementId(e) AS end"
                )
                .map {
                    @Suppress("UNCHECKED_CAST")
                    Element(
                        it["id"] as String,
                        emptySet(),
                        lists(it["p"] as Map<String, Any?>),
                        it["type"] as String,
                        it["start"] as String,
                        it["end"] as String,
                    )
                }
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        alice =
            GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic("alice", "alice-secret-1"),
            )
    }

    /**
     * [properties] with lists for arrays, as Bolt gives them, where Neo4j's embedded API does not.
     */
    private fun lists(properties: Map<String, Any?>): Map<String, Any?> =
        properties.mapValues { (_, value) ->
            if (value?.javaClass?.isArray == true) {
                List(java.lang.reflect.Array.getLength(value)) {
                    java.lang.reflect.Array.get(value, it)
                }
            } else {
                value
            }
        }

    @AfterEach
    fun stop() {
        alice.close()
        server.close()
    }

    /** A random GRANT or DENY of a graph privilege to `r`, as Cypher has them. */
    private fun Random.command(): String {
        fun <T> pick(vararg options: T): T = options.random(this)
        val graph = pick("GRAPH *", "GRAPH neo4j", "HOME GRAPH", "GRAPH films")
        val elements =
            pick(
                "NODES *",
                "NODES A",
                "NODES B",
                "NODES Z",
                "RELATIONSHIPS *",
                "RELATIONSHIPS R",
                "ELEMENTS *",
                "",
            )
        val nodes = pick("NODES *", "NODES A", "NODES B", "NODES Z", "")
        val properties = pick("{*}", "{p}", "{q}")
        val label = pick("*", "A", "C")
        val nodeRule =
            "FOR ${pick("(n)", "(n:A)", "(n:B|Z)")} WHERE " +
                pick(
                    "n.p > 1",
                    "n.p <= 3",
                    "n.q = 4",
                    "NOT n.q = 4",
                    "n.p IN [1, 5, 7]",
                    "n.q IS NULL",
                    "n.p IS NOT NULL",
                    "n.r IS NULL",
                    "2 IN n.l",
                    "NOT 1 IN n.l",
                )
        val relationshipRule =
            "FOR ${pick("()-[r]-()", "()-[r:R]-()", "()-[r:S|Z]-()")} WHERE " +
                pick(
                    "r.p > 8",
                    "r.q = 9",
                    "NOT r.q = 9",
                    "r.p IN [8, 10]",
                    "r.q IS NULL",
                    "r.p IS NOT NULL",
                    "9 IN r.l",
                    "NOT 8 IN r.l",
                )
        val rule = pick(nodeRule, relationshipRule)
        val privilege =
            when (nextInt(14)) {
                0 -> "TRAVERSE ON $graph $elements"
                1 -> "READ $properties ON $graph $elements"
                2 -> "MATCH $properties ON $graph $elements"
                3 -> "WRITE ON $graph"
                4 -> "CREATE ON $graph $elements"
                5 -> "DELETE ON $graph $elements"
                6 -> "SET LABEL $label ON $graph"
                7 -> "REMOVE LABEL $label ON $graph"
                8 -> "SET PROPERTY $properties ON $graph $elements"
                9 -> "ALL GRAPH PRIVILEGES ON $graph"
                // Property rules.
                10 -> "TRAVERSE ON $graph $rule"
                11 -> "READ $properties ON $graph $rule"
                12 -> "MATCH $properties ON $graph $rule"
                else -> "MERGE $properties ON $graph $nodes"
            }
        // MERGE cannot be denied.
        val access = if (privilege.startsWith("MERGE")) "GRANT" else pick("GRANT", "DENY")
        // A value in a list property is Cypher 25's.
        val version = if (Regex(" IN [nr]\\.l").containsMatchIn(privilege)) "CYPHER 25 " else ""
        return "$version$access $privilege TO r"
    }

    private fun Driver.rows(cypher: String, parameters: Map<String, Any> = emptyMap()) =
        session(SessionConfig.forDatabase("neo4j")).use { session ->
            session.run(cypher, parameters).list { it.asMap() }
        }

    /** Whether [cypher] succeeds as alice, rolled back either way. */
    private fun Driver.writes(cypher: String, parameters: Map<String, Any>): Boolean =
        session(SessionConfig.forDatabase("neo4j")).use { session ->
            session.beginTransaction().use { tx ->
                try {
                    tx.run(cypher, parameters).consume()
                    true
                } catch (e: ClientException) {
                    check(e.code() == "Neo.ClientError.Security.Forbidden") { e }
                    false
                } finally {
                    if (tx.isOpen) tx.rollback()
                }
            }
        }

    @Test
    fun `finds, reads, counts and writes what the stored privileges allow`() {
        val seed = System.getProperty("dozendb.seed")?.toLong() ?: System.nanoTime()
        val random = Random(seed)
        repeat(ROUNDS) { round ->
            server.administer("DROP ROLE r IF EXISTS")
            server.administer("CREATE ROLE r")
            server.administer("GRANT ROLE r TO alice")
            // Most rounds start finding things, so that what follows has something to deny.
            val base =
                listOf(
                        "GRANT TRAVERSE ON GRAPH * TO r",
                        "GRANT MATCH {*} ON GRAPH * TO r",
                        "GRANT MATCH {*} ON GRAPH neo4j NODES A TO r",
                        "GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO r",
                    )
                    .random(random)
            val commands =
                listOfNotNull(base.takeIf { random.nextInt(4) > 0 }) +
                    List(random.nextInt(1, 5)) { random.command() }
            commands.forEach(server::administer)
            val context = "seed $seed, round $round: $commands"
            check(context)
        }
    }

    private fun check(context: String) {
        val security =
            server.database(SYSTEM).beginTx().use { SecurityGraph.read(it, NullLog.getInstance()) }
        val neo4j = checkNotNull(security.databaseId("neo4j"))
        val policy =
            ReferencePolicy(security.privileges(security.roles("alice")), neo4j).graph(neo4j)
        fun properties(element: Element) = { key: String ->
            when (val value = element.properties[key]) {
                // Cypher gives lists for the arrays that properties hold.
                is List<*> -> Values.longArray(value.map { (it as Number).toLong() }.toLongArray())
                else -> Values.of(value)
            }
        }
        val found = nodes.filter { policy.traverseNode(it.labels, properties(it)) }
        val foundIds = found.map { it.id }.toSet()
        val traversed = relationships.filter {
            it.start in foundIds &&
                it.end in foundIds &&
                policy.traverseRelationship(it.type!!, properties(it))
        }
        val soft = SoftAssertions()

        // T1-T3, R1-R3: what MATCH finds and reads.
        soft
            .assertThat(
                alice
                    .rows(
                        "MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS p"
                    )
                    .associate { it["id"] to (it["labels"] as List<*>).toSet() to it["p"] }
            )
            .describedAs("nodes, $context")
            .isEqualTo(
                found.associate { node ->
                    node.id to
                        node.labels to
                        node.properties.filterKeys {
                            policy.readNodeProperty(node.labels, it, properties(node))
                        }
                }
            )
        soft
            .assertThat(
                alice
                    .rows("MATCH ()-[r]->() RETURN elementId(r) AS id, properties(r) AS p")
                    .associate { it["id"] to it["p"] }
            )
            .describedAs("relationships, $context")
            .isEqualTo(
                traversed.associate { relationship ->
                    relationship.id to
                        relationship.properties.filterKeys {
                            policy.readRelationshipProperty(
                                relationship.type!!,
                                it,
                                properties(relationship),
                            )
                        }
                }
            )

        // R4: counts and degrees, which Neo4j may take from the count store.
        for (label in listOf(null, "A", "B", "C", "Z")) {
            val pattern = if (label == null) "(n)" else "(n:$label)"
            soft
                .assertThat(alice.rows("MATCH $pattern RETURN count(n) AS c").single()["c"])
                .describedAs("count of $pattern, $context")
                .isEqualTo(found.count { label == null || label in it.labels }.toLong())
            for (type in listOf(null, "R", "S")) {
                val relationship = if (type == null) "[r]" else "[r:$type]"
                for ((cypher, matches) in
                    listOf(
                        "MATCH $pattern-$relationship->() RETURN count(r) AS c" to
                            { r: Element ->
                                nodes
                                    .single { it.id == r.start }
                                    .let { label == null || label in it.labels }
                            },
                        "MATCH ()-$relationship->$pattern RETURN count(r) AS c" to
                            { r: Element ->
                                nodes
                                    .single { it.id == r.end }
                                    .let { label == null || label in it.labels }
                            },
                    )) {
                    soft
                        .assertThat(alice.rows(cypher).single()["c"])
                        .describedAs("$cypher, $context")
                        .isEqualTo(
                            traversed
                                .count { (type == null || it.type == type) && matches(it) }
                                .toLong()
                        )
                }
            }
        }
        soft
            .assertThat(
                alice
                    .rows(
                        "MATCH (n) RETURN elementId(n) AS id, COUNT { (n)-->() } AS out, " +
                            "COUNT { (n)<--() } AS in, COUNT { (n)-[:R]-() } AS r"
                    )
                    .associate { it["id"] to listOf(it["out"], it["in"], it["r"]) }
            )
            .describedAs("degrees, $context")
            .isEqualTo(
                found.associate { node ->
                    node.id to
                        listOf(
                            traversed.count { it.start == node.id }.toLong(),
                            traversed.count { it.end == node.id }.toLong(),
                            traversed
                                .filter { it.type == "R" }
                                .sumOf {
                                    (if (it.start == node.id) 1L else 0L) +
                                        (if (it.end == node.id) 1L else 0L)
                                },
                        )
                }
            )

        // W2-W5: writes, rolled back.
        for (labels in listOf(emptySet(), setOf("A"), setOf("A", "B"), setOf("C"))) {
            val create = "CREATE (n${labels.joinToString("") { ":$it" }})"
            soft
                .assertThat(alice.writes(create, emptyMap()))
                .describedAs("$create, $context")
                .isEqualTo(policy.createNode(labels))
        }
        for (node in found) {
            val id = mapOf("id" to node.id)
            val match = "MATCH (n) WHERE elementId(n) = \$id"
            fun expect(write: String, expected: Boolean) =
                soft
                    .assertThat(alice.writes("$match $write", id))
                    .describedAs("$write on ${node.labels}, $context")
                    .isEqualTo(expected)
            expect("SET n.p = 0", policy.setNodeProperty(node.labels, "p"))
            for (label in listOf("A", "C") - node.labels) {
                expect("SET n:$label", policy.setLabel(label))
            }
            for (label in node.labels.intersect(setOf("A", "C"))) {
                expect("REMOVE n:$label", policy.removeLabel(label))
            }
            if (relationships.none { it.start == node.id || it.end == node.id }) {
                expect("DELETE n", policy.deleteNode(node.labels))
            }
            for (other in found.take(1)) {
                for (type in listOf("R", "S")) {
                    soft
                        .assertThat(
                            alice.writes(
                                "$match MATCH (m) WHERE elementId(m) = \$other " +
                                    "CREATE (n)-[:$type]->(m)",
                                id + ("other" to other.id),
                            )
                        )
                        .describedAs("CREATE -[:$type]-> from ${node.labels}, $context")
                        .isEqualTo(policy.createRelationship(type))
                }
            }
        }
        for (relationship in traversed) {
            val id = mapOf("id" to relationship.id)
            val type = checkNotNull(relationship.type)
            val match = "MATCH ()-[r]->() WHERE elementId(r) = \$id"
            soft
                .assertThat(alice.writes("$match SET r.q = 0", id))
                .describedAs("SET q of $type, $context")
                .isEqualTo(policy.setRelationshipProperty(type, "q"))
            soft
                .assertThat(alice.writes("$match DELETE r", id))
                .describedAs("DELETE $type, $context")
                .isEqualTo(policy.deleteRelationship(type))
        }
        soft.assertAll()
    }

    private companion object {
        const val ROUNDS = 60
    }
}
