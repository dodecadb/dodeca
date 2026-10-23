package org.dozendb.core.security

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.core.StandaloneServer
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

/**
 * Graph privileges granted and denied with Cypher, on the whole graph and on labels and types, as a
 * user reads and writes through Bolt.
 */
class GraphPrivilegesTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private val drivers = mutableListOf<Driver>()

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
        server.rows(
            "CREATE (a:A {p: 1, q: 2}), (b:B {p: 3, q: 4}), (ab:A:B {p: 5, q: 6}), (:Other {p: 7}), " +
                "(a)-[:R {p: 8}]->(b), (a)-[:S {p: 9}]->(ab)"
        )
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE ROLE r")
        server.administer("GRANT ROLE r TO alice")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private val alice by lazy {
        GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic("alice", "alice-secret-1"),
            )
            .also { drivers += it }
    }

    /** Runs [command] as the admin, with authentication disabled. */
    private fun grant(command: String) = server.administer(command)

    private fun Driver.run(cypher: String): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase("neo4j")).use { session ->
            session.run(cypher).list { it.asMap() }
        }

    private fun Driver.values(cypher: String): List<Any?> = run(cypher).map { it.values.single() }

    private fun Driver.refused(cypher: String): String =
        checkNotNull(catchThrowableOfType(ClientException::class.java) { run(cypher) }) {
                "$cypher succeeded"
            }
            .message!!

    private fun nodes() = alice.values("MATCH (n) RETURN count(n)").single()

    private fun relationships() = alice.values("MATCH ()-[x]->() RETURN count(x)").single()

    @Test
    fun `finds and reads what TRAVERSE, READ and MATCH allow, and nothing denied`() {
        assertThat(nodes()).isEqualTo(0L)

        grant("GRANT TRAVERSE ON GRAPH * NODES * TO r")

        assertThat(nodes()).isEqualTo(4L)
        assertThat(alice.values("MATCH (n) RETURN n.p")).containsOnlyNulls().hasSize(4)
        assertThat(relationships()).isEqualTo(0L)

        grant("GRANT READ {p} ON GRAPH * NODES A TO r")

        assertThat(alice.values("MATCH (n) WHERE n.p IS NOT NULL RETURN n.p ORDER BY n.p"))
            .containsExactly(1L, 5L)
        assertThat(alice.values("MATCH (n:A) RETURN n.q")).containsOnlyNulls()

        grant("DENY READ {p} ON GRAPH * NODES B TO r")

        assertThat(alice.values("MATCH (n) WHERE n.p IS NOT NULL RETURN n.p")).containsExactly(1L)

        grant("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS R TO r")

        assertThat(alice.values("MATCH ()-[x]->() RETURN type(x)")).containsExactly("R")

        grant("DENY TRAVERSE ON GRAPH * NODES B TO r")

        assertThat(nodes()).isEqualTo(2L)
        assertThat(alice.values("MATCH (n:B) RETURN count(n)")).containsExactly(0L)
        // A relationship is found only with both of its nodes.
        assertThat(relationships()).isEqualTo(0L)
        assertThat(alice.values("MATCH (n:A) RETURN labels(n)")).containsExactly(listOf("A"))

        grant("GRANT MATCH {*} ON GRAPH * TO r")

        assertThat(alice.run("MATCH (n) RETURN n.p AS p, n.q AS q ORDER BY p"))
            .containsExactly(mapOf("p" to 1L, "q" to 2L), mapOf("p" to 7L, "q" to null))
        assertThat(alice.values("MATCH ()-[x]->() RETURN x.p")).isEmpty()
    }

    @Test
    fun `denying MATCH of all properties denies traversal, of one property only reading it`() {
        grant("GRANT MATCH {*} ON GRAPH * TO r")
        grant("DENY MATCH {q} ON GRAPH * NODES A TO r")

        assertThat(nodes()).isEqualTo(4L)
        assertThat(alice.values("MATCH (n:A) RETURN n.q")).containsOnlyNulls().hasSize(2)

        grant("DENY MATCH {*} ON GRAPH * NODES B TO r")

        assertThat(nodes()).isEqualTo(2L)
        assertThat(alice.values("MATCH ()-[x]->() RETURN x.p")).isEmpty()
    }

    @Test
    fun `writes what CREATE, DELETE, SET LABEL, REMOVE LABEL and SET PROPERTY allow`() {
        grant("GRANT MATCH {*} ON GRAPH * TO r")

        assertThat(alice.refused("CREATE (:A)"))
            .isEqualTo(
                "Create node with labels 'A' on database 'neo4j' is not allowed for user 'alice' " +
                    "with roles [PUBLIC, r]."
            )

        grant("GRANT CREATE ON GRAPH * NODES A TO r")

        alice.run("CREATE (:A)")
        // Properties of a new node need SET PROPERTY, and each label CREATE.
        assertThat(alice.refused("CREATE (:A {p: 10})")).startsWith("Set property for property 'p'")
        assertThat(alice.refused("CREATE (:A:B)")).startsWith("Create node with labels")
        // A node without labels needs CREATE on all nodes.
        assertThat(alice.refused("CREATE ()"))
            .isEqualTo(
                "Create node with labels '' on database 'neo4j' is not allowed for user 'alice' " +
                    "with roles [PUBLIC, r]."
            )
        assertThat(alice.refused("MATCH (n:A {p: 1}) SET n.q = 20"))
            .startsWith("Set property for property 'q' on database 'neo4j' is not allowed")

        grant("GRANT SET PROPERTY {q} ON GRAPH * NODES A TO r")

        alice.run("MATCH (n:A {p: 1}) SET n.q = 20")
        assertThat(alice.refused("MATCH (n:A {p: 1}) SET n:Other"))
            .startsWith("Set label for label 'Other' on database 'neo4j' is not allowed")

        grant("GRANT SET LABEL Other ON GRAPH * TO r")

        alice.run("MATCH (n:A {p: 1}) SET n:Other")
        assertThat(alice.refused("MATCH (n:A {p: 1}) REMOVE n:Other"))
            .startsWith("Remove label for label 'Other' on database 'neo4j' is not allowed")

        grant("GRANT REMOVE LABEL * ON GRAPH * TO r")

        alice.run("MATCH (n:A {p: 1}) REMOVE n:Other")
        assertThat(alice.refused("MATCH (n:A) WHERE n.p IS NULL DELETE n"))
            .startsWith("Delete node with labels 'A' on database 'neo4j' is not allowed")

        grant("GRANT DELETE ON GRAPH * NODES A TO r")

        alice.run("MATCH (n:A) WHERE n.p IS NULL DELETE n")
        assertThat(server.rows("MATCH (n:A {p: 1}) RETURN n.q AS q, labels(n) AS labels"))
            .containsExactly(mapOf("q" to 20L, "labels" to listOf("A")))
        assertThat(server.rows("MATCH (n:A) WHERE n.p IS NULL RETURN n")).isEmpty()
    }

    @Test
    fun `WRITE covers every write, but what is denied`() {
        grant("GRANT MATCH {*} ON GRAPH * TO r")
        grant("GRANT WRITE ON GRAPH * TO r")
        grant("DENY SET PROPERTY {p} ON GRAPH * NODES * TO r")
        grant("DENY CREATE ON GRAPH * RELATIONSHIPS S TO r")

        alice.run("MATCH (a:A {p: 1}), (b:B {p: 3}) CREATE (a)-[:R]->(b)")
        alice.run("MATCH (n:B) SET n.q = 40")
        alice.run("MATCH (n:Other) DETACH DELETE n")

        assertThat(alice.refused("MATCH (n:B) SET n.p = 30"))
            .startsWith("Set property for property 'p'")
        assertThat(alice.refused("MATCH (a:A {p: 1}), (b:B {p: 3}) CREATE (a)-[:S]->(b)"))
            .startsWith("Create relationship with type 'S' on database 'neo4j' is not allowed")
        assertThat(server.rows("MATCH ()-[x:R]->() RETURN count(x) AS r").single()["r"])
            .isEqualTo(2L)
    }

    @Test
    fun `MERGE finds, creates and sets, and ALL GRAPH PRIVILEGES covers all but new names`() {
        grant("GRANT MERGE {*} ON GRAPH * NODES A TO r")

        assertThat(alice.values("MERGE (n:A {p: 42}) RETURN n.p")).containsExactly(42L)
        assertThat(alice.values("MERGE (n:A {p: 1}) RETURN n.q")).containsExactly(2L)
        assertThat(alice.refused("MATCH (n:A {p: 42}) DELETE n")).startsWith("Delete node")

        grant("REVOKE MERGE {*} ON GRAPH * NODES A FROM r")
        grant("GRANT ALL GRAPH PRIVILEGES ON GRAPH neo4j TO r")

        assertThat(nodes()).isEqualTo(5L)
        alice.run("MATCH (n:A {p: 42}) DELETE n")
        // No graph privilege creates names.
        assertThat(alice.refused("CREATE (:Fresh)"))
            .startsWith("Creating new node label on database 'neo4j' is not allowed")

        grant("DENY ALL GRAPH PRIVILEGES ON GRAPH * TO r")

        assertThat(nodes()).isEqualTo(0L)
        assertThat(alice.refused("CREATE (:A)")).startsWith("Create node with labels 'A'")
    }

    @Test
    fun `privileges on names that do not exist yet apply once they do`() {
        grant("GRANT TRAVERSE ON GRAPH * NODES Fresh TO r")
        grant("GRANT READ {fresh} ON GRAPH * NODES Fresh TO r")

        assertThat(nodes()).isEqualTo(0L)

        server.rows("CREATE (:Fresh {fresh: 1})")

        // Alice's privileges were compiled before the names existed.
        assertThat(alice.values("MATCH (n) RETURN n.fresh")).containsExactly(1L)
    }

    @Test
    fun `privileges on a named graph or the home graph apply there only`() {
        server.administer("CREATE DATABASE films")
        server.rows("CREATE (:A {p: 100})", "films")
        grant("GRANT ACCESS ON DATABASE films TO r")
        grant("GRANT MATCH {*} ON GRAPH films TO r")

        assertThat(nodes()).isEqualTo(0L)
        assertThat(
                alice.session(SessionConfig.forDatabase("films")).use {
                    it.run("MATCH (n) RETURN n.p AS p").list { row -> row["p"].asLong() }
                }
            )
            .containsExactly(100L)

        grant("GRANT TRAVERSE ON HOME GRAPH NODES A TO r")

        assertThat(nodes()).isEqualTo(2L)
    }
}
