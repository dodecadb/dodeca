package dev.dodeca.core.security

import com.sun.net.httpserver.HttpServer
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Stream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
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
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.procedure.Context
import org.neo4j.procedure.Procedure
import org.neo4j.procedure.UserFunction

/**
 * Index, constraint and name management, procedures and functions, settings and LOAD granted and
 * denied with Cypher, as a user runs them through Bolt.
 */
class AdministrationPrivilegesTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private lateinit var web: HttpServer
    private val drivers = mutableListOf<Driver>()

    @BeforeEach
    fun start() {
        val import = Files.createDirectories(root.resolve("import"))
        Files.writeString(import.resolve("films.csv"), "Alien\n")
        web =
            HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0).apply {
                createContext("/films.csv") { exchange ->
                    val body = "Aliens\n".toByteArray()
                    exchange.sendResponseHeaders(200, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                start()
            }
        server =
            StandaloneServer(
                    root,
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                        GraphDatabaseSettings.load_csv_file_url_root to import,
                    ),
                )
                .also { it.start() }
        server
            .database(SYSTEM)
            .dependencyResolver
            .resolveDependency(GlobalProcedures::class.java)
            .apply {
                registerFunction(Functions::class.java)
                registerProcedure(Procedures::class.java)
            }
        server.rows("CREATE (:A {p: 1})-[:R]->(:B {p: 2})")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE ROLE r")
        server.administer("GRANT ROLE r TO alice")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
        web.stop(0)
    }

    /** A user-defined function, of which Neo4j Community has none. */
    class Functions {
        @Context lateinit var transaction: Transaction

        @UserFunction("test.nodes") fun nodes(): Long = transaction.allNodes.stream().count()
    }

    /** A procedure outside Neo4j's namespaces, as a plugin's are. */
    class Procedures {
        @Context lateinit var transaction: Transaction

        class Count(@JvmField val count: Long)

        @Procedure("test.count")
        fun count(): Stream<Count> = Stream.of(Count(transaction.allNodes.stream().count()))
    }

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    private val alice by lazy { driver("alice", "alice-secret-1") }

    /** Runs [command] as the admin, with authentication disabled. */
    private fun grant(command: String) = server.administer(command)

    private fun Driver.run(cypher: String, database: String = "neo4j"): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(cypher).list { it.asMap() }
        }

    private fun Driver.values(cypher: String, database: String = "neo4j"): List<Any?> =
        run(cypher, database).map { it.values.single() }

    private fun Driver.refused(cypher: String, database: String = "neo4j"): String =
        checkNotNull(catchThrowableOfType(ClientException::class.java) { run(cypher, database) }) {
                "$cypher succeeded"
            }
            .message!!

    private fun denial(what: String) = "$what for user 'alice' with roles [PUBLIC, r]."

    /** [denial] as a procedure that only administers is refused, in its own mode. */
    private fun procedureDenial(what: String) =
        "$what for user 'alice' with roles [PUBLIC, r] restricted to ACCESS."

    @Test
    fun `manages indexes, constraints and names with what is granted on the database`() {
        grant("GRANT MATCH {*} ON GRAPH * TO r")
        grant("GRANT WRITE ON GRAPH * TO r")

        assertThat(alice.refused("CREATE INDEX a_p FOR (n:A) ON (n.p)"))
            .isEqualTo(denial("Schema operation 'create_index' on database 'neo4j' is not allowed"))

        grant("GRANT CREATE INDEX ON DATABASE neo4j TO r")

        alice.run("CREATE INDEX a_p FOR (n:A) ON (n.p)")
        assertThat(alice.refused("SHOW INDEXES"))
            .isEqualTo(denial("Show indexes on database 'neo4j' is not allowed"))
        assertThat(alice.refused("DROP INDEX a_p"))
            .isEqualTo(denial("Schema operation 'drop_index' on database 'neo4j' is not allowed"))

        grant("GRANT INDEX MANAGEMENT ON DATABASE * TO r")

        assertThat(alice.values("SHOW INDEXES YIELD name WHERE name = 'a_p'")).hasSize(1)
        alice.run("DROP INDEX a_p")
        assertThat(alice.refused("CREATE CONSTRAINT b_p FOR (n:B) REQUIRE n.p IS UNIQUE"))
            .startsWith("Schema operation 'create_constraint' on database 'neo4j' is not allowed")

        grant("GRANT CONSTRAINT MANAGEMENT ON DATABASE neo4j TO r")

        alice.run("CREATE CONSTRAINT b_p FOR (n:B) REQUIRE n.p IS UNIQUE")
        assertThat(alice.values("SHOW CONSTRAINTS YIELD name")).containsExactly("b_p")
        alice.run("DROP CONSTRAINT b_p")

        grant("DENY DROP CONSTRAINT ON DATABASE * TO r")
        alice.run("CREATE CONSTRAINT b_p FOR (n:B) REQUIRE n.p IS UNIQUE")

        assertThat(alice.refused("DROP CONSTRAINT b_p"))
            .isEqualTo(denial("Schema operation 'drop_constraint' on database 'neo4j' is denied"))
        // WRITE creates no names.
        assertThat(alice.refused("CREATE (:Fresh)"))
            .startsWith(
                "Creating new node label on database 'neo4j' is not allowed for user 'alice' " +
                    "with roles [PUBLIC, r]. See GRANT CREATE NEW NODE LABEL ON DATABASE `neo4j`"
            )
        assertThat(alice.refused("MATCH (a:A), (b:B) CREATE (a)-[:FRESH]->(b)"))
            .startsWith("Creating new relationship type on database 'neo4j' is not allowed")
        assertThat(alice.refused("MATCH (n:A) SET n.fresh = 1"))
            .startsWith("Creating new property name on database 'neo4j' is not allowed")

        grant("GRANT NAME MANAGEMENT ON DATABASE neo4j TO r")

        alice.run("CREATE (:Fresh {fresh: 1})")
        alice.run("MATCH (a:A), (b:B) CREATE (a)-[:FRESH]->(b)")

        grant("DENY CREATE NEW PROPERTY NAME ON DATABASE * TO r")

        assertThat(alice.refused("MATCH (n:A) SET n.newer = 1"))
            .startsWith("Creating new property name on database 'neo4j' is denied")
        alice.run("MATCH (n:A) SET n.fresh = 2")
    }

    @Test
    fun `ALL DATABASE PRIVILEGES covers access, indexes, constraints and names, not stopping`() {
        server.administer("CREATE DATABASE films")
        grant("GRANT MATCH {*} ON GRAPH * TO r")
        grant("GRANT WRITE ON GRAPH * TO r")

        assertThat(alice.refused("RETURN 1", "films"))
            .startsWith("Database access is not allowed for user 'alice' with roles [PUBLIC, r].")

        grant("GRANT ALL DATABASE PRIVILEGES ON DATABASE films TO r")

        alice.run("CREATE (:Fresh {fresh: 1})", "films")
        alice.run("CREATE INDEX fresh FOR (n:Fresh) ON (n.fresh)", "films")
        alice.run("CREATE CONSTRAINT unique_fresh FOR (n:Fresh) REQUIRE n.p IS UNIQUE", "films")
        assertThat(alice.values("SHOW CONSTRAINTS YIELD name", "films"))
            .containsExactly("unique_fresh")
        assertThat(alice.refused("CREATE (:Fresh)"))
            .startsWith("Creating new node label on database 'neo4j' is not allowed")
        assertThat(alice.refused("STOP DATABASE films", SYSTEM))
            .startsWith("Permission has not been granted for STOP DATABASE")
    }

    @Test
    fun `executes procedures with EXECUTE, boosted with EXECUTE BOOSTED, admin ones so too`() {
        // PUBLIC executes every procedure, unboosted, so alice sees no label.
        assertThat(alice.values("CALL db.labels() YIELD label")).isEmpty()

        // Globs ignore case.
        grant("DENY EXECUTE PROCEDURE DB.LAB* ON DBMS TO r")

        assertThat(alice.refused("CALL db.labels()"))
            .isEqualTo(denial("Executing procedure is not allowed"))

        grant("REVOKE DENY EXECUTE PROCEDURE DB.LAB* ON DBMS FROM r")
        grant("GRANT EXECUTE BOOSTED PROCEDURE db.labels ON DBMS TO r")

        assertThat(alice.values("CALL db.labels() YIELD label RETURN label ORDER BY label"))
            .containsExactly("A", "B")

        grant("DENY EXECUTE BOOSTED PROCEDURE db.l?bels ON DBMS TO r")

        assertThat(alice.values("CALL db.labels() YIELD label")).isEmpty()

        assertThat(alice.refused("CALL dbms.listConfig('dbms.security.auth_enabled')"))
            .isEqualTo(
                procedureDenial(
                    "Executing admin procedure 'dbms.listConfig' permission has not been granted"
                )
            )

        // An admin procedure runs with EXECUTE and EXECUTE BOOSTED on it.
        grant("GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO r")

        assertThat(alice.values("CALL dbms.listConfig('dbms.security.auth_enabled') YIELD value"))
            .containsExactly("true")

        grant("REVOKE EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS FROM r")
        grant("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO r")

        assertThat(alice.values("CALL dbms.listConfig('dbms.security.auth_enabled') YIELD value"))
            .containsExactly("true")

        grant("DENY EXECUTE BOOSTED PROCEDURE dbms.* ON DBMS TO r")

        assertThat(alice.refused("CALL dbms.listConfig('dbms.security.auth_enabled')"))
            .isEqualTo(
                procedureDenial("Executing admin procedure 'dbms.listConfig' is not allowed")
            )

        // ALL DBMS PRIVILEGES boosts every procedure, and denied, executes none.
        grant("REVOKE DENY EXECUTE BOOSTED PROCEDURE dbms.* ON DBMS FROM r")
        grant("REVOKE DENY EXECUTE BOOSTED PROCEDURE db.l?bels ON DBMS FROM r")
        grant("REVOKE EXECUTE BOOSTED PROCEDURE db.labels ON DBMS FROM r")
        grant("REVOKE EXECUTE ADMIN PROCEDURES ON DBMS FROM r")
        grant("GRANT ALL DBMS PRIVILEGES ON DBMS TO r")

        assertThat(alice.values("CALL db.labels() YIELD label")).hasSize(2)
        assertThat(alice.run("CALL dbms.listConfig('dbms.security.auth_enabled')")).hasSize(1)

        grant("DENY ALL DBMS PRIVILEGES ON DBMS TO r")

        assertThat(alice.refused("CALL db.labels()"))
            .isEqualTo(denial("Executing procedure is not allowed"))
    }

    @Test
    fun `PUBLIC narrowed as the operator guide shows executes only the procedures it lists`() {
        grant("REVOKE EXECUTE PROCEDURE * ON DBMS FROM PUBLIC")
        grant("GRANT EXECUTE PROCEDURE db.*, dbms.*, tx.* ON DBMS TO PUBLIC")

        assertThat(alice.values("CALL db.labels() YIELD label")).isEmpty()
        assertThat(alice.refused("CALL test.count()"))
            .isEqualTo(denial("Executing procedure is not allowed"))

        grant("GRANT EXECUTE PROCEDURE test.* ON DBMS TO r")

        assertThat(alice.values("CALL test.count() YIELD count")).containsExactly(0L)
    }

    @Test
    fun `lists and kills the connections of others only as an admin`() {
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        // The driver keeps its connection open.
        driver("bob", "bob-secret-1").run("RETURN 1")
        val bob =
            server
                .rows("CALL dbms.listConnections() YIELD connectionId, username")
                .single { it["username"] == "bob" }["connectionId"]
        fun users() =
            alice.values(
                "CALL dbms.listConnections() YIELD username RETURN DISTINCT username ORDER BY username"
            )

        assertThat(users()).containsExactly("alice")
        // Neo4j answers as for a connection it does not know, and keeps it open.
        assertThat(alice.run("CALL dbms.killConnection('$bob') YIELD username, message"))
            .containsExactly(
                mapOf("username" to "n/a", "message" to "No connection found with this id")
            )
        assertThat(server.rows("CALL dbms.listConnections() YIELD username").map { it["username"] })
            .contains("bob")

        // As for @Admin procedures.
        grant("GRANT EXECUTE BOOSTED PROCEDURE dbms.listConnections ON DBMS TO r")

        assertThat(users()).containsExactly("alice", "bob")

        grant("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO r")
        grant("DENY EXECUTE BOOSTED PROCEDURE dbms.listConnections ON DBMS TO r")

        assertThat(users()).containsExactly("alice")

        assertThat(alice.run("CALL dbms.killConnection('$bob') YIELD username, message"))
            .containsExactly(mapOf("username" to "bob", "message" to "Connection found"))
    }

    @Test
    fun `executes user-defined functions with EXECUTE, boosted with EXECUTE BOOSTED`() {
        assertThat(alice.values("RETURN test.nodes()")).containsExactly(0L)

        grant("GRANT EXECUTE BOOSTED FUNCTION test.* ON DBMS TO r")

        assertThat(alice.values("RETURN test.nodes()")).containsExactly(2L)

        grant("DENY EXECUTE FUNCTION TEST.NODES ON DBMS TO r")

        assertThat(alice.refused("RETURN test.nodes()"))
            .isEqualTo(denial("Executing a user defined function is not allowed"))

        // Built-in functions are always executable.
        grant("DENY EXECUTE FUNCTION * ON DBMS TO r")

        assertThat(alice.values("RETURN toUpper('a')")).containsExactly("A")
    }

    @Test
    fun `lists servers with SHOW SERVERS, and changes them with SERVER MANAGEMENT`() {
        val id = server.rows("SHOW SERVERS YIELD serverId", database = SYSTEM).single()["serverId"]

        assertThat(alice.refused("SHOW SERVERS", SYSTEM))
            .startsWith("Permission has not been granted for SHOW SERVERS.")

        grant("GRANT SHOW SERVERS ON DBMS TO r")

        assertThat(alice.values("SHOW SERVERS YIELD serverId", SYSTEM)).containsExactly(id)
        assertThat(alice.refused("ALTER SERVER '$id' SET OPTIONS {tags: ['eu']}", SYSTEM))
            .startsWith("Permission has not been granted for SERVER MANAGEMENT.")
        assertThat(alice.refused("RENAME SERVER '$id' TO 'only'", SYSTEM))
            .startsWith("Permission has not been granted for SERVER MANAGEMENT.")
        assertThat(alice.refused("ENABLE SERVER '$id'", SYSTEM))
            .startsWith("Permission has not been granted for SERVER MANAGEMENT.")

        grant("REVOKE GRANT SHOW SERVERS ON DBMS FROM r")
        grant("GRANT SERVER MANAGEMENT ON DBMS TO r")

        alice.run("ALTER SERVER '$id' SET OPTIONS {tags: ['eu']}", SYSTEM)
        alice.run("RENAME SERVER '$id' TO 'only'", SYSTEM)
        alice.run("ENABLE SERVER 'only' OPTIONS {tags: ['eu']}", SYSTEM)
        // SERVER MANAGEMENT includes SHOW SERVERS.
        assertThat(alice.run("SHOW SERVERS YIELD name, tags", SYSTEM))
            .containsExactly(mapOf("name" to "only", "tags" to listOf("eu")))
    }

    @Test
    fun `shows the settings SHOW SETTING allows and does not deny`() {
        assertThat(alice.values("SHOW SETTINGS YIELD name")).isEmpty()

        grant("GRANT SHOW SETTING dbms.security.* ON DBMS TO r")

        assertThat(alice.values("SHOW SETTINGS YIELD name"))
            .contains("dbms.security.auth_enabled")
            .allMatch { (it as String).startsWith("dbms.security.") }

        grant("DENY SHOW SETTING dbms.security.auth* ON DBMS TO r")

        assertThat(alice.values("SHOW SETTINGS YIELD name")).isNotEmpty().noneMatch {
            (it as String).startsWith("dbms.security.auth")
        }

        grant("GRANT ALL DBMS PRIVILEGES ON DBMS TO r")

        assertThat(alice.values("SHOW SETTINGS YIELD name"))
            .contains("server.memory.pagecache.size")
            .doesNotContain("dbms.security.auth_enabled")
    }

    @Test
    fun `loads what LOAD allows, a file only with ALL DATA`() {
        val url = "http://127.0.0.1:${web.address.port}/films.csv"
        val file = "file:///films.csv"
        fun load(source: String) = "LOAD CSV FROM '$source' AS row RETURN row[0]"

        // PUBLIC loads all data.
        assertThat(alice.values(load(url))).containsExactly("Aliens")
        assertThat(alice.values(load(file))).containsExactly("Alien")

        grant("REVOKE LOAD ON ALL DATA FROM PUBLIC")

        assertThat(alice.refused(load(url))).isEqualTo(denial("LOAD on URL '$url' is not allowed"))

        grant("GRANT LOAD ON CIDR '127.0.0.0/8' TO r")

        assertThat(alice.values(load(url))).containsExactly("Aliens")
        // A file has no address.
        assertThat(alice.refused(load(file)))
            .startsWith("LOAD on URL 'file:///")
            .endsWith(denial("/import/films.csv' is not allowed"))

        grant("DENY LOAD ON CIDR '127.0.0.1/32' TO r")

        assertThat(alice.refused(load(url))).isEqualTo(denial("LOAD on URL '$url' is denied"))

        grant("REVOKE DENY LOAD ON CIDR '127.0.0.1/32' FROM r")
        grant("REVOKE LOAD ON CIDR '127.0.0.0/8' FROM r")
        grant("GRANT LOAD ON ALL DATA TO r")

        assertThat(alice.values(load(url))).containsExactly("Aliens")
        assertThat(alice.values(load(file))).containsExactly("Alien")

        // A CIDR range denied overrides ALL DATA.
        grant("DENY LOAD ON CIDR '127.0.0.0/24' TO r")

        assertThat(alice.refused(load(url))).isEqualTo(denial("LOAD on URL '$url' is denied"))
        assertThat(alice.values(load(file))).containsExactly("Alien")

        grant("DENY LOAD ON ALL DATA TO r")

        assertThat(alice.refused(load(file))).endsWith(denial("/import/films.csv' is denied"))
    }
}
