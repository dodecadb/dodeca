package org.dozendb.cluster.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.lifecycle.Startables

/**
 * With clustering disabled, the DozenDB jar behaves like the stock DozerDB image, except for the
 * known differences at the end, where DozenDB fixes DozerDB's defects. Every other test runs the
 * same statements on both servers and compares rows or errors.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class DisabledParityIT {
    private val stock = DozerDbContainer(DozerDbContainer.stockImage, "stock")
    private val dozenDb = DozerDbContainer(DozerDbContainer.dozenDbImage, "dozendb")
    private val servers = listOf(stock, dozenDb)

    @BeforeAll
    fun start() {
        Startables.deepStart(servers).join()
    }

    @AfterAll
    fun stop() {
        servers.forEach {
            it.saveLogs(DozerDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
    }

    private fun assertSame(cypher: String, database: String = "neo4j"): Outcome {
        val expected = stock.run(cypher, database)
        assertThat(dozenDb.run(cypher, database)).describedAs(cypher).isEqualTo(expected)
        return expected
    }

    private fun names(server: DozerDbContainer, cypher: String): Set<String> =
        server.rows(cypher, "system").map { it.getValue("name") as String }.toSet()

    private fun assertBothFail(cypher: String, database: String = "neo4j"): Outcome.Failed {
        assertThat(stock.run(cypher, database))
            .describedAs(cypher)
            .isInstanceOf(Outcome.Failed::class.java)
        val outcome = dozenDb.run(cypher, database)
        assertThat(outcome).describedAs(cypher).isInstanceOf(Outcome.Failed::class.java)
        return outcome as Outcome.Failed
    }

    @Test
    fun `runs the DozenDB entry point with clustering disabled`() {
        assertThat(dozenDb.logs).containsPattern("DozenDB \\S+: clustering is disabled")
        assertThat(stock.logs).contains("Enhanced By DozerDB Plugin").doesNotContain("DozenDB")
        assertThat(dozenDb.logs).doesNotContain("DozerDB")
    }

    @Test
    fun `reports the same components`() {
        assertSame("CALL dbms.components() YIELD name, versions, edition")
    }

    @Test
    fun `offers the same procedures and functions`() {
        assertThat(names(dozenDb, "SHOW PROCEDURES YIELD name"))
            .isEqualTo(names(stock, "SHOW PROCEDURES YIELD name"))
        assertThat(names(dozenDb, "SHOW FUNCTIONS YIELD name"))
            .isEqualTo(names(stock, "SHOW FUNCTIONS YIELD name"))
    }

    @Test
    fun `declares only the DozenDB settings on top of DozerDB's`() {
        val stockSettings = names(stock, "SHOW SETTINGS YIELD name")
        val dozenDbSettings = names(dozenDb, "SHOW SETTINGS YIELD name")

        assertThat(stockSettings - dozenDbSettings).isEmpty()
        assertThat(dozenDbSettings - stockSettings)
            .containsExactlyInAnyOrder(
                "dbms.dozendb.cluster.enabled",
                "dbms.dozendb.cluster.initial_members",
                "dbms.dozendb.cluster.election_timeout_min",
                "dbms.dozendb.cluster.election_timeout_max",
                "dbms.dozendb.routing.reads_on_leader",
                "dbms.dozendb.reads.default_consistency",
                "server.dozendb.cluster.listen_address",
                "server.dozendb.cluster.advertised_address",
                "server.dozendb.cluster.raft_directory",
                "server.dozendb.cluster.single_fsync",
            )
    }

    @Test
    fun `answers administration queries the same way`() {
        listOf(
                "SHOW DATABASES YIELD name, type, aliases, access, role, writer, requestedStatus, " +
                    "currentStatus, statusMessage, default, home, constituents RETURN * ORDER BY name",
                "SHOW DEFAULT DATABASE YIELD name, currentStatus",
                "SHOW USERS",
                "SHOW ROLES",
                "SHOW SERVERS",
                "SHOW ALIASES FOR DATABASE",
                "SHOW PRIVILEGES",
                "SHOW CURRENT USER",
            )
            .forEach { assertSame(it, "system") }
    }

    @Test
    fun `creates and drops databases the same way`() {
        assertSame("CREATE DATABASE parity", "system")
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                servers.forEach {
                    assertThat(it.rows("SHOW DATABASE parity YIELD currentStatus", "system"))
                        .containsExactly(mapOf("currentStatus" to "online"))
                }
            }
        assertSame("CREATE (:Probe {value: 1})", "parity")
        assertSame("MATCH (p:Probe) RETURN p.value AS value", "parity")
        assertSame("STOP DATABASE parity", "system")
        assertSame("START DATABASE parity", "system")
        assertSame("DROP DATABASE parity", "system")
        assertSame("SHOW DATABASES YIELD name ORDER BY name", "system")
    }

    @Test
    fun `reads, writes and enforces constraints the same way`() {
        listOf(
                "CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS UNIQUE",
                "CREATE INDEX person_age FOR (p:Person) ON (p.age)",
                "CREATE (:Person {name: 'ada', born: 1815})-[:KNOWS {since: 1833}]->" +
                    "(:Person {name: 'charles', born: 1791})",
                "CREATE (:Person {name: 'ada', born: 1815})",
                "MATCH (p:Person) RETURN p.name AS name, p.born AS born ORDER BY name",
                "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS knows, max(k.since) AS since",
                "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties " +
                    "RETURN * ORDER BY name",
                "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties " +
                    "WHERE type <> 'LOOKUP' RETURN * ORDER BY name",
            )
            .forEach { assertSame(it) }
    }

    @Test
    fun `manages users the same way`() {
        listOf(
                "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED",
                "CREATE USER alice SET PASSWORD 'alice-secret-1'",
                "SHOW USERS YIELD user, passwordChangeRequired ORDER BY user",
                "DROP USER alice",
                "SHOW USERS YIELD user ORDER BY user",
            )
            .forEach { assertSame(it, "system") }
    }

    @Test
    fun `enforces existence constraints the same way`() {
        servers.forEach { it.rows("CREATE DATABASE constraints", "system") }
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                servers.forEach {
                    assertThat(it.rows("SHOW DATABASE constraints YIELD currentStatus", "system"))
                        .containsExactly(mapOf("currentStatus" to "online"))
                }
            }
        assertSame(
            "CREATE CONSTRAINT person_born FOR (p:Person) REQUIRE p.born IS NOT NULL",
            "constraints",
        )
        assertSame("CREATE (:Person {name: 'ada', born: 1815})", "constraints")
        assertThat(assertBothFail("CREATE (:Person {name: 'nobody'})", "constraints").code)
            .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
        assertSame("MATCH (p:Person) RETURN p.name AS name", "constraints")
    }

    /**
     * Known difference: DozenDB reports failed database commands with Neo4j's messages and status
     * codes, where DozerDB has its own, and says that WAIT is not supported.
     */
    @Test
    @Order(KNOWN_DIFFERENCES)
    fun `known difference - database commands fail with Neo4j's messages`() {
        servers.forEach { it.rows("CREATE DATABASE messages", "system") }

        val exists = assertBothFail("CREATE DATABASE messages", "system")
        assertThat(exists.code).isEqualTo("Neo.ClientError.Database.ExistingDatabaseFound")
        assertThat(exists.message)
            .isEqualTo(
                "Failed to create the specified database 'messages': Database already exists."
            )
        assertThat(assertBothFail("CREATE DATABASE waiting WAIT", "system").message)
            .isEqualTo(
                "Failed to wait for the specified database 'waiting': WAIT is not supported."
            )
        assertThat(assertBothFail("STOP DATABASE missing", "system").code)
            .isEqualTo("Neo.ClientError.Database.DatabaseNotFound")
    }

    /** Known difference: `ALTER USER` fails in DozerDB, which never offers what it needs. */
    @Test
    @Order(KNOWN_DIFFERENCES)
    fun `known difference - ALTER USER works`() {
        servers.forEach {
            it.rows("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED", "system")
        }
        val alter = "ALTER USER bob SET PASSWORD CHANGE REQUIRED"

        assertThat(stock.run(alter, "system")).isInstanceOf(Outcome.Failed::class.java)
        dozenDb.rows(alter, "system")
        assertThat(
                dozenDb.rows(
                    "SHOW USERS YIELD user, passwordChangeRequired WHERE user = 'bob'",
                    "system",
                )
            )
            .containsExactly(mapOf("user" to "bob", "passwordChangeRequired" to true))
    }

    /**
     * Known difference: DozerDB stores a user's status and home database but acts on neither, so a
     * suspended user still logs in and a home database is ignored. DozenDB refuses both, as Neo4j
     * Community does.
     */
    @Test
    @Order(KNOWN_DIFFERENCES)
    fun `known difference - a user's status and home database are refused, not ignored`() {
        val suspended =
            "CREATE USER carol SET PASSWORD 'carol-secret-1' CHANGE NOT REQUIRED " +
                "SET STATUS SUSPENDED"
        val homed =
            "CREATE USER dan SET PASSWORD 'dan-secret-1' CHANGE NOT REQUIRED " +
                "SET HOME DATABASE homes"
        stock.rows("CREATE DATABASE homes", "system")

        stock.rows(suspended, "system")
        stock.rows(homed, "system")
        assertThat(stock.runAs("carol", "carol-secret-1", "RETURN 1 AS one"))
            .isEqualTo(Outcome.Rows(listOf(mapOf("one" to 1L))))
        assertThat(stock.runAs("dan", "dan-secret-1", "CALL db.info() YIELD name"))
            .isEqualTo(Outcome.Rows(listOf(mapOf("name" to "neo4j"))))

        assertThat((dozenDb.run(suspended, "system") as Outcome.Failed).message)
            .contains("'SET STATUS' is not available")
        assertThat((dozenDb.run(homed, "system") as Outcome.Failed).message)
            .contains("'HOME DATABASE' is not available")
    }

    @Test
    fun `returns the same routing table shape`() {
        fun shape(server: DozerDbContainer): List<Any?> =
            server.rows("CALL dbms.routing.getRoutingTable({}, 'neo4j')").map { row ->
                val servers =
                    (row.getValue("servers") as List<*>).map {
                        val entry = it as Map<*, *>
                        entry["role"] to (entry["addresses"] as List<*>).size
                    }
                row["ttl"] to servers
            }

        assertThat(shape(dozenDb)).isEqualTo(shape(stock))
    }

    private companion object {
        /** Known differences run last: they leave the servers' users and databases different. */
        const val KNOWN_DIFFERENCES = Int.MAX_VALUE
    }
}
