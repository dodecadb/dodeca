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
 * With clustering disabled, the DozenDB jar must behave exactly like the stock DozerDB image. Every
 * test runs the same statements on both servers and compares rows or errors.
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

    @Test
    fun `runs the DozenDB entry point with clustering disabled`() {
        assertThat(dozenDb.logs).containsPattern("DozenDB \\S+: clustering is disabled")
        servers.forEach { assertThat(it.logs).contains("Enhanced By DozerDB Plugin") }
        assertThat(stock.logs).doesNotContain("DozenDB")
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
        assertSame("CREATE DATABASE parity", "system")
        assertSame("CREATE DATABASE parity2 WAIT", "system")
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
                "ALTER USER alice SET PASSWORD CHANGE REQUIRED",
                "SHOW USERS YIELD user, passwordChangeRequired ORDER BY user",
                "DROP USER alice",
                "SHOW USERS YIELD user ORDER BY user",
            )
            .forEach { assertSame(it, "system") }
    }

    /**
     * Known difference, see `cluster/docs/COMPATIBILITY.md`: the public DozerDB source fails the
     * database when it applies an existence constraint, while the DozerDB 5.26.27.0 release accepts
     * and enforces it. Runs last, on its own database, because it leaves that database failed.
     */
    @Test
    @Order(Int.MAX_VALUE)
    fun `known difference - an existence constraint fails the database on DozenDB`() {
        servers.forEach { it.rows("CREATE DATABASE constraints", "system") }
        await atMost
            Duration.ofSeconds(60) untilAsserted
            {
                servers.forEach {
                    assertThat(it.rows("SHOW DATABASE constraints YIELD currentStatus", "system"))
                        .containsExactly(mapOf("currentStatus" to "online"))
                }
            }
        val create = "CREATE CONSTRAINT person_born FOR (p:Person) REQUIRE p.born IS NOT NULL"

        assertThat(stock.run(create, "constraints")).isInstanceOf(Outcome.Rows::class.java)
        assertThat(stock.run("CREATE (:Person {name: 'nobody'})", "constraints"))
            .isInstanceOf(Outcome.Failed::class.java)
        val outcome = dozenDb.run(create, "constraints")
        assertThat(outcome).isInstanceOf(Outcome.Failed::class.java)
        assertThat((outcome as Outcome.Failed).code)
            .isEqualTo("Neo.DatabaseError.Transaction.TransactionCommitFailed")
        assertThat(dozenDb.debugLog())
            .contains("Property existence constraint requires Neo4j Enterprise Edition")
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
}
