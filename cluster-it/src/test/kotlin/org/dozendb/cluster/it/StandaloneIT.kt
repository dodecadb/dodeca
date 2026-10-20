package org.dozendb.cluster.it

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.lifecycle.Startables

/**
 * With clustering disabled, DozenDB is Neo4j Community with many databases and property existence
 * constraints. Most tests run the same statements on DozenDB and on the official Community image
 * and compare rows or errors; the tests at the end show what DozenDB adds, which Community refuses.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class StandaloneIT {
    private val community = DozenDbContainer(DozenDbContainer.communityImage, "community")
    private val dozenDb = DozenDbContainer(DozenDbContainer.dozenDbImage, "dozendb")
    private val servers = listOf(community, dozenDb)

    @BeforeAll
    fun start() {
        Startables.deepStart(servers).join()
    }

    @AfterAll
    fun stop() {
        servers.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
    }

    private fun assertSame(cypher: String, database: String = "neo4j"): Outcome {
        val expected = community.run(cypher, database)
        assertThat(dozenDb.run(cypher, database)).describedAs(cypher).isEqualTo(expected)
        return expected
    }

    private fun names(server: DozenDbContainer, cypher: String): Set<String> =
        server.rows(cypher, "system").map { it.getValue("name") as String }.toSet()

    private fun refusedByCommunity(cypher: String, database: String = "neo4j") {
        assertThat(community.run(cypher, database))
            .describedAs("$cypher on Community")
            .isInstanceOf(Outcome.Failed::class.java)
    }

    private fun failed(cypher: String, database: String = "neo4j"): Outcome.Failed {
        val outcome = dozenDb.run(cypher, database)
        assertThat(outcome).describedAs(cypher).isInstanceOf(Outcome.Failed::class.java)
        return outcome as Outcome.Failed
    }

    private fun statuses(): Map<String, String> =
        dozenDb.rows("SHOW DATABASES YIELD name, currentStatus", "system").associate {
            it["name"] as String to it["currentStatus"] as String
        }

    @Test
    fun `runs the DozenDB entry point with clustering disabled`() {
        assertThat(dozenDb.logs).containsPattern("DozenDB \\S+: clustering is disabled")
        assertThat(community.logs).doesNotContain("DozenDB")
    }

    /** As DozerDB did, so that tools offer many databases. */
    @Test
    fun `reports the Enterprise edition`() {
        assertSame("CALL dbms.components() YIELD name, versions")
        assertThat(community.rows("CALL dbms.components() YIELD edition"))
            .containsExactly(mapOf("edition" to "community"))
        assertThat(dozenDb.rows("CALL dbms.components() YIELD edition"))
            .containsExactly(mapOf("edition" to "enterprise"))
    }

    @Test
    fun `offers the same procedures and functions`() {
        assertThat(names(dozenDb, "SHOW PROCEDURES YIELD name"))
            .isEqualTo(names(community, "SHOW PROCEDURES YIELD name"))
        assertThat(names(dozenDb, "SHOW FUNCTIONS YIELD name"))
            .isEqualTo(names(community, "SHOW FUNCTIONS YIELD name"))
    }

    @Test
    fun `declares only the DozenDB settings on top of Community's`() {
        val communitySettings = names(community, "SHOW SETTINGS YIELD name")
        val dozenDbSettings = names(dozenDb, "SHOW SETTINGS YIELD name")

        assertThat(communitySettings - dozenDbSettings).isEmpty()
        assertThat(dozenDbSettings - communitySettings)
            .containsExactlyInAnyOrder(
                "dbms.max_databases",
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
    fun `reads, writes and enforces uniqueness the same way`() {
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
                "CREATE USER carol SET PASSWORD 'carol-secret-1' SET STATUS SUSPENDED",
                "CREATE USER dan SET PASSWORD 'dan-secret-1' SET HOME DATABASE neo4j",
                "SHOW USERS YIELD user, passwordChangeRequired ORDER BY user",
                "DROP USER alice",
                "SHOW USERS YIELD user ORDER BY user",
            )
            .forEach { assertSame(it, "system") }
    }

    @Test
    fun `returns the same routing table shape`() {
        fun shape(server: DozenDbContainer): List<Any?> =
            server.rows("CALL dbms.routing.getRoutingTable({}, 'neo4j')").map { row ->
                val servers =
                    (row.getValue("servers") as List<*>).map {
                        val entry = it as Map<*, *>
                        entry["role"] to (entry["addresses"] as List<*>).size
                    }
                row["ttl"] to servers
            }

        assertThat(shape(dozenDb)).isEqualTo(shape(community))
    }

    @Test
    @Order(ADDITIONS)
    fun `creates, stops, starts and drops databases`() {
        refusedByCommunity("CREATE DATABASE films", "system")

        dozenDb.rows("CREATE DATABASE films", "system")
        assertThat(statuses()).containsEntry("films", "online")
        dozenDb.rows("CREATE (:Film {title: 'Alien'})", "films")
        dozenDb.rows("STOP DATABASE films", "system")
        assertThat(statuses()).containsEntry("films", "offline")
        dozenDb.rows("START DATABASE films", "system")
        assertThat(dozenDb.rows("MATCH (f:Film) RETURN f.title AS title", "films"))
            .containsExactly(mapOf("title" to "Alien"))
        dozenDb.rows("DROP DATABASE films", "system")
        assertThat(statuses()).doesNotContainKey("films")
    }

    @Test
    @Order(ADDITIONS)
    fun `enforces property existence constraints`() {
        val create = "CREATE CONSTRAINT person_born FOR (p:Person) REQUIRE p.born IS NOT NULL"
        refusedByCommunity(create)
        dozenDb.rows("CREATE DATABASE constraints", "system")
        dozenDb.rows("CREATE (:Person {name: 'unborn'})", "constraints")

        assertThat(failed(create, "constraints").code)
            .isEqualTo("Neo.DatabaseError.Schema.ConstraintCreationFailed")
        dozenDb.rows("MATCH (p:Person) SET p.born = 1900", "constraints")
        dozenDb.rows(create, "constraints")
        assertThat(failed("CREATE (:Person {name: 'nobody'})", "constraints").code)
            .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
    }

    @Test
    @Order(ADDITIONS)
    fun `database commands fail with Neo4j's status codes`() {
        dozenDb.rows("CREATE DATABASE messages", "system")

        assertThat(failed("CREATE DATABASE messages", "system").code)
            .isEqualTo("Neo.ClientError.Database.ExistingDatabaseFound")
        assertThat(failed("STOP DATABASE missing", "system").code)
            .isEqualTo("Neo.ClientError.Database.DatabaseNotFound")
        assertThat(failed("CREATE DATABASE waiting WAIT", "system").message)
            .isEqualTo(
                "Failed to wait for the specified database 'waiting': WAIT is not supported."
            )
        assertThat(failed("DROP DATABASE neo4j", "system").message)
            .isEqualTo("Not allowed to delete the default database 'neo4j'.")
    }

    private companion object {
        /** Run last: they leave the servers' databases different. */
        const val ADDITIONS = Int.MAX_VALUE
    }
}
