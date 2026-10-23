package org.dozendb.core

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException

/** `dbms.setConfigValue`, as an administrator and a user run it through Bolt. */
class SettingsProceduresTest {
    @TempDir lateinit var root: Path
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
        server.administer("ALTER USER neo4j SET PASSWORD 'admin-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    private val admin by lazy { driver("neo4j", "admin-secret-1") }

    private fun Driver.run(query: String, database: String = SYSTEM): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(query).list { it.asMap() }
        }

    private fun Driver.set(setting: String, value: String) =
        run("CALL dbms.setConfigValue('$setting', '$value')")

    private fun Driver.setting(name: String): Any? =
        run("SHOW SETTINGS YIELD name, value WHERE name = '$name' RETURN value").single()["value"]

    private fun Driver.failure(query: String): String =
        catchThrowableOfType(Neo4jException::class.java) { run(query) }.message!!

    @Test
    fun `an administrator changes a dynamic setting until the server restarts`() {
        admin.set("db.logs.query.enabled", "info")
        assertThat(admin.setting("db.logs.query.enabled")).isEqualTo("INFO")
        admin.set("db.logs.query.enabled", "")
        assertThat(admin.setting("db.logs.query.enabled")).isEqualTo("VERBOSE")

        admin.set("db.logs.query.threshold", "2s")
        server.restart()
        assertThat(driver("neo4j", "admin-secret-1").setting("db.logs.query.threshold"))
            .isEqualTo("0s")
    }

    @Test
    fun `a changed limit applies at once`() {
        admin.set("dbms.max_databases", "3")
        admin.run("CREATE DATABASE films WAIT")

        assertThat(admin.failure("CREATE DATABASE books WAIT"))
            .contains("The total limit of databases is already reached")
    }

    @Test
    fun `bad values, unknown and fixed settings are refused`() {
        assertThat(admin.failure("CALL dbms.setConfigValue('db.logs.query.enabled', 'yes')"))
            .contains(
                "Bad value 'yes' for setting 'db.logs.query.enabled': 'yes' not one of " +
                    "[OFF, INFO, VERBOSE]"
            )
        assertThat(admin.failure("CALL dbms.setConfigValue('db.logs.query.enabld', 'INFO')"))
            .contains("Unknown setting: db.logs.query.enabld")
        assertThat(admin.failure("CALL dbms.setConfigValue('server.metrics.prefix', 'dozen')"))
            .contains("Setting 'server.metrics.prefix' is not dynamic")
        assertThat(admin.setting("db.logs.query.enabled")).isEqualTo("VERBOSE")
    }

    @Test
    fun `only administrators may change settings`() {
        val alice = driver("alice", "alice-secret-1")

        assertThat(alice.failure("CALL dbms.setConfigValue('db.logs.query.enabled', 'OFF')"))
            .contains(
                "Executing admin procedure 'dbms.setConfigValue' permission has not been granted " +
                    "for user 'alice' with roles [PUBLIC]"
            )
        assertThat(admin.setting("db.logs.query.enabled")).isEqualTo("VERBOSE")
    }
}
