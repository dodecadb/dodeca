package dev.dodeca.core.routing

import dev.dodeca.core.StandaloneServer
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.kernel.availability.AvailabilityRequirement
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard

/**
 * A server whose `system` database is unavailable, as while it stops, cannot tell the default
 * database of a login, nor which database a routing request names that it has not looked up yet.
 * Both get the transient error of an unavailable database, as Neo4j Community gives a routing
 * request for a database that is unavailable, so that drivers ask another server; neither is logged
 * as an unexpected error, which Bolt could not send back.
 */
class RoutingWhileSystemUnavailableTest {
    @TempDir lateinit var root: Path

    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root, mapOf(BoltConnector.enabled to true))
        server.start()
    }

    @AfterEach fun stop() = server.stop()

    private fun driver(): Driver =
        GraphDatabase.driver("neo4j://${server.boltAddress()}", AuthTokens.none())

    /** What reading [database] through [driver] fails with while `system` is unavailable. */
    private fun failureWhileSystemUnavailable(driver: Driver, database: String): Throwable? {
        val guard =
            server
                .database("system")
                .dependencyResolver
                .resolveDependency(DatabaseAvailabilityGuard::class.java)
        val requirement = AvailabilityRequirement("the test")
        guard.require(requirement)
        try {
            return catchThrowable {
                driver.session(SessionConfig.forDatabase(database)).use {
                    it.run("RETURN 1").consume()
                }
            }
        } finally {
            guard.fulfill(requirement)
        }
    }

    @Test
    fun `a login while system is unavailable gets a transient error`() {
        val failure = driver().use { failureWhileSystemUnavailable(it, "neo4j") }

        assertTransient(failure)
    }

    @Test
    fun `a routing request on an open connection while system is unavailable gets a transient error`() {
        val failure =
            driver().use { driver ->
                // Logs in, which reads the default database, and keeps the connection.
                driver.session(SessionConfig.forDatabase("neo4j")).use {
                    it.run("RETURN 1").consume()
                }
                // A name not looked up yet.
                failureWhileSystemUnavailable(driver, "films")
            }

        assertTransient(failure)
    }

    private fun assertTransient(failure: Throwable?) {
        assertThat(codes(failure))
            .describedAs(failure?.stackTraceToString())
            .contains("Neo.TransientError.General.DatabaseUnavailable")
        assertThat(server.debugMessages()).noneMatch { "unexpected error" in it }
        assertThat(server.debugMessages()).noneMatch { "Unable to send error back" in it }
    }

    /**
     * The status codes of [error] and of the errors it holds: the driver keeps each router's
     * failure, as the server sent it, among its suppressed errors' causes.
     */
    private fun codes(error: Throwable?): List<String> =
        if (error == null) emptyList()
        else
            listOfNotNull(
                (error as? Neo4jException)?.code() ?: (error as? BoltFailureException)?.code()
            ) + (listOfNotNull(error.cause) + error.suppressed).flatMap(::codes)
}
