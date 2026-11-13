package dev.dodeca.core.logging

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlin.io.path.exists
import kotlin.io.path.readLines
import kotlin.io.path.writeText
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.coreapi.InternalTransaction

class QueryLogTest {
    @TempDir lateinit var root: Path

    private fun server(vararg settings: Pair<Setting<*>, Any>) =
        StandaloneServer(root, mapOf<Setting<*>, Any>(BoltConnector.enabled to true) + settings)

    private val StandaloneServer.queryLog: Path
        get() = home.resolve("logs/query.log")

    /** The query log's lines from now on, once [action] ran and they are written. */
    private fun StandaloneServer.linesOf(action: () -> Unit): List<String> {
        val before = if (queryLog.exists()) queryLog.readLines().size else 0
        action()
        // Bolt may log a query's end after answering the client: wait until the log is still.
        var size = -1L
        var still = 0
        while (still < 4) {
            Thread.sleep(50)
            val now = Files.size(queryLog)
            still = if (now == size) still + 1 else 0
            size = now
        }
        return queryLog.readLines().drop(before)
    }

    private fun <T : Any> StandaloneServer.set(setting: Setting<T>, value: T) =
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(Config::class.java)
            .setDynamic(setting, value, "test")

    @Test
    fun `VERBOSE, Neo4j's default, logs the start and end of every query`() {
        server().use { server ->
            server.start()
            val lines = server.linesOf { server.rows("MATCH (n) RETURN count(n) AS nodes") }

            assertThat(lines).hasSize(2)
            assertThat(lines[0])
                .matches(
                    "\\S+ \\S+ INFO  Query started: id:(\\d+) - transaction id:\\d+ - \\d+ ms: " +
                        "\\(planning: \\d+, waiting: \\d+\\) - -?\\d+ B - \\d+ page hits, \\d+ " +
                        "page faults - embedded-session\\t\\tneo4j -  - " +
                        "MATCH \\(n\\) RETURN count\\(n\\) AS nodes - \\{\\} - runtime=\\w+ - \\{\\}"
                )
            val id = Regex("id:(\\d+)").find(lines[0])!!.groupValues[1]
            assertThat(lines[1])
                .startsWith(lines[1].substringBefore("INFO") + "INFO  id:$id - transaction id:")
                .contains(" - runtime=slotted - {}")
        }
    }

    @Test
    fun `Bolt queries show their source, parameters and metadata, never passwords`() {
        server(GraphDatabaseSettings.auth_enabled to true).use { server ->
            server.start()
            server.administer("ALTER USER neo4j SET PASSWORD 'admin-secret-1' CHANGE NOT REQUIRED")
            val lines = server.linesOf {
                GraphDatabase.driver(
                        "bolt://${server.boltAddress()}",
                        org.neo4j.driver.AuthTokens.basic("neo4j", "admin-secret-1"),
                    )
                    .use { driver ->
                        driver.session().use { session ->
                            session
                                .run(
                                    "RETURN \$name AS name",
                                    mapOf("name" to "Tom 'T' Hanks", "born" to 1956),
                                    TransactionConfig.builder()
                                        .withMetadata(mapOf("app" to "test"))
                                        .build(),
                                )
                                .consume()
                        }
                        driver.session(SessionConfig.forDatabase(SYSTEM)).use { session ->
                            session
                                .run(
                                    "CREATE USER bob SET PASSWORD 'bob-secret-1' " +
                                        "CHANGE NOT REQUIRED"
                                )
                                .consume()
                            session
                                .run(
                                    "CREATE USER carol SET PASSWORD \$password " +
                                        "CHANGE NOT REQUIRED",
                                    mapOf("password" to "carol-secret-1"),
                                )
                                .consume()
                            assertThat(
                                    catchThrowable {
                                        session
                                            .run("CREATE USER dave SET PASSWRD 'dave-secret-1'")
                                            .consume()
                                    }
                                )
                                .isNotNull()
                        }
                    }
            }

            val name = lines.single { "RETURN \$name AS name" in it && "Query started" !in it }
            assertThat(name)
                .contains(" INFO  id:")
                .contains("bolt-session\tbolt\t")
                .contains(">\tneo4j - neo4j - RETURN \$name AS name - ")
                .contains("{name: 'Tom \\'T\\' Hanks', born: 1956}")
                .endsWith(" - runtime=slotted - {app: 'test'}")
            assertThat(lines.joinToString("\n"))
                .contains("CREATE USER bob SET PASSWORD ****** CHANGE NOT REQUIRED")
                .doesNotContain("admin-secret-1", "bob-secret-1", "carol-secret-1", "dave-secret-1")
            // Not parsed, so not obfuscated by Neo4j: string literals are masked, also where the
            // error message quotes the query, and parameters left out.
            val unparsed = lines.single { " ERROR " in it && "PASSWRD" in it }
            assertThat(unparsed)
                .contains("\t<none> - neo4j - CREATE USER dave SET PASSWRD '******' - runtime=null")
                .contains("\"CREATE USER dave SET PASSWRD '******'\"")
        }
    }

    @Test
    fun `failures are errors, with their reason unless errors are obfuscated`() {
        server().use { server ->
            server.start()
            val failed = server.linesOf { catchThrowable { server.rows("MATCH (n) RETURN m") } }
            assertThat(failed.single { " ERROR " in it })
                // Not obfuscated by Neo4j, so treated as not parsed: no parameters.
                .contains("MATCH (n) RETURN m - runtime=null - {} - ")
                .contains("Variable `m` not defined")

            server.set(GraphDatabaseSettings.log_queries_obfuscate_errors, true)
            val obfuscated = server.linesOf { catchThrowable { server.rows("MATCH (n) RETURN m") } }
            assertThat(obfuscated.single { " ERROR " in it })
                .doesNotContain("Variable `m`")
                .containsPattern(" - Neo\\.ClientError\\.Statement\\.SyntaxError \\(\\w{5}\\)$")
        }
    }

    @Test
    fun `INFO logs the end of queries past the threshold, OFF nothing, and changes apply at once`() {
        server(
                GraphDatabaseSettings.log_queries to LogQueryLevel.INFO,
                GraphDatabaseSettings.log_queries_threshold to Duration.ofHours(1),
            )
            .use { server ->
                server.start()
                assertThat(server.linesOf { server.rows("RETURN 1") }).isEmpty()

                server.set(GraphDatabaseSettings.log_queries_threshold, Duration.ZERO)
                val info = server.linesOf { server.rows("RETURN 2") }
                assertThat(info).singleElement().asString().contains(" INFO  transaction id:")

                server.set(GraphDatabaseSettings.log_queries, LogQueryLevel.OFF)
                assertThat(server.linesOf { server.rows("RETURN 3") }).isEmpty()

                server.set(GraphDatabaseSettings.log_queries, LogQueryLevel.VERBOSE)
                assertThat(server.linesOf { server.rows("RETURN 4") }).hasSize(2)
            }
    }

    @Test
    fun `parameters are cut to their maximum length, or left out`() {
        server(GraphDatabaseSettings.query_log_max_parameter_length to 4).use { server ->
            server.start()
            val cut = server.linesOf {
                server.rows("RETURN \$s, \$n", parameters = mapOf("s" to "abcdef", "n" to 12))
            }
            assertThat(cut.last()).contains("s: 'abc...").contains("n: 12")

            server.set(GraphDatabaseSettings.log_queries_parameter_logging_enabled, false)
            val none = server.linesOf { server.rows("RETURN \$s", parameters = mapOf("s" to "x")) }
            assertThat(none.last()).contains(" - RETURN \$s - runtime=")
        }
    }

    @Test
    fun `transactions, raw starts and plans are logged when asked`() {
        server(
                GraphDatabaseSettings.log_queries_transactions_level to LogQueryLevel.VERBOSE,
                GraphDatabaseSettings.log_queries_early_raw_logging_enabled to true,
                GraphDatabaseSettings.log_queries_query_plan to true,
            )
            .use { server ->
                server.start()
                val lines = server.linesOf { server.rows("MATCH (n:Movie) RETURN n") }

                assertThat(lines.first())
                    .contains("INFO  Transaction started: transaction id:")
                    .endsWith(" - neo4j - ")
                assertThat(lines.joinToString("\n"))
                    .contains("Query started (raw): id:")
                    .contains("Query started: id:")
                    .contains("NodeByLabelScan")
                    .containsPattern(
                        "Transaction committed: transaction id:\\d+ - \\d+ ms - neo4j - "
                    )
            }
    }

    @Test
    fun `each query is logged once, however it runs`() {
        server(HttpConnector.enabled to true).use { server ->
            server.start()
            val http = HttpClient.newHttpClient()
            val port =
                server
                    .database(SYSTEM)
                    .dependencyResolver
                    .resolveDependency(ConnectorPortRegister::class.java)
                    .getLocalAddress(ConnectorType.HTTP)
                    .port
            val queries =
                listOf(
                    "RETURN 'over http'",
                    "EXPLAIN MATCH (n) RETURN n",
                    "PROFILE MATCH (n) RETURN n",
                    "UNWIND range(1, 3) AS i CALL { WITH i CREATE (:Batch {i: i}) } " +
                        "IN TRANSACTIONS OF 1 ROW",
                )
            val lines = server.linesOf {
                val body = """{"statements": [{"statement": "${queries[0]}"}]}"""
                val response =
                    http.send(
                        HttpRequest.newBuilder(URI("http://localhost:$port/db/neo4j/tx/commit"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build(),
                        HttpResponse.BodyHandlers.ofString(),
                    )
                assertThat(response.statusCode()).isEqualTo(200)
                for (query in queries.drop(1)) server.database().executeTransactionally(query)
            }

            for (query in queries) {
                assertThat(lines.filter { " - $query - " in it }).describedAs(query).hasSize(2)
            }
            assertThat(lines.single { " - ${queries[0]} - " in it && "Query started" !in it })
                .contains("server-session\thttp\t")
        }
    }

    @Test
    fun `the JSON layout Neo4j ships writes every field`() {
        val config = Files.createDirectories(root.resolve("conf")).resolve("server-logs.xml")
        config.writeText(
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <Configuration status="ERROR">
              <Appenders>
                <RandomAccessFile name="QueryLog" fileName="${'$'}{config:server.directories.logs}/query.log">
                  <JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/QueryLogJsonLayout.json"/>
                </RandomAccessFile>
              </Appenders>
              <Loggers>
                <Root level="INFO"/>
                <Logger name="QueryLogger" level="INFO" additivity="false">
                  <AppenderRef ref="QueryLog"/>
                </Logger>
              </Loggers>
            </Configuration>
            """
                .trimIndent()
        )
        server(
                GraphDatabaseSettings.server_logging_config_path to config,
                GraphDatabaseSettings.log_queries_annotation_data_format to
                    GraphDatabaseSettings.AnnotationDataFormat.FLAT_JSON,
            )
            .use { server ->
                server.start()
                val lines = server.linesOf {
                    catchThrowable {
                        server.database().executeTransactionally("MATCH (n) RETURN m")
                    }
                    server.database().beginTx().use { tx ->
                        (tx as InternalTransaction).setMetaData(
                            mapOf("app" to mapOf("name" to "test"))
                        )
                        tx.execute("RETURN 'annotated'").close()
                        tx.commit()
                    }
                }

                val failed = lines.single { "\"event\":\"fail\"" in it }
                assertThat(failed)
                    .contains("\"level\":\"ERROR\"")
                    .contains("\"type\":\"query\"")
                    .contains("\"database\":\"neo4j\"")
                    .contains("\"query\":\"MATCH (n) RETURN m\"")
                    .doesNotContain("queryParameters")
                    .contains("\"errorInfo\":{\"GQLSTATUS\":\"")
                    .contains("\"failureReason\":\"")
                    .contains("\"executionPlanCacheKeyHash\":\"")
                val started = lines.first { "\"event\":\"start\"" in it }
                assertThat(started).contains("\"executionPlanCacheKeyHash\":\"00000000\"")
                assertThat(lines.single { "annotated" in it && "\"event\":\"success\"" in it })
                    .contains("\"annotationData\":{\"app.name\":\"test\"}")
            }
    }
}
