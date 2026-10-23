package org.dozendb.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.images.builder.Transferable
import org.testcontainers.lifecycle.Startables

/**
 * The query log and the security log in the files Neo4j's image configures, as text and, with the
 * JSON layouts Neo4j ships configured in `server-logs.xml`, as JSON.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogsIT {
    private val text = DozenDbContainer(DozenDbContainer.dozenDbImage, "text")
    private val json =
        DozenDbContainer(DozenDbContainer.dozenDbImage, "json")
            .withCopyToContainer(
                Transferable.of(JSON_LOGS.toByteArray(), DozenDbContainer.READABLE),
                "/var/lib/neo4j/conf/server-logs.xml",
            )
    private val servers = listOf(text, json)

    @BeforeAll
    fun start() {
        Startables.deepStart(servers).join()
        for (server in servers) {
            server.rows("CREATE (:Movie {title: \$title})", parameters = mapOf("title" to "Alien"))
            server.rows(
                "CREATE USER carol SET PASSWORD 'carol-secret-1' CHANGE NOT REQUIRED",
                "system",
            )
            assertThat(server.runAs("carol", "wrong-secret-1", "RETURN 1"))
                .isInstanceOf(Outcome.Failed::class.java)
        }
    }

    @AfterAll
    fun stop() {
        servers.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
    }

    /** The lines of the log file [name] of [server], once they contain [expected]. */
    private fun lines(
        server: DozenDbContainer,
        name: String,
        vararg expected: String,
    ): List<String> {
        var lines = emptyList<String>()
        await withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofSeconds(30) untilAsserted
            {
                lines = server.logFile(name).lines()
                for (text in expected) {
                    assertThat(lines).describedAs("$name of ${server.alias}").anyMatch {
                        text in it
                    }
                }
            }
        return lines
    }

    @Test
    fun `the query log has each query with its parameters, as text`() {
        val lines = lines(text, "query.log", "CREATE (:Movie {title: \$title})")

        val query = lines.last { "CREATE (:Movie {title: \$title})" in it }
        assertThat(query)
            .contains("INFO  id:")
            .contains(">\tneo4j - neo4j - CREATE (:Movie {title: \$title}) - {title: 'Alien'} - ")
        assertThat(lines.joinToString("\n")).doesNotContain("carol-secret-1")
    }

    @Test
    fun `the query log has each query with its parameters, as JSON`() {
        val lines = lines(json, "query.log", "CREATE (:Movie {title: \$title})")

        val query = lines.last {
            "CREATE (:Movie {title: \$title})" in it && "\"event\":\"success\"" in it
        }
        assertThat(query)
            .startsWith("{")
            .contains("\"type\":\"query\"")
            .contains("\"database\":\"neo4j\"")
            .contains("\"executingUser\":\"neo4j\"")
            .contains("\"queryParameters\":\"{title: 'Alien'}\"")
        assertThat(lines.joinToString("\n")).doesNotContain("carol-secret-1")
    }

    @Test
    fun `the security log has logins and administration commands, as text`() {
        val lines =
            lines(
                text,
                "security.log",
                "[carol]: failed to log in",
                "[neo4j]: CREATE USER carol",
            )

        assertThat(lines.map { it.substringAfter("+0000 ") })
            .contains(
                "INFO  [neo4j]: logged in",
                "ERROR [carol]: failed to log in: invalid principal or credentials",
            )
        assertThat(lines.single { "CREATE USER carol" in it })
            .contains("INFO  [neo4j]: CREATE USER carol SET PASSWORD '******' CHANGE NOT REQUIRED")
        assertThat(lines.joinToString("\n")).doesNotContain("secret-1")
    }

    @Test
    fun `the security log has logins and administration commands, as JSON`() {
        val lines =
            lines(
                json,
                "security.log",
                "failed to log in: invalid principal or credentials",
                "CREATE USER carol",
            )

        assertThat(lines.single { "failed to log in" in it })
            .startsWith("{")
            .contains("\"level\":\"ERROR\"")
            .contains("\"type\":\"security\"")
            .contains("\"executingUser\":\"carol\"")
            .contains("\"message\":\"failed to log in: invalid principal or credentials\"")
        assertThat(lines.single { "CREATE USER carol" in it })
            .contains("\"level\":\"INFO\"")
            .contains("\"executingUser\":\"neo4j\"")
            .contains("\"message\":\"CREATE USER carol SET PASSWORD '******' CHANGE NOT REQUIRED\"")
        assertThat(lines.joinToString("\n")).doesNotContain("secret-1")
    }

    companion object {
        /** The image's logging configuration, with Neo4j's JSON layouts for the two logs. */
        private val JSON_LOGS =
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <Configuration status="ERROR" monitorInterval="30">
                <Appenders>
                    <RollingRandomAccessFile name="DebugLog" fileName="${'$'}{config:server.directories.logs}/debug.log"
                                             filePattern="${'$'}${'$'}{config:server.directories.logs}/debug.log.%02i">
                        <Neo4jDebugLogLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p [%c{1.}] %m%n"/>
                        <Policies>
                            <SizeBasedTriggeringPolicy size="20 MB"/>
                        </Policies>
                        <DefaultRolloverStrategy fileIndex="min" max="7"/>
                    </RollingRandomAccessFile>
                    <RollingRandomAccessFile name="QueryLog" fileName="${'$'}{config:server.directories.logs}/query.log"
                                             filePattern="${'$'}${'$'}{config:server.directories.logs}/query.log.%02i">
                        <JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/QueryLogJsonLayout.json"/>
                        <Policies>
                            <SizeBasedTriggeringPolicy size="20 MB"/>
                        </Policies>
                        <DefaultRolloverStrategy fileIndex="min" max="7"/>
                    </RollingRandomAccessFile>
                    <RollingRandomAccessFile name="SecurityLog" fileName="${'$'}{config:server.directories.logs}/security.log"
                                             filePattern="${'$'}${'$'}{config:server.directories.logs}/security.log.%02i">
                        <JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/StructuredJsonLayout.json"/>
                        <Policies>
                            <SizeBasedTriggeringPolicy size="20 MB"/>
                        </Policies>
                        <DefaultRolloverStrategy fileIndex="min" max="7"/>
                    </RollingRandomAccessFile>
                </Appenders>
                <Loggers>
                    <Root level="INFO">
                        <AppenderRef ref="DebugLog"/>
                    </Root>
                    <Logger name="QueryLogger" level="INFO" additivity="false">
                        <AppenderRef ref="QueryLog"/>
                    </Logger>
                    <Logger name="SecurityLogger" level="INFO" additivity="false">
                        <AppenderRef ref="SecurityLog"/>
                    </Logger>
                </Loggers>
            </Configuration>
            """
                .trimIndent()
    }
}
