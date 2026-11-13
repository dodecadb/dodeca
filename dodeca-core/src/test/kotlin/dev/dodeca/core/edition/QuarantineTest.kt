package dev.dodeca.core.edition

import dev.dodeca.core.StandaloneServer
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.monitoring.DatabaseHealth

/**
 * `dbms.quarantineDatabase` and `dbms.unquarantineDatabase` without clustering, and the quarantine
 * of a database that panics, as Neo4j Enterprise documents them.
 */
class QuarantineTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
        server.administer("CREATE DATABASE films")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun quarantine(
        database: String,
        set: Boolean,
        reason: String? = null,
        on: String = StandaloneServer.DEFAULT_DATABASE,
    ): Map<String, Any?> =
        server
            .rows(
                // Enterprise's Cypher 25 has no dbms.quarantineDatabase.
                if (reason == null) "CYPHER 5 CALL dbms.quarantineDatabase(\$name, \$set)"
                else "CYPHER 5 CALL dbms.quarantineDatabase(\$name, \$set, \$reason)",
                on,
                mapOf("name" to database, "set" to set) +
                    (reason?.let { mapOf("reason" to it) } ?: emptyMap()),
            )
            .single()

    private fun unquarantine(server: String, database: String, operation: String? = null) {
        this.server.rows(
            if (operation == null) "CALL dbms.unquarantineDatabase(\$server, \$name)"
            else "CALL dbms.unquarantineDatabase(\$server, \$name, \$operation)",
            StandaloneServer.DEFAULT_DATABASE,
            mapOf("server" to server, "name" to database) +
                (operation?.let { mapOf("operation" to it) } ?: emptyMap()),
        )
    }

    /** This server's [column] in SHOW SERVERS, `serverId` or `name`. */
    private fun self(column: String = "serverId"): String =
        server.rows("SHOW SERVERS YIELD *", StandaloneServer.SYSTEM).single()[column] as String

    private fun row(database: String): Map<String, Any?> =
        server
            .rows(
                "SHOW DATABASE $database YIELD requestedStatus, currentStatus, statusMessage, " +
                    "role, writer",
                StandaloneServer.SYSTEM,
            )
            .single()

    private fun films(): Long =
        server.rows("MATCH (f:Film) RETURN count(f) AS films", "films").single()["films"] as Long

    private fun started(database: String): Boolean = server.dbms!!.database(database).isAvailable

    @Test
    fun `a database in quarantine is stopped and stays stopped until the quarantine is lifted`() {
        val quarantined = quarantine("films", true, "Inspecting its files")

        assertThat(quarantined)
            .containsEntry("databaseName", "films")
            .containsEntry("quarantined", true)
        assertThat(quarantined["result"] as String)
            .matches(
                "By anonymous at \\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z: Inspecting its files"
            )
        assertThat(row("films"))
            .containsEntry("requestedStatus", "online")
            .containsEntry("currentStatus", "quarantined")
            .containsEntry("statusMessage", quarantined["result"])
            .containsEntry("role", "unknown")
            .containsEntry("writer", false)
        assertThat(started("films")).isFalse()

        server.administer("START DATABASE films")
        server.administer("STOP DATABASE films")
        server.administer("START DATABASE films")
        assertThat(row("films")).containsEntry("currentStatus", "quarantined")
        assertThat(started("films")).isFalse()

        val lifted = quarantine("films", false)
        assertThat(lifted)
            .containsEntry("databaseName", "films")
            .containsEntry("quarantined", false)
        assertThat(lifted["result"] as String).endsWith(": Lifted the quarantine")
        assertThat(row("films"))
            .containsEntry("currentStatus", "online")
            .containsEntry("statusMessage", "")
            .containsEntry("role", "primary")
        assertThat(films()).isEqualTo(1)
    }

    @Test
    fun `a call on the database it puts in quarantine stops it once the call ends`() {
        val quarantined = quarantine("films", true, on = "films")

        assertThat(quarantined).containsEntry("quarantined", true)
        assertThat(row("films")).containsEntry("currentStatus", "quarantined")
        await atMost Duration.ofSeconds(30) untilAsserted { assertThat(started("films")).isFalse() }
    }

    @Test
    fun `lifting brings the database to the status the system graph asks for`() {
        quarantine("films", true)
        server.administer("STOP DATABASE films")

        quarantine("films", false)

        assertThat(row("films")).containsEntry("currentStatus", "offline")
    }

    @Test
    fun `quarantining again replaces the reason, and the message says when none is given`() {
        quarantine("films", true, "first")

        val again = quarantine("films", true)

        assertThat(again["result"] as String).endsWith(": No reason given")
        assertThat(row("films")).containsEntry("statusMessage", again["result"])
        assertThat(quarantine("neo4j", false))
            .containsEntry("quarantined", false)
            .containsEntry("result", "The database is in no quarantine here")
    }

    @Test
    fun `a quarantine is kept across restarts, until lifted`() {
        val message = quarantine("films", true, "Inspecting its files")["result"]

        server.restart()

        assertThat(row("films"))
            .containsEntry("currentStatus", "quarantined")
            .containsEntry("statusMessage", message)
        assertThat(started("films")).isFalse()
        quarantine("films", false)
        assertThat(row("films")).containsEntry("currentStatus", "online")
        assertThat(films()).isEqualTo(1)
    }

    @Test
    fun `the quarantine of system is lifted by a restart, as in Neo4j Enterprise`() {
        quarantine("system", true, "Inspecting it")
        assertThat(started("system")).isFalse()
        assertThat(quarantineFile()).doesNotExist()

        server.restart()

        assertThat(started("system")).isTrue()
        assertThat(row("system")).containsEntry("currentStatus", "online")
    }

    @Test
    fun `lifting the quarantine of system starts it again`() {
        quarantine("system", true)

        quarantine("system", false)

        assertThat(row("system")).containsEntry("currentStatus", "online")
        server.administer("CREATE DATABASE books")
        assertThat(row("books")).containsEntry("currentStatus", "online")
    }

    @Test
    fun `a database that panics is put in quarantine, kept across restarts`() {
        server
            .database("films")
            .dependencyResolver
            .resolveDependency(DatabaseHealth::class.java)
            .panic(IOException("No space left on device"))

        val message = row("films")["statusMessage"] as String
        assertThat(message)
            .startsWith("By the server at ")
            .contains(
                "Database 'films' failed while running: No space left on device. Once its " +
                    "cause is dealt with, lift the quarantine with CALL " +
                    "dbms.unquarantineDatabase('${self()}', 'films')"
            )
        assertThat(row("films")).containsEntry("currentStatus", "quarantined")
        await atMost Duration.ofSeconds(30) untilAsserted { assertThat(started("films")).isFalse() }
        assertThat(row("neo4j")).containsEntry("currentStatus", "online")
        server.administer("STOP DATABASE films")
        server.administer("START DATABASE films")
        assertThat(row("films")).containsEntry("currentStatus", "quarantined")

        server.restart()
        assertThat(row("films"))
            .containsEntry("currentStatus", "quarantined")
            .containsEntry("statusMessage", message)

        quarantine("films", false)
        assertThat(row("films")).containsEntry("currentStatus", "online")
        assertThat(films()).isEqualTo(1)
    }

    @Test
    fun `dropping a database in quarantine drops its quarantine too`() {
        quarantine("films", true)

        server.administer("DROP DATABASE films")
        assertThat(Files.readString(quarantineFile())).isEmpty()
        server.administer("CREATE DATABASE films")

        assertThat(row("films")).containsEntry("currentStatus", "online")
    }

    @Test
    fun `dbms_unquarantineDatabase lifts the quarantine on this server, named by its id or name`() {
        quarantine("films", true)

        unquarantine(self(), "films")

        assertThat(row("films"))
            .containsEntry("currentStatus", "online")
            .containsEntry("statusMessage", "")
        assertThat(films()).isEqualTo(1)
        quarantine("films", true)
        unquarantine(self("name"), "films", "keepStateKeepStore")
        assertThat(row("films")).containsEntry("currentStatus", "online")
    }

    @Test
    fun `dbms_unquarantineDatabase refuses what it cannot do, as Neo4j Enterprise does`() {
        quarantine("films", true)

        fun refusal(server: String, database: String, operation: String? = null): String {
            val refused = catchThrowable { unquarantine(server, database, operation) }
            val statuses =
                generateSequence(refused) { it.cause }
                    .filterIsInstance<ErrorGqlStatusObject>()
                    .first()
                    .let { first -> generateSequence(first) { it.cause().orElse(null) } }
                    .map { it.gqlStatus() }
                    .toList()
            assertThat(statuses).startsWith("52N02", "52N17")
            return generateSequence(refused) { it.cause }
                .mapNotNull { it.message }
                .first {
                    it.startsWith("Setting/removing the quarantine marker failed: ")
                }
        }

        assertThat(refusal("0c000000", "films")).contains("Cannot find server '0c000000'")
        assertThat(refusal(self(), "neo4j")).contains("Database 'neo4j' is not in quarantine")
        assertThat(refusal(self(), "books")).contains("Database does not exist: books")
        assertThat(refusal(self(), "films", "keepEverything"))
            .contains(
                "Unknown operation 'keepEverything': the operations are keepStateKeepStore, " +
                    "replaceStateKeepStore and replaceStateReplaceStore"
            )
        for (operation in listOf("replaceStateKeepStore", "replaceStateReplaceStore")) {
            assertThat(refusal(self(), "films", operation))
                .contains("a server without clustering has no cluster state to replace")
        }
        assertThat(row("films")).containsEntry("currentStatus", "quarantined")
    }

    @Test
    fun `Cypher 25 has no dbms_quarantineDatabase, as Neo4j Enterprise's, and Cypher 5 warns`() {
        val missing = catchThrowable {
            server.rows("CYPHER 25 CALL dbms.quarantineDatabase('films', true)")
        }
        assertThat(generateSequence(missing) { it.cause }.mapNotNull { it.message }.toList())
            .anySatisfy { assertThat(it).contains("dbms.quarantineDatabase") }
        assertThat(row("films")).containsEntry("currentStatus", "online")

        val warnings =
            server.database().beginTx().use { tx ->
                tx.execute("CYPHER 5 CALL dbms.quarantineDatabase('films', true)").use { result ->
                    result.resultAsString()
                    result.gqlStatusObjects.map { it.gqlStatus() to it.statusDescription() }
                }
            }
        assertThat(warnings).anySatisfy { (status, description) ->
            assertThat(status).startsWith("01N")
            assertThat(description).contains("dbms.unquarantineDatabase")
        }
        assertThat(row("films")).containsEntry("currentStatus", "quarantined")
    }

    @Test
    fun `refuses a database that does not exist`() {
        val unknown = catchThrowable { quarantine("books", true) }

        assertThat(generateSequence(unknown) { it.cause }.mapNotNull { it.message }.toList())
            .anySatisfy { assertThat(it).contains("Database does not exist: books") }
    }

    private fun quarantineFile(): Path = server.home.resolve("data").resolve(Quarantines.FILE_NAME)
}
