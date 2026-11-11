package org.dozendb.core.importer

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.dozendb.core.importer.IncrementState.Stage
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.AdminTool
import org.neo4j.cli.ExecutionContext
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * `neo4j-admin database import incremental`, run as `neo4j-admin` runs it, beside a running server
 * whose database it imports into.
 */
class IncrementalImportTest {
    @TempDir lateinit var root: Path

    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root)
        server.start()
        server.administer("CREATE DATABASE films WAIT")
    }

    @AfterEach fun stop() = server.stop()

    private class Result(val exitCode: Int, val out: String, val err: String) {
        override fun toString() = "exit code $exitCode\n$out\n$err"
    }

    private fun admin(vararg arguments: String): Result {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val context =
            ExecutionContext(
                server.home,
                server.home.resolve("conf"),
                PrintStream(out, true),
                PrintStream(err, true),
                InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val exitCode = AdminTool.execute(context, *arguments)
        return Result(exitCode, out.toString(), err.toString())
    }

    /** `--nodes` and `--relationships` take several files: the database comes first. */
    private fun incremental(stage: String, database: String, vararg arguments: String) =
        admin(
            "database",
            "import",
            "incremental",
            "--force",
            "--stage=$stage",
            database,
            *arguments,
        )

    private fun csv(name: String, vararg lines: String): String {
        val file = root.resolve(name)
        Files.write(file, lines.toList())
        return file.toString()
    }

    private val increment: Increment
        get() =
            Increment(
                Neo4jLayout.of(Config.defaults(GraphDatabaseSettings.neo4j_home, server.home))
                    .databaseLayout("films")
            )

    private fun keys(group: Int): Map<Any, Long> {
        val keys = mutableMapOf<Any, Long>()
        readKeys(increment.keys(group)) { key, nodeId -> keys[key] = nodeId }
        return keys
    }

    private fun nodeIds(cypher: String): Map<Any, Long> =
        server.rows(cypher, "films").associate { it["key"]!! to it["id"] as Long }

    @Test
    fun `prepare keeps the keys of the existing nodes and the store's position`() {
        server.rows(
            "CREATE CONSTRAINT person_uuid FOR (p:Person) REQUIRE p.uuid IS UNIQUE",
            "films",
        )
        server.rows("CREATE CONSTRAINT movie_id FOR (m:Movie) REQUIRE m.id IS NODE KEY", "films")
        server.rows(
            "CREATE (:Person {uuid: 'a'}), (:Person {uuid: 'b'}), (:Person {uuid: 7}), (:Person), " +
                "(:Movie {id: 1}), (:Movie {id: 2.0}), (:Movie {id: 'three'})",
            "films",
        )
        val persons =
            nodeIds("MATCH (p:Person) WHERE p.uuid IN ['a', 'b'] RETURN p.uuid AS key, id(p) AS id")
        val movies =
            nodeIds(
                "MATCH (m:Movie) WHERE m.id IN [1, 2] RETURN toInteger(m.id) AS key, id(m) AS id"
            )
        val last =
            server
                .database("films")
                .dependencyResolver
                .resolveDependency(TransactionIdStore::class.java)
                .lastCommittedTransaction
        server.administer("STOP DATABASE films WAIT")

        val result =
            incremental(
                "prepare",
                "films",
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person},name", "c,Carla"),
                "--nodes=" + csv("movies.csv", "\"id:ID(Movie){label:Movie,id-type:long}\",title"),
            )

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(result.out)
            .contains("the global ID space: nodes identified by :Person(uuid), 2 of them")
            .contains("ID space 'Movie': nodes identified by :Movie(id), 2 of them")
        val state = checkNotNull(increment.state())
        assertThat(state.stage).isEqualTo(Stage.PREPARED)
        assertThat(state.lastTransaction).isEqualTo(last.id())
        assertThat(state.lastChecksum).isEqualTo(last.checksum())
        assertThat(state.groups)
            .containsExactly(
                KeyedGroup(null, "Person", "uuid", KeyType.TEXT, 2),
                KeyedGroup("Movie", "Movie", "id", KeyType.INTEGER, 2),
            )
        assertThat(keys(0)).isEqualTo(persons)
        assertThat(keys(1)).isEqualTo(movies)
        // The DBMS that read them is gone, its links with it.
        assertThat(increment.home).doesNotExist()
        assertThat(
                Neo4jLayout.of(Config.defaults(GraphDatabaseSettings.neo4j_home, server.home))
                    .databaseLayout("films")
                    .databaseDirectory()
            )
            .isDirectory()
    }

    @Test
    fun `prepare refuses a database that is running`() {
        server.rows("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE", "films")

        val result =
            incremental(
                "prepare",
                "films",
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"),
            )

        assertThat(result.exitCode).isNotZero
        assertThat(result.err)
            .contains("Database 'films' is in use. Stop it first, with STOP DATABASE films.")
            .doesNotContain("likely in an unusable state")
        assertThat(increment.state()).isNull()
    }

    @Test
    fun `prepare refuses node keys without a uniqueness or key constraint`() {
        server.rows("CREATE INDEX FOR (p:Person) ON (p.uuid)", "films")
        server.administer("STOP DATABASE films WAIT")

        val result =
            incremental(
                "prepare",
                "films",
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"),
            )

        assertThat(result.exitCode).isNotZero
        assertThat(result.err)
            .contains(
                "Incremental import finds the existing nodes of the global ID space by " +
                    ":Person(uuid), which needs a node property uniqueness or key constraint."
            )
            .doesNotContain("likely in an unusable state")
    }

    @Test
    fun `prepare refuses node files that do not name the label of their keys`() {
        server.administer("STOP DATABASE films WAIT")

        val result =
            incremental(
                "prepare",
                "films",
                "--nodes=" + csv("persons.csv", "uuid:ID", "c"),
            )

        assertThat(result.exitCode).isNotZero
        assertThat(result.err)
            .contains("such as uuid:ID{label:Person}: No label was specified for the node index")
    }

    @Test
    fun `prepare refuses IDs that are node ids`() {
        server.administer("STOP DATABASE films WAIT")

        val result =
            incremental(
                "prepare",
                "--id-type=actual",
                "--nodes=" + csv("persons.csv", ":ID", "1"),
            )

        assertThat(result.exitCode).isNotZero
        assertThat(result.err).contains("use --id-type=string or --id-type=integer")
    }

    @Test
    fun `incremental import refuses a database that does not exist`() {
        val result =
            incremental(
                "prepare",
                "series",
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"),
            )

        assertThat(result.exitCode).isNotZero
        assertThat(result.err)
            .contains("Database 'series' does not exist: an incremental import adds to an existing")
    }
}
