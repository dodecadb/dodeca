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

    private val report: Path
        get() = root.resolve("import.report")

    /** `--nodes` and `--relationships` take several files: the database comes first. */
    private fun incremental(stage: String, database: String, vararg arguments: String) =
        admin(
            "database",
            "import",
            "incremental",
            "--force",
            "--stage=$stage",
            "--report-file=$report",
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

    /** Films with :Person {uuid} keys 'a' and 'b', the database stopped; their node ids. */
    private fun persons(): Map<Any, Long> {
        server.rows("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE", "films")
        server.rows("CREATE (:Person {uuid: 'a'}), (:Person {uuid: 'b'})", "films")
        val ids = nodeIds("MATCH (p:Person) RETURN p.uuid AS key, id(p) AS id")
        server.administer("STOP DATABASE films WAIT")
        return ids
    }

    private fun builtNodes(): List<Pair<List<String>, Map<String, Any>>> {
        val tokens = Tokens.read(increment.tokens)
        val nodes = mutableListOf<Pair<List<String>, Map<String, Any>>>()
        EntityReader(increment.nodes, relationships = false).use { reader ->
            while (true) {
                val node = reader.node() ?: break
                nodes +=
                    node.labels.map(tokens.labels::get) to
                        node.properties.associate { tokens.keys[it.key] to it.value }
            }
        }
        return nodes
    }

    private fun builtRelationships(): List<Triple<Long, String, Long>> {
        val tokens = Tokens.read(increment.tokens)
        val relationships = mutableListOf<Triple<Long, String, Long>>()
        EntityReader(increment.relationships, relationships = true).use { reader ->
            while (true) {
                val relationship = reader.relationship() ?: break
                relationships +=
                    Triple(relationship.start, tokens.types[relationship.type], relationship.end)
            }
        }
        return relationships
    }

    @Test
    fun `build reads new nodes and relationships against the existing keys`() {
        val persons = persons()
        val files =
            arrayOf(
                "--nodes=" +
                    csv("persons.csv", "uuid:ID{label:Person},name,:LABEL", "c,Carla,Person;Actor"),
                // The label of the key is added where the node lacks it.
                "--nodes=" + csv("others.csv", "uuid:ID{label:Person},born:int", "d,1970"),
                "--relationships=" +
                    csv(
                        "knows.csv",
                        ":START_ID,:END_ID,:TYPE,since:int",
                        "a,c,KNOWS,2001",
                        "c,d,KNOWS,2002",
                        "b,a,LIKES,2003",
                    ),
            )
        assertThat(incremental("prepare", "films", *files).exitCode).isZero

        val result = incremental("build", "films", *files)

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(result.out).contains("Built an increment of 2 new nodes and 3 new relationships")
        assertThat(checkNotNull(increment.state()).stage).isEqualTo(Stage.BUILT)
        assertThat(builtNodes())
            .containsExactly(
                listOf("Person", "Actor") to mapOf("uuid" to "c", "name" to "Carla"),
                listOf("Person") to mapOf("uuid" to "d", "born" to 1970L),
            )
        val c = NewRelationship.new(0)
        val d = NewRelationship.new(1)
        assertThat(builtRelationships())
            .containsExactly(
                Triple(persons["a"]!!, "KNOWS", c),
                Triple(c, "KNOWS", d),
                Triple(persons["b"]!!, "LIKES", persons["a"]!!),
            )
    }

    @Test
    fun `build fails on nodes whose keys exist, or skips them with --skip-duplicate-nodes`() {
        persons()
        val files =
            arrayOf(
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "a", "c", "c"),
                "--relationships=" + csv("knows.csv", ":START_ID,:END_ID,:TYPE", "c,a,KNOWS"),
            )
        assertThat(incremental("prepare", "films", *files).exitCode).isZero

        val failed = incremental("build", "films", *files)

        assertThat(failed.exitCode).isNotZero
        assertThat(failed.err)
            .contains("Error importing csv file: Id 'a' is defined more than once in group")
        assertThat(failed.err).doesNotContain("likely in an unusable state")
        assertThat(checkNotNull(increment.state()).stage).isEqualTo(Stage.PREPARED)

        val skipped = incremental("build", "films", "--skip-duplicate-nodes", *files)

        assertThat(skipped.exitCode).describedAs(skipped.toString()).isZero
        assertThat(skipped.out)
            .contains(
                "Built an increment of 1 new nodes and 1 new relationships, skipping 2 nodes " +
                    "whose keys exist"
            )
        val state = checkNotNull(increment.state())
        assertThat(state.nodes).isEqualTo(3)
        assertThat(state.skippedNodes).isEqualTo(2)
        assertThat(Files.readString(report)).contains("'a'").contains("'c'")
    }

    @Test
    fun `build fails on relationships to unknown keys, or skips them with --skip-bad-relationships`() {
        val persons = persons()
        val files =
            arrayOf(
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"),
                "--relationships=" +
                    csv("knows.csv", ":START_ID,:END_ID,:TYPE", "a,c,KNOWS", "c,zed,KNOWS"),
            )
        assertThat(incremental("prepare", "films", *files).exitCode).isZero

        val failed = incremental("build", "films", *files)

        assertThat(failed.exitCode).isNotZero
        assertThat(failed.err).contains("Error importing csv file: ").contains("zed")
        assertThat(failed.err).doesNotContain("likely in an unusable state")

        val skipped = incremental("build", "films", "--skip-bad-relationships", *files)

        assertThat(skipped.exitCode).describedAs(skipped.toString()).isZero
        assertThat(builtRelationships())
            .containsExactly(Triple(persons["a"]!!, "KNOWS", NewRelationship.new(0)))
        assertThat(Files.readString(report)).contains("zed")
    }

    @Test
    fun `build needs a prepared increment and the --id-type it was prepared for`() {
        persons()
        val files = arrayOf("--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"))

        val unprepared = incremental("build", "films", *files)

        assertThat(unprepared.exitCode).isNotZero
        assertThat(unprepared.err)
            .contains("There is no increment of database 'films' to build: run the prepare stage")

        assertThat(incremental("prepare", "films", *files).exitCode).isZero
        val integers = incremental("build", "films", "--id-type=integer", *files)

        assertThat(integers.exitCode).isNotZero
        assertThat(integers.err)
            .contains("Prepare read the keys for --id-type=string: build with the same --id-type.")
    }
}
