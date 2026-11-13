package org.dozendb.core.importer

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.StandaloneServer
import org.dozendb.core.importer.IncrementState.Stage
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.cli.AdminTool
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.files.LogFilesBuilder

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

    @ParameterizedTest
    @CsvSource(
        delimiter = '|',
        value =
            [
                "uuid:ID{label:Person},code:ID,name|c,1,Carla|--id-type=string",
                "uuid:ID{label:Person},code:ID{label:Person},name|a,1,Ann|--id-type=string",
                "uuid:ID{label:Person},code:ID,name|3,1,Carla|--id-type=integer",
            ],
    )
    fun `incremental import refuses nodes with several ID columns, as Enterprise imports them in full only`(
        header: String,
        line: String,
        idType: String,
    ) {
        persons()
        val nodes = "--nodes=" + csv("persons.csv", header, line)

        // Prepare reads the headers only; the nodes show their IDs.
        val prepared = incremental("prepare", idType, "films", nodes)
        assertThat(prepared.exitCode).describedAs(prepared.toString()).isZero
        for (stage in listOf("build", "all")) {
            val result = incremental(stage, idType, "films", nodes)

            assertThat(result.exitCode).describedAs(result.toString()).isNotZero
            assertThat(result.err)
                .contains(
                    "incremental import takes one ID column per node file, which names the " +
                        "label and property key of the nodes' keys, such as " +
                        "uuid:ID{label:Person}. Several ID columns work in full imports only."
                )
        }
        server.administer("START DATABASE films WAIT")
        assertThat(server.rows("MATCH (p:Person) RETURN p.uuid AS uuid ORDER BY uuid", "films"))
            .containsExactly(mapOf("uuid" to "a"), mapOf("uuid" to "b"))
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
    fun `a dry run checks and estimates the input, needs no --force, and writes nothing`() {
        persons()
        val files =
            arrayOf(
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person},name", "c,Carla", "d,Dan"),
                "--relationships=" + csv("knows.csv", ":START_ID,:END_ID,:TYPE", "c,a,KNOWS"),
            )

        val dryRun = admin("database", "import", "incremental", "--dry-run", "films", *files)

        assertThat(dryRun.exitCode).describedAs("$dryRun").isZero
        assertThat(dryRun.out)
            .contains(
                "Checking the contents of the following files:",
                "Estimated entity counts:",
                "  Nodes: ",
                "  Relationships: ",
            )
        assertThat(increment.directory).doesNotExist()
        server.administer("START DATABASE films WAIT")
        assertThat(server.rows("MATCH (n) RETURN count(n) AS c", "films").single()["c"])
            .isEqualTo(2L)

        val merge =
            admin("database", "import", "incremental", "--dry-run", "--stage=merge", "films")
        assertThat(merge.exitCode).isEqualTo(ExitCode.USAGE)
        assertThat(merge.err).contains("--dry-run checks the input of an incremental import")
        val unforced = admin("database", "import", "incremental", "films", *files)
        assertThat(unforced.exitCode).isEqualTo(ExitCode.USAGE)
        assertThat(unforced.err).contains("Missing required option: '--force'")
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
            .contains("Error importing csv file.")
            .contains("'a' is defined more than once in group")
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
        assertThat(failed.err)
            .contains("Error importing csv file.")
            .contains("referring to missing node zed")
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

    private val filmsLayout
        get() =
            Neo4jLayout.of(Config.defaults(GraphDatabaseSettings.neo4j_home, server.home))
                .databaseLayout("films")

    /** Stops films and checks its consistency. */
    private fun assertConsistent() {
        server.administer("STOP DATABASE films WAIT")
        val config =
            Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, server.home)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .build()
        assertThat(
                ConsistencyCheckService(filmsLayout)
                    .with(config)
                    .runFullConsistencyCheck()
                    .isSuccessful
            )
            .describedAs("films is consistent")
            .isTrue
    }

    private fun count(cypher: String): Long = server.rows(cypher, "films").single()["c"] as Long

    private fun plan(cypher: String): String =
        server.database("films").executeTransactionally("EXPLAIN $cypher", emptyMap()) {
            it.executionPlanDescription.toString()
        }

    @Test
    fun `all merges into the database, which keeps its indexes, constraints and counts`() {
        server.rows("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE", "films")
        server.rows("CREATE INDEX person_name FOR (p:Person) ON (p.name)", "films")
        server.rows("CREATE INDEX knows_since FOR ()-[k:KNOWS]-() ON (k.since)", "films")
        server.rows(
            "CREATE (:Person {uuid: 'a', name: 'Ann'})-[:KNOWS {since: 1999}]->" +
                "(:Person {uuid: 'b', name: 'Bob'})",
            "films",
        )
        server.administer("STOP DATABASE films WAIT")

        val result =
            admin(
                "database",
                "import",
                "incremental",
                "--force",
                "--skip-duplicate-nodes",
                "--report-file=$report",
                "films",
                "--nodes=" +
                    csv(
                        "persons.csv",
                        "uuid:ID{label:Person},name,:LABEL,born:int,tags:string[],when:date," +
                            "loc:point",
                        "c,Carla,Person;Actor,1970,x;y,2026-11-10,\"{x: 1.0, y: 2.0}\"",
                        "d,Dan,,,,,",
                        "a,Again,,,,,",
                    ),
                "--relationships=" +
                    csv(
                        "knows.csv",
                        ":START_ID,:END_ID,:TYPE,since:int",
                        "a,c,KNOWS,2001",
                        "c,d,KNOWS,2002",
                        "b,a,LIKES,2003",
                    ),
            )

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(result.out)
            .contains("Merging 2 new nodes and 3 new relationships into database 'films'")
            .contains("Merged the increment into database 'films'")
        assertThat(increment.directory).doesNotExist()
        server.administer("START DATABASE films WAIT")
        assertThat(count("MATCH (p:Person) RETURN count(p) AS c")).isEqualTo(4)
        assertThat(count("MATCH ()-[k:KNOWS]->() RETURN count(k) AS c")).isEqualTo(3)
        assertThat(count("MATCH (:Person)-[l:LIKES]->(:Person) RETURN count(l) AS c")).isEqualTo(1)
        assertThat(
                server.rows(
                    "MATCH (a:Person {uuid: 'a'})-[:KNOWS]->(c:Person {uuid: 'c'})-[:KNOWS]->" +
                        "(d:Person {uuid: 'd'}), (:Person {uuid: 'b'})-[:LIKES]->(a) " +
                        "RETURN a.name AS a, labels(c) AS labels, c.born AS born, " +
                        "[t IN c.tags | t] AS tags, " +
                        "toString(c.when) AS when, c.loc.x AS x, d.name AS d",
                    "films",
                )
            )
            .containsExactly(
                mapOf(
                    "a" to "Ann",
                    "labels" to listOf("Person", "Actor"),
                    "born" to 1970L,
                    "tags" to listOf("x", "y"),
                    "when" to "2026-11-10",
                    "x" to 1.0,
                    "d" to "Dan",
                )
            )
        assertThat(plan("MATCH (p:Person {uuid: 'd'}) RETURN p")).contains("NodeUniqueIndexSeek")
        assertThat(plan("MATCH (p:Person {name: 'Dan'}) RETURN p")).contains("NodeIndexSeek")
        assertThat(count("MATCH (p:Person) WHERE p.name = 'Dan' RETURN count(p) AS c")).isEqualTo(1)
        assertThat(count("MATCH ()-[k:KNOWS]->() WHERE k.since = 2002 RETURN count(k) AS c"))
            .isEqualTo(1)
        assertThatThrownBy { server.rows("CREATE (:Person {uuid: 'c'})", "films") }
            .hasMessageContaining("already exists")
        assertConsistent()
    }

    @Test
    fun `build reads the input while the database runs read-only`() {
        persons()
        val files =
            arrayOf(
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"),
                "--relationships=" + csv("knows.csv", ":START_ID,:END_ID,:TYPE", "a,c,KNOWS"),
            )
        assertThat(incremental("prepare", "films", *files).exitCode).isZero
        server.administer("ALTER DATABASE films SET ACCESS READ ONLY")
        server.administer("START DATABASE films WAIT")

        assertThat(incremental("build", "films", *files).exitCode).isZero
        assertThat(count("MATCH (p:Person) RETURN count(p) AS c")).isEqualTo(2)

        server.administer("STOP DATABASE films WAIT")
        val merged = incremental("merge", "films")
        assertThat(merged.exitCode).describedAs(merged.toString()).isZero
        server.administer("ALTER DATABASE films SET ACCESS READ WRITE")
        server.administer("START DATABASE films WAIT")
        assertThat(
                count(
                    "MATCH (:Person {uuid: 'a'})-[k:KNOWS]->(:Person {uuid: 'c'}) RETURN count(k) AS c"
                )
            )
            .isEqualTo(1)
        assertConsistent()
    }

    @Test
    fun `merge refuses a database that changed since prepare`() {
        persons()
        val files = arrayOf("--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c"))
        assertThat(incremental("prepare", "films", *files).exitCode).isZero
        assertThat(incremental("build", "films", *files).exitCode).isZero
        server.administer("START DATABASE films WAIT")
        server.rows("CREATE (:Person {uuid: 'z'})", "films")
        server.administer("STOP DATABASE films WAIT")

        val merged = incremental("merge", "films")

        assertThat(merged.exitCode).isNotZero
        assertThat(merged.err)
            .contains("Database 'films' changed since the prepare stage read it")
            .contains("Run the prepare and build stages again.")
        server.administer("START DATABASE films WAIT")
        assertThat(count("MATCH (p:Person) RETURN count(p) AS c")).isEqualTo(3)
    }

    /** Runs the merge of films in this JVM, in batches of [batchSize], calling [onBatch]. */
    private fun merge(batchSize: Int, onBatch: (Boolean) -> Unit = {}): String {
        val config = Config.defaults(GraphDatabaseSettings.neo4j_home, server.home)
        val fs = DefaultFileSystemAbstraction()
        val engine = StorageEngineFactory.selectStorageEngine(fs, filmsLayout).orElseThrow()
        val out = ByteArrayOutputStream()
        IncrementalImporter(
                filmsLayout,
                fs,
                Configuration.DEFAULT,
                PrintStream(out, true),
                {
                    LogFilesBuilder.logFilesBasedOnlyBuilder(
                            filmsLayout.transactionLogsDirectory,
                            fs,
                        )
                        .withStorageEngineFactory(engine)
                        .build()
                        .tailMetadata
                },
                config,
                Collector.EMPTY,
                EmptyMemoryTracker.INSTANCE,
                batchSize,
                onBatch,
            )
            .use { it.merge() }
        return out.toString()
    }

    /** 7 new persons, c1 to c7, one of them duplicating 'a', and 6 relationships. */
    private fun buildSeven() {
        persons()
        val files =
            arrayOf(
                "--skip-duplicate-nodes",
                "--nodes=" +
                    csv(
                        "persons.csv",
                        "uuid:ID{label:Person},n:int",
                        "c1,1",
                        "a,2",
                        "c3,3",
                        "c4,4",
                        "c5,5",
                        "c6,6",
                        "c7,7",
                    ),
                "--relationships=" +
                    csv(
                        "knows.csv",
                        ":START_ID,:END_ID,:TYPE",
                        "a,c1,KNOWS",
                        "c1,c3,KNOWS",
                        "c3,c4,KNOWS",
                        "c4,c5,KNOWS",
                        "c5,c6,KNOWS",
                        "c6,c7,KNOWS",
                    ),
            )
        assertThat(incremental("prepare", "films", *files).exitCode).isZero
        assertThat(incremental("build", "films", *files).exitCode).isZero
    }

    private fun assertSevenMerged() {
        server.administer("START DATABASE films WAIT")
        assertThat(count("MATCH (p:Person) RETURN count(p) AS c")).isEqualTo(8)
        assertThat(count("MATCH (p:Person) RETURN count(DISTINCT p.uuid) AS c")).isEqualTo(8)
        assertThat(count("MATCH ()-[k:KNOWS]->() RETURN count(k) AS c")).isEqualTo(6)
        assertThat(
                count(
                    "MATCH p = (:Person {uuid: 'a'})-[:KNOWS*6]->(:Person {uuid: 'c7'}) " +
                        "RETURN count(p) AS c"
                )
            )
            .isEqualTo(1)
        assertConsistent()
    }

    private class Crash : RuntimeException("crash")

    @ParameterizedTest(name = "cut short {0} batch {1}")
    @CsvSource(
        "before the commit of, 1",
        "after the commit of, 1",
        "before the commit of, 2",
        "after the commit of, 3",
        // Relationship batches: nodes take four batches of two.
        "before the commit of, 5",
        "after the commit of, 6",
        "after the commit of, 7",
    )
    fun `a merge cut short continues after the batches the database committed`(
        point: String,
        batch: Int,
    ) {
        buildSeven()
        val afterCommit = point.startsWith("after")
        var batches = 0
        val crash = catchThrowable {
            merge(2) { committed ->
                if (committed == afterCommit && ++batches == batch) throw Crash()
            }
        }
        assertThat(generateSequence(crash) { it.cause }.any { it is Crash })
            .describedAs("cut short by the crash: $crash")
            .isTrue
        assertThat(checkNotNull(increment.state()).stage).isEqualTo(Stage.MERGING)
        assertThat(increment.directory).exists()

        val resumed = incremental("merge", "films")

        assertThat(resumed.exitCode).describedAs(resumed.toString()).isZero
        assertThat(resumed.out).contains("Continuing the merge into database 'films' after")
        assertSevenMerged()
    }

    @Test
    fun `a merge in batches of duplicates alone commits the others`() {
        buildSeven()

        // The second node, 'a', is a batch of its own.
        merge(1)

        assertSevenMerged()
    }

    @Test
    fun `a batch that violates a constraint fails the merge, which keeps the batches before it`() {
        server.rows("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE", "films")
        server.rows("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE", "films")
        server.rows("CREATE (:Person {uuid: 'a', email: 'a@example.com'})", "films")
        server.administer("STOP DATABASE films WAIT")
        val files =
            arrayOf(
                "--nodes=" +
                    csv(
                        "persons.csv",
                        "uuid:ID{label:Person},email",
                        "c,c@example.com",
                        "d,d@example.com",
                        "e,a@example.com",
                    )
            )
        assertThat(incremental("prepare", "films", *files).exitCode).isZero
        assertThat(incremental("build", "films", *files).exitCode).isZero

        assertThatThrownBy { merge(2) }
            .isInstanceOf(IncrementalImportException::class.java)
            .hasMessageStartingWith("Merging new nodes 3 to 3 into database 'films' failed: ")
            .hasMessageContaining("a@example.com")
            .hasMessageEndingWith(
                "The database keeps the 2 new nodes and 0 new relationships merged before " +
                    "them; the merge stage continues from there when run again."
            )

        server.administer("START DATABASE films WAIT")
        assertThat(server.rows("MATCH (p:Person) RETURN p.uuid AS uuid ORDER BY uuid", "films"))
            .containsExactly(mapOf("uuid" to "a"), mapOf("uuid" to "c"), mapOf("uuid" to "d"))
    }
}
