package dev.dodeca.core.copy

import dev.dodeca.core.StandaloneServer
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Stream
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.copyToRecursively
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readLines
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.cli.AdminTool
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector
import org.neo4j.internal.id.DefaultIdGeneratorFactory
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.DatabaseCreationOptions
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.kernel.impl.store.InlineNodeLabels
import org.neo4j.kernel.impl.store.StoreFactory
import org.neo4j.kernel.impl.store.record.RecordLoad
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StoreIdProvider
import org.neo4j.storageengine.api.cursor.StoreCursors
import org.neo4j.storageengine.util.IdUpdateListener

/**
 * `neo4j-admin database copy`, run as `neo4j-admin` runs it, beside a running server whose stopped
 * database it copies.
 */
@OptIn(ExperimentalPathApi::class)
class StoreCopyTest {
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

    /** Copies with the schema file, if any, written to [root], not the working directory. */
    private fun copy(vararg arguments: String) =
        admin("database", "copy", "--to-path-schema=$root", *arguments)

    private fun copied(vararg arguments: String): Result {
        val result = copy(*arguments)
        assertThat(result.exitCode).describedAs(result.toString()).isZero
        return result
    }

    private fun layout(name: String): DatabaseLayout =
        Neo4jLayout.of(Config.defaults(GraphDatabaseSettings.neo4j_home, server.home))
            .databaseLayout(name)

    private fun assertConsistent(name: String) {
        val config =
            Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, server.home)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .build()
        assertThat(
                ConsistencyCheckService(layout(name))
                    .with(config)
                    .runFullConsistencyCheck()
                    .isSuccessful
            )
            .describedAs("$name is consistent")
            .isTrue
    }

    /**
     * The nodes and relationships of [database], described by the names of the nodes, each with its
     * labels and other properties, sorted.
     */
    private fun graph(database: String): List<String> {
        val nodes =
            server
                .rows(
                    "MATCH (n) RETURN n.name AS name, labels(n) AS labels, properties(n) AS p",
                    database,
                )
                .map { row ->
                    @Suppress("UNCHECKED_CAST")
                    val labels =
                        (row["labels"] as List<String>).sorted().joinToString("") { ":$it" }
                    "(${row["name"]}$labels${properties(row["p"])})"
                }
        val relationships =
            server
                .rows(
                    "MATCH (a)-[r]->(b) RETURN a.name AS a, type(r) AS type, properties(r) AS p, " +
                        "b.name AS b",
                    database,
                )
                .map { row ->
                    "(${row["a"]})-[${row["type"]}${properties(row["p"])}]->(${row["b"]})"
                }
        return (nodes + relationships).sorted()
    }

    private fun properties(properties: Any?): String {
        @Suppress("UNCHECKED_CAST")
        return (properties as Map<String, Any?>)
            .filterKeys { it != "name" }
            .toSortedMap()
            .entries
            .joinToString("") { (key, value) -> " $key=${plain(value)}" }
    }

    /** [value] with its arrays as lists and its maps sorted, which compare and print by content. */
    private fun plain(value: Any?): Any? =
        when {
            value is Map<*, *> ->
                value.entries.associate { (k, v) -> k.toString() to plain(v) }.toSortedMap()
            value is List<*> -> value.map(::plain)
            value != null && value.javaClass.isArray ->
                (0 until java.lang.reflect.Array.getLength(value)).map {
                    plain(java.lang.reflect.Array.get(value, it))
                }
            else -> value
        }

    private fun nodeIds(database: String): Map<Any?, Long> =
        server.rows("MATCH (n) RETURN n.name AS name, id(n) AS id", database).associate {
            it["name"] to it["id"] as Long
        }

    @Test
    fun `a copy has every node, relationship, label and property of its source, at its node ids`() {
        val long = "x".repeat(5000)
        server.rows(
            "CREATE (a:Person:Actor {name: 'Ann', born: 1970, height: 1.71, alive: true, " +
                "nicknames: ['A', 'Annie'], scores: [1, 2, 3], ratios: [0.5, 1.5], bio: \$long, " +
                "home: point({x: 1.5, y: 2.5}), place: point({longitude: 4.4, latitude: 51.2, " +
                "height: 10.0}), birthday: date('1970-04-01'), " +
                "seen: datetime('2026-11-10T12:30:00+01:00[Europe/Brussels]'), " +
                "wakes: localtime('07:15'), runtime: duration('PT2H10M'), " +
                "letters: [\$long, \$long]}) " +
                "CREATE (m:Movie:L1:L2:L3:L4:L5:L6:L7:L8:L9:L10 {name: 'Up', year: 2009}) " +
                "CREATE (a)-[:ACTED_IN {role: 'Ellie', notes: \$long}]->(m) " +
                "CREATE (a)-[:LIKES]->(a) " +
                "WITH a, m UNWIND range(1, 120) AS i " +
                "CREATE (f:Fan {name: 'fan' + i, i: i})-[:LIKES {i: i}]->(a) " +
                "CREATE (f)-[:WATCHED]->(m)",
            "films",
            mapOf("long" to long),
        )
        // Leave holes in the node and relationship ids.
        server.rows("MATCH (f:Fan) WHERE f.i % 3 = 0 DETACH DELETE f", "films")
        server.rows("MATCH (:Fan)-[w:WATCHED]->() WHERE id(w) % 2 = 0 DELETE w", "films")
        val graph = graph("films")
        val ids = nodeIds("films")
        server.administer("STOP DATABASE films WAIT")

        val result = copied("films", "copy")

        assertThat(result.out).contains("Copied ${ids.size} nodes and ")
        // Only a clustered server marks a copy.
        assertThat(copiedMarker(layout("copy"))).doesNotExist()
        assertConsistent("copy")
        server.administer("CREATE DATABASE copy WAIT")
        assertThat(graph("copy")).isEqualTo(graph)
        assertThat(nodeIds("copy")).isEqualTo(ids)
        assertThat(server.rows("MATCH (n:L7) RETURN count(n) AS c", "copy").single()["c"])
            .isEqualTo(1L)
        // The source is as it was.
        server.administer("START DATABASE films WAIT")
        assertThat(graph("films")).isEqualTo(graph)
    }

    @Test
    fun `--compact-node-store numbers the nodes anew from 0`() {
        server.rows(
            "UNWIND range(0, 99) AS i CREATE (:Item {name: 'item' + i, i: i})",
            "films",
        )
        server.rows(
            "MATCH (a:Item), (b:Item) WHERE b.i = a.i + 2 CREATE (a)-[:NEXT {i: a.i}]->(b)",
            "films",
        )
        server.rows("MATCH (n:Item) WHERE n.i % 2 = 1 DETACH DELETE n", "films")
        val graph = graph("films")
        val ids = nodeIds("films")
        server.administer("STOP DATABASE films WAIT")

        copied("films", "compacted", "--compact-node-store")
        copied("films", "kept", "--compact-node-store=false")

        assertConsistent("compacted")
        server.administer("CREATE DATABASE compacted WAIT")
        server.administer("CREATE DATABASE kept WAIT")
        assertThat(graph("compacted")).isEqualTo(graph)
        assertThat(nodeIds("compacted").values).containsExactlyInAnyOrderElementsOf(0L..49L)
        assertThat(nodeIds("kept")).isEqualTo(ids)
    }

    /** A small graph with a label that holds a comma and a property key that holds a dot. */
    private fun films() {
        server.rows(
            "CREATE (ann:Person:Actor {name: 'Ann', born: 1970, secret: 'a'}), " +
                "(bob:Person:Director {name: 'Bob', secret: 'b'}), " +
                "(up:Movie {name: 'Up', year: 2009}), " +
                "(pix:Studio {name: 'Pix', secret: 's'}), " +
                "(:`Odd,label` {name: 'Odd', `odd.key`: 1}), " +
                "(ann)-[:ACTED_IN {role: 'Ellie', pay: 1}]->(up), " +
                "(bob)-[:DIRECTED {note: 'n', year: 2009}]->(up), " +
                "(pix)-[:PRODUCED]->(up), " +
                "(ann)-[:KNOWS {since: 2000}]->(bob)",
            "films",
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filters")
    fun `filters leave out nodes, relationships, labels and properties`(
        options: String,
        expected: List<String>,
    ) {
        films()
        assertThat(graph("films")).isEqualTo(FULL)
        server.administer("STOP DATABASE films WAIT")

        copied("films", "filtered", *options.split(" ").toTypedArray())

        assertConsistent("filtered")
        server.administer("CREATE DATABASE filtered WAIT")
        assertThat(graph("filtered")).isEqualTo(expected.sorted())
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "--copy-only-nodes-with-labels=A --ignore-nodes-with-labels=B",
                "--copy-only-relationships-with-types=A --ignore-relationships-with-types=B",
                "--skip-properties=a --skip-node-properties=A.a",
                "--skip-properties=a --copy-only-node-properties=A.a",
                "--skip-properties=a --skip-relationship-properties=A.a",
                "--skip-properties=a --copy-only-relationship-properties=A.a",
                "--skip-node-properties=A.a --copy-only-node-properties=A.b",
                "--skip-relationship-properties=A.a --copy-only-relationship-properties=A.b",
            ]
    )
    fun `options that exclude each other are refused`(options: String) {
        server.administer("STOP DATABASE films WAIT")

        val result = copy("films", "filtered", *options.split(" ").toTypedArray())

        assertThat(result.exitCode).describedAs(result.toString()).isEqualTo(ExitCode.USAGE)
        val (first, second) = options.split(" ").map { it.substringBefore('=') }
        assertThat(result.err).contains("$first and $second cannot be combined.")
        assertThat(layout("filtered").databaseDirectory()).doesNotExist()
    }

    @Test
    fun `a property pair that is not one is refused`() {
        server.administer("STOP DATABASE films WAIT")

        val result = copy("films", "filtered", "--skip-node-properties=Person")

        assertThat(result.exitCode).isEqualTo(ExitCode.USAGE)
        assertThat(result.err)
            .contains(
                "--skip-node-properties takes <label or type>.<property> pairs, not 'Person'."
            )
    }

    private val schema =
        listOf(
            "CREATE INDEX person_name FOR (p:Person) ON (p.name)",
            "CREATE INDEX person_name_born FOR (p:Person) ON (p.name, p.born)",
            "CREATE INDEX acted_role FOR ()-[r:ACTED_IN]-() ON (r.role)",
            "CREATE TEXT INDEX movie_title FOR (m:Movie) ON (m.title)",
            "CREATE POINT INDEX studio_place FOR (s:Studio) ON (s.place) OPTIONS {indexConfig: " +
                "{`spatial.cartesian.min`: [-100.0, -100.0], `spatial.cartesian.max`: [100.0, 100.0]}}",
            "CREATE FULLTEXT INDEX `odd ``index``` FOR (n:Person|Movie) ON EACH [n.name, n.title] " +
                "OPTIONS {indexConfig: {`fulltext.analyzer`: 'english'}}",
            "CREATE FULLTEXT INDEX roles FOR ()-[r:ACTED_IN]-() ON EACH [r.role]",
            "CREATE VECTOR INDEX movie_embedding FOR (m:Movie) ON (m.embedding) OPTIONS " +
                "{indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
            "CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT movie_key FOR (m:Movie) REQUIRE (m.title, m.year) IS NODE KEY",
            "CREATE CONSTRAINT person_born FOR (p:Person) REQUIRE p.born IS NOT NULL",
            "CREATE CONSTRAINT person_born_type FOR (p:Person) REQUIRE p.born IS :: INTEGER",
            "CREATE CONSTRAINT role_exists FOR ()-[r:ACTED_IN]-() REQUIRE r.role IS NOT NULL",
            "CREATE CONSTRAINT role_type FOR ()-[r:ACTED_IN]-() REQUIRE r.role IS :: STRING",
            "CREATE CONSTRAINT acted_id FOR ()-[r:ACTED_IN]-() REQUIRE r.id IS UNIQUE",
            "CREATE CONSTRAINT acted_key FOR ()-[r:ACTED_IN]-() REQUIRE r.key IS RELATIONSHIP KEY",
            // Six dependent constraints, only graph types make.
            "CYPHER 25 ALTER CURRENT GRAPH TYPE ADD { (:Movie => :Work {title :: STRING NOT NULL}), " +
                "(:Person)-[:DIRECTED => {year :: INTEGER}]->(:Movie =>) }",
        )

    /** The indexes and constraints the statements of [schema] create. */
    private val created = schema.size - 1 + 6

    private fun createSchema() {
        for (statement in schema) server.rows(statement, "films")
        server.rows(
            "CREATE (p:Person {id: 1, name: 'Ann', born: 1970})-[:ACTED_IN {role: 'Ellie', " +
                "id: 1, key: 1}]->(m:Movie:Work {title: 'Up', year: 2009, " +
                "embedding: [0.1, 0.2, 0.3]}), (p)-[:DIRECTED {year: 2009}]->(m), " +
                "(:Studio {place: point({x: 1, y: 2})})",
            "films",
        )
        server.rows("CALL db.awaitIndexes(60)", "films")
    }

    /** The indexes and constraints of [database], without their ids and providers. */
    private fun schema(database: String): List<String> {
        val indexes =
            server.rows(
                "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, " +
                    "owningConstraint, options, state RETURN name, type, entityType, " +
                    "labelsOrTypes, properties, owningConstraint, options.indexConfig AS config, " +
                    "state ORDER BY name",
                database,
            )
        val constraints =
            server.rows(
                "CYPHER 25 SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, " +
                    "properties, enforcedLabel, classification, ownedIndex, propertyType " +
                    "RETURN * ORDER BY name",
                database,
            )
        return (indexes + constraints).map { row ->
            row.toSortedMap().entries.joinToString(", ") { (key, value) -> "$key=${plain(value)}" }
        }
    }

    @Test
    fun `the schema file creates the indexes and constraints of the source on the copy`() {
        createSchema()
        val schema = schema("films")
        server.administer("STOP DATABASE films WAIT")

        val result = copied("films", "copy")

        val file = root.resolve("copy-schema.cypher")
        assertThat(result.out).contains("are in $file.")
        val statements = file.readLines()
        assertThat(statements).hasSize(this.schema.size).allMatch { it.endsWith(";") }
        server.administer("CREATE DATABASE copy WAIT")
        // Only the token lookup indexes, which the copy builds.
        assertThat(schema("copy")).hasSize(2)
        for (statement in statements) server.rows(statement.removeSuffix(";"), "copy")
        server.rows("CALL db.awaitIndexes(60)", "copy")
        assertThat(schema("copy")).isEqualTo(schema)
    }

    @Test
    fun `--copy-schema creates the indexes and constraints of the source on the copy`() {
        createSchema()
        val schema = schema("films")
        server.administer("STOP DATABASE films WAIT")

        val result = copied("films", "copy", "--copy-schema")

        assertThat(result.out).contains("Created $created indexes and constraints")
        assertThat(root.resolve("copy-schema.cypher")).doesNotExist()
        assertThat(layout("copy").neo4jLayout.databasesDirectory().resolve(".copy.copy"))
            .doesNotExist()
        assertConsistent("copy")
        server.administer("CREATE DATABASE copy WAIT")
        assertThat(schema("copy")).isEqualTo(schema)
    }

    @Test
    fun `the graph type of what the filters leave out is left out`() {
        createSchema()
        val leftOut =
            server
                .rows(
                    "CYPHER 25 SHOW CONSTRAINTS YIELD name, labelsOrTypes, enforcedLabel " +
                        "WHERE labelsOrTypes = ['DIRECTED'] OR enforcedLabel = 'Work' " +
                        "RETURN name ORDER BY name",
                    "films",
                )
                .map { "Left out the constraint ${it["name"]}, on what the filters left out." }
        server.administer("STOP DATABASE films WAIT")

        val result =
            copied(
                "films",
                "copy",
                "--skip-labels=Work",
                "--ignore-relationships-with-types=DIRECTED",
                "--copy-schema",
            )

        assertThat(leftOut).hasSize(4)
        assertThat(result.out.lines().filter { it.startsWith("Left out the constraint") })
            .containsExactlyElementsOf(leftOut)
        server.administer("CREATE DATABASE copy WAIT")
        assertThat(
                server.rows("CYPHER 25 SHOW CURRENT GRAPH TYPE", "copy").single()["specification"]
                    as String
            )
            .contains("(:`Movie` => {`title` :: STRING NOT NULL}),")
            .doesNotContain("Work")
            .doesNotContain("DIRECTED")
    }

    @Test
    fun `the schema of what the filters leave out is left out`() {
        createSchema()
        server.administer("STOP DATABASE films WAIT")

        val result =
            copied(
                "films",
                "copy",
                "--ignore-nodes-with-labels=Studio",
                "--skip-node-properties=Person.born",
                "--ignore-relationships-with-types=ACTED_IN",
                "--copy-schema",
            )

        assertThat(result.out.lines().filter { it.startsWith("Left out") })
            .containsExactly(
                "Left out the index acted_role, on what the filters left out.",
                "Left out the index person_name_born, on what the filters left out.",
                "Left out the index roles, on what the filters left out.",
                "Left out the index studio_place, on what the filters left out.",
                "Left out the constraint acted_id, on what the filters left out.",
                "Left out the constraint acted_key, on what the filters left out.",
                "Left out the constraint person_born, on what the filters left out.",
                "Left out the constraint person_born_type, on what the filters left out.",
                "Left out the constraint role_exists, on what the filters left out.",
                "Left out the constraint role_type, on what the filters left out.",
            )
        server.administer("CREATE DATABASE copy WAIT")
        assertThat(
                server
                    .rows(
                        "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name " +
                            "ORDER BY name",
                        "copy",
                    )
                    .map { it["name"] }
            )
            .containsExactly(
                "movie_embedding",
                "movie_key",
                "movie_title",
                "odd `index`",
                "person_id",
                "person_name",
            )
    }

    /**
     * Staged next to the database, under a `--temp-path` on the same file system, and under one on
     * another file system, from which the copy is copied into place, not moved.
     */
    @ParameterizedTest
    @ValueSource(strings = ["next to the database", "same file system", "other file system"])
    fun `a copy with the source's name replaces it`(staged: String) {
        server.rows(
            "UNWIND range(0, 99) AS i CREATE (:Item {name: 'item' + i, i: i})-[:HAS]->(:Part {name: 'part' + i})",
            "films",
        )
        server.rows("MATCH (n:Item) WHERE n.i % 2 = 1 DETACH DELETE n", "films")
        val graph = graph("films")
        val ids = nodeIds("films")
        server.administer("STOP DATABASE films WAIT")
        val staging =
            when (staged) {
                "same file system" -> Files.createDirectories(root.resolve("staging"))
                // The build directory, on the disk, where the tests run on a RAM disk.
                "other file system" ->
                    Files.createTempDirectory(
                            Files.createDirectories(Path.of("target").toAbsolutePath()),
                            "copy-staging",
                        )
                        .also {
                            assumeTrue(Files.getFileStore(it) != Files.getFileStore(root)) {
                                "the build directory is on the file system of the tests"
                            }
                        }
                else -> null
            }

        val result =
            try {
                if (staging != null) copied("films", "films", "--temp-path=$staging")
                else copied("films", "films")
            } finally {
                if (staged == "other file system")
                    DefaultFileSystemAbstraction().use {
                        it.deleteRecursively(staging)
                    }
            }

        assertThat(result.out).contains("Database 'films' is replaced by its copy.")
        val layout = layout("films").neo4jLayout
        for (directory in
            listOf(layout.databasesDirectory(), layout.transactionLogsRootDirectory())) {
            assertThat(directory.listDirectoryEntries().map { it.name }).noneMatch {
                it.startsWith(".dodeca")
            }
        }
        if (staged == "same file system") assertThat(staging!!.listDirectoryEntries()).isEmpty()
        assertConsistent("films")
        server.administer("START DATABASE films WAIT")
        assertThat(graph("films")).isEqualTo(graph)
        assertThat(nodeIds("films")).isEqualTo(ids)
    }

    @Test
    fun `a clustered server prepares a copy to a new name as a seed, and marks one to its own`() {
        films()
        server.administer("STOP DATABASE films WAIT")
        Files.createDirectories(server.home.resolve("conf"))
        Files.writeString(
            server.home.resolve("conf/neo4j.conf"),
            "dbms.dodeca.cluster.enabled=true\n",
        )
        val databases = layout("films").neo4jLayout.databasesDirectory()

        val seed = copied("films", "copy")

        assertThat(seed.out)
            .contains(
                "Prepared database 'copy' on this server.",
                "CREATE DATABASE copy OPTIONS {existingData: 'use', existingDataSeedServer: '",
            )
        assertThat(databases.resolve("copy")).doesNotExist()
        assertThat(databases.resolve(".dodeca-seed-copy/copy")).isDirectory()
        assertThat(copiedMarker(layout("copy"))).doesNotExist()

        copied("films", "films")

        assertThat(copiedMarker(layout("films"))).exists()
        assertConsistent("films")
    }

    @Test
    fun `records that cannot be read are left out and reported`() {
        films()
        val ids = nodeIds("films")
        val knows =
            server.rows("MATCH ()-[r:KNOWS]->() RETURN id(r) AS id", "films").single()["id"] as Long
        server.administer("STOP DATABASE films WAIT")
        corrupt(layout("films"), node = ids.getValue("Pix"), relationship = knows)

        val result = copied("films", "copy")

        assertThat(result.out)
            .contains(
                "Copied 4 nodes and 2 relationships of database 'films' in ",
                ", leaving out 1 relationship; 2 entities could not be read.",
            )
        val report = root.resolve("copy-copy.report")
        assertThat(result.err)
            .contains(
                "WARNING 2 entities of database 'films' could not be read and were left out of " +
                    "the copy: they are listed in $report."
            )
        assertThat(report.readLines())
            .anyMatch { it.startsWith("node ${ids.getValue("Pix")}: ") }
            .anyMatch { it.startsWith("relationship $knows: ") }
        assertConsistent("copy")
        server.administer("CREATE DATABASE copy WAIT")
        assertThat(graph("copy"))
            .isEqualTo(
                FULL.filter {
                    !it.contains("Pix") && !it.contains("KNOWS")
                }
            )
    }

    /** Gives node [node] a label and relationship [relationship] a type that do not exist. */
    private fun corrupt(layout: DatabaseLayout, node: Long, relationship: Long) {
        val fs = DefaultFileSystemAbstraction()
        val config = Config.defaults(GraphDatabaseSettings.neo4j_home, server.home)
        val contextFactory = CursorContextFactory.NULL_CONTEXT_FACTORY
        JobSchedulerFactory.createInitialisedScheduler().use { scheduler ->
            StandalonePageCacheFactory.createPageCache(fs, scheduler, PageCacheTracer.NULL).use {
                pageCache ->
                StoreFactory(
                        layout,
                        config,
                        DefaultIdGeneratorFactory(
                            fs,
                            RecoveryCleanupWorkCollector.immediate(),
                            PageCacheTracer.NULL,
                            layout.databaseName,
                        ),
                        pageCache,
                        PageCacheTracer.NULL,
                        fs,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        false,
                        DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                    )
                    .openAllNeoStores()
                    .use { stores ->
                        val context = CursorContext.NULL_CONTEXT
                        val memory = EmptyMemoryTracker.INSTANCE
                        val nodes = stores.nodeStore
                        nodes.openPageCursorForWriting(node, context).use { cursor ->
                            val record =
                                nodes.getRecordByCursor(
                                    node,
                                    nodes.newRecord(),
                                    RecordLoad.NORMAL,
                                    cursor,
                                    memory,
                                )
                            InlineNodeLabels(record)
                                .put(
                                    intArrayOf(999),
                                    nodes,
                                    null,
                                    context,
                                    StoreCursors.NULL,
                                    memory,
                                )
                            nodes.updateRecord(
                                record,
                                IdUpdateListener.IGNORE,
                                cursor,
                                context,
                                StoreCursors.NULL,
                            )
                        }
                        val relationships = stores.relationshipStore
                        relationships.openPageCursorForWriting(relationship, context).use { cursor
                            ->
                            val record =
                                relationships.getRecordByCursor(
                                    relationship,
                                    relationships.newRecord(),
                                    RecordLoad.NORMAL,
                                    cursor,
                                    memory,
                                )
                            record.type = 999
                            relationships.updateRecord(
                                record,
                                IdUpdateListener.IGNORE,
                                cursor,
                                context,
                                StoreCursors.NULL,
                            )
                        }
                    }
            }
        }
    }

    @Test
    fun `a store that needs recovery is copied only with --force`() {
        server.rows("CREATE (:Item {name: 'checkpointed'})", "films")
        server.rows("CALL db.checkpoint()", "films")
        server.rows("CREATE (:Item {name: 'logged'})", "films")
        // The files of the running database are those of a database that crashed.
        val crashed = root.resolve("crashed")
        Files.createDirectories(crashed.resolve("databases"))
        Files.createDirectories(crashed.resolve("transactions"))
        val layout = layout("films")
        layout
            .databaseDirectory()
            .copyToRecursively(
                crashed.resolve("databases/films"),
                followLinks = false,
                overwrite = false,
            )
        layout.transactionLogsDirectory.copyToRecursively(
            crashed.resolve("transactions/films"),
            followLinks = false,
            overwrite = false,
        )
        val from =
            arrayOf(
                "--from-path-data=${crashed.resolve("databases")}",
                "--from-path-txn=${crashed.resolve("transactions")}",
            )

        val refused = copy(*from, "films", "copy")

        assertThat(refused.exitCode).isEqualTo(ExitCode.FAIL)
        assertThat(refused.err)
            .contains("Database 'films' was not shut down cleanly and needs recovery")
        assertThat(layout("copy").databaseDirectory()).doesNotExist()

        val forced = copied(*from, "--force", "films", "copy")

        assertThat(forced.err).contains("WARNING Database 'films' needs recovery")
        assertConsistent("copy")
        server.administer("CREATE DATABASE copy WAIT")
        assertThat(nodeIds("copy").keys).contains("checkpointed")
    }

    @Test
    fun `a running database, an existing target and system are refused`() {
        val running = copy("films", "copy")
        assertThat(running.exitCode).isEqualTo(ExitCode.FAIL)
        assertThat(running.err)
            .contains("Database 'films' is in use. Stop it first, with STOP DATABASE films.")
        assertThat(layout("copy").databaseDirectory()).doesNotExist()

        server.administer("CREATE DATABASE copy WAIT")
        server.administer("STOP DATABASE films WAIT")
        server.administer("STOP DATABASE copy WAIT")
        val existing = copy("films", "copy")
        assertThat(existing.exitCode).isEqualTo(ExitCode.FAIL)
        assertThat(existing.err)
            .contains("Database 'copy' already exists in ${layout("copy").databaseDirectory()}")

        val system = copy("system", "copy2")
        assertThat(system.exitCode).isEqualTo(ExitCode.USAGE)
        assertThat(system.err).contains("copies user databases, not system")

        val missing = copy("nothing", "copy2")
        assertThat(missing.exitCode).isEqualTo(ExitCode.FAIL)
        assertThat(missing.err).contains("Database 'nothing' does not exist")

        val format = copy("films", "copy2", "--to-format=high_limit")
        assertThat(format.exitCode).isEqualTo(ExitCode.USAGE)
        assertThat(format.err)
            .contains(
                "Dodeca stores databases in the standard and aligned formats, not high_limit."
            )
        assertThat(layout("copy2").databaseDirectory()).doesNotExist()
    }

    @Test
    fun `--to-format gives the copy another format`() {
        films()
        server.administer("STOP DATABASE films WAIT")

        copied("films", "standard", "--to-format=standard")
        copied("films", "same")

        server.administer("START DATABASE films WAIT")
        server.administer("CREATE DATABASE standard WAIT")
        server.administer("CREATE DATABASE same WAIT")
        assertThat(format("standard")).isEqualTo("standard")
        assertThat(format("same")).isEqualTo(format("films"))
        assertThat(graph("standard")).isEqualTo(FULL)
    }

    private fun format(database: String): String =
        server
            .database(database)
            .dependencyResolver
            .resolveDependency(StoreIdProvider::class.java)
            .storeId
            .formatName

    companion object {
        private val FULL =
            listOf(
                    "(Ann:Actor:Person born=1970 secret=a)",
                    "(Bob:Director:Person secret=b)",
                    "(Odd:Odd,label odd.key=1)",
                    "(Pix:Studio secret=s)",
                    "(Up:Movie year=2009)",
                    "(Ann)-[ACTED_IN pay=1 role=Ellie]->(Up)",
                    "(Ann)-[KNOWS since=2000]->(Bob)",
                    "(Bob)-[DIRECTED note=n year=2009]->(Up)",
                    "(Pix)-[PRODUCED]->(Up)",
                )
                .sorted()

        @JvmStatic
        fun filters(): Stream<Arguments> =
            Stream.of(
                Arguments.of(
                    "--copy-only-nodes-with-labels=Person,Movie",
                    listOf(
                        "(Ann:Actor:Person born=1970 secret=a)",
                        "(Bob:Director:Person secret=b)",
                        "(Up:Movie year=2009)",
                        "(Ann)-[ACTED_IN pay=1 role=Ellie]->(Up)",
                        "(Ann)-[KNOWS since=2000]->(Bob)",
                        "(Bob)-[DIRECTED note=n year=2009]->(Up)",
                    ),
                ),
                Arguments.of(
                    "--ignore-nodes-with-labels=Director,`Odd,label`",
                    listOf(
                        "(Ann:Actor:Person born=1970 secret=a)",
                        "(Pix:Studio secret=s)",
                        "(Up:Movie year=2009)",
                        "(Ann)-[ACTED_IN pay=1 role=Ellie]->(Up)",
                        "(Pix)-[PRODUCED]->(Up)",
                    ),
                ),
                Arguments.of(
                    "--copy-only-relationships-with-types=ACTED_IN,KNOWS",
                    FULL.filter { !it.contains("DIRECTED") && !it.contains("PRODUCED") },
                ),
                Arguments.of(
                    "--ignore-relationships-with-types=KNOWS",
                    FULL.filter { !it.contains("KNOWS") },
                ),
                Arguments.of(
                    "--skip-labels=Actor,`Odd,label`",
                    FULL.map {
                        it.replace("(Ann:Actor:Person", "(Ann:Person")
                            .replace("(Odd:Odd,label", "(Odd")
                    },
                ),
                Arguments.of(
                    "--skip-properties=secret,pay",
                    FULL.map { it.replace(" secret=a", "").replace(" secret=b", "") }
                        .map { it.replace(" secret=s", "").replace(" pay=1", "") },
                ),
                Arguments.of(
                    "--skip-node-properties=Person.secret,`Odd,label`.`odd.key`",
                    FULL.map {
                        it.replace(" secret=a", "")
                            .replace(" secret=b", "")
                            .replace(" odd.key=1", "")
                    },
                ),
                Arguments.of(
                    "--skip-relationship-properties=DIRECTED.note,ACTED_IN.pay",
                    FULL.map { it.replace(" note=n", "").replace(" pay=1", "") },
                ),
                Arguments.of(
                    // Ann is an Actor, whose properties are not listed, and a Person, whose are.
                    "--copy-only-node-properties=Person.name,Person.born,Movie.name",
                    FULL.map {
                        it.replace(" secret=a", "")
                            .replace(" secret=b", "")
                            .replace("(Up:Movie year=2009)", "(Up:Movie)")
                    },
                ),
                Arguments.of(
                    "--copy-only-relationship-properties=ACTED_IN.role,KNOWS.name",
                    FULL.map { it.replace(" pay=1", "").replace(" since=2000", "") },
                ),
                Arguments.of(
                    "--copy-only-nodes-with-labels=Person --skip-labels=Director " +
                        "--skip-properties=secret --ignore-relationships-with-types=ACTED_IN",
                    listOf(
                        "(Ann:Actor:Person born=1970)",
                        "(Bob:Person)",
                        "(Ann)-[KNOWS since=2000]->(Bob)",
                    ),
                ),
            )
    }
}
