package org.dozendb.core.importer

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.io.path.appendText
import kotlin.io.path.readLines
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.dozendb.core.importer.ResumableBatchImporter.Point
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.neo4j.cli.AdminTool
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout

/**
 * `neo4j-admin database import full` stopped at each point an import may stop at, then run again:
 * from relationship linking and from post-processing it resumes, to the store an import that did
 * not stop writes; from earlier, or with other input, it does not.
 */
internal class ResumableImportTest {
    @TempDir lateinit var root: Path

    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root)
        server.start()
        csv("persons.csv", listOf("id:ID,name,n:int") + (0 until 300).map { "p$it,p$it,$it" })
        csv("movies.csv", listOf("id:ID,name") + (0 until 50).map { "m$it,m$it" })
        csv(
            "knows.csv",
            listOf(":START_ID,:END_ID,since:int") +
                (0 until 300).flatMap {
                    listOf("p$it,p${(it * 7 + 1) % 300},$it", "p$it,p${(it * 13 + 5) % 300},$it")
                },
        )
        // p0 is dense, with a like from every other person; p5 knows itself.
        csv(
            "other.csv",
            listOf(":START_ID,:END_ID,:TYPE") +
                (1 until 300).map { "p$it,p0,LIKES" } +
                (0 until 300).map { "p$it,m${it % 50},WATCHED" } +
                "p5,p5,KNOWS",
        )
    }

    @AfterEach fun stop() = server.stop()

    private class Result(val exitCode: Int, val out: String, val err: String) {
        override fun toString() = "exit code $exitCode\n$out\n$err"
    }

    private fun csv(name: String, lines: List<String>) {
        Files.write(root.resolve(name), lines)
    }

    private val report: Path
        get() = root.resolve("import.report")

    /** The database first: `--nodes` and `--relationships` take several files. */
    private fun arguments(database: String) =
        arrayOf(
            database,
            "--report-file=$report",
            "--nodes=Person=${root.resolve("persons.csv")}",
            "--nodes=Movie=${root.resolve("movies.csv")}",
            "--relationships=KNOWS=${root.resolve("knows.csv")}",
            "--relationships=${root.resolve("other.csv")}",
        )

    private fun context(out: ByteArrayOutputStream, err: ByteArrayOutputStream) =
        ExecutionContext(
            server.home,
            server.home.resolve("conf"),
            PrintStream(out, true),
            PrintStream(err, true),
            InputStream.nullInputStream(),
            DefaultFileSystemAbstraction(),
        )

    /** `neo4j-admin database import full`, as `neo4j-admin` runs it. */
    private fun full(vararg arguments: String): Result {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val exitCode =
            AdminTool.execute(context(out, err), "database", "import", "full", *arguments)
        return Result(exitCode, out.toString(), err.toString())
    }

    /** Stopped. */
    private class Stopped(point: Point) : RuntimeException("stopped at $point")

    /** `import full` stopped at [point], through the command `neo4j-admin` runs. */
    private fun stoppedAt(point: Point, vararg arguments: String): Result {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val command = importCommand(context(out, err)) { if (it == point) throw Stopped(it) }
        val exitCode = command.execute("full", *arguments)
        return Result(exitCode, out.toString(), err.toString())
    }

    /** `import full` in a JVM of its own, which halts at [point], as a crash would. */
    private fun crashedAt(point: Point, vararg arguments: String): Result {
        val output = root.resolve("crashed-$point.out")
        val process =
            ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx512m",
                    *moduleAccess(),
                    "-cp",
                    System.getProperty("java.class.path"),
                    CrashingImport::class.java.name,
                    point.name,
                    server.home.toString(),
                    *arguments,
                )
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start()
        check(process.waitFor(5, TimeUnit.MINUTES)) { "the import did not end" }
        return Result(process.exitValue(), Files.readString(output), "")
    }

    /**
     * The module access this JVM was started with, which Neo4j needs as `neo4j-admin` gives it, as
     * the build has it for the tests.
     */
    private fun moduleAccess(): Array<String> =
        ManagementFactory.getRuntimeMXBean()
            .inputArguments
            .filter { it.startsWith("--add-opens=") || it.startsWith("--enable-native-access=") }
            .toTypedArray()

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

    /** The nodes and relationships of [database] by the names of the nodes, and its counts. */
    private fun graph(database: String): List<String> {
        val nodes =
            server.rows(
                "MATCH (n) RETURN n.name AS name, labels(n) AS labels, n.n AS n",
                database,
            )
        val relationships =
            server.rows(
                "MATCH (a)-[r]->(b) RETURN a.name AS a, type(r) AS type, r.since AS since, " +
                    "b.name AS b",
                database,
            )
        // The counts store, which post-processing builds.
        val counts =
            listOf(
                "MATCH (n) RETURN count(n) AS c",
                "MATCH (n:Person) RETURN count(n) AS c",
                "MATCH ()-[r]->() RETURN count(r) AS c",
                "MATCH (:Person)-[r:KNOWS]->() RETURN count(r) AS c",
                "MATCH ()-[r:WATCHED]->(:Movie) RETURN count(r) AS c",
            )
        return (nodes.map { "(${it["name"]}:${it["labels"]} ${it["n"]})" } +
                relationships.map { "(${it["a"]})-[${it["type"]} ${it["since"]}]->(${it["b"]})" })
            .sorted() +
            counts.map { "$it: ${server.rows(it, database).single()["c"]}" } +
            // Degrees, of the dense node and a sparse one.
            server
                .rows(
                    "MATCH (p) WHERE p.name IN ['p0', 'p5'] RETURN p.name AS name, " +
                        "COUNT { (p)<-[:LIKES]-() } AS likes, COUNT { (p)-[:KNOWS]-() } AS knows " +
                        "ORDER BY name",
                    database,
                )
                .map { it.toString() }
    }

    /** The graph an import that does not stop writes. */
    private fun straight(): List<String> {
        val result = full(*arguments("straight"))
        assertThat(result.exitCode).describedAs(result.toString()).isZero
        server.administer("CREATE DATABASE straight WAIT")
        return graph("straight")
    }

    @ParameterizedTest
    @EnumSource(Point::class, mode = EnumSource.Mode.EXCLUDE, names = ["NODES_IMPORTED"])
    fun `an import stopped from relationship linking on resumes to the store of one that did not stop`(
        point: Point
    ) {
        val expected = straight()
        val stopped = stoppedAt(point, *arguments("films"))
        assertThat(stopped.exitCode).describedAs(stopped.toString()).isNotZero
        val from =
            if (
                point in
                    setOf(
                        Point.LINKED,
                        Point.DEFRAGMENTED,
                        Point.LOG_INITIALIZED,
                        Point.POST_PROCESSED,
                    )
            ) {
                "post-processing"
            } else {
                "relationship linking"
            }
        assertThat(stopped.err)
            .contains(
                "The import resumes from $from: run the same command again, without " +
                    "--overwrite-destination, which starts over."
            )

        // Options that leave what the import writes as it is may change.
        val resumed = full("--threads=2", "--max-off-heap-memory=200m", *arguments("films"))

        assertThat(resumed.exitCode).describedAs(resumed.toString()).isZero
        assertThat(resumed.out).contains("Resuming the import into database 'films' from $from.")
        assertThat(ImportProgress(layout("films")).file).doesNotExist()
        assertConsistent("films")
        server.administer("CREATE DATABASE films WAIT")
        assertThat(graph("films")).isEqualTo(expected)
    }

    @Test
    fun `an import into a backup that stopped resumes where it stopped, beside the backup`() {
        val backups = Files.createDirectories(root.resolve("backups"))
        val target = arrayOf("--target-format=backup", "--target-location=$backups")
        val staging = backups.resolve(".dozendb-import-films")

        val stopped = stoppedAt(Point.LINKED, *arguments("films"), *target)
        assertThat(stopped.exitCode).describedAs(stopped.toString()).isNotZero
        assertThat(staging).isDirectory()
        val resumed = full(*arguments("films"), *target)

        assertThat(resumed.exitCode).describedAs(resumed.toString()).isZero
        assertThat(resumed.out)
            .contains("Resuming the import into database 'films' from post-processing.")
            .contains("Wrote the imported database 'films' as a full backup to ")
        assertThat(staging).doesNotExist()
        assertThat(layout("films").databaseDirectory()).doesNotExist()
        val written = CopyingBackupArtifacts.layout(backups, "films")
        assertThat(
                ConsistencyCheckService(written)
                    .with(
                        Config.newBuilder()
                            .set(
                                GraphDatabaseSettings.neo4j_home,
                                written.neo4jLayout.homeDirectory(),
                            )
                            .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                            .build()
                    )
                    .runFullConsistencyCheck()
                    .isSuccessful
            )
            .describedAs("the backup's store is consistent")
            .isTrue
    }

    @ParameterizedTest
    @EnumSource(Point::class, mode = EnumSource.Mode.EXCLUDE, names = ["NODES_IMPORTED"])
    fun `an import that crashed from relationship linking on resumes to the store of one that did not`(
        point: Point
    ) {
        val expected = straight()
        val crashed = crashedAt(point, *arguments("films"))
        assertThat(crashed.exitCode)
            .describedAs(crashed.toString())
            .isEqualTo(CrashingImport.CRASHED)

        val resumed = full(*arguments("films"))

        assertThat(resumed.exitCode).describedAs(resumed.toString()).isZero
        assertThat(resumed.out).contains("Resuming the import into database 'films' from ")
        assertConsistent("films")
        server.administer("CREATE DATABASE films WAIT")
        assertThat(graph("films")).isEqualTo(expected)
    }

    @Test
    fun `a resume from post-processing builds again what a crash left torn`() {
        val expected = straight()
        stoppedAt(Point.POST_PROCESSED, *arguments("films"))
        // A crash leaves files half written, which an import that stopped closes whole.
        val layout = RecordDatabaseLayout.convert(layout("films"))
        val torn =
            Files.walk(layout.databaseDirectory().resolve("schema")).use { files ->
                files.filter(Files::isRegularFile).toList()
            } +
                listOf(layout.countStore(), layout.relationshipGroupDegreesStore()).map {
                    it.baseSegment()
                } +
                Files.list(layout.transactionLogsDirectory).use { it.toList() }
        assertThat(torn).hasSizeGreaterThan(4)
        for (file in torn) {
            val bytes = Files.readAllBytes(file)
            Files.write(
                file,
                bytes.copyOf(bytes.size / 2).also { it.fill(7, 0, minOf(64, it.size)) },
            )
        }

        val resumed = full(*arguments("films"))

        assertThat(resumed.exitCode).describedAs(resumed.toString()).isZero
        assertConsistent("films")
        server.administer("CREATE DATABASE films WAIT")
        assertThat(graph("films")).isEqualTo(expected)
    }

    @Test
    fun `an import stopped before its relationships were imported starts over`() {
        val expected = straight()
        assertThat(stoppedAt(Point.NODES_IMPORTED, *arguments("films")).err)
            .doesNotContain("The import resumes")

        val refused = full(*arguments("films"))

        assertThat(refused.exitCode).describedAs(refused.toString()).isEqualTo(ExitCode.FAIL)
        assertThat(refused.err)
            .contains(
                "The import into database 'films' stopped before all its nodes and " +
                    "relationships were imported: it resumes only from relationship linking or " +
                    "post-processing. Run it again with --overwrite-destination to start over."
            )

        val again = full("--overwrite-destination", *arguments("films"))

        assertThat(again.exitCode).describedAs(again.toString()).isZero
        assertThat(again.out).doesNotContain("Resuming")
        server.administer("CREATE DATABASE films WAIT")
        assertThat(graph("films")).isEqualTo(expected)
    }

    @Test
    fun `an import with other input or options does not resume`() {
        stoppedAt(Point.RELATIONSHIPS_IMPORTED, *arguments("films"))
        val persons = root.resolve("persons.csv")
        val before = Files.size(persons)
        val modified = Files.getLastModifiedTime(persons)
        persons.appendText("p300,p300,300\n")

        val changed = full(*arguments("films"))

        assertThat(changed.exitCode).describedAs(changed.toString()).isEqualTo(ExitCode.FAIL)
        assertThat(changed.err)
            .contains(
                "The import into database 'films' stopped after its nodes and relationships " +
                    "were imported, with other input or options than these, so it does not " +
                    "resume: it had --nodes Person=$persons ($before bytes, modified "
            )
            .contains("and now has --nodes Person=$persons (${before + 14} bytes, modified ")
            .doesNotContain("The import resumes")

        Files.write(persons, persons.readLines().dropLast(1))
        Files.setLastModifiedTime(persons, modified)
        val option = full("--normalize-types=false", *arguments("films"))

        assertThat(option.exitCode).describedAs(option.toString()).isEqualTo(ExitCode.FAIL)
        assertThat(option.err)
            .contains(
                "it had --normalize-types=true, and now has --normalize-types=false. Run it with " +
                    "the input and options it had to resume it, or with --overwrite-destination " +
                    "to start over."
            )

        val overwritten = full("--overwrite-destination", *arguments("films"))

        assertThat(overwritten.exitCode).describedAs(overwritten.toString()).isZero
        assertConsistent("films")
    }

    @Test
    fun `a resumed import keeps the report of bad entries of the import that stopped`() {
        csv("bad.csv", listOf(":START_ID,:END_ID,:TYPE", "p1,nobody,KNOWS"))
        val skip = arrayOf("--skip-bad-relationships", "--relationships=${root.resolve("bad.csv")}")
        stoppedAt(Point.LINKED, *arguments("films"), *skip)
        val bad = report.readLines()
        assertThat(bad).hasSize(1).allMatch { it.contains("nobody") }

        val resumed = full(*arguments("films"), *skip)

        assertThat(resumed.exitCode).describedAs(resumed.toString()).isZero
        assertThat(report.readLines()).isEqualTo(bad)
        assertThat(root.resolve("import.report.dozendb-kept")).doesNotExist()
    }

    @Test
    fun `an import that does not stop leaves no progress behind`() {
        val result = full(*arguments("films"))

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(ImportProgress(layout("films")).file).doesNotExist()
        assertConsistent("films")
    }

    @Test
    fun `a full import takes nodes with several ID columns, which incremental import refuses`() {
        csv("people.csv", listOf(":ID,:ID,name", "aa,11,Ann", "aa,22,Bob", "bb,11,Carla"))
        csv(
            "works.csv",
            listOf(":START_ID,:START_ID,:TYPE,:END_ID,:END_ID", "aa,11,WORKS_WITH,bb,11"),
        )

        val result =
            full(
                "people",
                "--report-file=$report",
                "--nodes=Person=${root.resolve("people.csv")}",
                "--relationships=${root.resolve("works.csv")}",
            )

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertConsistent("people")
        server.administer("CREATE DATABASE people WAIT")
        assertThat(
                server.rows(
                    "MATCH (a:Person)-[:WORKS_WITH]->(b:Person) RETURN a.name AS a, b.name AS b",
                    "people",
                )
            )
            .containsExactly(mapOf("a" to "Ann", "b" to "Carla"))
        assertThat(server.rows("MATCH (n:Person) RETURN count(n) AS c", "people").single()["c"])
            .isEqualTo(3L)
    }
}
