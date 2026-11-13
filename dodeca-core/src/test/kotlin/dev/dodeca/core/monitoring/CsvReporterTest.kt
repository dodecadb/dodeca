package dev.dodeca.core.monitoring

import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.exists
import kotlin.io.path.inputStream
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readText
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.time.FakeClock

/** The CSV files Neo4j Enterprise writes its metrics to. */
class CsvReporterTest {
    @TempDir lateinit var root: Path

    private val directory: Path by lazy { root.resolve("metrics") }
    private val output = ByteArrayOutputStream()
    private val log = Log4jLogProvider(output).getLog(CsvReporter::class.java)
    private val registry = MetricRegistry("neo4j", listOf(GlobbingPattern("*")), log)
    private val clock = FakeClock(1_700_000_000_123, java.util.concurrent.TimeUnit.MILLISECONDS)
    private val stoppedClock =
        object : com.codahale.metrics.Clock() {
            override fun getTick() = 0L
        }

    private fun reporter(
        rotationSize: Long = 0,
        keep: Int = 7,
        compression: CsvCompression = CsvCompression.NONE,
    ) = CsvReporter(registry, directory, rotationSize, keep, compression, clock, log)

    private fun file(name: String) = directory.resolve(name)

    private fun lines(name: String) = file(name).readText().lines().dropLast(1)

    @Test
    fun `each metric goes to its own file, a line a report, with the time in seconds`() {
        val dbms = registry.dbms()
        val counter = checkNotNull(dbms.counter("bolt.connections_opened", "Opened."))
        var heap: Long? = 1024
        dbms.gauge("vm.heap.used", "Heap.") { heap }
        dbms.gauge("page_cache.usage_ratio", "Ratio.") { 0.25 }
        // Its reservoir weighs values by when they came: on a clock that stands still, the same.
        val latency = SummaryHistogram(stoppedClock)
        registry
            .database("films")
            .histogram("db.query.execution.latency.millis", "Latency.", latency)
        val reporter = reporter()

        counter.inc(3)
        latency.update(10)
        latency.update(30)
        reporter.report()
        clock.forward(java.time.Duration.ofSeconds(30))
        counter.inc()
        heap = null
        reporter.report()

        assertThat(lines("neo4j.dbms.bolt.connections_opened.csv"))
            .containsExactly("t,count", "1700000000,3", "1700000030,4")
        assertThat(lines("neo4j.dbms.vm.heap.used.csv"))
            .describedAs("no line while the gauge has no value")
            .containsExactly("t,value", "1700000000,1024")
        assertThat(lines("neo4j.dbms.page_cache.usage_ratio.csv"))
            .containsExactly("t,value", "1700000000,0.25", "1700000030,0.25")
        val histogram = lines("neo4j.database.films.db.query.execution.latency.millis.csv")
        assertThat(histogram.first())
            .isEqualTo("t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999")
        // count, max, mean and min: on the stopped clock, both values weigh the same.
        assertThat(histogram[1]).startsWith("1700000000,2,30,20.000000,10,")
        assertThat(histogram[1].split(',')).hasSize(12)
    }

    @Test
    fun `a metric that cannot be read is left out of the report`() {
        val dbms = registry.dbms()
        dbms.gauge("vm.heap.used", "Heap.") { throw IllegalStateException("database stopped") }
        dbms.gauge("vm.threads", "Threads.") { 12 }

        reporter().report()

        assertThat(directory.listDirectoryEntries().map { it.name })
            .containsExactly("neo4j.dbms.vm.threads.csv")
    }

    @Test
    fun `a file of the rotation size is renamed, the older ones shift up to the keep number`() {
        registry.dbms().gauge("vm.threads", "Threads.") { 12 }
        // The header and a line: 22 bytes.
        val reporter = reporter(rotationSize = 20, keep = 2)

        repeat(4) {
            reporter.report()
            clock.forward(java.time.Duration.ofSeconds(1))
        }

        assertThat(directory.listDirectoryEntries().map { it.name })
            .containsExactlyInAnyOrder(
                "neo4j.dbms.vm.threads.csv",
                "neo4j.dbms.vm.threads.csv.1",
                "neo4j.dbms.vm.threads.csv.2",
            )
        assertThat(lines("neo4j.dbms.vm.threads.csv")).containsExactly("t,value", "1700000003,12")
        assertThat(lines("neo4j.dbms.vm.threads.csv.1")).containsExactly("t,value", "1700000002,12")
        assertThat(lines("neo4j.dbms.vm.threads.csv.2")).containsExactly("t,value", "1700000001,12")
    }

    @Test
    fun `rotation size 0 never rotates`() {
        registry.dbms().gauge("vm.threads", "Threads.") { 12 }
        val reporter = reporter(rotationSize = 0)

        repeat(100) { reporter.report() }

        assertThat(directory.listDirectoryEntries()).hasSize(1)
        assertThat(lines("neo4j.dbms.vm.threads.csv")).hasSize(101)
    }

    @Test
    fun `rotated files are compressed as configured`() {
        registry.dbms().gauge("vm.threads", "Threads.") { 12 }
        reporter(rotationSize = 1, compression = CsvCompression.GZ).apply {
            report()
            report()
        }
        reporter(rotationSize = 1, compression = CsvCompression.ZIP).report()

        assertThat(directory.listDirectoryEntries().map { it.name })
            .containsExactlyInAnyOrder(
                "neo4j.dbms.vm.threads.csv",
                "neo4j.dbms.vm.threads.csv.1.zip",
                "neo4j.dbms.vm.threads.csv.2.gz",
            )
        val gzipped =
            GZIPInputStream(file("neo4j.dbms.vm.threads.csv.2.gz").inputStream()).use {
                it.readAllBytes().decodeToString()
            }
        assertThat(gzipped).isEqualTo("t,value\n1700000000,12\n")
        ZipInputStream(file("neo4j.dbms.vm.threads.csv.1.zip").inputStream()).use { zip ->
            assertThat(zip.nextEntry?.name).isEqualTo("neo4j.dbms.vm.threads.csv")
            assertThat(zip.readAllBytes().decodeToString()).isEqualTo("t,value\n1700000000,12\n")
            assertThat(zip.nextEntry).isNull()
        }
    }

    @Test
    fun `a directory that cannot be written is logged once, until the reports succeed again`() {
        registry.dbms().gauge("vm.threads", "Threads.") { 12 }
        val reporter = reporter()
        directory.createFile()

        reporter.report()
        reporter.report()
        directory.deleteExisting()
        reporter.report()

        val logged = output.toString()
        assertThat(Regex("Cannot write metrics to CSV files in ").findAll(logged).count())
            .isEqualTo(1)
        assertThat(logged).contains("Writing metrics to CSV files in $directory again.")
        assertThat(file("neo4j.dbms.vm.threads.csv").exists()).isTrue()
    }
}
