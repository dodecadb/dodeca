package dev.dodeca.core.monitoring

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.time.Clock
import java.util.Locale
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.io.path.name
import org.neo4j.logging.InternalLog

/**
 * Writes the metrics of [registry] to CSV files in [directory], as Neo4j Enterprise does: a file
 * per metric, `<name>.csv`, with a header and a line each [report], `t` in seconds since the epoch.
 * Counters write `count`, gauges `value` (none while a gauge has none), histograms the columns of
 * Dropwizard's CSV reporter. A database's files grow only while it runs, as its metrics exist only
 * then.
 *
 * A file of [rotationSize] bytes or more (0 for none) is rotated before the next line: renamed
 * `<name>.csv.1`, compressed as [compression] says, the older ones shifted up to [keep] and the
 * oldest deleted; the next line starts a new file. A directory or file that cannot be written is
 * logged once, until the reports succeed again.
 */
internal class CsvReporter(
    private val registry: MetricRegistry,
    private val directory: Path,
    private val rotationSize: Long,
    private val keep: Int,
    private val compression: CsvCompression,
    private val clock: Clock,
    private val log: InternalLog,
) {
    private var failing = false

    /** Writes a line to the file of each metric. Called by one thread at a time. */
    @Synchronized
    fun report() {
        val seconds = clock.millis() / 1000
        var failure: IOException? = null
        try {
            Files.createDirectories(directory)
            for (metric in registry.metrics()) {
                val (header, values) =
                    try {
                        columns(metric) ?: continue
                    } catch (e: RuntimeException) {
                        // A source that went away while it was read, such as a stopping database.
                        log.debug("Metric ${metric.name} could not be read: $e")
                        continue
                    }
                try {
                    write(directory.resolve("${metric.name}.csv"), header, "$seconds,$values")
                } catch (e: IOException) {
                    failure = failure ?: e
                }
            }
        } catch (e: IOException) {
            failure = e
        }
        if (failure != null && !failing) {
            log.warn("Cannot write metrics to CSV files in $directory: $failure", failure)
        } else if (failure == null && failing) {
            log.info("Writing metrics to CSV files in $directory again.")
        }
        failing = failure != null
    }

    /** The header and the values of a line of [metric]; null when it has no value now. */
    private fun columns(metric: Metric): Pair<String, String>? =
        when (metric.type) {
            MetricType.COUNTER -> "count" to metric.count().toString()
            MetricType.GAUGE -> metric.gauge()?.let { "value" to it.toString() }
            MetricType.HISTOGRAM -> HISTOGRAM_HEADER to histogram(metric.histogram())
        }

    private fun histogram(histogram: SummaryHistogram): String {
        val snapshot = histogram.snapshot
        return String.format(
            Locale.ROOT,
            "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f",
            histogram.count,
            snapshot.max,
            snapshot.mean,
            snapshot.min,
            snapshot.stdDev,
            snapshot.median,
            snapshot.get75thPercentile(),
            snapshot.get95thPercentile(),
            snapshot.get98thPercentile(),
            snapshot.get99thPercentile(),
            snapshot.get999thPercentile(),
        )
    }

    private fun write(file: Path, header: String, line: String) {
        val size = if (Files.exists(file)) Files.size(file) else 0
        val fresh =
            if (rotationSize > 0 && size >= rotationSize) {
                rotate(file)
                true
            } else {
                size == 0L
            }
        Files.newBufferedWriter(file, CREATE, APPEND).use { out ->
            if (fresh) out.append("t,").append(header).append('\n')
            out.append(line).append('\n')
        }
    }

    /**
     * Renames [file] `<name>.1`, compressed as configured, after deleting the rotated files from
     * [keep] on and shifting the others up. Rotated files of any compression shift, so that a
     * changed compression leaves none behind.
     */
    private fun rotate(file: Path) {
        val name = file.name
        val rotated = Regex(Regex.escape(name) + """\.(\d+)(\.gz|\.zip)?""")
        val older =
            Files.list(directory).use { files ->
                files
                    .toList()
                    .mapNotNull { path ->
                        rotated.matchEntire(path.name)?.let { match ->
                            Triple(path, match.groupValues[1].toInt(), match.groupValues[2])
                        }
                    }
                    .sortedByDescending { it.second }
            }
        for ((path, index, suffix) in older) {
            if (index >= keep) {
                Files.delete(path)
            } else {
                Files.move(path, file.resolveSibling("$name.${index + 1}$suffix"))
            }
        }
        val first = file.resolveSibling("$name.1")
        Files.move(file, first)
        when (compression) {
            CsvCompression.NONE -> {}
            CsvCompression.GZ -> compress(first, "gz") { GZIPOutputStream(it) }
            CsvCompression.ZIP ->
                compress(first, "zip") { out ->
                    ZipOutputStream(out).also { it.putNextEntry(ZipEntry(name)) }
                }
        }
    }

    /** Replaces [file] with `<file>.<extension>`, written through [compressing]. */
    private fun compress(
        file: Path,
        extension: String,
        compressing: (java.io.OutputStream) -> java.io.OutputStream,
    ) {
        val compressed = file.resolveSibling("${file.name}.$extension")
        try {
            compressing(Files.newOutputStream(compressed)).use { out -> Files.copy(file, out) }
        } catch (e: IOException) {
            Files.deleteIfExists(compressed)
            throw e
        }
        Files.delete(file)
    }

    companion object {
        /** The columns of a histogram after `t`, as Dropwizard's CSV reporter writes them. */
        const val HISTOGRAM_HEADER = "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999"
    }
}
