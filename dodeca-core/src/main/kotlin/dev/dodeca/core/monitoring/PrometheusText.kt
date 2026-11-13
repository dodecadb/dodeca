package dev.dodeca.core.monitoring

import java.io.Writer
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.logging.InternalLog

/**
 * Writes metrics in Prometheus' text exposition format, version 0.0.4, as Neo4j Enterprise's
 * documentation describes its Prometheus names: every character but letters, digits and `_` turns
 * into `_`, counters end in `_total`, and `# HELP` starts with the original dotted name. Histograms
 * are summaries: Dropwizard's quantiles, `_sum` and `_count`.
 *
 * Two dotted names can sanitise to the same Prometheus name, for example the metrics of databases
 * `my-db` and `my.db`. Prometheus refuses a whole scrape that repeats a name, so only the first
 * metric in name order is written, and a warning is logged once per name.
 */
class PrometheusText(private val log: InternalLog) {
    private val warned = ConcurrentHashMap.newKeySet<String>()

    /** Writes [metrics], skipping a metric whose value cannot be read now. */
    fun write(metrics: Collection<Metric>, out: Writer) {
        val written = HashSet<String>()
        for (metric in metrics) {
            val name = name(metric)
            val samples =
                when (metric.type) {
                    MetricType.HISTOGRAM -> listOf(name, "${name}_sum", "${name}_count")
                    else -> listOf(name)
                }
            if (samples.any { it in written }) {
                if (warned.add(name)) {
                    log.warn(
                        "Metric ${metric.name} is not reported to Prometheus: another metric " +
                            "is reported as $name."
                    )
                }
                continue
            }
            val lines = StringBuilder()
            try {
                lines.append("# HELP ").append(name).append(' ')
                lines.append(escape("${metric.name}: ${metric.description}")).append('\n')
                lines.append("# TYPE ").append(name).append(' ')
                when (metric.type) {
                    MetricType.COUNTER -> {
                        lines.append("counter\n")
                        lines.append(name).append(' ').append(metric.count()).append('\n')
                    }
                    MetricType.GAUGE -> {
                        val value = metric.gauge() ?: continue
                        lines.append("gauge\n")
                        lines.append(name).append(' ').append(number(value)).append('\n')
                    }
                    MetricType.HISTOGRAM -> {
                        lines.append("summary\n")
                        summary(name, metric.histogram(), lines)
                    }
                }
            } catch (e: RuntimeException) {
                // A source that went away while it was read, such as a stopping database.
                log.debug("Metric ${metric.name} could not be read: $e")
                continue
            }
            written += samples
            out.append(lines)
        }
    }

    private fun summary(name: String, histogram: SummaryHistogram, lines: StringBuilder) {
        val snapshot = histogram.snapshot
        val empty = snapshot.size() == 0
        for ((quantile, value) in
            listOf(
                "0.5" to snapshot.median,
                "0.75" to snapshot.get75thPercentile(),
                "0.95" to snapshot.get95thPercentile(),
                "0.98" to snapshot.get98thPercentile(),
                "0.99" to snapshot.get99thPercentile(),
                "0.999" to snapshot.get999thPercentile(),
            )) {
            lines.append(name).append("{quantile=\"").append(quantile).append("\"} ")
            lines.append(if (empty) "NaN" else number(value)).append('\n')
        }
        lines.append(name).append("_sum ").append(histogram.sum()).append('\n')
        lines.append(name).append("_count ").append(histogram.count).append('\n')
    }

    companion object {
        const val CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

        /** The Prometheus name of [metric]. */
        fun name(metric: Metric): String {
            val name = sanitise(metric.name)
            return if (metric.type == MetricType.COUNTER) "${name}_total" else name
        }

        private fun sanitise(dotted: String): String {
            val name = StringBuilder(dotted.length + 1)
            if (dotted.isEmpty() || dotted[0] in '0'..'9') name.append('_')
            for (c in dotted) {
                name.append(
                    if (c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || c == '_') c else '_'
                )
            }
            return name.toString()
        }

        private fun escape(help: String): String = help.replace("\\", "\\\\").replace("\n", "\\n")

        private fun number(value: Number): String =
            when (value) {
                is Double,
                is Float -> {
                    val double = value.toDouble()
                    when {
                        double.isNaN() -> "NaN"
                        double == Double.POSITIVE_INFINITY -> "+Inf"
                        double == Double.NEGATIVE_INFINITY -> "-Inf"
                        else -> double.toString()
                    }
                }
                else -> value.toString()
            }
    }
}
