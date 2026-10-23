package org.dozendb.core.monitoring

import com.codahale.metrics.Counter
import com.codahale.metrics.Counting
import com.codahale.metrics.Gauge
import com.codahale.metrics.Histogram
import com.codahale.metrics.LockFreeExponentiallyDecayingReservoir
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.LongAdder
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.logging.InternalLog

/** What a metric reports, as Neo4j Enterprise's metrics reference types it. */
enum class MetricType {
    /** A count that only grows, from 0 when the DBMS or database started. */
    COUNTER,
    /** A value read when the metric is reported. */
    GAUGE,
    /** The distribution of recorded values. */
    HISTOGRAM,
}

/**
 * A metric: its full dotted [name], prefix included, its [type] and [description], and the
 * Dropwizard metric that holds its value: a [Counting] for counters, a [Gauge] of a [Number] for
 * gauges, a [SummaryHistogram] for histograms.
 */
class Metric(
    val name: String,
    val type: MetricType,
    val description: String,
    val value: com.codahale.metrics.Metric,
) {
    /** The count of a counter or histogram. */
    fun count(): Long = (value as Counting).count

    /** The value of a gauge; null when there is none now. */
    fun gauge(): Number? = (value as Gauge<*>).value as Number?

    fun histogram(): SummaryHistogram = value as SummaryHistogram
}

/**
 * A Dropwizard histogram that also sums the values recorded, for Prometheus summaries. Its
 * reservoir favours the last five minutes, like Dropwizard's default.
 */
class SummaryHistogram : Histogram(LockFreeExponentiallyDecayingReservoir.builder().build()) {
    private val sum = LongAdder()

    override fun update(value: Long) {
        super.update(value)
        sum.add(value)
    }

    /** The sum of every value recorded. */
    fun sum(): Long = sum.sum()
}

/** A counter read from elsewhere when it is reported. */
private class ReadCounter(private val read: () -> Long) : Counter() {
    override fun getCount(): Long = read()

    override fun inc(n: Long) = throw UnsupportedOperationException("read from its source")

    override fun dec(n: Long) = throw UnsupportedOperationException("read from its source")
}

/**
 * The metrics of a DBMS by full dotted name, in name order: `<prefix>.dbms.<name>` for the DBMS and
 * `<prefix>.database.<database>.<name>` for each database, as Neo4j Enterprise names them. Metrics
 * that [filter] leaves out are never registered, and cost nothing.
 *
 * Metrics are registered through [MetricGroup]s, which remove theirs when closed, so that a
 * database's metrics go when it stops. [dropwizard] holds the same metrics for Dropwizard's
 * reporters.
 */
class MetricRegistry(
    val prefix: String,
    private val filter: List<GlobbingPattern>,
    private val log: InternalLog,
) {
    private val metrics = ConcurrentSkipListMap<String, Metric>()

    /** The same metrics, for Dropwizard's JMX reporter. */
    val dropwizard = com.codahale.metrics.MetricRegistry()

    /** The metrics registered now, in name order. */
    fun metrics(): Collection<Metric> = metrics.values

    fun metric(name: String): Metric? = metrics[name]

    /** The DBMS's metrics, named `<prefix>.dbms.<name>`. */
    fun dbms(): MetricGroup = MetricGroup("$prefix.dbms.")

    /** The metrics of [database], named `<prefix>.database.<database>.<name>`. */
    fun database(database: String): MetricGroup = MetricGroup("$prefix.database.$database.")

    /**
     * Metrics registered together and removed together. Registering a name that is taken, by this
     * group or another, logs a warning and keeps the first metric.
     */
    inner class MetricGroup internal constructor(private val namePrefix: String) : AutoCloseable {
        private val registered = mutableListOf<String>()
        private var closed = false

        /** Whether the filter reports the metric [name] of this group. */
        fun enabled(name: String): Boolean = filter.any { it.matches(namePrefix + name) }

        /** A counter incremented by its source; null when the filter leaves it out. */
        fun counter(name: String, description: String): Counter? =
            Counter().takeIf { add(name, MetricType.COUNTER, description, it) }

        /** A counter that [read] returns when it is reported. */
        fun counter(name: String, description: String, read: () -> Long) {
            add(name, MetricType.COUNTER, description, ReadCounter(read))
        }

        /** A gauge that [read] returns when it is reported; null leaves it out of that report. */
        fun gauge(name: String, description: String, read: () -> Number?) {
            add(name, MetricType.GAUGE, description, Gauge { read() })
        }

        /** A histogram its source records values in; null when the filter leaves it out. */
        fun histogram(name: String, description: String): SummaryHistogram? =
            SummaryHistogram().takeIf { add(name, MetricType.HISTOGRAM, description, it) }

        /** [histogram], which outlives this group, such as counts kept across restarts. */
        fun histogram(name: String, description: String, histogram: SummaryHistogram) {
            add(name, MetricType.HISTOGRAM, description, histogram)
        }

        @Synchronized
        private fun add(
            name: String,
            type: MetricType,
            description: String,
            value: com.codahale.metrics.Metric,
        ): Boolean {
            val fullName = namePrefix + name
            if (closed || filter.none { it.matches(fullName) }) return false
            val metric = Metric(fullName, type, description, value)
            if (metrics.putIfAbsent(fullName, metric) != null) {
                log.warn("Metric $fullName is registered already; this one is not reported.")
                return false
            }
            registered += fullName
            dropwizard.register(fullName, value)
            return true
        }

        /** Removes the metrics of this group. */
        @Synchronized
        override fun close() {
            closed = true
            for (name in registered) {
                metrics.remove(name)
                dropwizard.remove(name)
            }
            registered.clear()
        }
    }
}
