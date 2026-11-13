package dev.dodeca.it

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName

/** Prometheus's `promtool`, from the official Prometheus image, and the text it checks. */
object Promtool {
    /**
     * What `promtool check metrics` made of a scrape: [exitCode] 0 when it found nothing, 3 when it
     * found only [problems], lines naming a metric and what it found wrong with it; 1 when the text
     * is not in the exposition format.
     */
    data class Check(val exitCode: Int, val problems: List<String>)

    private val image: DockerImageName by lazy {
        DockerImageName.parse(
            requireNotNull(System.getProperty("prometheus.image")) {
                "system property prometheus.image is not set; run with -Pit"
            }
        )
    }

    /** Checks [metrics], Prometheus's text exposition format, as `promtool check metrics`. */
    fun check(metrics: String): Check {
        val output =
            GenericContainer(image)
                .withCopyToContainer(Transferable.of(metrics), "/tmp/metrics.prom")
                .withCreateContainerCmdModifier {
                    it.withEntrypoint(
                        "/bin/sh",
                        "-c",
                        "promtool check metrics < /tmp/metrics.prom 2>&1; echo \"exit=$?\"",
                    )
                }
                .withStartupCheckStrategy(OneShotStartupCheckStrategy())
                .use {
                    it.start()
                    it.logs
                }
        val lines = output.lines().filter { it.isNotBlank() }
        val exitCode = lines.last().removePrefix("exit=").toInt()
        return Check(exitCode, lines.dropLast(1))
    }

    /** The samples in [metrics], Prometheus's text exposition format, by name with labels. */
    fun samples(metrics: String): Map<String, Double> =
        metrics
            .lines()
            .filter { it.isNotEmpty() && !it.startsWith("#") }
            .associate { it.substringBeforeLast(' ') to it.substringAfterLast(' ').toDouble() }
}
