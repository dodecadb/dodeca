package org.dozendb.core.monitoring

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM

private val http = HttpClient.newHttpClient()

/** The server's metrics service, if metrics are reported. */
fun StandaloneServer.metrics(): MetricsService? =
    database(SYSTEM)
        .dependencyResolver
        .resolveTypeDependencies(MetricsService::class.java)
        .firstOrNull()

/** Gets [path] from the server's Prometheus endpoint. */
fun StandaloneServer.scrape(
    path: String = "/metrics",
    gzip: Boolean = false,
): HttpResponse<ByteArray> =
    http.send(
        HttpRequest.newBuilder(
                URI("http://localhost:${metrics()!!.prometheus!!.boundAddress!!.port}$path")
            )
            .apply { if (gzip) header("Accept-Encoding", "gzip") }
            .build(),
        BodyHandlers.ofByteArray(),
    )

/** The samples the Prometheus endpoint reports now, by name with labels. */
fun StandaloneServer.samples(): Map<String, Double> {
    val response = scrape()
    check(response.statusCode() == 200) { "HTTP ${response.statusCode()}" }
    return response
        .body()
        .decodeToString()
        .lines()
        .filter { it.isNotEmpty() && !it.startsWith("#") }
        .associate { it.substringBeforeLast(' ') to it.substringAfterLast(' ').toDouble() }
}
