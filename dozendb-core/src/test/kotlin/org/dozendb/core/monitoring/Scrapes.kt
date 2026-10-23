package org.dozendb.core.monitoring

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.neo4j.kernel.internal.GraphDatabaseAPI

private val http = HttpClient.newHttpClient()

/** The metrics service of the DBMS that runs this database, if metrics are reported. */
fun GraphDatabaseAPI.metrics(): MetricsService? =
    dependencyResolver.resolveTypeDependencies(MetricsService::class.java).firstOrNull()

/** The server's metrics service, if metrics are reported. */
fun StandaloneServer.metrics(): MetricsService? = database(SYSTEM).metrics()

/** Gets [path] from the Prometheus endpoint. */
fun MetricsService.scrape(
    path: String = "/metrics",
    gzip: Boolean = false,
): HttpResponse<ByteArray> =
    http.send(
        HttpRequest.newBuilder(URI("http://localhost:${prometheus!!.boundAddress!!.port}$path"))
            .apply { if (gzip) header("Accept-Encoding", "gzip") }
            .build(),
        BodyHandlers.ofByteArray(),
    )

/** Gets [path] from the server's Prometheus endpoint. */
fun StandaloneServer.scrape(
    path: String = "/metrics",
    gzip: Boolean = false,
): HttpResponse<ByteArray> = metrics()!!.scrape(path, gzip)

/** The samples the Prometheus endpoint reports now, by name with labels. */
fun MetricsService.samples(): Map<String, Double> {
    val response = scrape()
    check(response.statusCode() == 200) { "HTTP ${response.statusCode()}" }
    return response
        .body()
        .decodeToString()
        .lines()
        .filter { it.isNotEmpty() && !it.startsWith("#") }
        .associate { it.substringBeforeLast(' ') to it.substringAfterLast(' ').toDouble() }
}

/** The samples the server's Prometheus endpoint reports now, by name with labels. */
fun StandaloneServer.samples(): Map<String, Double> = metrics()!!.samples()
