package org.dozendb.core.monitoring

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.net.InetSocketAddress
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.zip.GZIPOutputStream
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLog

/**
 * Serves [registry]'s metrics to Prometheus over HTTP at [address], on `/metrics` and `/`, gzipped
 * when the scraper accepts it. Like Neo4j Enterprise's endpoint, it has no TLS and no
 * authentication.
 */
class PrometheusEndpoint(
    private val address: SocketAddress,
    private val registry: MetricRegistry,
    private val log: InternalLog,
) : LifecycleAdapter() {
    private val text = PrometheusText(log)
    private var server: HttpServer? = null
    private var executor: ExecutorService? = null

    /** The address bound, with the port chosen when the configured one is 0; null when stopped. */
    @Volatile
    var boundAddress: InetSocketAddress? = null
        private set

    override fun start() {
        val server =
            try {
                HttpServer.create(InetSocketAddress(address.hostname, address.port), 0)
            } catch (e: NoClassDefFoundError) {
                throw IllegalStateException(
                    "server.metrics.prometheus.enabled needs the JDK module jdk.httpserver, " +
                        "which this Java runtime does not have",
                    e,
                )
            } catch (e: IOException) {
                throw IOException(
                    "The Prometheus endpoint cannot bind to $address " +
                        "(server.metrics.prometheus.endpoint): ${e.message}",
                    e,
                )
            }
        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "dozendb-prometheus").apply { isDaemon = true }
        }
        server.executor = executor
        server.createContext("/", ::handle)
        server.start()
        this.server = server
        this.executor = executor
        boundAddress = server.address
        log.info("Prometheus metrics endpoint listening on ${server.address}.")
    }

    override fun stop() {
        boundAddress = null
        server?.stop(0)
        executor?.shutdownNow()
        server = null
        executor = null
    }

    private fun handle(exchange: HttpExchange) {
        exchange.use {
            val path = exchange.requestURI.path
            when {
                path != "/" && path != "/metrics" -> exchange.sendResponseHeaders(404, -1)
                exchange.requestMethod != "GET" && exchange.requestMethod != "HEAD" -> {
                    exchange.responseHeaders.add("Allow", "GET, HEAD")
                    exchange.sendResponseHeaders(405, -1)
                }
                else -> respond(exchange)
            }
        }
    }

    private fun respond(exchange: HttpExchange) {
        val gzip =
            exchange.requestHeaders["Accept-Encoding"].orEmpty().any {
                it.split(',').any { encoding -> encoding.trim().startsWith("gzip") }
            }
        val body = ByteArrayOutputStream()
        val stream: OutputStream = if (gzip) GZIPOutputStream(body) else body
        stream.writer(Charsets.UTF_8).use { text.write(registry.metrics(), it) }
        exchange.responseHeaders.add("Content-Type", PrometheusText.CONTENT_TYPE)
        if (gzip) exchange.responseHeaders.add("Content-Encoding", "gzip")
        if (exchange.requestMethod == "HEAD") {
            exchange.sendResponseHeaders(200, -1)
        } else {
            exchange.sendResponseHeaders(200, body.size().toLong())
            exchange.responseBody.use { body.writeTo(it) }
        }
    }
}
