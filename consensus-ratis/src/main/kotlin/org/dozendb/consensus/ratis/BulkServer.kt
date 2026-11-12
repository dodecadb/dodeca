package org.dozendb.consensus.ratis

import java.io.IOException
import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.ratis.thirdparty.io.grpc.Server
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.HostPort

/**
 * Serves [BulkSource]s on an address of its own, to clients outside the cluster such as backups, as
 * members serve each other on the Raft port: over TLS with a [TlsConfig], or in plaintext.
 */
class BulkServer
private constructor(
    private val server: Server,
    private val executor: ExecutorService,
) : AutoCloseable {
    /** The port it listens on: the one asked for, or the one the system chose for port 0. */
    val port: Int
        get() = server.port

    /** Stops listening and cancels the transfers under way. */
    override fun close() {
        server.shutdownNow()
        executor.shutdownNow()
        server.awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS)
    }

    companion object {
        private const val SHUTDOWN_SECONDS = 10L

        /**
         * Starts serving the sources that [sources] finds by name on [address], over TLS with [tls]
         * or in plaintext if it is null. The sources run on threads named after [name].
         *
         * @throws java.io.IOException if it cannot listen on [address], saying why as the JDK words
         *   it, such as `Address already in use`.
         */
        fun start(
            address: InetSocketAddress,
            tls: ReloadableTls?,
            name: String,
            sources: (String) -> BulkSource?,
        ): BulkServer {
            val executor = Executors.newCachedThreadPool { runnable ->
                Thread(runnable, "$name-source").apply { isDaemon = true }
            }
            val server =
                NettyServerBuilder.forAddress(address)
                    .addService(BulkService.definition(sources, executor))
                    // Clients send keep-alive pings to detect dead servers.
                    .permitKeepAliveTime(BulkService.KEEP_ALIVE_SECONDS, TimeUnit.SECONDS)
                    .apply { if (tls != null) sslContext(TlsContexts(tls).server) }
                    .build()
            try {
                server.start()
            } catch (e: IOException) {
                executor.shutdownNow()
                throw IOException(whyNotListening(e), e)
            } catch (e: Exception) {
                executor.shutdownNow()
                throw e
            }
            return BulkServer(server, executor)
        }

        /**
         * How Netty's native transport, which gRPC uses on Linux, words why a call failed:
         * `bind(..) failed with error(-98): Address already in use`, where the JDK says `Address
         * already in use`.
         */
        private val NATIVE_ERROR = Regex("""\w+\(\.\.\) failed(?: with error\(-?\d+\))?: (.+)""")

        /** Why gRPC could not listen: [e] itself says only "Failed to bind to address". */
        internal fun whyNotListening(e: IOException): String {
            val reason = generateSequence<Throwable>(e) { it.cause }.last()
            val message = reason.message ?: return reason.toString()
            return NATIVE_ERROR.matchEntire(message)?.groupValues?.get(1) ?: message
        }

        /**
         * Asks the [BulkServer] at [address] for [name] with [request] and passes the response to
         * [consume] on the calling thread, over TLS with [tls] or in plaintext if it is null.
         *
         * @throws IOException if the server cannot be reached, does not serve [name], or fails
         *   while streaming, or if [consume] throws one; its message tells why in one line, as the
         *   gRPC status and the innermost cause.
         */
        fun <T> fetch(
            address: HostPort,
            tls: ReloadableTls?,
            name: String,
            request: ByteArray,
            consume: (InputStream) -> T,
        ): T =
            BulkService.fetch(
                address,
                name,
                request,
                tls?.let { TlsContexts(it).client },
                consume,
            ) { message, cause ->
                IOException(message, cause)
            }
    }
}
