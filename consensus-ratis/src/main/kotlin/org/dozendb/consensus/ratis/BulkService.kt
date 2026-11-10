package org.dozendb.consensus.ratis

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.withLock
import org.apache.ratis.thirdparty.io.grpc.CallOptions
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition
import org.apache.ratis.thirdparty.io.grpc.Status
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCallStreamObserver
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.HostPort
import org.slf4j.LoggerFactory

/**
 * Server-streaming gRPC method, on the Raft port and on a [BulkServer]'s, that carries [BulkSource]
 * output in chunks, with flow control: the source blocks while the receiver is not ready, so a
 * large store copy never piles up in memory.
 */
internal object BulkService {
    private val log = LoggerFactory.getLogger(BulkService::class.java)

    /**
     * gRPC logs to java.util.logging, which Neo4j sends to its user log, and warns with a stack
     * trace of every stream whose receiver went away: [serve] tells that in one line. Held, as
     * java.util.logging keeps loggers weakly.
     */
    val streamErrors: Logger =
        Logger.getLogger("org.apache.ratis.thirdparty.io.grpc.netty.NettyServerHandler").apply {
            level = Level.SEVERE
        }

    private const val SERVICE_NAME = "dozendb.cluster.Bulk"
    private const val FORMAT_VERSION: Byte = 1
    private const val CHUNK_SIZE = 1 shl 20
    const val KEEP_ALIVE_SECONDS = 20L
    private const val KEEP_ALIVE_TIMEOUT_SECONDS = 20L

    private object BytesMarshaller : MethodDescriptor.Marshaller<ByteArray> {
        override fun stream(value: ByteArray): InputStream = ByteArrayInputStream(value)

        override fun parse(stream: InputStream): ByteArray = stream.use { it.readAllBytes() }
    }

    private val method: MethodDescriptor<ByteArray, ByteArray> =
        MethodDescriptor.newBuilder<ByteArray, ByteArray>()
            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Fetch"))
            .setRequestMarshaller(BytesMarshaller)
            .setResponseMarshaller(BytesMarshaller)
            .build()

    private fun encodeRequest(name: String, request: ByteArray): ByteArray {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).use {
            it.writeByte(FORMAT_VERSION.toInt())
            it.writeUTF(name)
            it.writeInt(request.size)
            it.write(request)
        }
        return buffer.toByteArray()
    }

    private fun decodeRequest(bytes: ByteArray): Pair<String, ByteArray> =
        DataInputStream(ByteArrayInputStream(bytes)).use {
            val version = it.readByte()
            if (version != FORMAT_VERSION) {
                throw IOException("unsupported bulk format version $version")
            }
            val name = it.readUTF()
            val payload = ByteArray(it.readInt()).also { array -> it.readFully(array) }
            name to payload
        }

    /** The server side, serving sources found by [sources] on threads of [executor]. */
    fun definition(
        sources: (String) -> BulkSource?,
        executor: ExecutorService,
    ): ServerServiceDefinition =
        ServerServiceDefinition.builder(SERVICE_NAME)
            .addMethod(
                method,
                ServerCalls.asyncServerStreamingCall {
                    request: ByteArray,
                    responses: StreamObserver<ByteArray> ->
                    serve(
                        request,
                        responses as ServerCallStreamObserver<ByteArray>,
                        sources,
                        executor,
                    )
                },
            )
            .build()

    private fun serve(
        request: ByteArray,
        responses: ServerCallStreamObserver<ByteArray>,
        sources: (String) -> BulkSource?,
        executor: ExecutorService,
    ) {
        val (name, payload) =
            try {
                decodeRequest(request)
            } catch (e: IOException) {
                responses.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException()
                )
                return
            }
        val source = sources(name)
        if (source == null) {
            responses.onError(
                Status.NOT_FOUND.withDescription("no bulk source $name").asRuntimeException()
            )
            return
        }
        val out = FlowControlledOutputStream(responses)
        try {
            executor.execute {
                try {
                    out.use { source.serve(payload, it) }
                    responses.onCompleted()
                } catch (e: Exception) {
                    if (out.cancelled) {
                        log.info("Receiver of bulk source {} went away: {}", name, e.toString())
                    } else if (e is IllegalStateException) {
                        log.info("Bulk source {} refused a request: {}", name, e.message)
                        responses.onError(
                            Status.FAILED_PRECONDITION.withDescription(e.message)
                                .asRuntimeException()
                        )
                    } else {
                        log.warn("Bulk source {} failed", name, e)
                        responses.onError(
                            Status.INTERNAL.withDescription(e.toString()).asRuntimeException()
                        )
                    }
                }
            }
        } catch (e: RejectedExecutionException) {
            responses.onError(
                Status.UNAVAILABLE.withDescription("shutting down").asRuntimeException()
            )
        }
    }

    /** Buffers writes into chunks and sends each chunk once the transport is ready for it. */
    private class FlowControlledOutputStream(
        private val responses: ServerCallStreamObserver<ByteArray>
    ) : OutputStream() {
        private val lock = ReentrantLock()
        private val ready = lock.newCondition()
        private val buffer = ByteArrayOutputStream(CHUNK_SIZE)

        @Volatile
        var cancelled = false
            private set

        init {
            responses.setOnReadyHandler { lock.withLock { ready.signalAll() } }
            responses.setOnCancelHandler {
                cancelled = true
                lock.withLock { ready.signalAll() }
            }
        }

        override fun write(b: Int) {
            buffer.write(b)
            if (buffer.size() >= CHUNK_SIZE) sendChunk()
        }

        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            var position = offset
            var remaining = length
            while (remaining > 0) {
                val count = minOf(remaining, CHUNK_SIZE - buffer.size())
                buffer.write(bytes, position, count)
                position += count
                remaining -= count
                if (buffer.size() >= CHUNK_SIZE) sendChunk()
            }
        }

        override fun flush() {
            if (buffer.size() > 0) sendChunk()
        }

        override fun close() = flush()

        private fun sendChunk() {
            lock.withLock {
                while (!responses.isReady && !cancelled) ready.await(1, TimeUnit.SECONDS)
            }
            if (cancelled) throw IOException("the receiver cancelled the transfer")
            responses.onNext(buffer.toByteArray())
            buffer.reset()
        }
    }

    /**
     * The client side: streams the response into [consume] on the calling thread, over TLS with
     * [tls] if not null, failing once [timeout] passed if it is not null. A failed call, or an
     * [IOException] of [consume], throws what [failure] makes of a one-line account of it.
     */
    fun <T> fetch(
        address: HostPort,
        name: String,
        request: ByteArray,
        tls: SslContext?,
        consume: (InputStream) -> T,
        timeout: Duration? = null,
        failure: (String, Exception) -> Exception = { message, cause ->
            ConsensusUnavailableException("fetching $name from $address failed: $message", cause)
        },
    ): T {
        val channel: ManagedChannel =
            NettyChannelBuilder.forAddress(address.host, address.port)
                .apply { if (tls == null) usePlaintext() else sslContext(tls) }
                .keepAliveTime(KEEP_ALIVE_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build()
        try {
            val chunks =
                ClientCalls.blockingServerStreamingCall(
                    channel,
                    method,
                    timeout?.let {
                        CallOptions.DEFAULT.withDeadlineAfter(it.toNanos(), TimeUnit.NANOSECONDS)
                    } ?: CallOptions.DEFAULT,
                    encodeRequest(name, request),
                )
            return ChunkInputStream(chunks).use(consume)
        } catch (e: StatusRuntimeException) {
            throw failure(describe(e), e)
        } catch (e: IOException) {
            throw failure(e.message ?: e.toString(), e)
        } finally {
            // Cancels the call if the consumer stopped early.
            channel.shutdownNow()
        }
    }

    /** Reads chunks lazily, so the gRPC flow control window throttles the sender. */
    private class ChunkInputStream(private val chunks: Iterator<ByteArray>) : InputStream() {
        private var current = ByteArray(0)
        private var position = 0

        private fun nextChunk(): Boolean {
            while (position >= current.size) {
                val next =
                    try {
                        if (!chunks.hasNext()) return false
                        chunks.next()
                    } catch (e: StatusRuntimeException) {
                        throw IOException(describe(e), e)
                    }
                current = next
                position = 0
            }
            return true
        }

        override fun read(): Int = if (nextChunk()) current[position++].toInt() and 0xff else -1

        override fun read(target: ByteArray, offset: Int, length: Int): Int {
            if (length == 0) return 0
            if (!nextChunk()) return -1
            val count = minOf(length, current.size - position)
            System.arraycopy(current, position, target, offset, count)
            position += count
            return count
        }
    }
}
