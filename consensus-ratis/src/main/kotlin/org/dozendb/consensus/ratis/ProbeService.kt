package org.dozendb.consensus.ratis

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.future.await
import org.apache.ratis.thirdparty.io.grpc.CallOptions
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition
import org.apache.ratis.thirdparty.io.grpc.Status
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.PeerInfo
import org.slf4j.LoggerFactory

/**
 * Unary gRPC methods served on the Raft port, next to the Ratis services: a probe answers "who are
 * you and which groups do you host", and an announcement tells the same unasked. Cluster formation
 * uses probes to learn member ids before any consensus group exists.
 */
internal object ProbeService {
    private val log = LoggerFactory.getLogger(ProbeService::class.java)

    private const val SERVICE_NAME = "dozendb.cluster.Probe"

    private object BytesMarshaller : MethodDescriptor.Marshaller<ByteArray> {
        override fun stream(value: ByteArray): InputStream = ByteArrayInputStream(value)

        override fun parse(stream: InputStream): ByteArray = stream.use { it.readAllBytes() }
    }

    private val probeMethod = method("Probe")

    /** Absent on versions without announcements, which answer it as unimplemented. */
    private val announceMethod = method("Announce")

    private fun method(name: String): MethodDescriptor<ByteArray, ByteArray> =
        MethodDescriptor.newBuilder<ByteArray, ByteArray>()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, name))
            .setRequestMarshaller(BytesMarshaller)
            .setResponseMarshaller(BytesMarshaller)
            .build()

    /** The server side, answering probes with [answer] and handing announcements to [announced]. */
    fun definition(answer: () -> PeerInfo, announced: (PeerInfo) -> Unit): ServerServiceDefinition =
        ServerServiceDefinition.builder(SERVICE_NAME)
            .addMethod(
                probeMethod,
                serve("answer a probe") { request ->
                    val sender = ProbeCodec.decodeRequest(request)
                    log.debug("Probed by {}", sender)
                    ProbeCodec.encodeResponse(answer())
                },
            )
            .addMethod(
                announceMethod,
                serve("take an announcement") { request ->
                    val info = ProbeCodec.decodeAnnouncement(request)
                    log.debug("Announced by {}", info.member)
                    announced(info)
                    ByteArray(0)
                },
            )
            .build()

    private fun serve(what: String, handle: (ByteArray) -> ByteArray) =
        ServerCalls.asyncUnaryCall { request: ByteArray, response: StreamObserver<ByteArray> ->
            try {
                response.onNext(handle(request))
                response.onCompleted()
            } catch (e: ProbeCodec.UnsupportedFormatException) {
                response.onError(
                    Status.FAILED_PRECONDITION.withDescription(e.message).asRuntimeException()
                )
            } catch (e: Exception) {
                log.warn("Failed to {}", what, e)
                response.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException())
            }
        }

    /** Probes the member at [target] on behalf of [sender], over TLS with [tls] if not null. */
    suspend fun probe(
        target: HostPort,
        sender: Member,
        timeout: Duration,
        tls: SslContext?,
    ): PeerInfo =
        try {
            ProbeCodec.decodeResponse(
                call(
                    probeMethod,
                    "probe of",
                    target,
                    ProbeCodec.encodeRequest(sender),
                    timeout,
                    tls,
                )
            )
        } catch (e: ProbeCodec.UnsupportedFormatException) {
            throw ConsensusUnavailableException("probe of $target failed: ${e.message}", e)
        }

    /** Tells the member at [target] [info], over TLS with [tls] if not null. */
    suspend fun announce(target: HostPort, info: PeerInfo, timeout: Duration, tls: SslContext?) {
        call(
            announceMethod,
            "announcement to",
            target,
            ProbeCodec.encodeAnnouncement(info),
            timeout,
            tls,
        )
    }

    /** Calls [method] of the member at [target]; failures say "[what] [target] failed". */
    private suspend fun call(
        method: MethodDescriptor<ByteArray, ByteArray>,
        what: String,
        target: HostPort,
        request: ByteArray,
        timeout: Duration,
        tls: SslContext?,
    ): ByteArray {
        val channel =
            NettyChannelBuilder.forAddress(target.host, target.port)
                .apply { if (tls == null) usePlaintext() else sslContext(tls) }
                .build()
        try {
            val reply = CompletableFuture<ByteArray>()
            ClientCalls.asyncUnaryCall(
                channel.newCall(
                    method,
                    CallOptions.DEFAULT.withDeadlineAfter(
                        timeout.toMillis(),
                        TimeUnit.MILLISECONDS,
                    ),
                ),
                request,
                object : StreamObserver<ByteArray> {
                    override fun onNext(value: ByteArray) {
                        reply.complete(value)
                    }

                    override fun onError(t: Throwable) {
                        reply.completeExceptionally(t)
                    }

                    override fun onCompleted() {
                        reply.completeExceptionally(IllegalStateException("no response"))
                    }
                },
            )
            return reply.await()
        } catch (e: StatusRuntimeException) {
            throw ConsensusUnavailableException("$what $target failed: ${describe(e)}", e)
        } finally {
            channel.shutdownNow()
        }
    }
}
