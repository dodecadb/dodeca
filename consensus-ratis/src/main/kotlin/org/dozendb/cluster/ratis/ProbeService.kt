package org.dozendb.cluster.ratis

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
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.PeerInfo
import org.slf4j.LoggerFactory

/**
 * A unary gRPC method served on the Raft port, next to the Ratis services, that answers "who are
 * you and which groups do you host". Cluster formation uses it to learn member ids before any
 * consensus group exists.
 */
internal object ProbeService {
    private val log = LoggerFactory.getLogger(ProbeService::class.java)

    private const val SERVICE_NAME = "dozendb.cluster.Probe"

    private object BytesMarshaller : MethodDescriptor.Marshaller<ByteArray> {
        override fun stream(value: ByteArray): InputStream = ByteArrayInputStream(value)

        override fun parse(stream: InputStream): ByteArray = stream.use { it.readAllBytes() }
    }

    private val method: MethodDescriptor<ByteArray, ByteArray> =
        MethodDescriptor.newBuilder<ByteArray, ByteArray>()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Probe"))
            .setRequestMarshaller(BytesMarshaller)
            .setResponseMarshaller(BytesMarshaller)
            .build()

    /** The server side, answering with [answer]. */
    fun definition(answer: () -> PeerInfo): ServerServiceDefinition =
        ServerServiceDefinition.builder(SERVICE_NAME)
            .addMethod(
                method,
                ServerCalls.asyncUnaryCall { request: ByteArray, response: StreamObserver<ByteArray>
                    ->
                    try {
                        val sender = ProbeCodec.decodeRequest(request)
                        log.debug("Probed by {}", sender)
                        response.onNext(ProbeCodec.encodeResponse(answer()))
                        response.onCompleted()
                    } catch (e: ProbeCodec.UnsupportedFormatException) {
                        response.onError(
                            Status.FAILED_PRECONDITION.withDescription(e.message)
                                .asRuntimeException()
                        )
                    } catch (e: Exception) {
                        log.warn("Failed to answer a probe", e)
                        response.onError(
                            Status.INTERNAL.withDescription(e.toString()).asRuntimeException()
                        )
                    }
                },
            )
            .build()

    /** Probes the member at [target] on behalf of [sender]. */
    suspend fun probe(target: HostPort, sender: Member, timeout: Duration): PeerInfo {
        val channel =
            NettyChannelBuilder.forAddress(target.host, target.port).usePlaintext().build()
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
                ProbeCodec.encodeRequest(sender),
                object : StreamObserver<ByteArray> {
                    override fun onNext(value: ByteArray) {
                        reply.complete(value)
                    }

                    override fun onError(t: Throwable) {
                        reply.completeExceptionally(t)
                    }

                    override fun onCompleted() {
                        reply.completeExceptionally(IllegalStateException("no probe response"))
                    }
                },
            )
            return ProbeCodec.decodeResponse(reply.await())
        } catch (e: StatusRuntimeException) {
            throw ConsensusUnavailableException("probe of $target failed: ${e.status}", e)
        } catch (e: ProbeCodec.UnsupportedFormatException) {
            throw ConsensusUnavailableException("probe of $target failed: ${e.message}", e)
        } finally {
            channel.shutdownNow()
        }
    }
}
