package dev.dodeca.consensus.ratis

import java.util.UUID
import org.apache.ratis.grpc.GrpcUtil
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.exceptions.ServerNotReadyException
import org.apache.ratis.thirdparty.io.grpc.Metadata
import org.apache.ratis.thirdparty.io.grpc.ServerCall
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException

/**
 * Refuses the appends and snapshot notifications a leader sends for a group that cannot take them
 * on this member, as [unavailable] says why: one that is closed, as after an entry failed to apply
 * here, or one still starting, as when the others formed the group first. Ratis closes a failed
 * group on this member only, and the leader keeps trying the member as one that is down; Ratis
 * would log an error for each of these requests. The call is closed instead, before Ratis sees the
 * request, with the failure Ratis answers for a group that is not ready, which the leader handles
 * as such.
 */
internal class RefuseUnavailableGroups(private val unavailable: (UUID) -> String?) :
    ServerInterceptor {
    override fun <Q, R> interceptCall(
        call: ServerCall<Q, R>,
        headers: Metadata,
        next: ServerCallHandler<Q, R>,
    ): ServerCall.Listener<Q> {
        if (call.methodDescriptor.fullMethodName !in GROUP_STREAMS) {
            return next.startCall(call, headers)
        }
        return startRefusable(call, headers, next) { refusal(it) }
    }

    /** The failure for [message], if it is a request for a group that cannot take it. */
    private fun refusal(message: Any?): StatusRuntimeException? {
        val request: RaftRpcRequestProto =
            when (message) {
                is AppendEntriesRequestProto -> message.serverRequest
                is InstallSnapshotRequestProto -> message.serverRequest
                else -> return null
            }
        val group = RaftGroupId.valueOf(request.raftGroupId.id)
        val why = unavailable(group.uuid) ?: return null
        // With the call id the leader finds the request that failed, as for Ratis' own failures.
        val failure = ServerNotReadyException("$group is $why on this member")
        return if (message is AppendEntriesRequestProto) {
            GrpcUtil.wrapException(failure, request.callId, message.entriesCount == 0)
        } else {
            GrpcUtil.wrapException(failure, request.callId)
        }
    }

    private companion object {
        /** The streams in which a leader sends a group's requests, one group per stream. */
        val GROUP_STREAMS =
            setOf(
                RaftServerProtocolServiceGrpc.getAppendEntriesMethod().fullMethodName,
                RaftServerProtocolServiceGrpc.getInstallSnapshotMethod().fullMethodName,
            )
    }
}
