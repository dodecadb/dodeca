package dev.dodeca.consensus.ratis

import org.apache.ratis.grpc.GrpcUtil
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc
import org.apache.ratis.protocol.RaftPeerId
import org.apache.ratis.protocol.exceptions.ServerNotReadyException
import org.apache.ratis.thirdparty.io.grpc.Metadata
import org.apache.ratis.thirdparty.io.grpc.ServerCall
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException

/**
 * Refuses the Raft requests addressed to another member than this one, [self]. A server that
 * replaces a member under a new id at the member's address gets the requests of the groups that
 * still count the member, until it is removed from them; Ratis would answer them as this member's
 * own, so that leaders took the member for one that runs, and candidates its answers for its votes.
 * The call is closed instead, before Ratis sees the request, with the failure Ratis answers when it
 * is not ready: the member counts as down.
 */
internal class RefuseOtherMembers(private val self: RaftPeerId) : ServerInterceptor {
    override fun <Q, R> interceptCall(
        call: ServerCall<Q, R>,
        headers: Metadata,
        next: ServerCallHandler<Q, R>,
    ): ServerCall.Listener<Q> {
        if (call.methodDescriptor.fullMethodName !in SERVER_METHODS) {
            return next.startCall(call, headers)
        }
        return startRefusable(call, headers, next) { refusal(it) }
    }

    /** The failure for [message], if it is a request for another member. */
    private fun refusal(message: Any?): StatusRuntimeException? {
        val request: RaftRpcRequestProto =
            when (message) {
                is AppendEntriesRequestProto -> message.serverRequest
                is InstallSnapshotRequestProto -> message.serverRequest
                is RequestVoteRequestProto -> message.serverRequest
                is StartLeaderElectionRequestProto -> message.serverRequest
                is ReadIndexRequestProto -> message.serverRequest
                else -> return null
            }
        if (request.replyId.isEmpty) return null
        val target = RaftPeerId.valueOf(request.replyId)
        if (target == self) return null
        val failure = ServerNotReadyException("$target is not at this address: $self is")
        // With the call id the sender finds the request that failed, as for Ratis' own failures.
        return if (message is AppendEntriesRequestProto) {
            GrpcUtil.wrapException(failure, request.callId, message.entriesCount == 0)
        } else {
            GrpcUtil.wrapException(failure, request.callId)
        }
    }

    private companion object {
        /** The requests of a member of a group to another. */
        val SERVER_METHODS =
            setOf(
                RaftServerProtocolServiceGrpc.getAppendEntriesMethod().fullMethodName,
                RaftServerProtocolServiceGrpc.getInstallSnapshotMethod().fullMethodName,
                RaftServerProtocolServiceGrpc.getRequestVoteMethod().fullMethodName,
                RaftServerProtocolServiceGrpc.getStartLeaderElectionMethod().fullMethodName,
                RaftServerProtocolServiceGrpc.getReadIndexMethod().fullMethodName,
            )
    }
}
