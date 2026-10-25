package org.dozendb.consensus.ratis

import java.util.UUID
import org.apache.ratis.grpc.GrpcUtil
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.exceptions.ServerNotReadyException
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCall
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener
import org.apache.ratis.thirdparty.io.grpc.Metadata
import org.apache.ratis.thirdparty.io.grpc.ServerCall
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor
import org.apache.ratis.thirdparty.io.grpc.Status
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException

/**
 * Refuses the appends and snapshot notifications a leader sends for a group that is [closed] on
 * this member, as after an entry failed to apply here. Ratis closes such a group on this member
 * only, and the leader keeps trying the member as one that is down; Ratis would log an error for
 * each of these requests. The call is closed instead, before Ratis sees the request, with the
 * failure Ratis answers for a closed group, which the leader handles as such.
 */
internal class RefuseClosedGroups(private val closed: (UUID) -> Boolean) : ServerInterceptor {
    override fun <Q, R> interceptCall(
        call: ServerCall<Q, R>,
        headers: Metadata,
        next: ServerCallHandler<Q, R>,
    ): ServerCall.Listener<Q> {
        if (call.methodDescriptor.fullMethodName !in GROUP_STREAMS) {
            return next.startCall(call, headers)
        }
        val refusable = RefusableCall(call)
        return object :
            ForwardingServerCallListener.SimpleForwardingServerCallListener<Q>(
                next.startCall(refusable, headers)
            ) {
            override fun onMessage(message: Q) {
                if (refusable.refused) return
                val refusal = refusal(message)
                if (refusal != null) refusable.refuse(refusal) else super.onMessage(message)
            }

            // Once refused, Ratis learns nothing more of the call, which would log its end.
            override fun onHalfClose() {
                if (!refusable.refused) super.onHalfClose()
            }

            override fun onCancel() {
                if (!refusable.refused) super.onCancel()
            }

            override fun onComplete() {
                if (!refusable.refused) super.onComplete()
            }

            override fun onReady() {
                if (!refusable.refused) super.onReady()
            }
        }
    }

    /**
     * A call refused while Ratis may still answer an earlier request on it, from another thread:
     * its methods take turns, and do nothing once it is closed.
     */
    private class RefusableCall<Q, R>(call: ServerCall<Q, R>) :
        ForwardingServerCall.SimpleForwardingServerCall<Q, R>(call) {
        @Volatile
        var refused = false
            private set

        private var closed = false

        @Synchronized
        fun refuse(refusal: StatusRuntimeException) {
            refused = true
            close(refusal.status, refusal.trailers ?: Metadata())
        }

        @Synchronized
        override fun sendHeaders(headers: Metadata) {
            if (!closed) super.sendHeaders(headers)
        }

        @Synchronized
        override fun sendMessage(message: R) {
            if (!closed) super.sendMessage(message)
        }

        @Synchronized
        override fun close(status: Status, trailers: Metadata) {
            if (closed) return
            closed = true
            super.close(status, trailers)
        }
    }

    /** The failure for [message], if it is a request for a closed group. */
    private fun refusal(message: Any?): StatusRuntimeException? {
        val request: RaftRpcRequestProto =
            when (message) {
                is AppendEntriesRequestProto -> message.serverRequest
                is InstallSnapshotRequestProto -> message.serverRequest
                else -> return null
            }
        val group = RaftGroupId.valueOf(request.raftGroupId.id)
        if (!closed(group.uuid)) return null
        // With the call id the leader finds the request that failed, as for Ratis' own failures.
        val failure = ServerNotReadyException("$group is closed on this member")
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
