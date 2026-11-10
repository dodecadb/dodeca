package org.dozendb.consensus.ratis

import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCall
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener
import org.apache.ratis.thirdparty.io.grpc.Metadata
import org.apache.ratis.thirdparty.io.grpc.ServerCall
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler
import org.apache.ratis.thirdparty.io.grpc.Status
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException

/**
 * Starts [call] so that the first of its messages that [refusal] gives a failure for closes it with
 * that failure, before Ratis sees the message. Ratis then learns nothing more of the call, which it
 * would log the end of.
 */
internal fun <Q, R> startRefusable(
    call: ServerCall<Q, R>,
    headers: Metadata,
    next: ServerCallHandler<Q, R>,
    refusal: (Q) -> StatusRuntimeException?,
): ServerCall.Listener<Q> {
    val refusable = RefusableCall(call)
    return object :
        ForwardingServerCallListener.SimpleForwardingServerCallListener<Q>(
            next.startCall(refusable, headers)
        ) {
        override fun onMessage(message: Q) {
            if (refusable.refused) return
            val failure = refusal(message)
            if (failure != null) refusable.refuse(failure) else super.onMessage(message)
        }

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
 * A call refused while Ratis may still answer an earlier request on it, from another thread: its
 * methods take turns, and do nothing once it is closed.
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
