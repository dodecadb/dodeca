package org.dozendb.consensus.ratis

import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener
import org.apache.ratis.thirdparty.io.grpc.Metadata
import org.apache.ratis.thirdparty.io.grpc.ServerCall
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor
import org.apache.ratis.thirdparty.io.grpc.Status

/**
 * Takes no more requests from other members once this member is [stopping]: Ratis closes a server's
 * groups before it stops taking requests, so each append a leader sends meanwhile would fail on a
 * closing group and be logged as an error. New calls are refused; the messages of calls already
 * open, such as a leader's stream of appends, are dropped, and the leader times out on them as on a
 * member that is down, until the server closes the calls.
 */
internal class RefuseWhileStopping(private val stopping: () -> Boolean) : ServerInterceptor {
    override fun <Q, R> interceptCall(
        call: ServerCall<Q, R>,
        headers: Metadata,
        next: ServerCallHandler<Q, R>,
    ): ServerCall.Listener<Q> {
        if (stopping()) {
            call.close(STOPPING, Metadata())
            return object : ServerCall.Listener<Q>() {}
        }
        return object :
            ForwardingServerCallListener.SimpleForwardingServerCallListener<Q>(
                next.startCall(call, headers)
            ) {
            override fun onMessage(message: Q) {
                if (!stopping()) super.onMessage(message)
            }
        }
    }

    private companion object {
        val STOPPING: Status = Status.UNAVAILABLE.withDescription("this member is stopping")
    }
}
