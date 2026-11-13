package dev.dodeca.consensus.ratis

import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException

/**
 * A one-line account of a failed gRPC call: its status code and description, and the innermost
 * cause, which tells why a TLS handshake failed. gRPC's own `Status.toString` holds the stack trace
 * of the cause.
 */
internal fun describe(e: StatusRuntimeException): String {
    val status = e.status
    val root = generateSequence(status.cause) { it.cause }.lastOrNull()
    return buildString {
        append(status.code)
        status.description?.lineSequence()?.first()?.let { append(": ").append(it) }
        root?.let { append(" (").append(it.toString().lineSequence().first()).append(')') }
    }
}
