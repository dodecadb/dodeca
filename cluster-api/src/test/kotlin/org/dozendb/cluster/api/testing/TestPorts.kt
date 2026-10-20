package org.dozendb.cluster.api.testing

import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * Ports for test servers that are chosen before the servers bind them. They come from a range that
 * operating systems do not hand out for outgoing connections (Linux uses 32768 and up, macOS and
 * Windows 49152 and up), so a port that is free when chosen stays free unless another test takes
 * it; each JVM walks the range from a random point.
 */
object TestPorts {
    private const val FIRST = 20000
    private const val COUNT = 12000
    private val next = AtomicInteger(Random.nextInt(COUNT))

    /** A port that is free on every interface now. */
    fun free(): Int {
        repeat(COUNT) {
            val port = FIRST + Math.floorMod(next.getAndIncrement(), COUNT)
            if (available(port)) return port
        }
        error("no free port in $FIRST..${FIRST + COUNT - 1}")
    }

    private fun available(port: Int): Boolean = runCatching {
        ServerSocket(port).close()
        ServerSocket(port, 1, InetAddress.getLoopbackAddress()).close()
    }
        .isSuccess
}
