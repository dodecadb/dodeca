package org.dozendb.consensus.testing

import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * Ports for test servers that are chosen before the servers bind them. They come from a range that
 * operating systems do not hand out for outgoing connections (Linux uses 32768 and up, macOS and
 * Windows 49152 and up), so a port that is free when chosen stays free unless another test takes
 * it. The JVMs that a build runs tests in at once each take their own part of the range, by the
 * fork number Maven gives them (`dozendb.test.fork`), so that none takes a port another chose but
 * has yet to bind; each JVM walks its part from a random point.
 */
object TestPorts {
    private const val FIRST = 20000
    private const val COUNT = 12000

    /** Parts of the range, as many as JVMs run at once at most: a later fork shares a part. */
    private const val PARTS = 6

    private val part: Int? =
        System.getProperty("dozendb.test.fork")?.toIntOrNull()?.let { Math.floorMod(it - 1, PARTS) }
    private val first = if (part == null) FIRST else FIRST + part * (COUNT / PARTS)
    private val count = if (part == null) COUNT else COUNT / PARTS
    private val next = AtomicInteger(Random.nextInt(count))

    /** A port that is free on every interface now. */
    fun free(): Int {
        repeat(count) {
            val port = first + Math.floorMod(next.getAndIncrement(), count)
            if (available(port)) return port
        }
        error("no free port in $first..${first + count - 1}")
    }

    private fun available(port: Int): Boolean = runCatching {
        ServerSocket(port).close()
        ServerSocket(port, 1, InetAddress.getLoopbackAddress()).close()
    }
        .isSuccess
}
