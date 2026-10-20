package org.dozendb.cluster.ratis

import org.apache.ratis.util.ExitUtils

/**
 * Keeps Ratis from ending the JVM it shares with Neo4j. Ratis calls `System.exit` when, for
 * example, its gRPC server cannot bind its port, and the first use of its exit utilities replaces
 * the JVM's default uncaught exception handler with one that exits on any uncaught exception in any
 * thread. Applied before any Ratis server is built: Ratis then throws instead of exiting, and the
 * previous handler is restored.
 */
internal object RatisJvmSafety {
    init {
        val previous = Thread.getDefaultUncaughtExceptionHandler()
        ExitUtils.disableSystemExit()
        ExitUtils.setTerminateOnUncaughtException(false)
        Thread.setDefaultUncaughtExceptionHandler(previous)
    }

    /** Applies the protection once per JVM. */
    fun ensure() {}
}
