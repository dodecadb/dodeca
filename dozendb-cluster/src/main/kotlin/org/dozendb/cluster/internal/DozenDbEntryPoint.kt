package org.dozendb.cluster.internal

import kotlin.system.exitProcess
import org.neo4j.server.BlockingBootstrapper
import org.neo4j.server.Bootstrapper
import org.neo4j.server.NeoBootstrapper
import org.neo4j.server.startup.EntryPoint

/**
 * The JVM entry point Neo4j's launcher starts. Registered in `META-INF/services` with a higher
 * priority than Neo4j Community's entry point, so it wins regardless of classpath order.
 */
class DozenDbEntryPoint : EntryPoint {
    override fun getPriority(): Int = EntryPoint.Priority.MEDIUM.ordinal

    companion object {
        @Volatile private var bootstrapper: Bootstrapper? = null

        @JvmStatic
        fun main(args: Array<String>) {
            val status = NeoBootstrapper.start(DozenDbBootstrapper(), *args)
            if (status != 0) exitProcess(status)
        }

        /** Used by the Windows service wrapper. */
        @JvmStatic
        fun start(args: Array<String>) {
            val blocking = BlockingBootstrapper(DozenDbBootstrapper())
            bootstrapper = blocking
            exitProcess(NeoBootstrapper.start(blocking, *args))
        }

        /** Used by the Windows service wrapper. */
        @JvmStatic
        fun stop(@Suppress("UNUSED_PARAMETER") args: Array<String>) {
            bootstrapper?.stop()
        }
    }
}
