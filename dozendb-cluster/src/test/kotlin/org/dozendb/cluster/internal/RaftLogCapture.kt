package org.dozendb.cluster.internal

import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import org.junit.platform.launcher.LauncherSession
import org.junit.platform.launcher.LauncherSessionListener
import org.neo4j.logging.Level
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.server.logging.slf4j.SLF4JLogBridge
import org.slf4j.LoggerFactory
import org.slf4j.helpers.NOPLogger

/**
 * Captures the SLF4J logging of Ratis and DozenDB, which embedded DBMSes do not route to their
 * debug logs: Neo4j's SLF4J bridge is bound once per JVM, not per DBMS. While [writeTo] sends it to
 * a file it is kept; otherwise it is discarded.
 */
internal object RaftLogCapture {
    /** Guarded by this, so that no write reaches a closed file. */
    private var target: OutputStream? = null
    private var installed = false

    /**
     * Routes the logging here. SLF4J binds its provider when first used, so this must run before
     * anything in the JVM logs through SLF4J; [Session] does that.
     */
    @Synchronized
    fun install() {
        if (installed) return
        installed = true
        val output =
            object : OutputStream() {
                override fun write(b: Int) =
                    synchronized(this@RaftLogCapture) {
                        target?.write(b) ?: Unit
                    }

                override fun write(b: ByteArray, off: Int, len: Int) =
                    synchronized(this@RaftLogCapture) { target?.write(b, off, len) ?: Unit }

                override fun flush() = synchronized(this@RaftLogCapture) { target?.flush() ?: Unit }
            }
        SLF4JLogBridge.setInstantiationContext(
            Log4jLogProvider(output, Level.INFO),
            listOf("org.apache.ratis", "org.dozendb"),
        )
        check(LoggerFactory.getLogger("org.dozendb.cluster.LogCapture") !is NOPLogger) {
            "SLF4J was initialised before the Raft log capture"
        }
    }

    /**
     * Appends the captured logging to [file] until the returned handle is closed, or another file
     * takes over.
     */
    fun writeTo(file: Path): AutoCloseable {
        install()
        val stream =
            Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        synchronized(this) {
            target?.close()
            target = stream
        }
        return AutoCloseable {
            synchronized(this) {
                if (target === stream) target = null
                stream.close()
            }
        }
    }

    /**
     * Installs the capture when JUnit starts, before its engines discover tests: ArchUnit's engine
     * logs through SLF4J meanwhile.
     */
    class Session : LauncherSessionListener {
        override fun launcherSessionOpened(session: LauncherSession) = install()
    }
}
