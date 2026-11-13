package dev.dodeca.consensus.ratis

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.PrintStream

/**
 * Keeps a copy of what is logged while open. The tests log through SLF4J's simple logger, which
 * writes each line to `System.err` as it is then; the lines still go there too.
 */
internal class LogCapture : AutoCloseable {
    private val original = System.err
    private val captured = ByteArrayOutputStream()

    init {
        val copy =
            object : OutputStream() {
                override fun write(b: Int) {
                    synchronized(captured) { captured.write(b) }
                    original.write(b)
                }

                override fun write(b: ByteArray, off: Int, len: Int) {
                    synchronized(captured) { captured.write(b, off, len) }
                    original.write(b, off, len)
                }

                override fun flush() = original.flush()
            }
        System.setErr(PrintStream(copy, true, Charsets.UTF_8))
    }

    /** The lines logged so far. */
    fun lines(): List<String> = synchronized(captured) { captured.toString(Charsets.UTF_8) }.lines()

    override fun close() = System.setErr(original)
}
