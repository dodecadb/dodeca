package org.dozendb.core.copy

import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path

/**
 * The entities a store copy left out because their records could not be read, as in a store with
 * inconsistencies: counted, and listed in [file] as they are found, which exists only if there are
 * any. A report of an earlier copy is deleted.
 */
internal class CopyReport(val file: Path) : AutoCloseable {
    private var writer: BufferedWriter? = null

    init {
        Files.deleteIfExists(file)
    }

    /** How many entities could not be read. */
    @get:Synchronized
    var unreadable = 0L
        private set

    @Synchronized
    fun unreadable(entity: String, id: Long, cause: Exception) {
        unreadable++
        val writer =
            writer
                ?: Files.newBufferedWriter(
                        Files.createDirectories(file.toAbsolutePath().parent).resolve(file.fileName)
                    )
                    .also { writer = it }
        writer.write("$entity $id: ${cause.javaClass.simpleName}: ${cause.message}")
        writer.newLine()
    }

    @Synchronized
    override fun close() {
        writer?.close()
        writer = null
    }
}
