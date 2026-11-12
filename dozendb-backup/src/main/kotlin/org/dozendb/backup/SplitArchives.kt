package org.dozendb.backup

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.SequenceInputStream
import java.io.UncheckedIOException
import java.nio.file.Path
import java.util.UUID
import org.neo4j.configuration.Config
import org.neo4j.dbms.archive.ArchiveFormat
import org.neo4j.dbms.archive.ArchiveInput
import org.neo4j.dbms.archive.Dumper.SplitFileOutput
import org.neo4j.io.SplittingOutputStream
import org.neo4j.io.fs.FileSystemAbstraction

/**
 * Archives split into parts, as Neo4j writes dumps and backups with `--split-archive-part-size`,
 * and Neo4j Community's `neo4j-admin database load` reads them: the archive's own file, the
 * manifest, holds the archive's id and the number of its parts; each part, `<file>.1`, `<file>.2`
 * and so on, starts with the id and its number and holds the next bytes of the archive.
 */
internal object SplitArchives {
    /** The size of each part of an archive [option] asks for, or with 0 the config; 0 for none. */
    fun partSize(config: Config, option: Long): Long =
        try {
            SplitFileOutput.determineSplitArtifactSize(config, option)
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException(
                "invalid split archive part size, as --split-archive-part-size or " +
                    "server.split_archive.part_size give it: ${e.message}",
                e,
            )
        }

    /** The [n]th part of the archive in [file]. */
    fun part(file: Path, n: Int): Path = file.resolveSibling("${file.fileName}.$n")

    /**
     * The archive in [file] as one stream: the parts of a split one, in order. A part that is
     * missing, or of another archive, fails reading with an [IOException].
     */
    fun open(fs: FileSystemAbstraction, file: Path): InputStream {
        val opened =
            try {
                ArchiveInput.FileInput.of(fs, file.toAbsolutePath()).open()
            } catch (e: IllegalArgumentException) {
                throw IOException(e.message, e)
            } catch (e: UncheckedIOException) {
                throw e.cause ?: e
            }
        return Rethrowing(SequenceInputStream(ByteArrayInputStream(opened.magic), opened.stream))
    }

    /** The parts of the archive in [file]; none if it is not split. */
    fun parts(fs: FileSystemAbstraction, file: Path): List<Path> {
        val count =
            fs.openAsInputStream(file).use { input ->
                val magic = input.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH)
                if (!SplitFileOutput.MAGIC_MANIFEST_HEADER.matches(magic)) return emptyList()
                DataInputStream(input).readInt()
            }
        return (1..count).map { part(file, it) }
    }

    /**
     * Deletes the archive in [file] and its parts: the file first, so that it is never seen without
     * them.
     */
    fun delete(fs: FileSystemAbstraction, file: Path) {
        val parts = parts(fs, file)
        fs.deleteFile(file)
        parts.forEach(fs::deleteFile)
    }

    /**
     * Writes an archive with [write], which must close the stream it gets, into parts of at most
     * [partSize] bytes each beside [file], which it does not write; passes each part to [created]
     * as it creates it, and returns the archive's id and the number of parts.
     */
    fun writeParts(
        fs: FileSystemAbstraction,
        file: Path,
        partSize: Long,
        created: (Path) -> Unit,
        write: (OutputStream) -> Unit,
    ): Pair<UUID, Int> {
        val id = UUID.randomUUID()
        var count = 0
        val parts = generateSequence {
            count++
            val part = part(file, count)
            if (fs.fileExists(part)) throw IOException("$part exists")
            val stream = fs.openAsOutputStream(part, false)
            created(part)
            try {
                stream.write(SplitFileOutput.MAGIC_DATA_HEADER.bytes)
                DataOutputStream(stream).apply {
                    writeInt(count)
                    writeId(id)
                    flush()
                }
            } catch (e: IOException) {
                stream.close()
                throw UncheckedIOException(e)
            }
            stream
        }
            .iterator()
        try {
            write(SplittingOutputStream(parts, partSize - SplitFileOutput.HEADER_SIZE, null))
        } catch (e: UncheckedIOException) {
            throw e.cause ?: e
        }
        return id to count
    }

    /** Writes the manifest of the archive [id] in [count] parts to [out], and closes it. */
    fun writeManifest(out: OutputStream, id: UUID, count: Int) {
        DataOutputStream(out).use {
            it.write(SplitFileOutput.MAGIC_MANIFEST_HEADER.bytes)
            it.writeInt(count)
            it.writeId(id)
        }
    }

    private fun DataOutputStream.writeId(id: UUID) {
        writeLong(id.mostSignificantBits)
        writeLong(id.leastSignificantBits)
    }

    /**
     * Neo4j's reader of split archives throws [IllegalArgumentException] for a part of another
     * archive; DozenDB's readers take [IOException]s.
     */
    private class Rethrowing(input: InputStream) : FilterInputStream(input) {
        override fun read(): Int = rethrowing { super.read() }

        override fun read(b: ByteArray, off: Int, len: Int): Int = rethrowing {
            super.read(b, off, len)
        }

        override fun skip(n: Long): Long = rethrowing { super.skip(n) }

        private inline fun <T> rethrowing(block: () -> T): T =
            try {
                block()
            } catch (e: IllegalArgumentException) {
                throw IOException(e.message, e)
            }
    }
}
