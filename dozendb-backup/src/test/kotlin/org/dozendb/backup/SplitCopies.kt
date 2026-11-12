package org.dozendb.backup

import java.nio.file.Files
import java.nio.file.Path
import org.neo4j.io.fs.DefaultFileSystemAbstraction

/** Split archives for tests, of parts far smaller than Neo4j's minimum of 1 GiB. */
internal object SplitCopies {
    /**
     * Copies the archive in [file] to [target], split into parts of at most [partSize] bytes;
     * returns the parts.
     */
    fun split(file: Path, target: Path, partSize: Long = 8192): List<Path> {
        val fs = DefaultFileSystemAbstraction()
        val (id, count) =
            SplitArchives.writeParts(fs, target, partSize, {}) { out ->
                out.use { Files.copy(file, it) }
            }
        SplitArchives.writeManifest(Files.newOutputStream(target), id, count)
        return SplitArchives.parts(fs, target)
    }
}
