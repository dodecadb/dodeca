package org.dozendb.core.pagecache

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.BitSet
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import org.neo4j.io.fs.FileSystemAbstraction

/**
 * Which pages of one file the page cache held: of the [pageCount] pages the file had, those in
 * [pages].
 */
internal data class PageCacheProfile(val pageCount: Long, val pages: BitSet)

/**
 * Where a database keeps its page cache profiles, as Neo4j Enterprise does: in `profiles/` of the
 * database directory ([databaseDirectory]), one for each page-cached file, at the file's path in
 * the database directory with [SUFFIX] added, so that a store copy of the directory carries them.
 *
 * A profile is gzip of: [MAGIC], the format [VERSION] (a byte), the file's page count (a long), the
 * number of words of the bitmap of its pages (an int) and the words (longs, page n in bit n % 64 of
 * word n / 64, up to the last word with a page). It is written to a file of its own first and moved
 * over the last one, so that a profile is either the last one or the one before.
 */
internal class PageCacheProfiles(
    private val fs: FileSystemAbstraction,
    private val databaseDirectory: Path,
) {
    /** The directory of the profiles. */
    val directory: Path = databaseDirectory.resolve(DIRECTORY)

    /** Where the profile of [file], a file in the database directory, is. */
    fun profileOf(file: Path): Path =
        directory.resolve(databaseDirectory.relativize(file).toString() + SUFFIX)

    /** The file whose profile is at [profile]. */
    fun fileOf(profile: Path): Path =
        databaseDirectory.resolve(directory.relativize(profile).toString().removeSuffix(SUFFIX))

    /** Every profile there is, in the order of their paths. */
    fun list(): List<Path> {
        if (!fs.isDirectory(directory)) return emptyList()
        val profiles = mutableListOf<Path>()
        fun walk(path: Path) {
            for (entry in fs.listFiles(path)) {
                when {
                    fs.isDirectory(entry) -> walk(entry)
                    entry.fileName.toString().endsWith(SUFFIX) -> profiles.add(entry)
                }
            }
        }
        walk(directory)
        return profiles.sorted()
    }

    /** Writes [profile] at [path]. */
    fun write(path: Path, profile: PageCacheProfile) {
        fs.mkdirs(path.parent)
        val written = path.resolveSibling(path.fileName.toString() + WRITING)
        DataOutputStream(GZIPOutputStream(fs.openAsOutputStream(written, false))).use { out ->
            val words = profile.pages.toLongArray()
            out.writeInt(MAGIC)
            out.writeByte(VERSION)
            out.writeLong(profile.pageCount)
            out.writeInt(words.size)
            words.forEach(out::writeLong)
        }
        fs.renameFile(
            written,
            path,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
    }

    /** The profile at [path]; an [IOException] if it is none this version reads. */
    fun read(path: Path): PageCacheProfile =
        DataInputStream(GZIPInputStream(fs.openAsInputStream(path))).use { input ->
            if (input.readInt() != MAGIC) throw IOException("$path is no page cache profile")
            val version = input.readByte().toInt()
            if (version != VERSION) {
                throw IOException("$path is a page cache profile of version $version")
            }
            val pageCount = input.readLong()
            val words = input.readInt()
            // The bitmap leaves out the words after the last page held.
            if (
                pageCount < 0 ||
                    words < 0 ||
                    words > (pageCount + Long.SIZE_BITS - 1) / Long.SIZE_BITS
            ) {
                throw IOException("$path is a damaged page cache profile")
            }
            PageCacheProfile(pageCount, BitSet.valueOf(LongArray(words) { input.readLong() }))
        }

    /** Deletes the profiles of files that are gone, such as dropped indexes; returns them. */
    fun deleteStale(): List<Path> =
        list().filter { !fs.fileExists(fileOf(it)) }.onEach { fs.deleteFile(it) }

    companion object {
        /** The directory of the profiles in a database directory, as Neo4j Enterprise has it. */
        const val DIRECTORY = "profiles"

        const val SUFFIX = ".cacheprof"

        /** Added to a profile's name while it is written. */
        private const val WRITING = ".writing"

        /** "DZPC". */
        private const val MAGIC = 0x445A5043

        private const val VERSION = 1
    }
}
