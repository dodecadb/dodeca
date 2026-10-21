package org.dozendb.backup.seed

import java.io.IOException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/** Reads the seeds at URIs of some schemes, as Neo4j Enterprise's seed providers do. */
internal sealed class SeedProvider(val name: String, val schemes: Set<String>) {
    /**
     * Passes [block] the seed at [uri] as a file: the file itself, or a copy in [directory], an
     * existing directory, that it deletes afterwards.
     */
    abstract fun <T> fetch(uri: URI, directory: Path, block: (Path) -> T): T

    /** Seeds on this server's file system: `file:` URIs. */
    data object File : SeedProvider("FileSeedProvider", setOf("file")) {
        override fun <T> fetch(uri: URI, directory: Path, block: (Path) -> T): T {
            val path = Path.of(uri)
            if (!Files.isRegularFile(path)) throw IOException("$path is not a file")
            return block(path)
        }
    }

    /** Seeds that Java's URL connections read: `http:`, `https:` and `ftp:` URIs. */
    data object UrlConnection :
        SeedProvider("URLConnectionSeedProvider", setOf("http", "https", "ftp")) {
        override fun <T> fetch(uri: URI, directory: Path, block: (Path) -> T): T {
            val download = Files.createTempFile(directory, "seed-", ".download")
            try {
                uri.toURL().openStream().use {
                    Files.copy(it, download, StandardCopyOption.REPLACE_EXISTING)
                }
                return block(download)
            } finally {
                Files.deleteIfExists(download)
            }
        }
    }

    companion object {
        // A getter: the providers extend this class, so a list made when it initialises would
        // hold nulls.
        val ALL: List<SeedProvider>
            get() = listOf(File, UrlConnection)

        /** The provider named [name]; null for one DozenDB does not have. */
        fun named(name: String): SeedProvider? = ALL.firstOrNull { it.name == name }
    }
}
