package org.dozendb.backup.seed

import java.io.IOException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StorageSystemProviderFactory
import org.neo4j.service.Services

/** Reads the seeds at URIs of some schemes, as Neo4j Enterprise's seed providers do. */
internal sealed class SeedProvider(val name: String) {
    /** Whether it reads URIs of [scheme], in lower case. */
    abstract fun reads(scheme: String): Boolean

    /**
     * Passes [block] the seed at [uri]: a file or a folder, local or in cloud storage through [fs],
     * or a copy in [directory], an existing directory, that it deletes afterwards.
     */
    abstract fun <T> fetch(
        uri: URI,
        fs: SchemeFileSystemAbstraction,
        directory: Path,
        block: (Path) -> T,
    ): T

    /** Seeds on this server's file system: `file:` URIs. */
    data object File : SeedProvider("FileSeedProvider") {
        override fun reads(scheme: String) = scheme == "file"

        override fun <T> fetch(
            uri: URI,
            fs: SchemeFileSystemAbstraction,
            directory: Path,
            block: (Path) -> T,
        ): T {
            val path = Path.of(uri)
            if (!Files.exists(path)) throw IOException("$path does not exist")
            return block(path)
        }
    }

    /** Seeds that Java's URL connections read: `http:`, `https:` and `ftp:` URIs. */
    data object UrlConnection : SeedProvider("URLConnectionSeedProvider") {
        override fun reads(scheme: String) = scheme in setOf("http", "https", "ftp")

        override fun <T> fetch(
            uri: URI,
            fs: SchemeFileSystemAbstraction,
            directory: Path,
            block: (Path) -> T,
        ): T {
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

    /**
     * Seeds in cloud storage: the schemes of Neo4j's storage interface, `s3:`, `gs:` and `azb:`,
     * read where they are, with each cloud's own credentials.
     */
    data object Cloud : SeedProvider("CloudSeedProvider") {
        override fun reads(scheme: String) = scheme in STORAGE_SCHEMES

        override fun <T> fetch(
            uri: URI,
            fs: SchemeFileSystemAbstraction,
            directory: Path,
            block: (Path) -> T,
        ): T = block(fs.resolve(uri.toString()))
    }

    /**
     * Neo4j Enterprise's deprecated provider of `s3:` seeds, read as [Cloud] reads them: with the
     * AWS SDK's credentials, not the `seedConfig` and `seedCredentials` it took.
     */
    data object S3 : SeedProvider("S3SeedProvider") {
        override fun reads(scheme: String) = scheme == "s3" && scheme in STORAGE_SCHEMES

        override fun <T> fetch(
            uri: URI,
            fs: SchemeFileSystemAbstraction,
            directory: Path,
            block: (Path) -> T,
        ): T = Cloud.fetch(uri, fs, directory, block)
    }

    companion object {
        // A getter: the providers extend this class, so a list made when it initialises would
        // hold nulls.
        val ALL: List<SeedProvider>
            get() = listOf(File, UrlConnection, Cloud, S3)

        /** The provider named [name]; null for one DozenDB does not have. */
        fun named(name: String): SeedProvider? = ALL.firstOrNull { it.name == name }

        /** The schemes that the storage providers on the classpath resolve. */
        private val STORAGE_SCHEMES: Set<String> by lazy {
            Services.loadAll(StorageSystemProviderFactory::class.java).map { it.scheme() }.toSet()
        }
    }
}
