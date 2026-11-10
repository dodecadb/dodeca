package org.dozendb.backup.seed

import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.nio.file.Path
import org.dozendb.backup.ArtifactChain
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.StorePreparer
import org.dozendb.core.edition.DatabaseSeeder
import org.dozendb.core.store.StorePlacement
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StorageUtils
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider

/**
 * Creates the stores of databases from seeds at URIs, read by the first provider of
 * `dbms.databases.seed_from_uri_providers` for their scheme: a dump, a full backup, a differential
 * backup with the backups it continues in its folder, or a folder of backups, whose latest chain it
 * restores. A store is prepared next to the database's directories, with the DBMS's file system and
 * page cache, then moved into place.
 */
internal class StoreSeeder(
    private val fs: FileSystemAbstraction,
    private val pageCache: PageCache,
    private val config: Config,
    private val logProvider: InternalLogProvider,
) : DatabaseSeeder {
    private val placement = StorePlacement(fs, Neo4jLayout.of(config))

    override fun check(uri: String) {
        provider(parse(uri))
    }

    override fun seed(database: NamedDatabaseId, uri: String) {
        val parsed = parse(uri)
        val provider = provider(parsed)
        val name = database.name()
        val staged = placement.stage(name, PURPOSE)
        try {
            SchemeFileSystemAbstraction(fs, config, logProvider).use { schemes ->
                provider.fetch(parsed, schemes, staged.databaseDirectory().parent) { seed ->
                    val preparer = StorePreparer(schemes, pageCache, config, logProvider)
                    when (val contents = contents(seed, schemes, name)) {
                        is Contents.Archive ->
                            preparer.prepare(contents.path, emptyList(), null, staged, true)
                        is Contents.Chain -> preparer.prepare(contents.chain, null, staged, true)
                    }
                }
            }
            placement.moveIntoPlace(staged, name)
        } finally {
            placement.unstage(name, PURPOSE)
        }
    }

    /** What a seed holds. */
    private sealed interface Contents {
        /** A dump or a full backup, or what the preparer will tell is neither. */
        data class Archive(val path: Path) : Contents

        data class Chain(val chain: ArtifactChain) : Contents
    }

    /** What [seed], the seed of database [name], holds. */
    private fun contents(seed: Path, fs: FileSystemAbstraction, name: String): Contents {
        val shown = StorageUtils.toString(seed)
        if (!fs.fileExists(seed)) throw IOException("$shown does not exist")
        if (fs.isDirectory(seed)) {
            val byName =
                ArtifactChains.find(listOf(seed), fs).artifacts.groupBy {
                    it.header.databaseName
                }
            val artifacts =
                byName.values.singleOrNull()
                    ?: byName[name]
                    ?: throw IOException(
                        if (byName.isEmpty()) "$shown holds no backups"
                        else
                            "$shown holds backups of ${byName.keys.sorted().joinToString()}, " +
                                "none of $name: seed from one of its backups"
                    )
            return Contents.Chain(ArtifactChains.select(artifacts, null))
        }
        val artifact =
            try {
                ArtifactChains.find(listOf(seed), fs).artifacts.single()
            } catch (_: IOException) {
                return Contents.Archive(seed)
            }
        if (artifact.header.full) return Contents.Archive(seed)
        val continued =
            ArtifactChains.find(listOfNotNull(seed.toAbsolutePath().parent), fs).artifacts.filter {
                it.header.databaseName == artifact.header.databaseName
            }
        return Contents.Chain(ArtifactChains.select(continued + artifact, null, artifact))
    }

    private fun parse(uri: String): URI =
        try {
            URI(uri).also { require(it.scheme != null) { "seedURI '$uri' has no scheme" } }
        } catch (e: URISyntaxException) {
            throw IllegalArgumentException("seedURI '$uri' is not a URI: ${e.reason}", e)
        }

    private fun provider(uri: URI): SeedProvider {
        val setting = BackupSettings.seedProviders.name()
        val scheme = uri.scheme.lowercase()
        val enabled = config.get(BackupSettings.seedProviders).mapNotNull(SeedProvider::named)
        return enabled.firstOrNull { it.reads(scheme) }
            ?: throw IllegalArgumentException(
                "no seed provider for '${uri.scheme}' URIs is enabled: $setting is " +
                    "${enabled.joinToString { it.name }.ifEmpty { "empty" }}" +
                    (SeedProvider.ALL.firstOrNull { it.reads(scheme) }
                        ?.let { "; ${it.name} reads them" } ?: "")
            )
    }

    private companion object {
        const val PURPOSE = "seeding"
    }
}
