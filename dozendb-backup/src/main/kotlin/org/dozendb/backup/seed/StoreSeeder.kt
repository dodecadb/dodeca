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
import org.dozendb.core.edition.UriSeed
import org.dozendb.core.store.StorePlacement
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StorageUtils
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
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

    override fun check(seed: UriSeed) {
        val uri = parse(seed.uri)
        val provider = provider(uri)
        if (seed.restoreUntil != null && provider !in RESTORING_UNTIL) {
            throw IllegalArgumentException(
                "seedRestoreUntil is supported by " +
                    "${RESTORING_UNTIL.joinToString(" and ") { it.name }} only, as in Neo4j " +
                    "Enterprise; ${provider.name} reads '${uri.scheme}' URIs"
            )
        }
    }

    override fun seed(database: NamedDatabaseId, seed: UriSeed) {
        check(seed)
        val parsed = parse(seed.uri)
        val provider = provider(parsed)
        val until = seed.restoreUntil
        val name = database.name()
        val staged = placement.stage(name, PURPOSE)
        try {
            SchemeFileSystemAbstraction(fs, config, logProvider).use { schemes ->
                provider.fetch(parsed, schemes, staged.databaseDirectory().parent) { path ->
                    val preparer = StorePreparer(schemes, pageCache, config, logProvider)
                    when (val contents = contents(path, schemes, name, seed)) {
                        is Contents.Archive ->
                            preparer.prepare(contents.path, emptyList(), until, staged, true)
                        is Contents.Chain -> preparer.prepare(contents.chain, until, staged, true)
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

    /**
     * What [path], the seed of database [name] that [seed] describes, holds: of the backups it
     * finds, those of `seedSourceDatabase` if given, and of their chains, the one that restores up
     * to `seedRestoreUntil` if given.
     */
    private fun contents(
        path: Path,
        fs: FileSystemAbstraction,
        name: String,
        seed: UriSeed,
    ): Contents {
        val until = seed.restoreUntil
        val source = seed.sourceDatabase
        val shown = StorageUtils.toString(path)
        if (!fs.fileExists(path)) throw IOException("$shown does not exist")
        if (fs.isDirectory(path)) {
            val byName =
                ArtifactChains.find(listOf(path), fs).artifacts.groupBy {
                    NormalizedDatabaseName(it.header.databaseName).name()
                }
            val wanted = source ?: name
            val artifacts =
                (if (source == null) byName.values.singleOrNull() else null)
                    ?: byName[wanted]
                    ?: throw IOException(
                        if (byName.isEmpty()) "$shown holds no backups"
                        else
                            "$shown holds backups of ${byName.keys.sorted().joinToString()}, " +
                                "none of $wanted: seed from one of its backups"
                    )
            return Contents.Chain(ArtifactChains.select(artifacts, until))
        }
        val artifact =
            try {
                ArtifactChains.find(listOf(path), fs).artifacts.single()
            } catch (_: IOException) {
                // A dump, which names no database.
                return Contents.Archive(path)
            }
        if (
            source != null && NormalizedDatabaseName(artifact.header.databaseName).name() != source
        ) {
            throw IOException(
                "$shown is a backup of database '${artifact.header.databaseName}', not of " +
                    "'$source' as seedSourceDatabase names"
            )
        }
        if (artifact.header.full) return Contents.Archive(path)
        val continued =
            ArtifactChains.find(listOfNotNull(path.toAbsolutePath().parent), fs).artifacts.filter {
                it.header.databaseName == artifact.header.databaseName
            }
        return Contents.Chain(ArtifactChains.select(continued + artifact, until, artifact))
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

        /** The providers whose seeds are restored up to a point, as in Neo4j Enterprise. */
        val RESTORING_UNTIL = listOf(SeedProvider.File, SeedProvider.Cloud)
    }
}
