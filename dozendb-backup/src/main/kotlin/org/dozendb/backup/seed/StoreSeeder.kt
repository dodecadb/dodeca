package org.dozendb.backup.seed

import java.net.URI
import java.net.URISyntaxException
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.StorePreparer
import org.dozendb.core.edition.DatabaseSeeder
import org.dozendb.core.store.StorePlacement
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider

/**
 * Creates the stores of databases from seeds, a dump or a full backup at a URI, read by the first
 * provider of `dbms.databases.seed_from_uri_providers` for its scheme. A store is prepared next to
 * the database's directories, with the DBMS's file system and page cache, then moved into place.
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
            provider.fetch(parsed, staged.databaseDirectory().parent) { file ->
                StorePreparer(fs, pageCache, config, logProvider)
                    .prepare(file, emptyList(), null, staged, detach = true)
            }
            placement.moveIntoPlace(staged, name)
        } finally {
            placement.unstage(name, PURPOSE)
        }
    }

    private fun parse(uri: String): URI =
        try {
            URI(uri).also { require(it.scheme != null) { "seedURI '$uri' has no scheme" } }
        } catch (e: URISyntaxException) {
            throw IllegalArgumentException("seedURI '$uri' is not a URI: ${e.reason}", e)
        }

    private fun provider(uri: URI): SeedProvider {
        val setting = BackupSettings.seedProviders.name()
        val enabled = config.get(BackupSettings.seedProviders).mapNotNull(SeedProvider::named)
        return enabled.firstOrNull { uri.scheme.lowercase() in it.schemes }
            ?: throw IllegalArgumentException(
                "no seed provider for '${uri.scheme}' URIs is enabled: $setting is " +
                    "${enabled.joinToString { it.name }.ifEmpty { "empty" }}" +
                    (SeedProvider.ALL.firstOrNull { uri.scheme.lowercase() in it.schemes }
                        ?.let { "; ${it.name} reads them" } ?: "")
            )
    }

    private companion object {
        const val PURPOSE = "seeding"
    }
}
