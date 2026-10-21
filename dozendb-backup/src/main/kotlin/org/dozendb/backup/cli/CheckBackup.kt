package org.dozendb.backup.cli

import org.dozendb.backup.ArtifactChains
import org.neo4j.configuration.Config
import org.neo4j.dbms.archive.CheckDatabase
import org.neo4j.dbms.archive.CheckDatabase.Source
import org.neo4j.dbms.archive.CheckDatabase.Source.PathSource
import org.neo4j.io.IOUtils.AutoCloseables
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLogProvider

/**
 * Lets `neo4j-admin database check --from-path` check DozenDB's backups: the store the latest chain
 * of the database restores, prepared in a temporary directory. Registered in `META-INF/services`.
 */
class CheckBackup : CheckDatabase {
    override fun name() = "backup"

    override fun containsPotentiallyCheckableDatabase(
        fs: FileSystemAbstraction,
        source: Source,
        database: NormalizedDatabaseName,
    ): Boolean =
        source is PathSource &&
            fs.fileExists(source.path) &&
            ArtifactChains.find(listOf(source.path), fs).artifacts.any {
                it.header.databaseName == database.name() && !it.header.empty
            }

    override fun targetLayoutFrom(
        fs: FileSystemAbstraction,
        source: Source,
        database: NormalizedDatabaseName,
        autoCloseables: AutoCloseables<*>,
    ): DatabaseLayout {
        val temporary =
            autoCloseables.add(
                Source.expected(PathSource::class.java, source).createTemporaryDirectory(fs)
            )
        return Neo4jLayout.ofFlat(temporary.path).databaseLayout(database.name())
    }

    override fun tryExtract(
        fs: FileSystemAbstraction,
        config: Config,
        targetLayout: DatabaseLayout,
        source: Source,
        database: NormalizedDatabaseName,
        logProvider: InternalLogProvider,
        verbose: Boolean,
        force: Boolean,
    ) {
        val path = Source.expected(PathSource::class.java, source).path
        val artifacts =
            ArtifactChains.find(listOf(path), fs).artifacts.filter {
                it.header.databaseName == database.name()
            }
        val chain = ArtifactChains.select(artifacts, null)
        logProvider
            .getLog(javaClass)
            .info("Restoring %s into %s to check it", chain, targetLayout.databaseDirectory())
        AdminResources(fs, config, logProvider).use {
            it.preparer().prepare(chain, null, targetLayout, detach = false)
        }
    }
}
