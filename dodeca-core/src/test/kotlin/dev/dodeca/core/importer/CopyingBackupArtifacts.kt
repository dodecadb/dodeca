package dev.dodeca.core.importer

import dev.dodeca.core.store.BackupArtifacts
import java.nio.file.Path
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.logging.InternalLogProvider

/**
 * The [BackupArtifacts] of core's tests, which lack the backup module: it "writes a backup" by
 * copying the store as it is into a home of its own, [home] of the location.
 */
class CopyingBackupArtifacts : BackupArtifacts {
    override fun write(
        layout: DatabaseLayout,
        database: String,
        source: String,
        location: String,
        option: String,
        compress: Boolean,
        partSize: Long,
        config: Config,
        fs: FileSystemAbstraction,
        logProvider: InternalLogProvider,
    ): BackupArtifacts.Written {
        val target = layout(Path.of(location), database)
        fs.copyRecursively(layout.databaseDirectory(), target.databaseDirectory())
        fs.copyRecursively(layout.transactionLogsDirectory, target.transactionLogsDirectory)
        return BackupArtifacts.Written(target.databaseDirectory().toString(), 0)
    }

    override fun restore(
        location: String,
        option: String,
        database: String,
        target: DatabaseLayout,
        config: Config,
        fs: FileSystemAbstraction,
        logProvider: InternalLogProvider,
    ): String = throw UnsupportedOperationException("core's tests restore no backups")

    companion object {
        /** Where a store written to [location] is. */
        fun layout(location: Path, database: String): DatabaseLayout =
            Neo4jLayout.of(Config.defaults(GraphDatabaseSettings.neo4j_home, home(location)))
                .databaseLayout(database)

        fun home(location: Path): Path = location.resolve("home")
    }
}
