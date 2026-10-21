package org.dozendb.core.store

import java.nio.file.Path
import java.nio.file.StandardCopyOption
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout

/**
 * Puts the files of a database's store in place: they are first written to a staging place next to
 * their final places, on the same file systems, then moved into place, with a marker file present
 * while they are moved. A database with a marker was left half-moved; its files are not a store to
 * start.
 *
 * Staging places and markers are named with a leading dot, which no database name has.
 */
class StorePlacement(private val fs: FileSystemAbstraction, private val layout: Neo4jLayout) {
    /**
     * A new, empty staging place for the store of database [name], for [purpose] (`copy`, `restore`
     * or `seed`): a database layout whose directories are next to the database's own. Whatever an
     * earlier staging place for the same purpose held is deleted.
     */
    fun stage(name: String, purpose: String): DatabaseLayout {
        val roots = stagingRoots(name, purpose)
        roots.forEach(fs::deleteRecursively)
        val staged =
            Neo4jLayout.of(
                    Config.newBuilder()
                        .set(GraphDatabaseSettings.neo4j_home, layout.homeDirectory())
                        .set(GraphDatabaseInternalSettings.databases_root_path, roots[0])
                        .set(GraphDatabaseSettings.transaction_logs_root_path, roots[1])
                        .build()
                )
                .databaseLayout(name)
        fs.mkdirs(staged.databaseDirectory())
        fs.mkdirs(staged.transactionLogsDirectory)
        return staged
    }

    /** Deletes the staging place of database [name] for [purpose], if any. */
    fun unstage(name: String, purpose: String) {
        stagingRoots(name, purpose).forEach(fs::deleteRecursively)
    }

    /**
     * Replaces the files of database [name], which must not be running, with those [staged] holds,
     * and deletes the staging place.
     */
    fun moveIntoPlace(staged: DatabaseLayout, name: String) {
        val marker = marker(name)
        fs.mkdirs(marker.parent)
        fs.write(marker).close()
        val target = layout.databaseLayout(name)
        replace(staged.transactionLogsDirectory, target.transactionLogsDirectory)
        replace(staged.databaseDirectory(), target.databaseDirectory())
        fs.deleteFile(marker)
        fs.deleteRecursively(staged.transactionLogsDirectory.parent)
        fs.deleteRecursively(staged.databaseDirectory().parent)
    }

    /** Whether the files of database [name] were left half-moved. */
    fun incomplete(name: String): Boolean = fs.fileExists(marker(name))

    /** Deletes the files of database [name], which must not be running, and its marker. */
    fun delete(name: String) {
        val target = layout.databaseLayout(name)
        fs.deleteRecursively(target.databaseDirectory())
        fs.deleteRecursively(target.transactionLogsDirectory)
        fs.deleteFile(marker(name))
    }

    /** The name of the marker of database [name], if [fileName] is one. */
    fun markedDatabase(fileName: String): String? =
        fileName.takeIf { it.startsWith(MARKER_PREFIX) }?.removePrefix(MARKER_PREFIX)

    private fun marker(name: String): Path =
        layout.databasesDirectory().resolve(MARKER_PREFIX + name)

    private fun stagingRoots(name: String, purpose: String): List<Path> {
        require(purpose.all { it.isLetter() }) { "invalid purpose $purpose" }
        return listOf(layout.databasesDirectory(), layout.transactionLogsRootDirectory()).map {
            it.resolve(".dozendb-$purpose-$name")
        }
    }

    private fun replace(staging: Path, target: Path) {
        fs.deleteRecursively(target)
        fs.mkdirs(target.parent)
        fs.renameFile(staging, target, StandardCopyOption.ATOMIC_MOVE)
    }

    private companion object {
        const val MARKER_PREFIX = ".dozendb-incomplete-"
    }
}
