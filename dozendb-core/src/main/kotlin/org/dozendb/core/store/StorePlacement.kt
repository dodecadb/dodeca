package org.dozendb.core.store

import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.UUID
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.storageengine.api.StorageEngineFactory

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
        stagingRoots(name, purpose).forEach(fs::deleteRecursively)
        val staged = stagingLayout(name, purpose)
        fs.mkdirs(staged.databaseDirectory())
        fs.mkdirs(staged.transactionLogsDirectory)
        return staged
    }

    /** The store staged for database [name] for [purpose], or null if none is. */
    fun staged(name: String, purpose: String): DatabaseLayout? =
        stagingLayout(name, purpose).takeIf {
            StorageEngineFactory.selectStorageEngine(fs, it).isPresent
        }

    private fun stagingLayout(name: String, purpose: String): DatabaseLayout {
        val roots = stagingRoots(name, purpose)
        return Neo4jLayout.of(
                Config.newBuilder()
                    .set(GraphDatabaseSettings.neo4j_home, layout.homeDirectory())
                    .set(GraphDatabaseInternalSettings.databases_root_path, roots[0])
                    .set(GraphDatabaseSettings.transaction_logs_root_path, roots[1])
                    .build()
            )
            .databaseLayout(name)
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

    /** Whether database [name] has a store in place. */
    fun holdsStore(name: String): Boolean =
        StorageEngineFactory.selectStorageEngine(fs, layout.databaseLayout(name)).isPresent

    /** Whether the files of database [name] were left half-moved. */
    fun incomplete(name: String): Boolean = fs.fileExists(marker(name))

    /**
     * Deletes the files of database [name], which must not be running, its marker, and its seed
     * mark.
     */
    fun delete(name: String) {
        val target = layout.databaseLayout(name)
        fs.deleteRecursively(target.databaseDirectory())
        fs.deleteRecursively(target.transactionLogsDirectory)
        fs.deleteFile(marker(name))
        unmarkSeed(name)
    }

    /**
     * Marks the store of database [name] in place the seed of the database with the id [id], which
     * `dbms.cluster.recreateDatabase` recreated from it: it is kept, not deleted, when its own
     * database goes. Written to a file of its own, then moved over the mark, so that a mark is
     * whole or absent.
     */
    fun markSeed(name: String, id: UUID) {
        val mark = seedMark(name)
        val written = mark.resolveSibling("${mark.fileName}.tmp")
        fs.mkdirs(mark.parent)
        fs.write(written).use { it.write(ByteBuffer.wrap("$id\n".toByteArray())) }
        fs.renameFile(
            written,
            mark,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
    }

    /** The id of the database whose seed the store of database [name] is, if it is marked one. */
    fun seedOf(name: String): UUID? {
        val mark = seedMark(name)
        if (!fs.fileExists(mark)) return null
        val text =
            fs.openAsInputStream(mark).use { String(it.readAllBytes(), Charsets.UTF_8) }.trim()
        return runCatching { UUID.fromString(text) }.getOrNull()
    }

    /** The names of the databases whose stores are marked seeds, with the ids they seed. */
    fun seeds(): Map<String, UUID> {
        val directory = layout.databasesDirectory()
        if (!fs.isDirectory(directory)) return emptyMap()
        return fs.listFiles(directory)
            .mapNotNull { it.fileName.toString().takeIf { name -> name.startsWith(SEED_PREFIX) } }
            .map { it.removePrefix(SEED_PREFIX) }
            .filterNot { it.endsWith(".tmp") }
            .mapNotNull { name -> seedOf(name)?.let { name to it } }
            .toMap()
    }

    /** Takes the seed mark off the store of database [name], if it has one. */
    fun unmarkSeed(name: String) {
        fs.deleteFile(seedMark(name))
    }

    private fun seedMark(name: String): Path =
        layout.databasesDirectory().resolve(SEED_PREFIX + name)

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

    companion object {
        /**
         * The purpose of the store that `neo4j-admin database restore` prepares on a clustered
         * server, which `CREATE DATABASE ... OPTIONS {existingDataSeedServer: ...}` creates the
         * database from.
         */
        const val RESTORED = "seed"

        private const val MARKER_PREFIX = ".dozendb-incomplete-"
        private const val SEED_PREFIX = ".dozendb-seedmark-"
    }
}
