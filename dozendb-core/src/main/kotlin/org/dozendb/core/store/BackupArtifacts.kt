package org.dozendb.core.store

import java.util.ServiceLoader
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.logging.InternalLogProvider

/**
 * Backup artifacts for the `neo4j-admin` commands outside DozenDB's backup module, which provides
 * this as a service: `database copy` and `database import` write their store as a full backup with
 * `--target-format=backup`, and `copy` restores its source from backups with
 * `--source-format=backup`.
 *
 * Locations are as the backup commands take them: local paths, or folders and files in cloud
 * storage, such as `s3://<bucket>/<folder>/`. Failures throw [java.io.IOException] or
 * [IllegalArgumentException] telling why.
 */
interface BackupArtifacts {
    /** What a full backup [write] wrote, as `neo4j-admin` shows it. */
    class Written(val file: String, val lastTransaction: Long)

    /**
     * Writes the store in [layout], stopped and recovered, as a new full backup of database
     * [database] into the folder [location], which [option] names in errors; compressed with
     * [compress], and split into parts of [partSize] bytes unless it is 0. [source] tells where the
     * store came from, as a backup tells the server it was taken from.
     */
    fun write(
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
    ): Written

    /**
     * Puts in [target], which holds no store, the store of the backup file at [location], which
     * [option] names in errors, with the backups before it that it continues, which must be in its
     * folder. The backup must be of database [database]. Returns how it was restored, as
     * `neo4j-admin database restore` tells it.
     */
    fun restore(
        location: String,
        option: String,
        database: String,
        target: DatabaseLayout,
        config: Config,
        fs: FileSystemAbstraction,
        logProvider: InternalLogProvider,
    ): String

    companion object {
        /** The backup module's; null without it. */
        fun load(): BackupArtifacts? =
            ServiceLoader.load(BackupArtifacts::class.java, BackupArtifacts::class.java.classLoader)
                .firstOrNull()
    }
}
