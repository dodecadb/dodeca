package org.dozendb.backup.cli

import java.io.IOException
import java.nio.file.Path
import org.neo4j.cli.CommandFailedException
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StoragePath
import org.neo4j.cloud.storage.StorageUtils

/**
 * The paths of backup commands: local paths, or URIs of cloud storage, such as
 * `s3://<bucket>/<folder>/`, which resolve through Neo4j's storage interface.
 */
internal object CommandPaths {
    /** [value] of [option] as a path, local or in cloud storage. */
    fun resolve(fs: SchemeFileSystemAbstraction, value: String, option: String): Path =
        try {
            fs.resolve(value.trim())
        } catch (e: IOException) {
            throw CommandFailedException("Invalid $option $value: ${e.message}", e)
        } catch (e: RuntimeException) {
            throw CommandFailedException("Invalid $option $value: ${e.message}", e)
        }

    fun isCloud(path: Path) = path is StoragePath

    /** [path] for messages: a cloud URI, or the local path. */
    fun display(path: Path): String = StorageUtils.toString(path)
}
