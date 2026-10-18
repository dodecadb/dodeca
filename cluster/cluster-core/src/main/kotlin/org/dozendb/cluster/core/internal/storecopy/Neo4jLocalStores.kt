package org.dozendb.cluster.core.internal.storecopy

import java.nio.file.Path
import java.nio.file.StandardCopyOption
import org.dozendb.cluster.api.BulkTransfer
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.core.LocalStores
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.logging.InternalLog
import org.neo4j.storageengine.api.StorageEngineFactory

/**
 * The `system` and replicated databases of this member, which it copies from another member when it
 * has none.
 *
 * All databases are first copied to staging directories next to their final places and synced. Only
 * then are they moved into place, with a marker file present while moving. A start that finds the
 * marker knows a previous copy was interrupted and treats the member as having no databases, so it
 * copies them again instead of starting from a mix.
 */
internal class Neo4jLocalStores(
    private val fs: FileSystemAbstraction,
    private val layout: Neo4jLayout,
    private val databaseNames: List<String>,
    private val transfer: BulkTransfer,
    private val log: InternalLog,
) : LocalStores {
    private val incompleteMarker = layout.databasesDirectory().resolve(INCOMPLETE_MARKER)

    override val existed: Boolean =
        !fs.fileExists(incompleteMarker) && databaseNames.any(::storageExists)

    override fun copyFrom(source: HostPort) {
        val staged = databaseNames.map { fetch(source, it) }
        fs.mkdirs(incompleteMarker.parent)
        fs.write(incompleteMarker).close()
        for (copy in staged) {
            val target = layout.databaseLayout(copy.name)
            replace(copy.transactionLogs, target.transactionLogsDirectory)
            replace(copy.database, target.databaseDirectory())
        }
        fs.deleteFile(incompleteMarker)
        staged.forEach {
            log.info(
                "Copied database %s from %s: %d files, %d bytes, %s",
                it.name,
                source,
                it.summary.files,
                it.summary.bytes,
                it.summary.description,
            )
        }
    }

    private fun storageExists(name: String): Boolean =
        StorageEngineFactory.allAvailableStorageEngines().any { it.storageExists(fs, layout, name) }

    private class Staged(
        val name: String,
        val database: Path,
        val transactionLogs: Path,
        val summary: StoreArchive.Summary,
    )

    private fun fetch(source: HostPort, name: String): Staged {
        val database = layout.databasesDirectory().resolve(STAGING_PREFIX + name)
        val transactionLogs = layout.transactionLogsRootDirectory().resolve(STAGING_PREFIX + name)
        for (staging in listOf(database, transactionLogs)) {
            fs.deleteRecursively(staging)
            fs.mkdirs(staging)
        }
        val summary =
            transfer.fetchBulk(source, StoreCopySource.NAME, name.encodeToByteArray()) { input ->
                StoreArchive.read(input, fs, database, transactionLogs)
            }
        check(summary.database == name) {
            "asked for database $name but received ${summary.database}"
        }
        return Staged(name, database, transactionLogs, summary)
    }

    private fun replace(staging: Path, target: Path) {
        fs.deleteRecursively(target)
        fs.mkdirs(target.parent)
        fs.renameFile(staging, target, StandardCopyOption.ATOMIC_MOVE)
    }

    private companion object {
        const val STAGING_PREFIX = ".dozendb-copy-"
        const val INCOMPLETE_MARKER = ".dozendb-copy-incomplete"
    }
}
