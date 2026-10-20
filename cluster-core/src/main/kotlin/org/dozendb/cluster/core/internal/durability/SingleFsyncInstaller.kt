package org.dozendb.cluster.core.internal.durability

import org.dozendb.cluster.core.internal.DozenDbSettings
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout

/**
 * Gives a clustered DBMS a [SingleFsyncFileSystem], unless `server.dozendb.cluster.single_fsync` is
 * off. It wraps the file system that [dependencies] already supply, or Neo4j's default one, and
 * keeps every other dependency.
 *
 * It also makes recovery truncate a damaged transaction log tail instead of failing, unless
 * [config] sets `internal.dbms.tx_log.fail_on_corrupted_log_files` explicitly: a power loss can
 * leave holes in the unforced tail, and the Raft log holds the transactions recovery drops.
 * Recovery reads the setting from the DBMS configuration only, so it applies to every database; the
 * others force their log at every commit, and a crash can only damage their last, unacknowledged
 * transactions.
 */
internal fun withSingleFsync(
    config: Config,
    dependencies: GraphDatabaseDependencies,
): GraphDatabaseDependencies {
    if (!config.get(DozenDbSettings.singleFsync)) return dependencies
    config.setIfNotSet(GraphDatabaseInternalSettings.fail_on_corrupted_log_files, false)
    val given = dependencies.dependencies()
    val fileSystem =
        given
            ?.takeIf { it.containsDependency(FileSystemAbstraction::class.java) }
            ?.resolveDependency(FileSystemAbstraction::class.java) ?: DefaultFileSystemAbstraction()
    val layout = Neo4jLayout.of(config)
    val wrapped =
        SingleFsyncFileSystem(
            fileSystem,
            layout.databasesDirectory(),
            layout.transactionLogsRootDirectory(),
        )
    val combined = if (given != null) Dependencies(given) else Dependencies()
    combined.satisfyDependency(wrapped)
    return dependencies.dependencies(combined)
}
