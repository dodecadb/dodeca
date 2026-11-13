package dev.dodeca.backup

import java.time.Instant
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.storageengine.api.StoreId

/**
 * What a backup artifact holds, as its header tells: the fields of Neo4j's backup description,
 * which Community's `neo4j-admin database load` reads, and the checksums that chain artifacts.
 *
 * A full backup holds a store with every transaction from 1 to [highestAppendIndex]. A differential
 * backup holds the transactions from [lowestAppendIndex] to [highestAppendIndex] that follow the
 * transaction whose checksum is [previousChecksum]; an empty one holds none and has both indexes 0,
 * as Enterprise shows them.
 *
 * @property time when the backup was taken, in whole seconds.
 * @property lastChecksum the checksum of the transaction at [highestAppendIndex], null if empty.
 * @property lastCommitTime when that transaction committed, in milliseconds, null if empty.
 * @property source the server that served the backup.
 * @property metadataScript the Cypher that gives a DBMS back the database's users, roles and
 *   privileges, as `--include-metadata` asked for them; null without.
 * @property recovered whether a full backup's store is recovered; one taken with `--skip-recovery`
 *   is not, and restoring it recovers it. Never for a differential backup, which holds no store.
 */
data class ArtifactHeader(
    val databaseName: String,
    val databaseId: DatabaseId,
    val storeId: StoreId,
    val time: Instant,
    val full: Boolean,
    val compressed: Boolean,
    val lowestAppendIndex: Long,
    val highestAppendIndex: Long,
    val previousChecksum: Int?,
    val lastChecksum: Int?,
    val lastCommitTime: Instant?,
    val source: String,
    val metadataScript: String? = null,
    val recovered: Boolean = full,
) {
    init {
        require(time.nano == 0) { "time $time is not in whole seconds" }
        require(full || !recovered) { "a differential backup holds no store to recover" }
        if (empty) {
            require(!full) { "a full backup holds at least one transaction" }
            require(previousChecksum == null && lastChecksum == null && lastCommitTime == null) {
                "an empty backup has no checksums and no last commit time"
            }
        } else {
            require(lowestAppendIndex in 1..highestAppendIndex) {
                "invalid transactions $lowestAppendIndex to $highestAppendIndex"
            }
            require(!full || lowestAppendIndex == 1L) { "a full backup starts at transaction 1" }
            require(full == (previousChecksum == null)) {
                "a differential backup, and only one, follows a previous checksum"
            }
            requireNotNull(lastChecksum) { "the last checksum is missing" }
            requireNotNull(lastCommitTime) { "the last commit time is missing" }
        }
    }

    /** Whether it holds no transactions. */
    val empty: Boolean
        get() = lowestAppendIndex == 0L && highestAppendIndex == 0L
}
