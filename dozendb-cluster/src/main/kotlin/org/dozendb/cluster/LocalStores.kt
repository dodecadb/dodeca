package org.dozendb.cluster

import org.dozendb.consensus.HostPort

/**
 * A store as its files have it.
 *
 * @property storeId the id Neo4j gave the store when it created it, as `<creation time>/<random>`:
 *   copies of a store keep it, so the copies of one cluster's `system` share it.
 * @property lastTransaction the id of the last transaction committed to it.
 */
data class StoreState(val storeId: String, val lastTransaction: Long)

/**
 * This member's databases as far as cluster formation is concerned: whether they existed before
 * startup, which are missing, and replacing them with copies from another member.
 */
interface LocalStores {
    /** Whether this member had databases before it started, none of them left half-copied. */
    val existed: Boolean

    /** The databases this member lacks or that an interrupted copy left behind. */
    fun missing(): List<String>

    /**
     * The databases this member has, complete: those it needs to form a cluster, and any other in
     * its databases directory.
     */
    fun present(): List<String>

    /**
     * The store of [database] here as its files have it, or null without a complete one. Only while
     * the database does not run.
     */
    fun storeState(database: String): StoreState?

    /**
     * Replaces [databases] with copies from the member at [source]. Called before any database
     * starts. Throws if a copy fails; databases not yet moved into place are then unchanged.
     */
    fun copyFrom(source: HostPort, databases: List<String>)
}
