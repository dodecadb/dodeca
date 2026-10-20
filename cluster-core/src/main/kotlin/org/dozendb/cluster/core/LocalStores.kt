package org.dozendb.cluster.core

import org.dozendb.consensus.HostPort

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
     * Replaces [databases] with copies from the member at [source]. Called before any database
     * starts. Throws if a copy fails; databases not yet moved into place are then unchanged.
     */
    fun copyFrom(source: HostPort, databases: List<String>)
}
