package org.dozendb.cluster.core

import org.dozendb.cluster.api.HostPort

/**
 * This member's databases as far as cluster formation is concerned: whether they existed before
 * startup, and replacing them with copies from another member.
 */
interface LocalStores {
    /** Whether this member had databases before it started. */
    val existed: Boolean

    /**
     * Replaces this member's databases with copies from the member at [source]. Called before any
     * database starts. Throws if the copy fails; the local databases are then unchanged.
     */
    fun copyFrom(source: HostPort)
}
