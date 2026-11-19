package dev.dodeca.cluster.internal.replication

/** How Dodeca's own tasks read the system database here (see [ReplicationService]). */
internal interface SystemReading {
    /**
     * Runs [block], which reads the system database, while `system` is not being replaced by a
     * copy. [block] must not wait for anything that needs `system` replaced first.
     */
    fun <T> readingSystem(block: () -> T): T

    /** Whether this member is the system leader and can commit system transactions. */
    val systemWritable: Boolean
}
