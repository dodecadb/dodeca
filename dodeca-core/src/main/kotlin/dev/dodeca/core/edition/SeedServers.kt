package dev.dodeca.core.edition

import java.util.UUID

/**
 * The servers a database can be created from with `CREATE DATABASE ... OPTIONS
 * {existingDataSeedServer: ...}`: with clustering, the cluster's members. The cluster's edition
 * provides it as a global dependency.
 */
fun interface SeedServers {
    /**
     * Checks that the server [id] can seed a database.
     *
     * @throws IllegalArgumentException telling why not.
     */
    fun check(id: UUID)
}
