package org.dozendb.core.edition

/**
 * The servers that serve backups of a database, which `neo4j-admin database backup
 * --remote-address-resolution` backs up from, as Neo4j Enterprise chooses them: the secondaries
 * first, then the primaries that do not lead it, then the one that does. The edition provides it as
 * a global dependency.
 */
fun interface BackupSources {
    /**
     * The advertised backup addresses (`server.backup.advertised_address`) of the servers that host
     * [database], as `host:port`, in the order to try them; none if no server is known to.
     */
    fun of(database: String): List<String>
}
