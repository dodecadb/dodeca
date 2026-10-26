package org.dozendb.cluster

import java.util.UUID

/** The keys members use to report facts about themselves to each other. */
object MemberMetadata {
    const val NEO4J_VERSION = "neo4j.version"
    const val DOZENDB_VERSION = "dozendb.version"

    /** "true" if the member had databases before it started; used to pick the store source. */
    const val HAS_STORES = "dozendb.hasStores"

    /** "true" while the member's system database runs, so other members can copy it. */
    const val STORES_READY = "dozendb.storesReady"

    /** The member's advertised Bolt address, for routing tables. */
    const val BOLT_ADDRESS = "dozendb.boltAddress"

    /**
     * The member's advertised routing address, to which other members forward writes of the
     * databases it leads. Absent if it takes no forwarded writes, as without mutual TLS between
     * members, and from members of versions before 0.1.
     */
    const val ROUTING_ADDRESS = "dozendb.routingAddress"

    /** The member's advertised HTTP address, for SHOW SERVERS; absent while HTTP is off. */
    const val HTTP_ADDRESS = "dozendb.httpAddress"

    /** The member's advertised HTTPS address, for SHOW SERVERS; absent while HTTPS is off. */
    const val HTTPS_ADDRESS = "dozendb.httpsAddress"

    /**
     * The member's `initial.server.tags`, separated by commas, which tags cannot contain: the tags
     * its server record takes when it is enabled.
     */
    const val INITIAL_SERVER_TAGS = "dozendb.initialServerTags"

    /** The member's [MemberState]: whether it waits to join, is a member, or was removed. */
    const val MEMBER_STATE = "dozendb.memberState"

    /**
     * The id of the cluster's default database, once it runs on the member. Members of one cluster
     * report the same, members of another one not.
     */
    const val CLUSTER_ID = "dozendb.clusterId"

    /** The databases on the member and their states, for WAIT; see [DatabaseStatesCodec]. */
    const val DATABASES = "dozendb.databases"

    /**
     * The ids of the databases that are read-only on the member by its configuration
     * (`server.databases.read_only` and the like), separated by commas.
     */
    const val READ_ONLY_DATABASES = "dozendb.readOnlyDatabases"

    /**
     * The id of the last transaction committed to each database that runs on the member, as
     * `<database id>=<transaction id>` separated by commas.
     */
    const val LAST_COMMITTED_TRANSACTIONS = "dozendb.lastCommittedTransactions"

    /**
     * The groups the member leads, as `<group id>=<term>` separated by commas, so that members that
     * do not run a group know its leader; the higher term wins while two report one.
     */
    const val LEADING = "dozendb.leading"

    /** The [FormatLevels] the member's DozenDB version supports. */
    const val FORMAT_LEVELS = "dozendb.formatLevels"

    /** The cluster's format level, as the member last saw it. */
    const val FORMAT_LEVEL = "dozendb.formatLevel"
    const val INITIAL_MEMBERS_SETTING = "dbms.dozendb.cluster.initial_members"

    /** The items of a list reported separated by commas. */
    fun splitList(value: String): List<String> = value.split(',').filter { it.isNotEmpty() }

    /** Entries reported as `<id>=<number>` separated by commas; malformed ones are left out. */
    fun numbersById(value: String): Map<UUID, Long> =
        value
            .split(',')
            .mapNotNull { entry ->
                val (id, number) =
                    entry.split('=').takeIf { it.size == 2 } ?: return@mapNotNull null
                runCatching { UUID.fromString(id.trim()) to number.trim().toLong() }.getOrNull()
            }
            .toMap()

    /** [numbers] as [numbersById] reads them. */
    fun numbersText(numbers: Map<UUID, Long>): String =
        numbers.entries.joinToString(",") { "${it.key}=${it.value}" }
}
