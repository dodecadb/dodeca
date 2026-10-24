package org.dozendb.cluster

/** The keys members use to report facts about themselves to each other. */
object MemberMetadata {
    const val NEO4J_VERSION = "neo4j.version"
    const val DOZENDB_VERSION = "dozendb.version"

    /** "true" if the member had databases before it started; used to pick the store source. */
    const val HAS_STORES = "dozendb.hasStores"

    /** "true" while the member's replicated databases run, so other members can copy them. */
    const val STORES_READY = "dozendb.storesReady"

    /** The member's advertised Bolt address, for routing tables. */
    const val BOLT_ADDRESS = "dozendb.boltAddress"

    /**
     * The member's advertised routing address, to which other members forward writes of the
     * databases it leads. Absent if it takes no forwarded writes, as without mutual TLS between
     * members, and from members of versions before 0.1.
     */
    const val ROUTING_ADDRESS = "dozendb.routingAddress"

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

    /** The [FormatLevels] the member's DozenDB version supports. */
    const val FORMAT_LEVELS = "dozendb.formatLevels"

    /** The cluster's format level, as the member last saw it. */
    const val FORMAT_LEVEL = "dozendb.formatLevel"
    const val INITIAL_MEMBERS_SETTING = "dbms.dozendb.cluster.initial_members"
}
