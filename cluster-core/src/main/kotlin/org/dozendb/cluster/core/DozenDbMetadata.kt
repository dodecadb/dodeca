package org.dozendb.cluster.core

import java.util.Properties

/** Facts about this build, and the keys members use to report them to each other. */
object DozenDbMetadata {
    const val NEO4J_VERSION = "neo4j.version"
    const val DOZENDB_VERSION = "dozendb.version"

    /** "true" if the member had databases before it started; used to pick the store source. */
    const val HAS_STORES = "dozendb.hasStores"

    /** "true" while the member's replicated databases run, so other members can copy them. */
    const val STORES_READY = "dozendb.storesReady"

    /** The member's advertised Bolt address, for routing tables. */
    const val BOLT_ADDRESS = "dozendb.boltAddress"

    /** The member's [MemberState]: whether it waits to join, is a member, or was removed. */
    const val MEMBER_STATE = "dozendb.memberState"

    /**
     * The id of the cluster's default database, once it runs on the member. Members of one cluster
     * report the same, members of another one not.
     */
    const val CLUSTER_ID = "dozendb.clusterId"
    const val INITIAL_MEMBERS_SETTING = "dbms.dozendb.cluster.initial_members"

    private val properties: Properties by lazy {
        Properties().apply {
            DozenDbMetadata::class.java.getResourceAsStream("dozendb.properties")?.use(::load)
        }
    }

    /** The DozenDB version of this build. */
    val version: String
        get() = properties.getProperty("version", "unknown")

    /** The Neo4j version this build was compiled and tested against. */
    val builtForNeo4j: String
        get() = properties.getProperty("neo4j.version", "unknown")
}
