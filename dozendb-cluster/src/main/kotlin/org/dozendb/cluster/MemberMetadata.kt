package org.dozendb.cluster

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

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

    /** The member's `initial.server.mode_constraint`, which its server record takes likewise. */
    const val INITIAL_SERVER_MODE_CONSTRAINT = "dozendb.initialServerModeConstraint"

    /**
     * The member's `initial.server.allowed_databases`, separated by commas, which database names
     * cannot contain; its server record takes them likewise.
     */
    const val INITIAL_SERVER_ALLOWED_DATABASES = "dozendb.initialServerAllowedDatabases"

    /** The member's `initial.server.denied_databases`, as [INITIAL_SERVER_ALLOWED_DATABASES]. */
    const val INITIAL_SERVER_DENIED_DATABASES = "dozendb.initialServerDeniedDatabases"

    /** The member's [MemberState]: whether it waits to join, is a member, or was removed. */
    const val MEMBER_STATE = "dozendb.memberState"

    /**
     * The id of the database `initial.dbms.default_database` names, once it runs on the member.
     * Members of one cluster report the same, members of another one not, as long as that database
     * exists. Older versions tell clusters apart by it alone; see [SYSTEM_ID].
     */
    const val CLUSTER_ID = "dozendb.clusterId"

    /**
     * The random id Neo4j gave the `system` database's node when it created the cluster's system
     * graph, once `system` runs on the member: members of one cluster report the same, members of
     * another one not. Compared instead of [CLUSTER_ID] when both members report it, as it also
     * holds once another database became the default and that one was dropped.
     */
    const val SYSTEM_ID = "dozendb.systemId"

    /**
     * The id of the Raft group of `system` on the member (see [DatabaseGroups]): the database's id,
     * unless a recovery re-formed the group under an id of its own.
     */
    const val SYSTEM_GROUP = "dozendb.systemGroup"

    /**
     * The id of the member's store of `system` ([StoreState.storeId]), read before `system` starts
     * and after a copy replaced it: the copies of one cluster's `system` share it, so that it tells
     * a cluster whose `system` group was re-formed from another cluster.
     */
    const val SYSTEM_STORE = "dozendb.systemStore"

    /**
     * "true" while the member waits to re-form `system`, or to join a group that re-formed it,
     * after `neo4j-admin dbms unbind-system-db` removed its Raft state of it. Its [SYSTEM_GROUP] is
     * then the group it was unbound from.
     */
    const val SYSTEM_UNBOUND = "dozendb.systemUnbound"

    /**
     * While [SYSTEM_UNBOUND], the id of the last transaction of the member's store of `system`: the
     * member with the latest store re-forms `system` from it.
     */
    const val SYSTEM_LAST_TRANSACTION = "dozendb.systemLastTransaction"

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

    /**
     * Which report of the member this is, as `<run>/<number>`: the member numbers its reports in
     * order under a random id of its process's run ([ReportNumbers]), so that the others keep its
     * latest one when an earlier one arrives late. Reports of different runs are not ordered.
     */
    const val REPORT_NUMBER = "dozendb.reportNumber"

    /**
     * The member's `server.cluster.system_database_mode`, PRIMARY or SECONDARY: the mode it is
     * admitted in. Members of versions before system secondaries report none, and are primaries.
     */
    const val SYSTEM_DATABASE_MODE = "dozendb.systemDatabaseMode"
    const val INITIAL_MEMBERS_SETTING = "dbms.dozendb.cluster.initial_members"

    /**
     * Whether [report] is later than [last], by their [REPORT_NUMBER]s: also when they are of
     * different runs, or either has none, as from a version without them.
     */
    fun laterReport(report: Map<String, String>, last: Map<String, String>?): Boolean {
        val number = reportNumber(report) ?: return true
        val lastNumber = last?.let(::reportNumber) ?: return true
        return number.first != lastNumber.first || number.second > lastNumber.second
    }

    private fun reportNumber(report: Map<String, String>): Pair<UUID, Long>? {
        val (run, number) =
            report[REPORT_NUMBER]?.split('/')?.takeIf { it.size == 2 } ?: return null
        return runCatching { UUID.fromString(run) to number.toLong() }.getOrNull()
    }

    /** Numbers the reports of this run of the member's process, for [REPORT_NUMBER]. */
    class ReportNumbers {
        private val run = UUID.randomUUID()
        private val last = AtomicLong()

        fun next(): String = "$run/${last.incrementAndGet()}"
    }

    /**
     * The group of `system` a member reports ([SYSTEM_GROUP]): the database's id if it reports
     * none, as members of versions before re-forming.
     */
    fun systemGroup(report: Map<String, String>): UUID =
        report[SYSTEM_GROUP]?.let { runCatching { UUID.fromString(it) }.getOrNull() }
            ?: DatabaseGroups.SYSTEM_DATABASE

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
