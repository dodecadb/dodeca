package org.dozendb.cluster

import java.util.UUID
import org.dozendb.consensus.DatabaseRef

/**
 * The Raft group each database is replicated in on this member, as a [DatabaseRef], whose id is the
 * group's: a database's group has the database's id, except `system`'s once a recovery re-formed
 * it, which then has an id of its own, [systemGroup], kept in the membership record
 * ([MembershipRecord.systemGroup]). A group that lost its majority is never re-formed under its old
 * id: the old members that came back would lead it.
 *
 * Neo4j's id of `system` never changes, so that its id stays apart from group ids wherever both
 * meet: in the states members report ([org.dozendb.core.edition.DatabaseState]), secondary marks
 * and store copies, which name databases, and in Raft groups and leaders, which name groups.
 */
class DatabaseGroups(systemGroup: UUID = SYSTEM_DATABASE) {
    /** The id of the group of `system`. */
    @Volatile
    var systemGroup: UUID = systemGroup
        private set

    /** The group of `system`. */
    val system: DatabaseRef
        get() = DatabaseRef(SYSTEM_NAME, systemGroup)

    /** The group of the database [name] whose Neo4j id is [databaseId]. */
    fun of(name: String, databaseId: UUID): DatabaseRef =
        if (databaseId == SYSTEM_DATABASE) system else DatabaseRef(name, databaseId)

    /** The id of the group of the database whose Neo4j id is [databaseId]. */
    fun groupOf(databaseId: UUID): UUID =
        if (databaseId == SYSTEM_DATABASE) systemGroup else databaseId

    /** The Neo4j id of the database replicated in the group [group]. */
    fun databaseOf(group: UUID): UUID = if (group == systemGroup) SYSTEM_DATABASE else group

    companion object {
        /**
         * Neo4j's id of `system` (`DatabaseId.SYSTEM_DATABASE_ID`), and the id of its group unless
         * a recovery re-formed it.
         */
        val SYSTEM_DATABASE: UUID = UUID(0, 1)

        private const val SYSTEM_NAME = "system"
    }
}
